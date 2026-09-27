import express from 'express';
import os from 'node:os';
import { config, keys } from './config.js';
import { client, connect, disconnect, ping, state, key } from './redis.js';
import {
  createJob,
  getJob,
  listRecentJobs,
  listWorkers,
  queueDepth,
  readEvents,
  readStats,
  redisInfo,
  recordEvent,
  bumpStat,
} from './store.js';
import { taskCatalog, taskNames } from './worker/tasks.js';

const app = express();
app.use(express.json());

const bootedAt = Date.now();
const api = express.Router();

/**
 * Each probe writes itself into the payload. Being explicit about the target
 * path avoids the easy mistake of parking a value at the wrong depth, which the
 * dashboard would silently render as "missing".
 */
const applyProbe = {
  latency: (p, v) => { p.redis.latencyMs = v; },
  info: (p, v) => { p.redis.info = v; },
  workers: (p, v) => { p.workers = v; },
  queue: (p, v) => { p.queue.depth = v; },
  stats: (p, v) => { p.stats = v; },
  jobs: (p, v) => { p.jobs = v; },
  events: (p, v) => { p.events = v; },
};

/**
 * One call that returns everything the dashboard renders, so the UI never has
 * to stitch several requests together and can never show a half-updated view.
 */
api.get('/status', async (_req, res) => {
  const redisUp = client.isReady;
  const payload = {
    server: {
      ok: true,
      pid: process.pid,
      host: os.hostname(),
      node: process.version,
      uptimeSeconds: Math.round(process.uptime()),
      bootedAt,
    },
    redis: {
      status: state.status,
      url: config.redis.safeUrl,
      database: config.redis.db,
      prefix: config.redis.prefix,
      latencyMs: null,
      lastError: state.lastError,
      lastErrorAt: state.lastErrorAt,
      connectedAt: state.connectedAt,
      info: null,
      error: null,
      // Per-probe failures are collected here instead of failing the whole call.
      warnings: [],
    },
    workers: [],
    queue: { depth: 0 },
    stats: {},
    jobs: [],
    events: [],
  };

  if (!redisUp) {
    payload.redis.error = state.lastError || 'Not connected to Redis';
    // Worker state lives in Redis, so it is genuinely unknown here. Say so
    // rather than reporting "0 workers" which would look like a working setup.
    payload.workers = null;
    return res.json(payload);
  }

  // Every probe is settled independently: a single failing command must not
  // blank the whole dashboard, and the reason is logged for the operator.
  const probes = {
    latency: () => ping(),
    info: () => redisInfo(),
    workers: () => listWorkers(),
    queue: () => queueDepth(),
    stats: () => readStats(),
    jobs: () => listRecentJobs(12),
    events: () => readEvents(25),
  };
  const names = Object.keys(probes);
  const results = await Promise.allSettled(names.map((name) => probes[name]()));

  results.forEach((result, i) => {
    if (result.status === 'fulfilled') {
      applyProbe[names[i]](payload, result.value);
    } else {
      const reason = result.reason?.message || String(result.reason);
      payload.redis.warnings.push(`${names[i]}: ${reason}`);
      console.error(`[http] status probe "${names[i]}" failed: ${reason}`);
    }
  });

  res.json(payload);
});

api.get('/tasks', (_req, res) => res.json({ tasks: taskCatalog }));

api.get('/jobs', async (_req, res) => {
  try {
    res.json({ jobs: await listRecentJobs(25) });
  } catch (err) {
    res.status(503).json({ error: err.message });
  }
});

api.post('/jobs', async (req, res) => {
  if (!client.isReady) {
    return res.status(503).json({ error: 'Redis is not connected, cannot enqueue' });
  }
  const type = String(req.body?.type || '');
  if (!taskNames.includes(type)) {
    return res.status(400).json({ error: `Unknown task type "${type}"`, allowed: taskNames });
  }
  const payload = req.body?.payload && typeof req.body.payload === 'object' ? req.body.payload : {};
  try {
    const job = await createJob(type, payload);
    await bumpStat('jobs:submittedByApi');
    res.status(201).json({ job });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

api.get('/jobs/:id', async (req, res) => {
  const job = await getJob(req.params.id).catch(() => null);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json({ job });
});

/** Puts a failed (or stuck) job back on the queue. */
api.post('/jobs/:id/retry', async (req, res) => {
  if (!client.isReady) return res.status(503).json({ error: 'Redis is not connected' });
  const job = await getJob(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  if (job.status === 'running') {
    return res.status(409).json({ error: 'Job is currently running, cannot retry' });
  }
  await client
    .multi()
    .hSet(key(keys.job(job.id)), {
      status: 'queued',
      error: '',
      workerId: '',
      startedAt: 0,
      finishedAt: 0,
      progress: 0,
    })
    .lPush(key(keys.queue), job.id)
    .exec();
  await recordEvent('job.retried', `Job ${job.id} re-queued by API`, { jobId: job.id });
  res.json({ job: await getJob(job.id) });
});

/** Wipes this app's keys. Test-only convenience. */
api.post('/reset', async (_req, res) => {
  if (!client.isReady) return res.status(503).json({ error: 'Redis is not connected' });
  const keysOfInterest = [
    key(keys.queue),
    key(keys.jobs),
    key(keys.stats),
    key(keys.events),
  ];
  const workers = await listWorkers();
  for (const w of workers) {
    keysOfInterest.push(key(keys.worker(w.id)), key(keys.processing(w.id)));
  }
  const jobIds = await client.zRange(key(keys.jobs), 0, -1);
  for (const id of jobIds) keysOfInterest.push(key(keys.job(id)));

  await client.del(keysOfInterest);
  await recordEvent('app.reset', 'Dashboard requested a full reset', {});
  res.json({ ok: true, removed: keysOfInterest.length });
});

api.get('/health', (_req, res) => res.json({ ok: true }));

app.use('/api', api);

app.use(express.static(config.publicDir, { extensions: ['html'] }));

// eslint-disable-next-line no-unused-vars -- Express needs the 4-arg signature
app.use((err, _req, res, _next) => {
  console.error('[http] unhandled error:', err);
  res.status(500).json({ error: err.message || 'Internal error' });
});

function start() {
  // Listen first and connect second. node-redis retries a failed connect using
  // its reconnect strategy, so awaiting it would block forever and the
  // dashboard - the thing that exists to report a dead Redis - would never come
  // up. Instead the API serves immediately and /api/status reports the outage.
  app.listen(config.server.port, config.server.host, () => {
    console.log(`[http] listening on http://${config.server.host}:${config.server.port}`);
    console.log(`[http] redis target ${config.redis.safeUrl} (db ${config.redis.db}, prefix "${config.redis.prefix}:")`);
  });

  connect().then(() => {
    if (client.isReady) {
      console.log('[http] redis connected');
    } else {
      console.warn(`[http] redis is not reachable yet (${state.lastError || 'unknown'}) - retrying in the background`);
    }
  });
}

async function stop() {
  console.log('\n[http] shutting down');
  await disconnect();
  process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGTERM', stop);

try {
  start();
} catch (err) {
  // Only a failure to bind the port lands here; a dead Redis does not.
  console.error('[http] failed to start:', err);
  process.exit(1);
}
