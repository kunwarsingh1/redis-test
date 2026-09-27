import { randomUUID } from 'node:crypto';
import { client, key, state } from './redis.js';
import { config, keys } from './config.js';

const now = () => Date.now();

/* ------------------------------------------------------------------ *
 * Activity log + counters
 * ------------------------------------------------------------------ */

export async function recordEvent(type, message, meta = {}) {
  if (!client.isReady) return;
  const event = {
    id: randomUUID(),
    type,
    message,
    meta,
    at: now(),
  };
  try {
    await client
      .multi()
      .lPush(key(keys.events), JSON.stringify(event))
      .lTrim(key(keys.events), 0, config.worker.eventLogSize - 1)
      .hIncrBy(key(keys.stats), `events:${type}`, 1)
      .exec();
  } catch (err) {
    console.error('[events] failed to record:', err.message);
  }
}

export async function bumpStat(field, amount = 1) {
  if (!client.isReady) return;
  try {
    await client.hIncrBy(key(keys.stats), field, amount);
  } catch (err) {
    console.error('[stats] failed to bump:', err.message);
  }
}

export async function readStats() {
  if (!client.isReady) return {};
  const raw = await client.hGetAll(key(keys.stats));
  return Object.fromEntries(
    Object.entries(raw).map(([k, v]) => [k, Number(v) || 0]),
  );
}

export async function readEvents(limit = config.worker.eventLogSize) {
  if (!client.isReady) return [];
  const raw = await client.lRange(key(keys.events), 0, limit - 1);
  return raw.map((line) => {
    try {
      return JSON.parse(line);
    } catch {
      return { id: 'unknown', type: 'unknown', message: line, at: now() };
    }
  });
}

/* ------------------------------------------------------------------ *
 * Jobs
 * ------------------------------------------------------------------ */

const serializeJob = (hash) => {
  if (!hash || !hash.id) return null;
  let payload = null;
  let result = null;
  try {
    payload = hash.payload ? JSON.parse(hash.payload) : null;
  } catch {
    payload = hash.payload;
  }
  try {
    result = hash.result ? JSON.parse(hash.result) : null;
  } catch {
    result = hash.result;
  }
  return {
    id: hash.id,
    type: hash.type,
    status: hash.status,
    payload,
    result,
    error: hash.error || null,
    workerId: hash.workerId || null,
    progress: Number(hash.progress) || 0,
    attempts: Number(hash.attempts) || 0,
    createdAt: Number(hash.createdAt) || 0,
    startedAt: Number(hash.startedAt) || 0,
    finishedAt: Number(hash.finishedAt) || 0,
  };
};

export async function createJob(type, payload) {
  const id = `job_${Date.now().toString(36)}_${randomUUID().slice(0, 8)}`;
  const job = {
    id,
    type,
    status: 'queued',
    payload: JSON.stringify(payload ?? {}),
    result: '',
    error: '',
    workerId: '',
    progress: 0,
    attempts: 0,
    createdAt: now(),
    startedAt: 0,
    finishedAt: 0,
  };

  // Written atomically with the push onto the queue so a job can never be
  // visible in the list without its data (or vice versa).
  await client
    .multi()
    .hSet(key(keys.job(id)), job)
    .zAdd(key(keys.jobs), { score: job.createdAt, value: id })
    // LPUSH puts the newest job at the head so the tail stays the oldest.
    // Workers pop from the tail with BRPOPLPUSH, which keeps order FIFO.
    .lPush(key(keys.queue), id)
    .hIncrBy(key(keys.stats), 'jobs:enqueued', 1)
    .exec();

  await recordEvent('job.queued', `Job ${id} (${type}) queued`, { jobId: id, type });
  return serializeJob(job);
}

export async function getJob(jobId) {
  if (!client.isReady) return null;
  return serializeJob(await client.hGetAll(key(keys.job(jobId))));
}

export async function patchJob(jobId, patch) {
  if (!client.isReady) return null;
  const flat = {};
  for (const [k, v] of Object.entries(patch)) {
    if (v === undefined || v === null) continue;
    flat[k] = typeof v === 'object' ? JSON.stringify(v) : String(v);
  }
  if (Object.keys(flat).length) await client.hSet(key(keys.job(jobId)), flat);
  return getJob(jobId);
}

export async function listRecentJobs(limit = 15) {
  if (!client.isReady) return [];
  const ids = await client.zRange(key(keys.jobs), 0, -1, { REV: true });
  const slice = ids.slice(0, limit);
  if (!slice.length) return [];
  const pipeline = client.multi();
  for (const id of slice) pipeline.hGetAll(key(keys.job(id)));
  const results = await pipeline.exec();
  return results
    .map((hash) => serializeJob(hash))
    .filter(Boolean)
    .sort((a, b) => b.createdAt - a.createdAt);
}

export async function queueDepth() {
  if (!client.isReady) return 0;
  return client.lLen(key(keys.queue));
}

/* ------------------------------------------------------------------ *
 * Worker registry
 * ------------------------------------------------------------------ */

export async function registerWorker(workerId, info) {
  await client
    .multi()
    .hSet(key(keys.worker(workerId)), {
      id: workerId,
      status: 'idle',
      currentJobId: '',
      currentJobType: '',
      jobsProcessed: 0,
      jobsFailed: 0,
      startedAt: now(),
      lastSeen: now(),
      ...info,
    })
    .zAdd(key(keys.workers), { score: now(), value: workerId })
    .hIncrBy(key(keys.stats), 'workers:started', 1)
    .exec();
  await recordEvent('worker.online', `Worker ${workerId} started`, { workerId });
}

/**
 * Re-registers the worker if its record has disappeared - for example after
 * someone hits "Reset data" on the dashboard. Without this a live worker would
 * keep heartbeating into a void and the UI would report it as missing forever.
 * Returns true if a re-registration happened.
 */
export async function ensureWorkerRegistered(workerId, info) {
  if (!client.isReady) return false;
  const exists = await client.exists(key(keys.worker(workerId)));
  if (exists) return false;
  console.log(`[${workerId}] record missing from Redis, re-registering`);
  await registerWorker(workerId, info);
  return true;
}

export async function heartbeat(workerId, patch = {}) {
  if (!client.isReady) return;
  const flat = { lastSeen: now() };
  for (const [k, v] of Object.entries(patch)) {
    if (v === undefined || v === null || v === '') continue;
    flat[k] = String(v);
  }
  try {
    await client
      .multi()
      .hSet(key(keys.worker(workerId)), flat)
      .zAdd(key(keys.workers), { score: now(), value: workerId })
      .exec();
  } catch (err) {
    console.error(`[worker ${workerId}] heartbeat failed:`, err.message);
  }
}

/**
 * Lists every known worker. A worker whose lastSeen is older than
 * WORKER_STALE_MS is reported as `offline` even though its hash is still there —
 * that is exactly the "is the script actually running?" check the UI needs.
 */
export async function listWorkers() {
  if (!client.isReady) return [];
  const ids = await client.zRange(key(keys.workers), 0, -1);
  if (!ids.length) return [];

  const pipeline = client.multi();
  for (const id of ids) pipeline.hGetAll(key(keys.worker(id)));
  const results = await pipeline.exec();

  const at = now();
  return results
    .map((hash) => {
      if (!hash?.id) return null;
      const lastSeen = Number(hash.lastSeen) || 0;
      const age = at - lastSeen;
      return {
        id: hash.id,
        pid: hash.pid || null,
        host: hash.host || null,
        status: age > config.worker.staleMs ? 'offline' : hash.status || 'unknown',
        currentJobId: hash.currentJobId || null,
        currentJobType: hash.currentJobType || null,
        jobsProcessed: Number(hash.jobsProcessed) || 0,
        jobsFailed: Number(hash.jobsFailed) || 0,
        startedAt: Number(hash.startedAt) || 0,
        lastSeen,
        ageMs: age,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.id.localeCompare(b.id));
}

/** Drops hash + index entries for workers that have been gone for a long time. */
export async function pruneWorkers(olderThanMs = config.worker.staleMs * 20) {
  if (!client.isReady) return [];
  const ids = await client.zRange(key(keys.workers), 0, -1);
  const cutoff = now() - olderThanMs;
  const doomed = [];
  for (const id of ids) {
    const lastSeen = Number(await client.hGet(key(keys.worker(id)), 'lastSeen')) || 0;
    if (lastSeen < cutoff) doomed.push(id);
  }
  if (doomed.length) {
    await client
      .multi()
      .zRem(key(keys.workers), doomed)
      .del(doomed.map((id) => key(keys.worker(id))))
      .exec();
  }
  return doomed;
}

/**
 * Re-queues jobs that are stuck in a dead worker's in-flight list. Without this
 * a job interrupted by a crash (see the `crash` task) would sit in "running"
 * forever, because the worker that owned it will never come back to clean up.
 *
 * Guarded by a short lock so only one process reclaims at a time, and the
 * staleness of each worker is re-checked inside the lock to avoid stealing work
 * from a worker that was merely slow and has just woken up.
 */
export async function reclaimStalledJobs() {
  if (!client.isReady) return [];

  const lockKey = key('lock:reclaim');
  const gotLock = await client.set(lockKey, String(process.pid), { NX: true, PX: 10000 });
  if (!gotLock) return [];

  const reclaimed = [];
  try {
    const workers = await listWorkers();
    for (const worker of workers) {
      if (worker.status !== 'offline' || worker.id === undefined) continue;
      if (now() - worker.lastSeen < config.worker.staleMs) continue;

      const listKey = key(keys.processing(worker.id));
      const stranded = await client.lRange(listKey, 0, -1);
      if (!stranded.length) {
        await client.del(listKey);
        continue;
      }

      // The dead worker may have come back in the meantime - leave it alone.
      const lastSeen = Number(await client.hGet(key(keys.worker(worker.id)), 'lastSeen')) || 0;
      if (now() - lastSeen < config.worker.staleMs) continue;

      // Only jobs that never reached a terminal state may be re-queued. A worker
      // killed between "mark done" and "remove from in-flight" leaves a settled
      // job in the list, and replaying that would run its side effects twice.
      const toRequeue = [];
      for (const jobId of stranded) {
        const job = await getJob(jobId);
        if (!job || job.status === 'done' || job.status === 'failed') {
          await client
            .multi()
            .lRem(listKey, 1, jobId)
            .hIncrBy(key(keys.stats), 'jobs:dropped', 1)
            .exec();
          continue;
        }
        toRequeue.push(jobId);
      }
      if (!toRequeue.length) continue;

      await client
        .multi()
        .rPush(key(keys.queue), toRequeue.slice().reverse())
        .del(listKey)
        .hIncrBy(key(keys.stats), 'jobs:reclaimed', toRequeue.length)
        .exec();

      for (const jobId of toRequeue) {
        await patchJob(jobId, {
          status: 'queued',
          workerId: '',
          startedAt: 0,
          error: `Re-queued after worker ${worker.id} went offline`,
        });
      }
      reclaimed.push(...toRequeue);
      await recordEvent(
        'job.reclaimed',
        `Re-queued ${toRequeue.length} job(s) stranded by offline worker ${worker.id}`,
        { count: toRequeue.length, workerId: worker.id },
      );
    }
  } finally {
    await client.del(lockKey).catch(() => {});
  }
  return reclaimed;
}

/* ------------------------------------------------------------------ *
 * Redis server introspection (for the dashboard)
 * ------------------------------------------------------------------ */

/** Parses the flat `key:value` text returned by the INFO command. */
function parseInfo(text) {
  const out = {};
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const idx = trimmed.indexOf(':');
    if (idx === -1) continue;
    out[trimmed.slice(0, idx)] = trimmed.slice(idx + 1);
  }
  return out;
}

export async function redisInfo() {
  if (!client.isReady) {
    return { reachable: false, error: state.lastError || 'not connected' };
  }
  const [serverRaw, memoryRaw, clientsRaw, statsRaw] = await Promise.all([
    client.info('server'),
    client.info('memory'),
    client.info('clients'),
    client.info('stats'),
  ]);
  const server = parseInfo(serverRaw);
  const memory = parseInfo(memoryRaw);
  const clients = parseInfo(clientsRaw);
  const stats = parseInfo(statsRaw);

  return {
    reachable: true,
    version: server.redis_version,
    mode: server.redis_mode,
    os: server.os,
    archBits: server.arch_bits,
    uptimeSeconds: Number(server.uptime_in_seconds) || 0,
    usedMemoryHuman: memory.used_memory_human,
    usedMemory: Number(memory.used_memory) || 0,
    maxMemoryHuman: memory.maxmemory_human || '0',
    connectedClients: Number(clients.connected_clients) || 0,
    totalCommands: Number(stats.total_commands_processed) || 0,
    keyspaceHits: Number(stats.keyspace_hits) || 0,
    keyspaceMisses: Number(stats.keyspace_misses) || 0,
    dbsize: await client.dbSize(),
  };
}
