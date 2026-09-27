import os from 'node:os';
import { randomUUID } from 'node:crypto';
import { config, keys } from '../config.js';
import { client, connect, disconnect, key, state } from '../redis.js';
import {
  bumpStat,
  createJob,
  ensureWorkerRegistered,
  getJob,
  patchJob,
  pruneWorkers,
  readStats,
  reclaimStalledJobs,
  recordEvent,
  registerWorker,
  heartbeat,
} from '../store.js';
import { tasks } from './tasks.js';

const workerId = process.env.WORKER_ID || `worker_${os.hostname()}_${process.pid}_${randomUUID().slice(0, 4)}`;

let running = true;
let active = 0;
/** jobId -> AbortController, so shutdown can interrupt long tasks. */
const inFlight = new Map();

const processingKey = () => key(keys.processing(workerId));
const log = (...args) => console.log(`[${workerId}]`, ...args);

/** The immutable half of a worker's registration record. */
const workerInfo = () => ({
  pid: process.pid,
  host: os.hostname(),
  version: process.version,
  concurrency: config.worker.concurrency,
});

/* ------------------------------------------------------------------ *
 * Crash recovery
 * ------------------------------------------------------------------ */

/**
 * A job that is still sitting in the in-flight list was interrupted - either we
 * were killed mid-task, or the previous process with this id died. Put it back
 * on the queue so no work is silently lost.
 */
async function recoverOrphans() {
  const orphans = await client.lRange(processingKey(), 0, -1);
  if (!orphans.length) return 0;

  log(`recovering ${orphans.length} interrupted job(s): ${orphans.join(', ')}`);
  await client
    .multi()
    // RPUSH appends at the tail, which is where the consumer pops from, so the
    // recovered jobs are handled before any newer ones. The in-flight list is
    // newest-first, hence the reverse.
    .rPush(key(keys.queue), orphans.slice().reverse())
    .del(processingKey())
    .exec();

  for (const jobId of orphans) {
    await patchJob(jobId, { status: 'queued', workerId: '', startedAt: 0 });
  }
  await recordEvent('worker.recovered', `Re-queued ${orphans.length} interrupted job(s)`, {
    count: orphans.length,
  });
  return orphans.length;
}

/* ------------------------------------------------------------------ *
 * Job execution
 * ------------------------------------------------------------------ */

async function runJob(jobId) {
  const job = await getJob(jobId);
  if (!job) {
    log(`job ${jobId} has no data hash, dropping it`);
    await client.lRem(processingKey(), 1, jobId);
    return;
  }

  if (job.status === 'done' || job.status === 'failed') {
    // Already settled before we picked it up (duplicate delivery).
    await client.lRem(processingKey(), 1, jobId);
    return;
  }

  const task = tasks[job.type];
  const controller = new AbortController();
  inFlight.set(jobId, controller);

  await patchJob(jobId, {
    status: 'running',
    startedAt: Date.now(),
    workerId,
    attempts: job.attempts + 1,
    progress: 0,
    error: '',
  });

  // Hand the task the job as it is now stored, so fields like `attempts`
  // reflect this run rather than the value from before the patch above.
  const runningJob = { ...job, status: 'running', workerId, attempts: job.attempts + 1 };
  await heartbeat(workerId, { status: 'busy', currentJobId: jobId, currentJobType: job.type });
  await recordEvent('job.started', `Job ${jobId} (${job.type}) picked up`, { jobId, type: job.type });
  log(`started ${jobId} (${job.type})`);

  // Progress writes are throttled so a chatty task cannot flood Redis.
  let lastReport = 0;
  const report = async (percent) => {
    const now = Date.now();
    if (now - lastReport < 200 && percent < 100) return;
    lastReport = now;
    await patchJob(jobId, { progress: percent });
  };

  const timer = setTimeout(() => {
    controller.abort();
  }, config.worker.jobTimeoutMs);

  let outcome;
  const startedAt = Date.now();
  try {
    if (!task) throw new Error(`Unknown task type "${job.type}"`);

    const result = await task({
      payload: job.payload || {},
      job: runningJob,
      report,
      signal: controller.signal,
      enqueue: (type, payload) => createJob(type, payload),
    });
    outcome = { ok: true, result };
  } catch (err) {
    const timedOut = controller.signal.aborted;
    outcome = {
      ok: false,
      error: timedOut
        ? `Task timed out after ${config.worker.jobTimeoutMs}ms`
        : err?.message || String(err),
    };
  } finally {
    clearTimeout(timer);
    inFlight.delete(jobId);
  }

  const durationMs = Date.now() - startedAt;

  if (outcome.ok) {
    await patchJob(jobId, {
      status: 'done',
      finishedAt: Date.now(),
      result: outcome.result,
      progress: 100,
      error: '',
    });
    await client.multi().lRem(processingKey(), 1, jobId).hIncrBy(key(keys.worker(workerId)), 'jobsProcessed', 1).exec();
    await bumpStat('jobs:completed');
    await recordEvent('job.done', `Job ${jobId} finished in ${durationMs}ms`, {
      jobId,
      durationMs,
      type: job.type,
    });
    log(`finished ${jobId} in ${durationMs}ms`);
  } else {
    await patchJob(jobId, {
      status: 'failed',
      finishedAt: Date.now(),
      error: outcome.error,
    });
    await client.multi().lRem(processingKey(), 1, jobId).hIncrBy(key(keys.worker(workerId)), 'jobsFailed', 1).exec();
    await bumpStat('jobs:failed');
    await recordEvent('job.failed', `Job ${jobId} failed: ${outcome.error}`, {
      jobId,
      error: outcome.error,
      type: job.type,
    });
    log(`failed ${jobId}: ${outcome.error}`);
  }

  await heartbeat(workerId, { status: inFlight.size > 0 ? 'busy' : 'idle', currentJobId: '', currentJobType: '' });
}

/* ------------------------------------------------------------------ *
 * Main loop
 * ------------------------------------------------------------------ */

async function loop() {
  let lastJanitor = 0;
  while (running) {
    if (!client.isReady) {
      // Redis went away - wait for the client to reconnect, keep heartbeating.
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    // Housekeeping: hand back work stranded by dead workers and forget workers
    // that have been gone for a very long time.
    if (Date.now() - lastJanitor > 10000) {
      lastJanitor = Date.now();
      reclaimStalledJobs().catch((err) => log(`reclaim failed: ${err.message}`));
      pruneWorkers().catch((err) => log(`prune failed: ${err.message}`));
    }

    if (active >= config.worker.concurrency) {
      await new Promise((r) => setTimeout(r, 50));
      continue;
    }

    let jobId;
    try {
      // Blocks up to blockSeconds. Moves the job to this worker's in-flight
      // list atomically, so a crash never loses it.
      jobId = await client.brPopLPush(key(keys.queue), processingKey(), config.worker.blockSeconds);
    } catch (err) {
      log(`queue read failed: ${err.message}`);
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    if (!jobId) {
      // Idle tick: keeps lastSeen fresh so the dashboard sees us as online, and
      // restores our record if the dashboard reset the data underneath us.
      await ensureWorkerRegistered(workerId, workerInfo());
      await heartbeat(workerId, { status: inFlight.size > 0 ? 'busy' : 'idle' });
      continue;
    }

    active += 1;
    // Fire and forget so up to WORKER_CONCURRENCY jobs run in parallel.
    runJob(jobId)
      .catch((err) => log(`unhandled error in runJob(${jobId}):`, err.message))
      .finally(() => {
        active -= 1;
      });
  }
}

async function shutdown(signal) {
  if (!running) return;
  running = false;
  log(`received ${signal}, draining ${inFlight.size} in-flight job(s)...`);

  await heartbeat(workerId, { status: 'stopping' }).catch(() => {});
  await recordEvent('worker.stopping', `Worker ${workerId} received ${signal}`, { signal });

  // Give running tasks a moment to finish on their own.
  const deadline = Date.now() + 8000;
  while (inFlight.size > 0 && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 100));
  }
  // Anything still running is aborted; its job stays in the in-flight list and
  // gets re-queued the next time this worker id starts.
  for (const controller of inFlight.values()) controller.abort();

  await heartbeat(workerId, { status: 'stopped', currentJobId: '', currentJobType: '' }).catch(() => {});
  await recordEvent('worker.stopped', `Worker ${workerId} stopped`, {});
  log('bye');
  await disconnect();
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
// A stray rejection should log loudly rather than take the worker down silently.
process.on('unhandledRejection', (err) => log('unhandledRejection:', err?.message || err));

async function main() {
  log(`starting (concurrency=${config.worker.concurrency}, redis=${config.redis.safeUrl})`);
  // connect() retries in the background, so if Redis is down this simply waits
  // here until it appears - announce that, otherwise the process looks hung.
  const announce = setTimeout(() => {
    if (!client.isReady) log('still waiting for redis - will start as soon as it is reachable');
  }, 3000);
  announce.unref?.();

  await connect();
  clearTimeout(announce);

  if (!client.isReady) {
    log('redis is not reachable yet - waiting for the reconnect loop');
  } else {
    await registerWorker(workerId, workerInfo());
    await recoverOrphans();
    const stats = await readStats().catch(() => ({}));
    log(`ready. jobs enqueued so far: ${stats['jobs:enqueued'] ?? 0}`);
  }

  await loop();
}

main().catch((err) => {
  log('fatal:', err?.stack || err);
  process.exit(1);
});
