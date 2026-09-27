import os from 'node:os';
import { config } from '../config.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const randomDelay = () =>
  config.worker.taskMinMs +
  Math.floor(Math.random() * (config.worker.taskMaxMs - config.worker.taskMinMs));

const number = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

/**
 * A task receives:
 *   payload  - whatever the API submitted
 *   job      - the full job record
 *   report   - async (percent) => void  -> updates job progress in Redis
 *   signal   - AbortSignal, fired on SIGINT/SIGTERM
 * Throw to mark the job as failed.
 */
export const tasks = {
  /** Sleeps for a while, reporting progress. The default "is it working?" task. */
  async sleep({ payload, report, signal }) {
    const total = Math.max(0, number(payload.ms, randomDelay()));
    const steps = 10;
    const stepMs = total / steps;
    for (let i = 1; i <= steps; i += 1) {
      if (signal.aborted) throw new Error('Aborted by shutdown');
      await sleep(stepMs);
      await report(Math.round((i / steps) * 100));
    }
    return { sleptMs: Math.round(total) };
  },

  /** Busy-loops without yielding, to show CPU-bound work. */
  async burn({ payload, signal }) {
    const total = Math.max(0, number(payload.ms, 300));
    const started = Date.now();
    let iterations = 0;
    while (Date.now() - started < total) {
      if (signal.aborted) throw new Error('Aborted by shutdown');
      iterations += 1;
    }
    return { burnedMs: Date.now() - started, iterations };
  },

  /** Returns its payload unchanged. */
  async echo({ payload, job }) {
    await sleep(50);
    return { echoed: payload, jobId: job.id };
  },

  /** Always throws - used to verify the failure path and the error counters. */
  async fail({ payload }) {
    await sleep(number(payload.ms, 200));
    throw new Error(payload.reason || 'Task failed on purpose (test task)');
  },

  /** Fails roughly half the time. */
  async flaky({ payload }) {
    await sleep(number(payload.ms, 500));
    if (Math.random() < 0.5) throw new Error(`Flaky task failed (roll: ${Math.random().toFixed(2)})`);
    return { ok: true };
  },

  /** Enqueues child jobs - shows that the worker can feed the same queue. */
  async fanout({ payload, job, enqueue }) {
    const count = Math.min(25, Math.max(1, number(payload.count, 3)));
    const childType = payload.childType || 'echo';
    const created = [];
    for (let i = 0; i < count; i += 1) {
      created.push(await enqueue(childType, { parent: job.id, index: i }));
    }
    return { spawned: created.length, ids: created.map((c) => c.id) };
  },

  /**
   * Hard-kills this worker process mid-job. The dashboard should flip the
   * worker card to "offline" once WORKER_STALE_MS elapses, and the job stays
   * in the in-flight list until another worker's janitor re-queues it.
   *
   * It only kills on the first attempt, so the job can then be retried to
   * completion and the whole crash -> detect -> re-queue -> finish cycle is
   * visible in one go.
   */
  async crash({ payload, job }) {
    if (job.attempts > 1) {
      return { killed: false, note: 'Already crashed once, surviving this attempt' };
    }
    await sleep(number(payload.ms, 300));
    console.warn(`[task crash] exiting hard on purpose (pid ${process.pid})`);
    process.kill(process.pid, 'SIGKILL');
    // SIGKILL cannot be trapped; this line only runs on unsupported platforms.
    return { killed: false };
  },

  /** Reports where the job is actually executing. */
  async whereami({ payload }) {
    await sleep(number(payload.ms, 100));
    return {
      pid: process.pid,
      host: os.hostname(),
      platform: `${process.platform} ${process.arch}`,
      node: process.version,
      uptimeSeconds: Math.round(process.uptime()),
    };
  },
};

export const taskNames = Object.keys(tasks);

/** Minimal metadata so the UI can render a form for every task. */
export const taskCatalog = [
  { name: 'sleep', label: 'Sleep', description: 'Waits a random 0.4-2.5s, reports progress.', fields: [{ key: 'ms', label: 'Duration (ms)', type: 'number', value: 1500 }] },
  { name: 'echo', label: 'Echo payload', description: 'Returns the submitted payload.', fields: [{ key: 'message', label: 'Message', type: 'text', value: 'hello from the dashboard' }] },
  { name: 'burn', label: 'Burn CPU', description: 'Busy-loops the worker core.', fields: [{ key: 'ms', label: 'Duration (ms)', type: 'number', value: 500 }] },
  { name: 'whereami', label: 'Where am I?', description: 'Reports pid, host and node version.', fields: [] },
  { name: 'flaky', label: 'Flaky (~50% fail)', description: 'Randomly fails, to exercise retries.', fields: [] },
  { name: 'fail', label: 'Always fail', description: 'Throws, to exercise the error path.', fields: [] },
  { name: 'fanout', label: 'Fan out child jobs', description: 'Enqueues more jobs from inside a worker.', fields: [{ key: 'count', label: 'Children', type: 'number', value: 3 }] },
  { name: 'crash', label: 'Crash the worker', description: 'SIGKILLs the worker to test offline detection.', fields: [] },
];
