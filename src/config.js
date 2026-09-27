import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, '..');

const num = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const prefix = process.env.REDIS_KEY_PREFIX || 'testbed';

/**
 * The connection string is taken straight from the environment so the app can be
 * pointed at any Redis instance without touching code. REDIS_URL wins; otherwise
 * the URL is assembled from the REDIS_* parts.
 */
const redisUrl =
  process.env.REDIS_URL ||
  `redis://${process.env.REDIS_HOST || '127.0.0.1'}:${num(process.env.REDIS_PORT, 6379)}`;

export const config = {
  server: {
    port: num(process.env.PORT, 3000),
    host: process.env.HOST || '0.0.0.0',
  },
  redis: {
    url: redisUrl,
    // The URL may contain a password, so only the safe part is ever displayed.
    safeUrl: redisUrl.replace(/\/\/[^@/]*@/, '//***:***@'),
    username: process.env.REDIS_USERNAME || undefined,
    password: process.env.REDIS_PASSWORD || undefined,
    db: num(process.env.REDIS_DB, 0),
    connectTimeout: num(process.env.REDIS_CONNECT_TIMEOUT_MS, 5000),
    commandTimeout: num(process.env.REDIS_COMMAND_TIMEOUT_MS, 3000),
    prefix,
  },
  worker: {
    concurrency: Math.max(1, num(process.env.WORKER_CONCURRENCY, 2)),
    staleMs: num(process.env.WORKER_STALE_MS, 15000),
    jobTimeoutMs: num(process.env.JOB_TIMEOUT_MS, 60000),
    taskMinMs: num(process.env.TASK_MIN_MS, 400),
    taskMaxMs: num(process.env.TASK_MAX_MS, 2500),
    // BLPOP block time. Kept short so heartbeats stay fresh even when idle.
    blockSeconds: 1,
    // How many recent activity events to keep in Redis.
    eventLogSize: 40,
    maxEventLogSize: 200,
  },
  publicDir: path.join(ROOT, 'public'),
};

// One place that knows every key this app touches.
export const keys = {
  queue: 'queue:pending',
  processing: (workerId) => `queue:processing:${workerId}`,
  worker: (workerId) => `worker:${workerId}`,
  workers: 'workers',
  job: (jobId) => `job:${jobId}`,
  jobs: 'jobs',
  stats: 'stats',
  events: 'events',
};
