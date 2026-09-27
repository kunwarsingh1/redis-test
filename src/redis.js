import { createClient } from 'redis';
import { config } from './config.js';

/** Prepend the app's namespace so it can share a Redis DB safely. */
export const key = (...parts) => [config.redis.prefix, ...parts].join(':');

/**
 * Shared connection state. The dashboard reads this to decide whether to show
 * Redis as "connected" or "down", so every transition is recorded here.
 */
export const state = {
  status: 'connecting', // connecting | ready | reconnecting | error | closed
  lastError: null,
  lastErrorAt: null,
  connectedAt: null,
  latencyMs: null,
};

const client = createClient({
  url: config.redis.url,
  username: config.redis.username,
  password: config.redis.password,
  database: config.redis.db,
  socket: {
    connectTimeout: config.redis.connectTimeout,
    // node-redis retries on its own; this only bounds one attempt.
    reconnectStrategy: (retries) => Math.min(retries * 200, 5000),
  },
  commandOptions: { timeout: config.redis.commandTimeout },
});

// Without a listener node-redis throws on socket errors and kills the process.
client.on('error', (err) => {
  state.status = 'error';
  state.lastError = err?.message || String(err);
  state.lastErrorAt = Date.now();
  console.error(`[redis] ${state.lastError}`);
});

client.on('ready', () => {
  state.status = 'ready';
  state.lastError = null;
  state.connectedAt = Date.now();
  console.log(`[redis] connected to ${config.redis.safeUrl}`);
});

client.on('reconnecting', () => {
  state.status = 'reconnecting';
});

client.on('end', () => {
  state.status = 'closed';
});

/** Idempotent connect: safe to call from both the server and the worker. */
export async function connect() {
  if (client.isOpen) return client;
  try {
    await client.connect();
  } catch (err) {
    // Do not rethrow: the app should boot and show "Redis down" in the UI
    // instead of crashing. node-redis keeps retrying in the background.
    state.status = 'error';
    state.lastError = err?.message || String(err);
    state.lastErrorAt = Date.now();
    console.error(`[redis] initial connect failed: ${state.lastError}`);
  }
  return client;
}

export async function disconnect() {
  if (!client.isOpen) return;
  await client.quit().catch(() => client.disconnect().catch(() => {}));
}

/** Round-trip time of a PING, or null when Redis is unreachable. */
export async function ping() {
  if (!client.isReady) {
    state.latencyMs = null;
    return null;
  }
  const started = performance.now();
  try {
    await client.ping();
    state.latencyMs = Math.round((performance.now() - started) * 100) / 100;
    return state.latencyMs;
  } catch (err) {
    state.latencyMs = null;
    throw err;
  }
}

export { client };
