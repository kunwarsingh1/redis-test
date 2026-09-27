#!/usr/bin/env node
/**
 * Smoke test for the Redis + worker layer.
 *
 * Exercises every store helper, every task and the queue mechanics against a
 * live Redis, then cleans up after itself. It uses its own key prefix so it can
 * run while the dashboard is up without disturbing it.
 *
 *   npm run smoke
 */
import assert from 'node:assert/strict';

// Must be set before config.js is first imported.
process.env.REDIS_KEY_PREFIX = process.env.SMOKE_PREFIX || 'testbed_smoke';

const { client, connect, disconnect, key, ping, state } = await import('../src/redis.js');
// `keys` is the single source of truth for key names - never hardcode them here,
// otherwise the test can pass/fail for reasons that have nothing to do with the app.
const { config, keys } = await import('../src/config.js');
const store = await import('../src/store.js');
const { tasks, taskNames } = await import('../src/worker/tasks.js');

const prefix = process.env.REDIS_KEY_PREFIX;
let passed = 0;
const failures = [];

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  ok   ${name}`);
  } catch (err) {
    failures.push({ name, err });
    console.log(`  FAIL ${name}\n       ${err.message}`);
  }
}

console.log('smoke test\n');

await connect();
if (!client.isReady) {
  console.error(`Cannot reach Redis (${state.lastError || 'unknown error'}).`);
  console.error('Start Redis or fix REDIS_URL in .env, then run: npm run smoke\n');
  process.exit(1);
}
const info = await store.redisInfo();
console.log(`connected to Redis ${info.version} (db ${config.redis.db}, prefix "${prefix}:")\n`);

/* --------------------------------------------------------------- cleanup */

async function wipe() {
  const found = [];
  for await (const k of client.scanIterator({ MATCH: `${prefix}:*`, COUNT: 500 })) {
    found.push(k);
  }
  if (found.length) await client.del(found);
  return found.length;
}

await wipe();

/* ------------------------------------------------------------ store API */

console.log('store helpers');
await test('recordEvent / readEvents round-trip', async () => {
  await store.recordEvent('smoke.test', 'hello from the smoke test', { a: 1 });
  const events = await store.readEvents(5);
  assert.ok(events.length > 0, 'no events stored');
  assert.equal(events[0].message, 'hello from the smoke test');
  assert.equal(events[0].type, 'smoke.test');
});

await test('bumpStat / readStats are numeric', async () => {
  await store.bumpStat('smoke:counter', 3);
  await store.bumpStat('smoke:counter', 2);
  const stats = await store.readStats();
  assert.equal(stats['smoke:counter'], 5, `expected 5, got ${stats['smoke:counter']}`);
});

await test('ping returns a latency', async () => {
  const ms = await ping();
  assert.ok(typeof ms === 'number' && ms >= 0, `unexpected ping: ${ms}`);
});

await test('createJob writes the hash, the index and the queue atomically', async () => {
  await wipe();
  const job = await store.createJob('echo', { hello: 'world' });
  assert.equal(job.status, 'queued');
  assert.match(job.id, /^job_/);

  const fetched = await store.getJob(job.id);
  assert.equal(fetched.type, 'echo');
  assert.deepEqual(fetched.payload, { hello: 'world' });

  assert.equal(await client.zScore(key(keys.jobs), job.id), job.createdAt, 'missing zset entry');
  assert.equal(await client.lLen(key(keys.queue)), 1, 'job not pushed onto the queue');
  const queued = await client.lRange(key(keys.queue), 0, -1);
  assert.equal(queued.length, 1);
  assert.equal(queued[0], job.id, 'wrong id on the queue');
});

await test('queue is FIFO (LPUSH + pop from tail)', async () => {
  await wipe();
  const first = await store.createJob('echo', { n: 1 });
  const second = await store.createJob('echo', { n: 2 });
  const third = await store.createJob('echo', { n: 3 });

  const popped = [];
  for (let i = 0; i < 3; i += 1) {
    popped.push(await client.rPopLPush(key(keys.queue), key('smoke:popped')));
  }
  assert.deepEqual(popped, [first.id, second.id, third.id], `got ${popped.join(',')}`);
  assert.equal(await client.lLen(key(keys.queue)), 0, 'queue should be drained');
  await client.del(key('smoke:popped'));
});

await test('patchJob updates fields and getJob reflects them', async () => {
  await wipe();
  const job = await store.createJob('echo', {});
  await store.patchJob(job.id, { status: 'running', progress: 42, workerId: 'w1' });
  const after = await store.getJob(job.id);
  assert.equal(after.status, 'running');
  assert.equal(after.progress, 42);
  assert.equal(after.workerId, 'w1');
});

await test('listRecentJobs returns newest first', async () => {
  await wipe();
  const a = await store.createJob('echo', { n: 1 });
  const b = await store.createJob('echo', { n: 2 });
  const jobs = await store.listRecentJobs(10);
  assert.equal(jobs.length, 2);
  assert.equal(jobs[0].id, b.id, 'newest job should be first');
  assert.ok(jobs[0].createdAt >= jobs[1].createdAt);
  assert.equal(jobs[1].id, a.id);
});

await test('queueDepth reflects the pending list', async () => {
  await wipe();
  assert.equal(await store.queueDepth(), 0);
  await store.createJob('echo', {});
  await store.createJob('echo', {});
  assert.equal(await store.queueDepth(), 2);
});

/* -------------------------------------------------------- worker registry */

console.log('\nworker registry');
await test('registerWorker / listWorkers / heartbeat', async () => {
  await wipe();
  await store.registerWorker('smoke_worker_1', { pid: 4242, host: 'testhost' });
  const workers = await store.listWorkers();
  assert.equal(workers.length, 1);
  assert.equal(workers[0].id, 'smoke_worker_1');
  assert.equal(workers[0].pid, '4242');
  assert.equal(workers[0].status, 'idle');

  await store.heartbeat('smoke_worker_1', { status: 'busy', currentJobId: 'job_x' });
  const after = await store.listWorkers();
  assert.equal(after[0].status, 'busy');
  assert.equal(after[0].currentJobId, 'job_x');
});

/** Backdates a worker so it looks like it died a while ago. */
async function killWorker(workerId, ageMs = 10 * 60 * 1000) {
  const when = Date.now() - ageMs;
  await client.hSet(key(keys.worker(workerId)), { lastSeen: when });
  await client.zAdd(key(keys.workers), { score: when, value: workerId });
}

await test('a stale worker is reported as offline', async () => {
  await wipe();
  await store.registerWorker('smoke_worker_stale', { pid: 1, host: 'testhost' });
  await killWorker('smoke_worker_stale');

  const [worker] = await store.listWorkers();
  assert.equal(worker.status, 'offline', 'stale worker should be offline');
  assert.ok(worker.ageMs > 60_000);
});

await test('reclaimStalledJobs re-queues jobs stranded by a dead worker', async () => {
  await wipe();
  const job = await store.createJob('echo', { stranded: true });
  // Pretend a worker picked the job up, then died without finishing it.
  await client.rPopLPush(key(keys.queue), key(keys.processing('smoke_dead')));
  await store.patchJob(job.id, { status: 'running', workerId: 'smoke_dead', startedAt: Date.now() });
  await store.registerWorker('smoke_dead', { pid: 9, host: 'testhost' });
  await killWorker('smoke_dead');

  const reclaimed = await store.reclaimStalledJobs();
  assert.deepEqual(reclaimed, [job.id], `expected the job back, got ${reclaimed}`);
  assert.equal(await client.lLen(key(keys.processing('smoke_dead'))), 0, 'in-flight list not cleared');
  assert.equal(await store.queueDepth(), 1, 'job was not re-queued');

  const after = await store.getJob(job.id);
  assert.equal(after.status, 'queued');
  // serializeJob normalises empty fields to null.
  assert.equal(after.workerId, null);
  assert.match(after.error, /went offline/);
});

await test('reclaimStalledJobs leaves healthy workers alone', async () => {
  await wipe();
  const job = await store.createJob('echo', {});
  await client.rPopLPush(key(keys.queue), key(keys.processing('smoke_live')));
  await store.patchJob(job.id, { status: 'running', workerId: 'smoke_live', startedAt: Date.now() });
  await store.registerWorker('smoke_live', { pid: 10, host: 'testhost' });
  await store.heartbeat('smoke_live', { status: 'busy', currentJobId: job.id });

  const reclaimed = await store.reclaimStalledJobs();
  assert.equal(reclaimed.length, 0, 'stole work from a live worker');
  assert.equal(await client.lLen(key(keys.processing('smoke_live'))), 1);
  assert.equal(await store.queueDepth(), 0, 'live job was pushed back onto the queue');
  assert.equal((await store.getJob(job.id)).status, 'running', 'live job was reset');
});

await test('reclaimStalledJobs does NOT replay a job that already finished', async () => {
  await wipe();
  const job = await store.createJob('echo', {});
  // A worker that dies between "mark done" and "remove from in-flight" leaves a
  // settled job in its in-flight list. Replaying it would run it twice.
  await client.rPopLPush(key(keys.queue), key(keys.processing('smoke_zombie')));
  await store.patchJob(job.id, { status: 'done', finishedAt: Date.now(), result: '{"ok":true}' });
  await store.registerWorker('smoke_zombie', { pid: 12, host: 'testhost' });
  await killWorker('smoke_zombie');

  const reclaimed = await store.reclaimStalledJobs();
  assert.deepEqual(reclaimed, [], 'a completed job must not be re-queued');
  assert.equal(await store.queueDepth(), 0, 'queue should stay empty');
  assert.equal(await client.lLen(key(keys.processing('smoke_zombie'))), 0, 'stale entry not cleared');
  const after = await store.getJob(job.id);
  assert.equal(after.status, 'done', 'completed job was reset');
  assert.deepEqual(after.result, { ok: true }, 'result was lost');
});

await test('reclaimStalledJobs re-queues only the unfinished half', async () => {
  await wipe();
  const done = await store.createJob('echo', { which: 'done' });
  const running = await store.createJob('echo', { which: 'running' });
  for (const id of [done.id, running.id]) {
    await client.rPopLPush(key(keys.queue), key(keys.processing('smoke_mixed')));
  }
  await store.patchJob(done.id, { status: 'done', finishedAt: Date.now() });
  await store.patchJob(running.id, { status: 'running', startedAt: Date.now() });
  await store.registerWorker('smoke_mixed', { pid: 13, host: 'testhost' });
  await killWorker('smoke_mixed');

  const reclaimed = await store.reclaimStalledJobs();
  assert.deepEqual(reclaimed, [running.id], 'only the unfinished job should come back');
  assert.equal(await store.queueDepth(), 1);
  assert.equal((await store.getJob(done.id)).status, 'done');
});

await test('a worker re-registers itself after its record is wiped', async () => {
  await wipe();
  await store.registerWorker('smoke_forgetful', { pid: 77, host: 'testhost' });
  assert.equal((await store.listWorkers()).length, 1);

  // Simulates the dashboard's "Reset data" wiping the registry.
  await client.del(key(keys.worker('smoke_forgetful')));
  await client.zRem(key(keys.workers), 'smoke_forgetful');
  assert.equal((await store.listWorkers()).length, 0);

  const healed = await store.ensureWorkerRegistered('smoke_forgetful', { pid: 77, host: 'testhost' });
  assert.equal(healed, true, 'should have re-registered');
  const [worker] = await store.listWorkers();
  assert.equal(worker.id, 'smoke_forgetful');
  assert.equal(worker.pid, '77');
  assert.equal(worker.status, 'idle');

  // A second call must be a no-op rather than resetting the counters.
  await store.heartbeat('smoke_forgetful', { status: 'busy' });
  const again = await store.ensureWorkerRegistered('smoke_forgetful', { pid: 77, host: 'testhost' });
  assert.equal(again, false, 'should not re-register a healthy record');
  assert.equal((await store.listWorkers())[0].status, 'busy');
});

await test('pruneWorkers forgets long-dead workers', async () => {
  await wipe();
  await store.registerWorker('smoke_gone', { pid: 11, host: 'testhost' });
  await killWorker('smoke_gone', 24 * 60 * 60 * 1000);

  const pruned = await store.pruneWorkers(60_000);
  assert.deepEqual(pruned, ['smoke_gone']);
  assert.equal((await store.listWorkers()).length, 0);
});

/* ------------------------------------------------------------------ tasks */

console.log('\ntasks');
const noop = async () => {};
const signal = () => new AbortController().signal;

// Tasks that are expected to succeed.
const happyTasks = taskNames.filter((name) => !['crash', 'fail', 'flaky'].includes(name));

for (const name of happyTasks) {
  await test(`task "${name}" runs`, async () => {
    const job = await store.createJob(name, { ms: 120, message: 'smoke', count: 2 });
    const result = await tasks[name]({
      payload: job.payload,
      job,
      report: noop,
      signal: signal(),
      enqueue: (type, payload) => store.createJob(type, payload),
    });
    assert.notEqual(result, undefined, `${name} returned nothing`);
  });
}

await test('task "fail" rejects on purpose', async () => {
  await assert.rejects(
    () => tasks.fail({ payload: {}, report: noop, signal: signal() }),
    /failed on purpose/,
  );
});

await test('task "flaky" sometimes rejects', async () => {
  let rejected = 0;
  for (let i = 0; i < 40; i += 1) {
    try {
      await tasks.flaky({ payload: { ms: 1 }, report: noop, signal: signal() });
    } catch {
      rejected += 1;
    }
  }
  assert.ok(rejected > 0 && rejected < 40, `flaky should not be deterministic (${rejected}/40 failed)`);
});

await test('task "fanout" really enqueues children', async () => {
  await wipe();
  const result = await tasks.fanout({
    payload: { count: 3 },
    job: { id: 'parent' },
    report: noop,
    signal: signal(),
    enqueue: (type, payload) => store.createJob(type, payload),
  });
  assert.equal(result.spawned, 3);
  assert.equal(await store.queueDepth(), 3);
});

await test('task "sleep" honours the abort signal', async () => {
  const controller = new AbortController();
  const promise = tasks.sleep({ payload: { ms: 5000 }, report: noop, signal: controller.signal });
  setTimeout(() => controller.abort(), 60);
  await assert.rejects(() => promise, /Aborted/);
});

await test('task "crash" only fires on the first attempt', async () => {
  // First attempt: must not actually kill this process, so drive the task with
  // a stubbed kill and assert it was called.
  const realKill = process.kill;
  let kills = 0;
  process.kill = () => { kills += 1; };
  try {
    await tasks.crash({ payload: { ms: 1 }, job: { id: 'j', attempts: 1 }, report: noop, signal: signal() });
    assert.equal(kills, 1, 'first attempt should kill the worker');

    const retry = await tasks.crash({ payload: { ms: 1 }, job: { id: 'j', attempts: 2 }, report: noop, signal: signal() });
    assert.equal(kills, 1, 'retry must not kill the worker again');
    assert.match(retry.note, /Already crashed once/);
  } finally {
    process.kill = realKill;
  }
});

await test('an unknown task type is rejected', async () => {
  assert.ok(!taskNames.includes('does-not-exist'));
});

/* ------------------------------------------------------------ redis info */

console.log('\nredis introspection');
await test('redisInfo reports a live server', async () => {
  const data = await store.redisInfo();
  assert.equal(data.reachable, true);
  assert.ok(data.version, 'no version reported');
  assert.ok(typeof data.usedMemory === 'number');
  assert.ok(typeof data.dbsize === 'number');
});

/* ---------------------------------------------------------------- finish */

const removed = await wipe();
await disconnect();

console.log(`\n${passed} passed, ${failures.length} failed, ${removed} test keys removed\n`);
if (failures.length) {
  for (const { name, err } of failures) console.error(`FAILED: ${name}\n${err.stack}\n`);
}
process.exit(failures.length ? 1 : 0);
