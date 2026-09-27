// One-off check: every field public/app.js reads must exist in the API payload.
const base = 'http://127.0.0.1:3000';
const problems = [];
const need = (obj, path, cond = (v) => v !== undefined && v !== null) => {
  let cur = obj;
  for (const part of path.split('.')) {
    if (cur === undefined || cur === null) { problems.push(`${path} -> missing parent at "${part}"`); return; }
    cur = cur[part];
  }
  if (!cond(cur)) problems.push(`${path} -> ${JSON.stringify(cur)}`);
};

const status = await (await fetch(`${base}/api/status`)).json();
const tasks = await (await fetch(`${base}/api/tasks`)).json();

for (const p of ['server.pid','server.host','server.node','server.uptimeSeconds',
  'redis.status','redis.url','redis.database','redis.prefix',
  'redis.connectedAt','redis.info.reachable','redis.info.version',
  'redis.info.uptimeSeconds','redis.info.usedMemoryHuman','redis.info.connectedClients',
  'redis.info.dbsize','redis.info.totalCommands','redis.warnings',
  'queue.depth','stats','jobs','events']) need(status, p);

// latencyMs and error are legitimately null when Redis is unreachable, but when
// Redis IS up a missing latency means the probe never wrote to the payload.
if (status.redis.status === 'ready') {
  if (typeof status.redis.latencyMs !== 'number') problems.push('redis.latencyMs -> probe did not populate it');
  if (status.redis.warnings?.length) problems.push('redis.warnings -> ' + status.redis.warnings.join(' | '));
}

if (!Array.isArray(status.workers)) problems.push('workers must be an array (or null when Redis is down)');
else if (status.workers.length) {
  for (const p of ['id','pid','host','status','currentJobId','currentJobType','jobsProcessed',
                   'jobsFailed','startedAt','lastSeen','ageMs']) need(status.workers[0], p);
}
for (const j of status.jobs) {
  for (const p of ['id','type','status','workerId','progress','attempts',
                   'createdAt','startedAt','finishedAt']) need(j, p);
}
for (const e of status.events) for (const p of ['type','message','at']) need(e, p);

if (!Array.isArray(tasks.tasks) || !tasks.tasks.length) problems.push('tasks.catalog empty');
for (const t of tasks.tasks ?? []) {
  need(t, 'name'); need(t, 'label'); need(t, 'description');
  if (!Array.isArray(t.fields)) problems.push(`task ${t.name}: fields must be an array`);
}

console.log(problems.length ? 'CONTRACT PROBLEMS:' : 'Contract OK - dashboard can render every field it reads.');
for (const p of problems) console.log('  - ' + p);
console.log(`\nsample: ${status.workers.length} worker(s), ${status.jobs.length} job(s), ${status.events.length} event(s), ${tasks.tasks.length} task(s)`);
process.exit(problems.length ? 1 : 0);
