/* Redis + Worker Testbed dashboard.
 * Polls GET /api/status and renders the live state of Redis and the workers. */
(() => {
  'use strict';

  const $ = (id) => document.getElementById(id);
  const el = {
    dot: $('live-dot'),
    verdict: $('verdict'),
    verdictIcon: $('verdict-icon'),
    verdictTitle: $('verdict-title'),
    verdictDetail: $('verdict-detail'),
    pollRate: $('poll-rate'),
    refreshNow: $('refresh-now'),
    reset: $('reset'),

    redisState: $('redis-state'),
    redisUrl: $('redis-url'),
    redisLatency: $('redis-latency'),
    redisVersion: $('redis-version'),
    redisUptime: $('redis-uptime'),
    redisMemory: $('redis-memory'),
    redisClients: $('redis-clients'),
    redisDbSize: $('redis-dbsize'),
    redisCommands: $('redis-commands'),
    redisError: $('redis-error'),

    workerState: $('worker-state'),
    workerCount: $('worker-count'),
    workerOnline: $('worker-online'),
    workerCompleted: $('worker-completed'),
    workerFailed: $('worker-failed'),
    queueDepth: $('queue-depth'),
    workerList: $('worker-list'),
    workerError: $('worker-error'),

    apiState: $('api-state'),
    apiPid: $('api-pid'),
    apiNode: $('api-node'),
    apiHost: $('api-host'),
    apiUptime: $('api-uptime'),
    apiPrefix: $('api-prefix'),

    form: $('enqueue-form'),
    taskType: $('task-type'),
    taskDesc: $('task-desc'),
    taskFields: $('task-fields'),
    taskJson: $('task-json'),
    enqueueBtn: $('enqueue-btn'),
    enqueueError: $('enqueue-error'),

    jobsBody: $('jobs-body'),
    jobsCount: $('jobs-count'),
    events: $('events'),
    footerUpdated: $('footer-updated'),
    footerPrefix: $('footer-prefix'),
  };

  let catalog = [];
  let timer = null;
  let lastStatus = null;
  let lastSuccessAt = null;
  let failures = 0;

  /* ------------------------------------------------------------ helpers */

  const text = (node, value) => { node.textContent = value == null ? '–' : String(value); };

  const seconds = (total) => {
    if (!total && total !== 0) return '–';
    const s = Math.floor(total);
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
    return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
  };

  const clock = (ts) =>
    new Date(ts).toLocaleTimeString(undefined, { hour12: false });

  const duration = (job) => {
    if (job.status === 'queued') return '–';
    const end = job.finishedAt || Date.now();
    if (!job.startedAt) return '–';
    const ms = end - job.startedAt;
    return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(2)}s`;
  };

  const setPill = (node, state, label) => {
    node.className = `pill ${state}`;
    node.textContent = label;
  };

  const setError = (node, message) => {
    if (message) {
      node.textContent = message;
      node.classList.remove('hidden');
    } else {
      node.classList.add('hidden');
      node.textContent = '';
    }
  };

  const verdict = (kind, icon, title, detail) => {
    el.verdict.className = `verdict verdict-${kind}`;
    el.verdictIcon.textContent = icon;
    el.verdictTitle.textContent = title;
    el.verdictDetail.textContent = detail;
  };

  /* -------------------------------------------------------------- render */

  function renderRedis(redis) {
    const up = redis.status === 'ready' && redis.info?.reachable;
    setPill(el.redisState, up ? 'ok' : 'bad', up ? 'connected' : redis.status || 'down');

    text(el.redisUrl, redis.url);
    text(el.redisLatency, redis.latencyMs == null ? 'n/a' : `${redis.latencyMs} ms`);
    text(el.redisVersion, redis.info?.version);
    text(el.redisUptime, seconds(redis.info?.uptimeSeconds));
    text(el.redisMemory, redis.info?.usedMemoryHuman);
    text(el.redisClients, redis.info?.connectedClients);
    text(el.redisDbSize, redis.info?.dbsize);
    text(el.redisCommands, redis.info?.totalCommands?.toLocaleString());

    // Individual probe failures are reported even when Redis itself is fine,
    // otherwise a partially failed refresh looks like a healthy system.
    const warnings = (redis.warnings || []).join('; ');
    const problem = !up ? redis.error || redis.lastError : warnings || null;
    setError(el.redisError, problem);
    if (up && warnings) el.redisError.textContent = `Partial read: ${warnings}`;
  }

  function renderWorkers(workers, stats, depth) {
    text(el.queueDepth, depth);

    if (workers === null) {
      setPill(el.workerState, 'bad', 'unknown');
      el.workerList.innerHTML = '';
      setError(el.workerError, 'Worker state is stored in Redis, so it cannot be read right now.');
      return;
    }

    const online = workers.filter((w) => w.status !== 'offline');
    const processed = workers.reduce((sum, w) => sum + w.jobsProcessed, 0);
    const failed = workers.reduce((sum, w) => sum + w.jobsFailed, 0);

    text(el.workerCount, workers.length);
    text(el.workerOnline, online.length);
    text(el.workerCompleted, processed + (stats['jobs:completed'] ? ` (${stats['jobs:completed']} total)` : ''));
    text(el.workerFailed, failed + (stats['jobs:failed'] ? ` (${stats['jobs:failed']} total)` : ''));

    if (workers.length === 0) {
      setPill(el.workerState, 'bad', 'not started');
      el.workerList.innerHTML = '<li><span class="worker-meta">No worker has ever registered. Start one with <code>npm run worker</code>.</span></li>';
      setError(el.workerError, null);
      return;
    }
    if (online.length === 0) {
      setPill(el.workerState, 'bad', 'offline');
    } else if (online.length < workers.length) {
      setPill(el.workerState, 'warn', 'partial');
    } else {
      setPill(el.workerState, 'ok', 'online');
    }
    setError(el.workerError, null);

    el.workerList.innerHTML = workers
      .map((w) => {
        const isOnline = w.status !== 'offline';
        const busy = w.status === 'busy';
        const cls = !isOnline ? 'bad' : busy ? 'info' : 'ok';
        const label = isOnline ? (busy ? 'busy' : w.status) : 'offline';
        const current = w.currentJobType
          ? `running <b>${escapeHtml(w.currentJobType)}</b> (${escapeHtml(w.currentJobId || '')})`
          : 'idle';
        return `
          <li>
            <div class="worker-row">
              <span class="worker-id">${escapeHtml(shortId(w.id))}</span>
              <span class="pill ${cls}">${escapeHtml(label)}</span>
            </div>
            <div class="worker-meta">
              pid ${escapeHtml(w.pid)} · ${escapeHtml(w.host)} · ${current}<br />
              completed <b>${w.jobsProcessed}</b> · failed <b>${w.jobsFailed}</b>
              · last seen <b>${(w.ageMs / 1000).toFixed(1)}s</b> ago
              · up ${seconds(Math.round((Date.now() - w.startedAt) / 1000))}
            </div>
          </li>`;
      })
      .join('');
  }

  function renderApi(server, redis) {
    setPill(el.apiState, 'ok', 'running');
    text(el.apiPid, server.pid);
    text(el.apiNode, server.node);
    text(el.apiHost, server.host);
    text(el.apiUptime, seconds(server.uptimeSeconds));
    text(el.apiPrefix, `${redis.prefix}:`);
    text(el.footerPrefix, `${redis.prefix}:`);
  }

  function renderJobs(jobs) {
    text(el.jobsCount, jobs.length ? `${jobs.length} most recent` : '');
    if (!jobs.length) {
      el.jobsBody.innerHTML = '<tr class="empty"><td colspan="8">No jobs yet — enqueue one above.</td></tr>';
      return;
    }

    el.jobsBody.innerHTML = jobs
      .map((job) => {
        const statusCls = `status-${['queued', 'running', 'done', 'failed'].includes(job.status) ? job.status : 'unknown'}`;
        const barCls = job.status === 'done' ? 'bar done' : job.status === 'failed' ? 'bar failed' : 'bar';
        const pct = job.status === 'done' ? 100 : job.progress || 0;
        const detail = job.error
          ? escapeHtml(job.error)
          : job.result
            ? escapeHtml(truncate(JSON.stringify(job.result), 120))
            : job.payload && Object.keys(job.payload).length
              ? escapeHtml(truncate(JSON.stringify(job.payload), 120))
              : '–';
        const detailCls = job.error ? 'detail err' : 'detail';
        const canRetry = job.status === 'failed' || job.status === 'queued';
        return `
          <tr data-job="${escapeHtml(job.id)}">
            <td><span class="job-id">${escapeHtml(job.id)}</span></td>
            <td>${escapeHtml(job.type)}</td>
            <td><span class="status ${statusCls}">${escapeHtml(job.status)}</span></td>
            <td>
              <div class="${barCls}"><span style="width:${pct}%"></span></div>
              <span class="mono">${pct}%</span>
            </td>
            <td class="mono">${escapeHtml(job.workerId ? shortId(job.workerId) : '–')}</td>
            <td class="mono">${duration(job)}</td>
            <td class="${detailCls}">${detail}</td>
            <td>${canRetry ? `<button class="btn btn-retry" data-retry="${escapeHtml(job.id)}">Retry</button>` : ''}</td>
          </tr>`;
      })
      .join('');
  }

  function renderEvents(events) {
    if (!events.length) {
      el.events.innerHTML = '<li><span class="ev-msg">Nothing yet.</span></li>';
      return;
    }
    el.events.innerHTML = events
      .map((ev) => {
        const [group, name] = String(ev.type).split('.');
        const tone = name ? `${group}.${name}` : group;
        return `
          <li>
            <time>${clock(ev.at)}</time>
            <span class="ev-type ${escapeHtml(tone)}">${escapeHtml(ev.type)}</span>
            <span class="ev-msg">${escapeHtml(ev.message)}</span>
          </li>`;
      })
      .join('');
  }

  function renderVerdict(status) {
    const redisUp = status.redis.status === 'ready' && status.redis.info?.reachable;
    const workers = status.workers;
    const online = workers ? workers.filter((w) => w.status !== 'offline') : [];

    if (!redisUp) {
      verdict('bad', '!', 'Redis is NOT working',
        `The app cannot reach Redis at ${status.redis.url}. ${status.redis.error || ''}`.trim());
      el.dot.className = 'dot bad pulse';
      return;
    }
    if (workers === null) {
      verdict('warn', '?', 'Redis is up, worker state unknown',
        'Redis responded, but the worker registry could not be read.');
      el.dot.className = 'dot warn';
      return;
    }
    if (online.length === 0) {
      verdict('bad', '!', 'Redis works, but no worker is running',
        `Redis is healthy (ping ${status.redis.latencyMs ?? '?'} ms) and ${status.queue.depth} job(s) are waiting, but no worker has checked in recently. Start one with "npm run worker".`);
      el.dot.className = 'dot bad pulse';
      return;
    }    if (online.length < workers.length) {
      verdict('warn', '!', 'Some workers are offline',
        `${online.length} of ${workers.length} workers are alive. The rest stopped reporting; their in-flight jobs get re-queued automatically.`);
      el.dot.className = 'dot warn';
      return;
    }

    const done = status.stats['jobs:completed'] || 0;
    const failed = status.stats['jobs:failed'] || 0;
    verdict('ok', '✓', 'Everything is working',
      `Redis responded in ${status.redis.latencyMs ?? '?'} ms · ${online.length} worker(s) online · ${done} job(s) completed, ${failed} failed · ${status.queue.depth} queued.`);
    el.dot.className = 'dot ok';
  }

  function render(status) {
    lastStatus = status;
    renderRedis(status.redis);
    renderWorkers(status.workers, status.stats || {}, status.queue?.depth ?? 0);
    renderApi(status.server, status.redis);
    renderJobs(status.jobs || []);
    renderEvents(status.events || []);
    renderVerdict(status);
    text(el.footerUpdated, `Updated ${clock(Date.now())}`);
  }

  const shortId = (value) => (value && value.length > 26 ? `${value.slice(0, 24)}…` : value || '–');
  const truncate = (value, max) => (value.length > max ? `${value.slice(0, max)}…` : value);
  const escapeHtml = (value) =>
    String(value ?? '').replace(/[&<>"']/g, (c) =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  /* ---------------------------------------------------------------- poll */

  async function poll() {
    try {
      const res = await fetch('/api/status', { cache: 'no-store' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      render(await res.json());
      failures = 0;
      lastSuccessAt = Date.now();
    } catch (err) {
      failures += 1;
      verdict('bad', '!', 'Cannot reach the API server',
        `Request to /api/status failed (attempt ${failures}): ${err.message}`);
      el.dot.className = 'dot bad pulse';
      // Keep the previously rendered state on screen, but be explicit that it
      // is stale - a frozen dashboard is indistinguishable from a healthy one.
      text(el.footerUpdated, lastSuccessAt
        ? `Stale: last successful update ${clock(lastSuccessAt)}`
        : 'Never received a status response');
    }
  }

  function schedule() {
    if (timer) clearInterval(timer);
    const rate = Number(el.pollRate.value);
    if (rate > 0) timer = setInterval(poll, rate);
  }

  /* ----------------------------------------------------------- enqueue UI */

  function renderTaskFields() {
    const task = catalog.find((t) => t.name === el.taskType.value);
    text(el.taskDesc, task?.description || '');
    el.taskFields.innerHTML = (task?.fields || [])
      .map(
        (f) => `
        <div class="field">
          <label for="f_${f.key}">${escapeHtml(f.label)}</label>
          <input id="f_${f.key}" type="${f.type === 'number' ? 'number' : 'text'}"
                 data-key="${escapeHtml(f.key)}"
                 value="${escapeHtml(f.value ?? '')}" />
        </div>`
      )
      .join('');
  }

  async function loadCatalog() {
    try {
      const res = await fetch('/api/tasks');
      const data = await res.json();
      catalog = data.tasks || [];
      el.taskType.innerHTML = catalog
        .map((t) => `<option value="${escapeHtml(t.name)}">${escapeHtml(t.label)} — ${escapeHtml(t.name)}</option>`)
        .join('');
      renderTaskFields();
    } catch {
      setError(el.enqueueError, 'Could not load the task list from the API.');
    }
  }

  /** JSON box wins if it parses; otherwise the dynamic fields are used. */
  function collectPayload() {
    const raw = el.taskJson.value.trim();
    if (raw) {
      try {
        return { payload: JSON.parse(raw), invalidJson: null };
      } catch (err) {
        return { payload: null, invalidJson: err.message };
      }
    }
    const payload = {};
    for (const input of el.taskFields.querySelectorAll('input[data-key]')) {
      const value = input.value.trim();
      if (value === '') continue;
      payload[input.dataset.key] = input.type === 'number' ? Number(value) : value;
    }
    return { payload, invalidJson: null };
  }

  async function enqueue(event) {
    event.preventDefault();
    setError(el.enqueueError, null);
    const { payload, invalidJson } = collectPayload();
    if (invalidJson) {
      setError(el.enqueueError, `Payload is not valid JSON: ${invalidJson}`);
      return;
    }

    el.enqueueBtn.disabled = true;
    try {
      const res = await fetch('/api/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: el.taskType.value, payload }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      await poll();
    } catch (err) {
      setError(el.enqueueError, err.message);
    } finally {
      el.enqueueBtn.disabled = false;
    }
  }

  async function retry(jobId) {
    try {
      const res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}/retry`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      await poll();
    } catch (err) {
      setError(el.enqueueError, `Retry failed: ${err.message}`);
    }
  }

  /* --------------------------------------------------------------- wiring */

  el.pollRate.addEventListener('change', schedule);
  el.refreshNow.addEventListener('click', poll);
  el.taskType.addEventListener('change', renderTaskFields);
  el.form.addEventListener('submit', enqueue);
  el.reset.addEventListener('click', async () => {
    if (!window.confirm('Delete every key owned by this app (jobs, events, worker registry)?')) return;
    try {
      const res = await fetch('/api/reset', { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      await poll();
    } catch (err) {
      setError(el.enqueueError, `Reset failed: ${err.message}`);
    }
  });

  // Event delegation for the Retry buttons, which are re-created on every render.
  el.jobsBody.addEventListener('click', (event) => {
    const button = event.target.closest('[data-retry]');
    if (button) retry(button.dataset.retry);
  });

  loadCatalog();
  poll();
  schedule();
})();
