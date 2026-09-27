#!/usr/bin/env node
/**
 * Convenience launcher: starts the API server and N worker processes in one
 * terminal, prefixes their output, and shuts everything down on Ctrl+C.
 *
 *   npm run start:all
 *   WORKERS=3 npm run start:all
 */
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import 'dotenv/config';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');

const workerCount = Math.max(1, Number(process.env.WORKERS) || 1);
const COLORS = ['\x1b[36m', '\x1b[35m', '\x1b[33m', '\x1b[32m', '\x1b[34m', '\x1b[95m'];
const RESET = '\x1b[0m';

const children = [];
let shuttingDown = false;

function launch(name, script, color, restarts = 0) {
  const child = spawn(process.execPath, [script], {
    cwd: root,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const prefix = `${color}[${name}]${RESET} `;
  const pipe = (stream, target) => {
    let buffer = '';
    stream.on('data', (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) target.write(`${prefix}${line}\n`);
    });
  };
  pipe(child.stdout, process.stdout);
  pipe(child.stderr, process.stderr);

  // Restart with a growing delay so a crash-looping worker (a real config
  // error, say) cannot bury the log in a loop of stack traces.
  let crashes = 0;
  child.on('exit', (code, signal) => {
    if (shuttingDown) return;
    crashes += 1;
    const delay = Math.min(1000 * crashes, 15000);
    console.log(`${prefix}exited (code=${code} signal=${signal}) - restarting in ${delay / 1000}s`);
    setTimeout(() => {
      const index = children.findIndex((c) => c.name === name);
      if (index !== -1) children[index] = launch(name, script, color, crashes);
    }, delay);
  });

  children.push({ name, child, script, color });
  return child;
}

launch('server', 'src/server.js', COLORS[0]);
for (let i = 0; i < workerCount; i += 1) {
  launch(`worker${i + 1}`, 'src/worker/index.js', COLORS[(i + 1) % COLORS.length]);
}

function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('\n[launcher] stopping everything...');
  for (const { child } of children) {
    if (!child.killed) child.kill('SIGTERM');
  }
  setTimeout(() => process.exit(0), 1500);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
