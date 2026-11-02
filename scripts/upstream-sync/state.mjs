import {
  mkdirSync, openSync, writeSync, fsyncSync, closeSync, readFileSync,
  unlinkSync, readdirSync, statSync,
} from 'node:fs';
import { join, isAbsolute } from 'node:path';
import { hostname } from 'node:os';
import { randomUUID } from 'node:crypto';

const scope = 'MTG-Thomas/bifrost-upstream:main';
const sha = /^[a-f0-9]{40}$/;
const limit = 30;

function durableWrite(path, data, flags) {
  const fd = openSync(path, flags, 0o600);
  try {
    const bytes = Buffer.from(data);
    let offset = 0;
    while (offset < bytes.length) offset += writeSync(fd, bytes, offset, bytes.length - offset);
    fsyncSync(fd);
  } finally { closeSync(fd); }
}

// One approved, stable, local filesystem directory must be shared by ALL
// invocations. Never select a new directory to evade a hold or stale lock.
export class State {
  constructor(directory) {
    if (!isAbsolute(directory)) throw new Error('State directory must be absolute.');
    this.directory = directory;
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    this.lockPath = join(directory, 'process.lock');
    this.journalPath = join(directory, 'receipts.jsonl');
  }

  async locked(action) {
    const owner = { scope, host: hostname(), pid: process.pid, id: randomUUID() };
    try { durableWrite(this.lockPath, JSON.stringify(owner), 'wx'); }
    catch (error) {
      if (error.code === 'EEXIST') throw new Error('Process lock exists; no API calls. Never auto-steal a stale lock.');
      throw error;
    }
    try {
      this.logPath = this.beginLog();
      this.log('started');
      return await action();
    } catch (error) {
      if (this.logPath) this.log('stopped');
      throw error;
    } finally {
      // Never delete a lock now owned by another process or an operator.
      if (readFileSync(this.lockPath, 'utf8') !== JSON.stringify(owner)) {
        throw new Error('Lock ownership changed; manual recovery required.');
      }
      unlinkSync(this.lockPath);
      this.logPath = undefined;
    }
  }

  beginLog() {
    const directory = join(this.directory, 'logs');
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    const files = readdirSync(directory).filter(name => /^run-[0-9a-f-]+\.jsonl$/.test(name))
      .map(name => ({ name, time: statSync(join(directory, name)).mtimeMs }))
      .sort((a, b) => b.time - a.time || b.name.localeCompare(a.name));
    for (const file of files.slice(limit - 1)) unlinkSync(join(directory, file.name));
    const path = join(directory, `run-${randomUUID()}.jsonl`);
    durableWrite(path, '', 'wx');
    return path;
  }

  log(event) {
    if (!this.logPath || !/^[a-z-]+$/.test(event)) throw new Error('Invalid operational log event.');
    const line = JSON.stringify({ time: new Date().toISOString(), event }) + '\n';
    if (statSync(this.logPath).size + Buffer.byteLength(line) > 16384) {
      throw new Error('Operational log size limit exceeded.');
    }
    durableWrite(this.logPath, line, 'a');
  }

  outstanding() {
    let content;
    try { content = readFileSync(this.journalPath, 'utf8'); }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
    if (content && !content.endsWith('\n')) throw new Error('Torn journal; hold for manual recovery.');
    let pending = null;
    for (const line of content.split('\n').filter(Boolean)) {
      let record;
      try { record = JSON.parse(line); }
      catch { throw new Error('Corrupt journal; hold for manual recovery.'); }
      if (record.scope !== scope || !sha.test(record.before) || !sha.test(record.after) ||
          typeof record.attempt !== 'string' || !record.attempt ||
          !['intent', 'uncertain', 'synced'].includes(record.status)) {
        throw new Error('Invalid journal receipt; hold for manual recovery.');
      }
      if (record.status === 'intent') {
        if (pending) throw new Error('Overlapping journal intents; hold for manual recovery.');
        pending = record;
      } else {
        if (!pending || record.attempt !== pending.attempt || record.before !== pending.before || record.after !== pending.after) {
          throw new Error('Unmatched journal receipt; hold for manual recovery.');
        }
        if (record.status === 'synced') pending = null;
      }
    }
    return pending;
  }

  receipt(intent, status) {
    const record = { scope, attempt: intent.attempt, before: intent.before, after: intent.after,
      status, time: new Date().toISOString() };
    // The immutable safety ledger is separate from rotating operational logs.
    // A partial write or fsync failure leaves a hold; never repair it automatically.
    durableWrite(this.journalPath, JSON.stringify(record) + '\n', 'a');
    this.log(status);
    return record;
  }

  intent(candidate) {
    return this.receipt({ ...candidate, attempt: randomUUID() }, 'intent');
  }
}
