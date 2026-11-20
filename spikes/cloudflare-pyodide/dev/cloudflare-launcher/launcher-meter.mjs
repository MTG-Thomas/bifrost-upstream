import {CLOUDFLARE_MAX_LOGICAL_BYTES, MAX_WORKFLOW_SLOTS} from './protocol.mjs';

export const METER_NAMES = ['sqlite_rows_written', 'do_requests', 'log_events'];
export const METER_ERROR = 'CloudflareLauncherMeterLimitExceeded';
export const STORAGE_CHUNK = 1 << 20;
// Maximum terminal insert + deletion, entering slot, compact ACK fence and
// tombstone deletion for each simultaneous slot. Count both ledger indexes.
const TERMINAL_ROWS = Math.ceil(CLOUDFLARE_MAX_LOGICAL_BYTES / STORAGE_CHUNK) + 1;
export const SQLITE_HEADROOM = MAX_WORKFLOW_SLOTS * (4 * TERMINAL_ROWS + 12) + 8;

export function launcherMeter(sql, binding) {
  if (binding === undefined) return null;
  const caps = JSON.parse(binding);
  if (Object.keys(caps).length !== METER_NAMES.length ||
      METER_NAMES.some(name => !Number.isSafeInteger(caps[name]) || caps[name] < 1) ||
      caps.sqlite_rows_written <= SQLITE_HEADROOM || caps.do_requests <= MAX_WORKFLOW_SLOTS) {
    throw new Error('Invalid launcher meter bounds');
  }
  sql.exec('CREATE TABLE IF NOT EXISTS meter (id INTEGER PRIMARY KEY, value TEXT)');
  let state = JSON.parse(sql.exec('SELECT value FROM meter WHERE id = 1').toArray()[0]?.value ??
    '{"counts":{"sqlite_rows_written":0,"do_requests":0,"log_events":0}}');

  function save(next) {
    // This journal write is workload too. Charge it before dispatching the
    // operation. An interrupted operation may be reserved without running.
    next.counts = {...next.counts, sqlite_rows_written: next.counts.sqlite_rows_written + 1};
    sql.exec('INSERT OR REPLACE INTO meter VALUES (1, ?)', JSON.stringify(next));
    state = next;
  }

  return {
    get exhausted() { return state.exhausted; },
    get final() { return state.final === true; },
    snapshot() { return {...state.counts, exhausted: Boolean(state.exhausted)}; },
    charge(units, essential = false) {
      if (state.final || (state.exhausted && !essential)) return false;
      const counts = {...state.counts};
      for (const name of METER_NAMES) counts[name] += units[name] ?? 0;
      const limit = counts.sqlite_rows_written + 1 > caps.sqlite_rows_written - (essential ? 0 : SQLITE_HEADROOM)
        ? 'sqlite_rows_written'
        : counts.do_requests > caps.do_requests - (essential ? 0 : MAX_WORKFLOW_SLOTS) ? 'do_requests' : null;
      if (limit && !essential) {
        // The denied operation consumed nothing. The exhaustion marker did.
        save({...state, exhausted: limit});
        return false;
      }
      save({...state, counts, ...(limit ? {exhausted: state.exhausted ?? limit} : {})});
      return true;
    },
    log() {
      // Diagnostic logs stop at their quota and leave the workload budget
      // alone while settlement headroom is in use.
      if (state.final || state.exhausted || state.counts.log_events >= caps.log_events ||
          state.counts.sqlite_rows_written + 1 > caps.sqlite_rows_written - SQLITE_HEADROOM) return false;
      save({...state, counts: {...state.counts, log_events: state.counts.log_events + 1}});
      return true;
    },
    seal() {
      if (!state.final) save({...state, final: true});
      return this.snapshot();
    },
  };
}
