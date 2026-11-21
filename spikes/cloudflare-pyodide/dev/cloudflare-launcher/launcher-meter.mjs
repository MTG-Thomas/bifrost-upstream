import {CLOUDFLARE_MAX_LOGICAL_BYTES, MAX_WORKFLOW_SLOTS} from './protocol.mjs';

export const METER_NAMES = ['sqlite_rows_written', 'do_requests', 'log_events'];
export const METER_ERROR = 'CloudflareLauncherMeterLimitExceeded';
export const STORAGE_CHUNK = 1 << 20;
// Maximum terminal insert + deletion, entering slot, compact ACK fence and
// tombstone deletion for each simultaneous slot. Count both ledger indexes.
const TERMINAL_ROWS = Math.ceil(CLOUDFLARE_MAX_LOGICAL_BYTES / STORAGE_CHUNK) + 1;
const sqliteHeadroom = slots => slots * (4 * TERMINAL_ROWS + 12) + 8;
export const SQLITE_HEADROOM = sqliteHeadroom(MAX_WORKFLOW_SLOTS);
const MAX_RECEIPT_ROUTES = 128;
const ROUTE_ENTRY_WRITES = 16;
const ROUTE_ENTRY_REQUESTS = 5; // initial run, up to 3 warm calls, confirmed run

export function launcherMeter(sql, binding, {settle = false} = {}) {
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
  let slots = MAX_WORKFLOW_SLOTS;

  function admissionLimit() {
    if (state.exhausted) return state.exhausted;
    if (state.counts.sqlite_rows_written + ROUTE_ENTRY_WRITES > caps.sqlite_rows_written - sqliteHeadroom(slots)) {
      return 'sqlite_rows_written';
    }
    if (state.counts.do_requests + ROUTE_ENTRY_REQUESTS > caps.do_requests - slots) return 'do_requests';
    return null;
  }

  function save(next) {
    // This journal write is workload too. Charge it before dispatching the
    // operation. An interrupted operation may be reserved without running.
    next.counts = {...next.counts, sqlite_rows_written: next.counts.sqlite_rows_written + 1};
    sql.exec('INSERT OR REPLACE INTO meter VALUES (1, ?)', JSON.stringify(next));
    state = next;
  }

  return {
    setSlots(count) { slots = count; },
    get exhausted() { return admissionLimit(); },
    get final() { return state.final === true; },
    snapshot() {
      return {...state.counts, sqlite_database_bytes: sql.databaseSize, exhausted: Boolean(admissionLimit())};
    },
    charge(units, essential = false) {
      if (state.final || (state.exhausted && !essential)) return false;
      const counts = {...state.counts};
      for (const name of METER_NAMES) counts[name] += units[name] ?? 0;
      const limit = counts.sqlite_rows_written + 1 > caps.sqlite_rows_written - (essential ? 0 : sqliteHeadroom(slots))
        ? 'sqlite_rows_written'
        : counts.do_requests > caps.do_requests - (essential ? 0 : slots) ? 'do_requests' : null;
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
          state.counts.sqlite_rows_written + 1 > caps.sqlite_rows_written - sqliteHeadroom(slots)) return false;
      save({...state, counts: {...state.counts, log_events: state.counts.log_events + 1}});
      return true;
    },
    recordRoute(routeId, invocations) {
      // Routes beyond the receipt capacity keep their Bifrost reservations.
      if (settle && !state.final && !state.routes?.[routeId] &&
          Object.keys(state.routes ?? {}).length < MAX_RECEIPT_ROUTES) {
        save({...state, routes: {...state.routes, [routeId]: invocations}});
      }
    },
    seal(ledgerCleanupRows) {
      if (!state.final) {
        // Reserve the ledger table/index deletes and the meter row deleted by
        // the subsequent clear. Persist this in the seal exactly once.
        const cleanup = ledgerCleanupRows === undefined ? 0 : ledgerCleanupRows + 1;
        const counts = {...state.counts, sqlite_rows_written: state.counts.sqlite_rows_written + cleanup};
        save({...state, counts, final: true});
      }
      return {...this.snapshot(), ...(settle ? {routes: state.routes ?? {}} : {})};
    },
  };
}
