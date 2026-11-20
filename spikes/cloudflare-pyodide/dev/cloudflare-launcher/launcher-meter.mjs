export const METER_NAMES = ['sqlite_rows_written', 'do_requests', 'log_events'];
export const METER_ERROR = 'CloudflareLauncherMeterLimitExceeded';

// One synchronous row allocates credit before side effects. The write of
// that row is also charged. A reset may overcount an interrupted operation,
// but it must never restore already spent credit.
export function launcherMeter(sql, binding) {
  if (binding === undefined) return null;
  const caps = JSON.parse(binding);
  if (Object.keys(caps).length !== METER_NAMES.length ||
      METER_NAMES.some(name => !Number.isSafeInteger(caps[name]) || caps[name] < 1) ||
      caps.sqlite_rows_written < 100 || caps.do_requests < 10) {
    throw new Error('Invalid launcher meter bounds');
  }
  sql.exec('CREATE TABLE IF NOT EXISTS meter (id INTEGER PRIMARY KEY, value TEXT)');
  let state = JSON.parse(sql.exec('SELECT value FROM meter WHERE id = 1').toArray()[0]?.value ??
    '{"counts":{"sqlite_rows_written":0,"do_requests":0,"log_events":0}}');

  function save(next) {
    sql.exec('INSERT OR REPLACE INTO meter VALUES (1, ?)', JSON.stringify(next));
    state = next;
  }

  return {
    get exhausted() { return state.exhausted; },
    get final() { return state.final === true; },
    snapshot() { return {...state.counts}; },
    markUncertain() {
      if (!state.incomplete && state.counts.sqlite_rows_written < caps.sqlite_rows_written) {
        save({...state, incomplete: true, counts: {...state.counts,
          sqlite_rows_written: state.counts.sqlite_rows_written + 1}});
      }
    },
    // Leave space to settle four slots, release the journal and stop alarms.
    // Ordinary operations cannot consume this space.
    charge(units, essential = false) {
      if (state.final || (state.exhausted && !essential)) return false;
      const next = {...state, counts: Object.fromEntries(METER_NAMES.map(name =>
        [name, state.counts[name] + (units[name] ?? 0) + (name === 'sqlite_rows_written' ? 1 : 0)]))};
      const headroom = essential ? {} : {sqlite_rows_written: 80, do_requests: 4};
      const limit = METER_NAMES.find(name => next.counts[name] > caps[name] - (headroom[name] ?? 0));
      if (limit) {
        if (state.counts.sqlite_rows_written < caps.sqlite_rows_written) {
          save({...state, exhausted: state.exhausted ?? limit, incomplete: state.incomplete || essential,
            counts: {...state.counts, sqlite_rows_written: state.counts.sqlite_rows_written + 1}});
        }
        return false;
      }
      save(next);
      return true;
    },
    seal() {
      if (!state.final && state.counts.sqlite_rows_written < caps.sqlite_rows_written) {
        save({...state, final: true, counts: {...state.counts,
          sqlite_rows_written: state.counts.sqlite_rows_written + 1}});
      }
      return state.final && !state.incomplete && !state.exhausted ? this.snapshot() : undefined;
    },
  };
}
