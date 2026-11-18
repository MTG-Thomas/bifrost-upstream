# Database Timeout Overrides

Application connections preserve PostgreSQL role/database timeout settings by
default. No timeout listener is installed when both overrides are unset.

Set these environment variables only when overriding that policy:

| Variable | Unit | Meaning |
| --- | --- | --- |
| `BIFROST_DATABASE_STATEMENT_TIMEOUT_MS` | milliseconds | Maximum duration of each SQL statement, including lock waits |
| `BIFROST_DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_MS` | milliseconds | Maximum idle time inside an open transaction |

A positive value sets a bound. `0` explicitly disables the corresponding timeout;
unset leaves its PostgreSQL policy intact. Values must be between 0 and
2147483647. Apply settings to each API, worker and scheduler process that should
use them; restart those processes to rebuild their connection pools.

Overrides apply with `SET LOCAL` at transaction begin on the main, dispatch and
liveness pools. Commit and rollback reset them, including behind PgBouncer in
transaction mode. Each configured override adds one SQL round trip per
transaction. Alembic and separately created engines are outside this boundary.

Choose bounds from observed workload durations and lock waits. A statement bound
can cancel legitimate maintenance queries; an idle bound can terminate a session
while application code awaits a long build or external operation inside a
transaction. Do not apply an arbitrary platform-wide bound to address an
unverified incident. A canceled statement requires rollback before its session
can execute more SQL.
