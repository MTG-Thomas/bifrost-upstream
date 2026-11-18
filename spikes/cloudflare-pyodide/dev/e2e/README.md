# Phase 5a local launcher qualification

Use a private, disposable Git archive on the Linux host running Docker. The
suite binds HTTP to loopback and uses local Miniflare/workerd with the real
launcher and Python child. It rejects outbound origins other than its local
API relay and held-response fixture. No Cloudflare account is required.

## Run on CT128

Archive the desired local commit into a fresh `/opt/bifrost-tests/phase5-<sha>`
directory. Create the directory with `umask 077`. Inside it, run `git init`,
`git add -A`, and make an initial local commit without adding a remote.
Use Node 26 on PATH and the existing Wrangler version:

```sh
(cd spikes/cloudflare-pyodide/dev/tenant-workers && npm install --no-audit --no-fund)
umask 077
python3 spikes/cloudflare-pyodide/dev/e2e/phase5a.py prepare
BIFROST_FORCE_PORT=1 nohup setsid ./debug.sh up >.phase5-debug.log 2>&1 </dev/null &
# Wait for "Stack is up." in the private boot log.
python3 spikes/cloudflare-pyodide/dev/e2e/phase5a.py run /absolute/path/generated-runtime.mjs
BIFROST_FORCE_PORT=1 ./debug.sh down
```

Supply the prepared runtime from
[Building the packaged runtime](../staging/README.md#building-the-packaged-runtime).
The harness prints the SHA-256 of the supplied artifact and stages the loader
modules from the checkout. On CT128, a user-local Node tarball can be used with
the Debian `libatomic1` package extracted to the task directory and its
library directory on `LD_LIBRARY_PATH`. No system install is needed.

Run `./debug.sh down` on failure as well. Compose and dotenv changes belong to
the archive. Run the suite once per fresh stack. The operator seeds a new
workflow and incarnation, and passes only the derived control key into the
Worker. The debug log and dotenv are private. Do not share them.

## Evidence

The harness prints JSON lines with the execution ID, prepared incarnation,
claim and worker IDs, sanitized wire events, DO ledger summaries, and PostgreSQL
settlement fields. It blocks each fixture after a real SDK call so the
operator can read the active claim and trigger the next fault deterministically.
The harness pauses both Linux launchers (`worker` and `scheduler`) during the
claim sequence, waits for their three-second API admission window to close, and
unpauses both on exit. PostgreSQL retains each claim for provenance.

The relay drops exactly one welcome and one ACK. API takeover uses Uvicorn's
reload supervisor. A test RPC aborts the launcher DO while preserving its
SQLite journal, and a test hook calls the real TenantHost egress gate after
stop. Cancellation uses the execution API. Once the engine has started, the
cooperative stop leaves an uncertain outcome and the API stores a failure.
Retirement marks the enrolled row
and forces a reconnect so the API can return close code 4001. These hooks are
loaded only by Miniflare in this harness.
