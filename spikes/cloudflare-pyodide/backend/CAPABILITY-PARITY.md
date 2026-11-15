# Can a Cloudflare worker run anything a Bifrost Linux worker can?

**No. The claim is disproved**, by six platform counterexamples that no
configuration of this design removes. What *is* proved is narrower and still
large: every SDK, engine and standard-library behaviour tested runs with
identical results, once five defects found by this matrix were fixed.

Measured 2026-11-15. Evidence: `results/capability-matrix-default.json`,
`results/capability-matrix-lab.json`,
`results/capability-matrix-lab-cpu-cap-cloud.json`.

## Method

`dev/staging/capability-matrix.mjs` writes one workflow file per case to the
staging API, registers and executes it on the **staging Linux worker**
(`python:3.15.2-slim`, the production image), and runs the same pinned file
through the **deployed Cloudflare engine** (TenantHost → dynamic Pyodide child).
Each case returns a small JSON verdict of what it attempted, so the backends are
compared on behaviour, not status. Every case carried a predicted `cause`
*before* it ran, so a difference can be attributed:

| cause | meaning |
|---|---|
| platform | a property of Workers/Pyodide; no configuration removes it |
| policy | a choice of this spike's gateway/limits; configuration removes it |
| bundle | a package not shipped in the runtime; packaging removes it |
| version | CPython 3.15 (image) vs 3.14.2 (Pyodide) |
| none | expected identical |

Two Cloudflare configurations ran the same 34 cases, so policy is separable from
platform:

- **default**: the spike's real policy (narrow SDK surface, 1000 ms CPU,
  1000 subrequests, 1 MiB output budget, declared egress only).
- **lab**: every `/api/` path admitted, CPU raised to the 300 s maximum.

| Run | parity | differs | cloud-gap |
|---|---|---|---|
| default | 21 | 11 | 2 |
| lab | 23 | 9 | 2 |

**All 21 cases predicted `none` reached parity on both configurations.** Every
remaining difference is one that was predicted, and each is classified below.

## Counterexamples (platform — disproves the claim)

| Case | Linux worker | Cloudflare | Why no configuration helps |
|---|---|---|---|
| `memory-200mib` | Success, 200 MiB touched | `memory_limit` | 128 MB isolate cap; Linux has no per-run cap |
| `cpu-over-platform-cap` (3×10⁹ iterations) | Success in 246 s | `cpu_limit`: `CpuLimitExceeded: Python Worker exceeded CPU time limit` after 325 s wall, with the child budget at the 300 s maximum | Workers cap CPU per invocation; Linux has no CPU limit, only a wall-clock timeout (default 1800 s, up to 24 h). Pyodide is also slower per instruction, so the same work needs more CPU there |
| `subprocess` | `echo` ran | `OSError: emscripten does not support processes` | no processes in WebAssembly |
| `threading-thread`, `thread-pool-executor` | ran | `RuntimeError: can't start new thread` | no threads (`asyncio.to_thread` "works" only because Pyodide runs it inline, blocking the loop) |
| `raw-tcp-socket` | connected to `example.com:443` | `ConnectionRefusedError` | no sockets: DB drivers (asyncpg, psycopg, pymssql), SMTP, LDAP, SSH, websockets, DNS queries are all out; HTTP works only through `fetch` |
| `process-time` | ok | `RuntimeError: processor time … cannot be represented` | no process clock |

Also structural, from the package census: native extensions with no WebAssembly
build (`asyncpg`, `uvloop`) can never load, and runtime `pip install` of a
workspace `requirements.txt` (what Linux workers do) has no equivalent —
everything must be bundled at deploy time, inside the Worker size limits.

## Version gap (structural, time-bounded)

`python-version` is 3.15 vs 3.14; `py315-lazy-import-syntax` compiles on Linux
and is a `SyntaxError` on Cloudflare. Workflows written for 3.15 syntax or
stdlib fail until Pyodide ships 3.15 and Cloudflare gates it in — historically
months after CPython.

## Policy differences (configurable — not counterexamples)

| Case | default | lab | Note |
|---|---|---|---|
| `workflows-list-sdk`, `executions-list-sdk` | 403 at gateway | parity | the child SDK surface is an allowlist; `SDK_SCOPE=all` admits it |
| `cpu-fixed-work` (2×10⁷ iterations) | passed in the final run, `cpu_limit` in an earlier one | parity | 1000 ms is at the edge of ordinary loops; `ENGINE_CPU_MS` |
| `result-6mb` | `output-budget` | `output-budget` | 1 MiB retention budget (`OUTPUT_BUDGET_BYTES`); the platform bound is memory |
| `egress-undeclared-host` | 403 (`egress not declared`) | 403 | intended: only hosts the attempt declares |

## Bundle differences (packaging — curable)

`package-imports` checks 26 packages preinstalled on the Linux image. All 26
import on Linux; on Cloudflare only `httpx`, `pydantic`, `yaml`, `redis` exist.

- **Proven curable here:** `zoneinfo` failed for lack of `tzdata`; bundling the
  pure-Python `tzdata` wheel made `zoneinfo` reach parity.
- **Proven curable earlier** (`DEPENDENCY-CENSUS.md`, real Worker):
  `requests`, `msal`, `PyJWT`, `pillow`, `reportlab`, `pypdf`, `beautifulsoup4`.
- **Plausibly curable** (pure Python or a Pyodide build exists), untested here:
  `cryptography`, `lxml`, `bcrypt`, `pynacl`, `tiktoken`, `jinja2`, `openpyxl`,
  `python-docx`, `croniter`, `openai`, `anthropic`, `azure-identity` —
  each still subject to the no-socket/no-thread rules above.
- **Not curable:** `asyncpg`, `uvloop`; `aiohttp`/`websockets`/`dnspython`
  import-able at best but cannot open connections.

## Defects found and fixed by this matrix

Each was a false "Cloudflare can't" that was really a bug:

1. **Every SDK call answered with 204 failed** (`config.set`, deletes, file
   writes): httpx's Pyodide fetch transport called `.getReader()` on a null body.
   Fixed as a verified vendor patch (`dev/staging/vendor-patches.mjs`).
2. **Results with `datetime`/`UUID`/`Decimal` failed**: the child serialized with
   plain `json.dumps`. Now `default=str`, the Linux persistence contract
   (`src/repositories/executions.py::_make_json_safe`).
3. **Exceeding the child's subrequest limit inside a synchronous import
   deadlocked the attempt forever** (the platform's exception is lost in the sync
   bridge). The limit was a fixture value of 12 — one missing optional import
   costs one — now 1000 (`ENGINE_SUBREQUESTS`); and every attempt has a deadline
   (default 1800 s, the Linux default) after which the child is retired under a
   new loader generation, so a wedged isolate is never reused. Verified: over the
   old limit → `attempt-deadline` in 34 s, next attempt succeeds.
4. **An oversized result was reported as a generic runtime error** (the error
   subclass did not survive RPC); now `output-budget`.
5. **Gateway refusals were invisible** in the call breakdown; every outcome is
   now recorded against the attempt.

## Parity proven (selected)

SDK reads and writes (`organizations`, `tables` create/insert/query/delete,
`config` set/get/delete, `files` write/read/delete), parameters with type
coercion, concurrent SDK calls (`asyncio.gather`), exceptions → `Failed`, rich
result types, optional-import fallbacks, `ssl`, `sqlite3`, `zoneinfo`, hashing/
regex/decimal/csv, temp files, declared HTTPS egress with `httpx`, 64 MiB
allocations, a 90 s sleep. (`sync-def-workflow` fails on **both** backends: the
engine requires `async def`.)

## Limits of this evidence

- The Cloudflare side runs with an execution id the API never registered (there
  is no dispatch seam yet), so staging refuses its log writes (403). Log capture
  parity was shown separately (`dev/staging/differential.mjs`, logs/print cases);
  log *persistence* from Cloudflare is not proven.
- Wall-clock beyond 90 s was not exercised; Linux allows up to 24 h.
- One staging stack, one tenant at a time; no concurrency or cost comparison.
