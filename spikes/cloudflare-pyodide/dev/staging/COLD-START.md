# Dynamic Python Worker cold start

This experiment compares the fork baseline `ec14b6115` with the staging runtime
built from the same companion sources. All new runs used local Miniflare and
workerd with a synthetic workflow and an in-process module gateway.

## Snapshot verdict

WorkerLoader cannot attach a caller-supplied memory snapshot to managed Python
children. The platform can supply its own interpreter baseline. Workerd's
dedicated snapshot path can capture application imports for a normally deployed
Worker, and the snapshot validator explicitly permits a managed baseline for a
Dynamic Worker even when `python_dedicated_snapshot` is set.
[Dynamic snapshot validation](https://github.com/cloudflare/workerd/blob/7caf6dee5bfe9af1b7710ddf66222900007af080/src/pyodide/internal/snapshot.ts#L922-L958)
[Compatibility flags](https://github.com/cloudflare/workerd/blob/7caf6dee5bfe9af1b7710ddf66222900007af080/src/workerd/io/compatibility-date.capnp)

- The loader's JavaScript `WorkerCode` has compatibility settings, modules,
  limits and bindings. Its Python/text/data/Wasm module kinds describe code
  and files. There is no snapshot field or snapshot module type. Python startup
  reads snapshot bytes from the internal metadata or artifact bundler.
  [Loader schema](https://github.com/cloudflare/workerd/blob/7caf6dee5bfe9af1b7710ddf66222900007af080/src/workerd/api/worker-loader.h#L58-L137)
  [Source extraction](https://github.com/cloudflare/workerd/blob/7caf6dee5bfe9af1b7710ddf66222900007af080/src/workerd/api/worker-loader.c++#L248-L407)
  [Snapshot reader selection](https://github.com/cloudflare/workerd/blob/7caf6dee5bfe9af1b7710ddf66222900007af080/src/pyodide/internal/metadata.ts#L23-L36)
- Standalone workerd has a process-level `--python-load-snapshot` option backed
  by a local snapshot directory. This feeds internal metadata through server
  configuration. It does not expose a per-child WorkerLoader argument or a
  hosted equivalent. [Workerd Python module registration](https://github.com/cloudflare/workerd/blob/7caf6dee5bfe9af1b7710ddf66222900007af080/src/workerd/io/worker-modules.h#L565-L618)
- In the local negative control, a fresh child with `python_dedicated_snapshot`
  accepted `pythonSnapshot` containing invalid bytes as an extra option. The
  same bytes supplied as `{data}` at `python_modules/snapshot.bin` were
  readable at `/session/metadata/python_modules/snapshot.bin`. Top-level
  Python ran and the child answered successfully. A restore reader would have
  tried to decode the bytes as a snapshot header.
  [Snapshot decoding](https://github.com/cloudflare/workerd/blob/7caf6dee5bfe9af1b7710ddf66222900007af080/src/pyodide/internal/snapshot.ts#L852-L877)

Snapshot artifact size, restore wall and restore CPU are therefore unavailable
for the application-supplied path. The historical 10 MB gzip budget would leave
about 3.45 MB for additions to the baseline runtime artifact. Current Workers
uploads are capped at 64 MiB uncompressed.
[Worker size limit change](https://developers.cloudflare.com/changelog/post/2026-09-04-increased-worker-size-limit/)

## Local staircase

Linux shared test container with four CPUs, Miniflare from Wrangler 4.156.0,
workerd 1.20261023.1 and Node 24.11.1. One parent per variant in the same
workerd process. Every sample used a fresh loader identity and awaited the
whole response. The four stages ran in sequence within each round and the
variant order alternated between rounds. Wall is from the parent request
boundary measured by the Node driver. CPU is workerd process user plus
system time from `/proc/<pid>/stat` at 100 ticks per second. It includes
parallel runtime work and can exceed elapsed wall. These are local A/B
measurements. For prior hosted context, see
[the production canary staircase](../../backend/PROD-CANARY.md).

Ten samples per variant and stage. Each cell is median (min–max) in
milliseconds. The host's one-minute load average was 4.59 at start, ranged
from 4.33 to 7.84 in the samples and was 6.85 at completion.

| Stage | Baseline wall | Candidate wall | Baseline CPU | Candidate CPU |
| --- | ---: | ---: | ---: | ---: |
| Bare Pyodide | 2,300.5 (2,163–2,759) | 2,320 (2,150–2,630) | 2,340 (2,200–3,880) | 2,360 (2,190–2,680) |
| + Runtime module map | 3,468 (3,251–3,749) | 2,497 (2,353–2,762) | 3,540 (3,290–3,790) | 2,550 (2,400–2,800) |
| + Boot imports | 4,225 (3,909–5,031) | 3,112.5 (2,851–3,476) | 4,300 (4,010–5,110) | 3,190 (2,930–3,550) |
| First successful workflow | 4,243 (3,996–4,541) | 3,120.5 (2,825–3,522) | 4,320 (4,070–4,610) | 3,280 (2,910–3,570) |

The candidate's first workflow took 26% less median wall time and 24%
less median process CPU. It was faster in all ten paired first-workflow
samples, with a median paired wall reduction of 975.5 ms. The median
within-round runtime-only increment over bare fell from 1,233 to 191 ms.
The additional boot-import increment fell from 640 to 571 ms. Most of the
measured gain appears before application imports, where native decoding
replaces the per-byte JavaScript callback.

The bare stage runs the managed interpreter with only the probe script.
The runtime stage adds the full staged module map. The import stage also
executes `import runtime_runner` at module level. The attempt stage uses
`tenantEngineCode` and checks a successful workflow returning a Chicago
timezone result. The fixture gateway requires no real token and supplies the
source and its SHA-256 matching the attempt context. The measured scope
starts at the parent request and does not include launcher dispatch or result
persistence.

## Boot imports

Baseline profile of `from runtime_runner import run_authorized_attempt`,
after loading the Worker SDK entrypoint. Both child process and monotonic
clocks returned zero throughout Python top-level execution. The profile
wrapped Python's first-load hook and timestamped nested start/end console
events as they arrived in the parent process. Approximate wall milliseconds
from one instrumented run are below. Self subtracts nested import intervals.
Console and pipe scheduling are included, so the values rank suspects and
do not substitute for the uninstrumented staircase.

| Module | Self ms | Inclusive ms |
| --- | ---: | ---: |
| http.cookiejar | 56.96 | 87.76 |
| urllib.request | 54.22 | 60.08 |
| bifrost.models | 48.96 | 264.11 |
| pydantic_core.core_schema | 33.04 | 68.74 |
| calendar | 30.80 | 30.80 |
| platform | 30.23 | 30.54 |
| fractions | 25.88 | 25.88 |
| uuid | 24.45 | 24.53 |
| pydantic_core._pydantic_core | 21.57 | 21.57 |
| redis.asyncio.cluster | 15.30 | 33.77 |
| annotated_types | 15.15 | 15.15 |
| src.services.execution.module_loader | 14.85 | 15.54 |
| mimetypes | 13.91 | 14.26 |
| pydantic.types | 13.01 | 21.44 |
| stringprep | 12.52 | 12.73 |
| encodings.idna | 11.44 | 24.17 |
| queue | 10.37 | 10.69 |
| bifrost | 9.49 | 535.16 |
| encodings.cp437 | 8.99 | 8.99 |
| pydantic._internal._decorators | 8.30 | 11.13 |

The staging overlay moves Redis imports to the parent-only cache paths and
moves the SDK's `webbrowser` import to interactive device login. The
boot-time SDK refresh bridge and model initialization retain their existing
order. Splitting the public SDK model exports further would touch the
package's import and singleton behavior and needs separate qualification.
The engine still imports the attempt path inside the existing import-trace
window.

## Runtime payload

| Artifact | Modules | Bytes | gzip bytes |
| --- | ---: | ---: | ---: |
| Compiled baseline | 1,170 | 19,205,778 | 6,548,892 |
| Compiled candidate | 1,155 | 19,124,988 | 6,528,842 |

The reduction is 80,790 bytes raw and 20,050 bytes gzipped. The staging compiler
omits 15 vendor typing-only files and precompiles all 408 Python sources.
Across the module maps, only the two edited sources, their bytecode, and the
15 omitted files differ. Python's standard library is embedded in Cloudflare's
managed runtime rather than in this module map.
[workerd stdlib registration](https://github.com/cloudflare/workerd/blob/7caf6dee5bfe9af1b7710ddf66222900007af080/src/workerd/io/worker-modules.h#L530-L535)

The [dependency census](../../backend/DEPENDENCY-CENSUS.md) and
[capability matrix](capability-matrix.mjs) document explicit package imports
including Redis. The [capability gate](../executor/capability-requirements.mjs)
checks workspace file paths against the manifest. It provides no vendored
module allowlist, so the build keeps the existing importable package set and
timezone data. The loader also decodes packaged bytes using
`Uint8Array.fromBase64`, avoiding the per-byte JavaScript callback for
native libraries, timezone files and bytecode.

Source companion SHA-256
`06a24befad22c8bf1fe7037fa558d575a46053b86f27d44b2157912f7896fc6d`.

Compiled baseline SHA-256
`2f933c2fce86ea172f229b74dbb64b9d041b47318b09bd22b84fc9503f8c89c4`.

Compiled candidate SHA-256
`7582fa1c11e36129adf78db7a06160eb2e55b58df17c5ec1ab8d28eb0cafb6ad`.

## Verification

- Local workerd boot inventory fell from 457 to 399 modules. Redis and
  `webbrowser` were absent from the candidate's boot inventory. A successful
  attempt supplied the actual trace to `mergePrimeList`. A fresh child's
  `_prime` loaded 13 modules, including the execution engine, and reported
  zero skips in both variants.
- Both artifacts imported `httpx`, `pydantic`, `yaml`, `redis`, `idna`,
  `dotenv`, `typing_extensions`, `tzdata`, `ssl` and `sqlite3` in a fresh
  child. The staircase also exercised timezone data through the workflow.
- The unchanged companion input precompiled successfully with CPython
  3.14.4. Windows Node 26.13.1 passed all 26 focused tests. The native-decoding
  loader is covered by `stage-runtime-loader.test.mjs` and the workerd runs
  above; the archived SDK-close and staged-runtime verifiers check historical
  receipts against the loader revision that produced them, not this loader.
  Node-side callers of `runtimeCode` need Node 25 or later. Paths below are
  relative to the spike root.

```sh
node --test dev/staging/runtime-footprint.test.mjs \
  dev/staging/vendor-patches.test.mjs dev/stage-runtime-loader.test.mjs \
  backend/cooperative-runtime.test.mjs dev/tenant-workers/generic-code.test.mjs \
  dev/executor/capability-requirements.test.mjs
node backend/verify-owned-sdk-close.mjs
node backend/verify-sdk-close.mjs
node backend/verify-staged-runtime-execution.mjs
```

## Hosted validation proposal

With separate deployment approval, create two disposable supervisors on an
existing paid test account, using the same compatibility date and CPU
limits. Run 12 interleaved rounds across baseline and candidate with a
shared bare control. Measure runtime-only, boot imports and first synthetic
workflow, with fresh loader names, no persistent tenant state, no keep-warm
traffic and an in-process fixture gateway. Verify successful response
contents. Record caller-observed wall and provider CPU if available. The
full staircase creates 84 unique children and requires at least 84 remaining
included creations before starting. With insufficient allowance, measure
only the first-attempt pair in each round, for 24 unique children at most.
Cap either run at 100 external requests, 300 billable invocations and
10 seconds of CPU per invocation. Expected incremental cost is **under
$0.05**: up to $0.048 in creation charges plus well under a cent of request
and CPU overage at the measured ~3–5 s of CPU per cold child. At the caps
(300 × 10 s = 3,000,000 CPU-ms at $0.02 per million) CPU could add about
$0.06, so the worst case is about $0.11.
Disable storage, durable objects and persisted logs for the two lab scripts.
End the experiment at 15 minutes or the first cap or unexpected response,
then remove both scripts and their lab routing and verify teardown.
No hosted validation was run.
[Dynamic Workers pricing](https://developers.cloudflare.com/dynamic-workers/pricing/)
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)
