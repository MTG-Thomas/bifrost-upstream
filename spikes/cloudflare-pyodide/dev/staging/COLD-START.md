# Dynamic Python Worker cold start

This experiment compares the fork baseline `ec14b6115` with the staging runtime
built from the same companion sources. The local and hosted runs used a
synthetic workflow and an in-process module gateway.

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

## Admission boot and short linger

CT128, Node 26.13.1, Wrangler 4.156.0, local Miniflare/workerd. The test
compared baseline `8d08546f9` with `108f373d6`. Both used the same packaged
Python runtime (SHA-256
`7582fa1c11e36129adf78db7a06160eb2e55b58df17c5ec1ab8d28eb0cafb6ad`).
The launcher completed its signed hello before timing began, as it does
before a production claim. A loopback control peer applied the real launcher
protocol and acknowledged each terminal. Both variants simulated one second
of API-side preparation before the workflow command. The candidate sent the
admission hint at the start of that interval. The workflow made a real SDK
tables request to the loopback fixture and returned the count.

Timing starts at simulated claim admission and ends at the first launcher
result. Each cold trial used a new tenant, with execution order alternating
per pair. The candidate repeated the workflow immediately on each of its
tenants, inside the default 90-second linger. The measured interval includes
the one-second preparation fixture in every sample.

| Trial | Samples | Median | Min–max |
| --- | ---: | ---: | ---: |
| Baseline cold | 8 | 4,170 ms | 3,962–5,439 ms |
| Admission boot cold | 8 | 3,166.5 ms | 3,057–3,915 ms |
| Admission boot, burst reuse | 8 | 1,013 ms | 1,012–1,025 ms |

The median cold time fell 24.1%, with a median paired reduction of 1,012 ms.
All eight burst runs reused the child and required no new loader identity
authorization. A separate workerd test advanced the clock past the linger
deadline and observed the host require a fresh confirmation with no alarm
scheduled. Linger is a readiness window. Workerd controls physical cache
eviction and can evict an idle isolate sooner.

Run the same comparison as
`node spikes/cloudflare-pyodide/dev/e2e/admission-latency.mjs <baseline-spike-dir> <generated-runtime.mjs>`
from a disposable candidate checkout with its Wrangler dependencies installed.
The loopback peer emulates the API generation grant. It does not measure
PostgreSQL claim latency, API-side token minting, hosted network latency,
provider eviction or the invoice. The actual gain depends on how much real
preparation overlaps startup.

For a hosted follow-up, use one disposable launcher per version and eight
interleaved fresh-tenant pairs. Include one candidate burst per pair, one
cancelled preparation and one probe after linger expiry. Keep the run to 15
minutes and require a pre-approved USD 1 incremental spend target. Capture
claim, prep completion, command send and first-result timestamps, then verify
the terminal/deletion receipts, identity deduplication and zero idle alarms.
Stop on the first ownership or spend anomaly and verify both deployments
were deleted. This hosted follow-up was not run.

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
[Dynamic Workers pricing](https://developers.cloudflare.com/dynamic-workers/pricing/)
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)

## Hosted results

On 2026-11-21 UTC, all **84 cold samples** passed across 12 interleaved
rounds on Cloudflare Workers. Median first-workflow wall time fell from
**3,618 ms to 2,034.5 ms (44%)**. The candidate was faster in all 12
workflow pairs, with a median paired reduction of **1,278 ms**.

Two disposable supervisors used the compiled artifacts above and the same
compatibility date, `2026-10-30`. Each round ran one bare-Pyodide control
and a baseline/candidate pair for each of the other three stages. The order
within each pair alternated, and every sample used a fresh loader identity.
The first-workflow probes checked the exact synthetic result, including
`America/Chicago`, and confirmed that the child had served one request.
The fixture gateway served hash-pinned source in process. No storage,
Durable Objects, keep-warm traffic or persisted logs were used.
Wall time starts at the caller's request and ends after receipt of the
full response. Each populated cell below represents 12/12 successes.

| Stage | Baseline median (min–max), ms | Candidate median (min–max), ms | Bare median (min–max), ms |
| --- | ---: | ---: | ---: |
| Bare Pyodide | — | — | 940.5 (545–2,903) |
| Runtime module map | 2,632.5 (1,629–3,744) | 1,511.5 (737–2,890) | — |
| Boot imports | 3,659 (2,559–5,480) | 2,547.5 (1,121–3,875) | — |
| First synthetic workflow | 3,618 (2,369–5,457) | 2,034.5 (1,485–4,028) | — |

Paired differences subtract wall times within the same round. Positive
baseline minus candidate values favor the candidate.

| Stage | Baseline minus candidate, ms | Candidate faster | Baseline minus bare, ms | Candidate minus bare, ms |
| --- | ---: | ---: | ---: | ---: |
| Runtime module map | 810.5 (212–2,434) | 12/12 | 1,508 (−502–3,113) | 742 (−1,312–1,334) |
| Boot imports | 1,309 (−1,076–3,070) | 11/12 | 2,734.5 (673–4,367) | 1,089 (111–3,287) |
| First synthetic workflow | 1,278 (443–3,603) | 12/12 | 2,660.5 (62–4,552) | 1,279 (−1,120–2,235) |

The bare control ranged from 545 ms to 2,903 ms. Several paired samples
with a runtime took less wall time than that round's bare control. Those
differences include variation between separate cold starts.

### Usage and cost

| Meter | Observed usage | Limit |
| --- | ---: | ---: |
| External requests | 86 to active scripts + 2 teardown checks | 100 |
| Billable invocations | At most 268 by request-path count | 300 |
| CPU limit configured on supervisors and children | 7,500 ms | 10,000 ms per invocation |
| Elapsed through verified teardown | 4 min 7 s | 15 min |

The request-path count includes 84 supervisor requests, 84 child requests,
two unauthenticated probes, up to 96 in-process fixture calls, and the two
post-delete URL checks. The 7,500 ms setting leaves margin for provider
termination delay. The cost model prices up to 10,000 CPU-ms per invocation.

The account-wide Cloudflare GraphQL meter reported **84 distinct Dynamic
Workers** in the measurement window, matching the 84 successful fresh loader
identities. It also reported 89 requests and 4,171.248 CPU-ms across the
account in that window. The script-filtered metrics query returned no
supervisor rows after deletion. The account totals have no parent/child
split or attributable startup CPU, so they cannot settle the lab's CPU
usage or its exact billing request count. The 2 ms startup value reported
at each upload measures the supervisor startup associated with deployment.

Estimated incremental charge was **$0.00** based on the included usage
visible at readback. The month-to-date creation count was 663 before the
run and 691 after it in the date-filtered view, which lagged the
measurement-window count. Even adding all 84 lab children to the latter
total keeps the estimate below the 1,000 included creations. The same
month-to-date view showed 18,625 requests and about 898,053 CPU-ms against
included usage of 10 million requests and 30 million CPU-ms. Charging
the modeled request and CPU ceilings at full overage rates, with creations
covered by the allowance, would cost **$0.05368**. The estimate is below the
$0.05 expected-spend cap and that compute-and-request ceiling is below the
approximately $0.11 worst-case cap. The charge remains an estimate until
billing settles.

### Teardown

Only the two temporary supervisors and their default Workers URLs were
created. Both supervisors returned HTTP 403 to unauthenticated probes.
Deletion succeeded for both scripts, and an independent account script-list
read confirmed both were gone. Both former URLs returned HTTP 404. The
temporary local lab secret file was removed.

The [sanitized receipt](../../results/cloudflare-cold-start-hosted.json)
records the source hashes, per-sample measurements and teardown checks.

## Managed Product Trial With Admission Boot

The November 21 combined release included boot at admission and configured
90-second linger. Its product pilot stopped at the Enable ingress TLS
probe before the runtime canary. It produced no managed cold-first or
warm-reuse latency. The hosted staircase above used the separate
synthetic supervisor and gateway setup.

See the [hosted attempt receipt](../../results/cloudflare-scaleout-hosted-attempt.json).

## Managed Product Retry

The API image passed the TLS preflight on both routable addresses. Enable
then reported an IPv4 ingress TLS failure before the signed canary. No
managed cold-first or warm-reuse latency was measured in the retry.

See the [sanitized hosted retry receipt](../../results/cloudflare-scaleout-hosted-retry.json).

## Managed Cold Workflow With Admission Boot

The 2026-11-22 UTC combined run from `58029df89` passed the Existing HTTPS
ingress and signed canary. With the primary held at 24 whole MiB free,
the first approved workflow ran on a fresh Workers incarnation at slot 0.
The durable queue timestamp and entry log give **10.178 s queue to entry**.
The persisted completion gives **10.261 s queue to completion**. The
workflow succeeded and released its sent slot fence.

The execution detail API omitted the queue timestamp, so the measurement
harness stopped while formatting the result. The run used the
admission-boot bundle with 90-second linger. It produced no warm-reuse
measurement. The latency values above come from post-stop PostgreSQL and
execution-log readback.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-observable-gates.json).

## Hosted Cold and Warm Workflows With Admission Boot

Source `788ffc641` passed signed Enable and ran both workflows with
24 MiB free on the primary. PostgreSQL supplied the queue timestamp.

| Run | Queue to entry | Queue to completion |
| --- | ---: | ---: |
| Cold | 9.959 s | 10.032 s |
| Warm | 0.582 s | 0.650 s |

Both runs used slot 0 on the same launcher, with one dispatch and a
terminal ACK per execution. Cold booked generation 0 and warm booked
no new generation. The 90-second linger stayed enabled. A separate
post-reuse query tried to reread the cold execution from the slot row
after the row had been reassigned, and the run stopped at that gate.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-latency-reuse.json).

### Hosted 2-Second Gate Attempt at `db39a3698`

The policy configured the 2 s overflow gate. Enable stopped on
`CF_OWNERSHIP_CONFLICT` before any launcher or workflow started. The
configured age and prior local rehearsal provide context for this
attempt. Hosted eligibility offset and cold and warm latency are
unmeasured.

See the [sanitized ownership-stop receipt](../../results/cloudflare-scaleout-combined-hosted-ownership-stop.json).
