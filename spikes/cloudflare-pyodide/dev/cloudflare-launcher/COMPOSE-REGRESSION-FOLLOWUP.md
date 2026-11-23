# Compose Workers Regression Follow-up

## Cold Start

Five pairs on CT128 compared `f6cf00b94` (A) and `0dcf4ae13` (B).
The order was AB, BA, AB, BA, AB. Each sample used a fresh Compose stack,
installation, provider process and launcher. Both heads used the same
verified Pyodide cache, packaged runtime, workerd version, 2-second launch
gate and 24 MiB primary hold. Containers were disabled. The warm control
reused the cold slot and each run received its terminal ACK.

| Pair | A cold | B cold | A prepare to ready | B prepare to ready |
| --- | ---: | ---: | ---: | ---: |
| 1 | 9.975 s | 8.104 s | 6.656 s | 4.718 s |
| 2 | 8.264 s | 6.877 s | 5.274 s | 3.967 s |
| 3 | 8.536 s | 8.681 s | 5.502 s | 5.448 s |
| 4 | 10.683 s | 7.793 s | 7.239 s | 4.844 s |
| 5 | 7.947 s | 8.492 s | 4.893 s | 5.187 s |
| Median | 8.536 s | 8.104 s | 5.502 s | 4.844 s |

The cold paired differences ranged from 2.890 s faster to 0.545 s slower
on B, with a paired median of 1.387 s faster. Warm medians were 0.682 s and
0.548 s. The earlier one-run 1.968 s slowdown did not reproduce. Variation
within each head spans the earlier gap, and most movement is in the
prepare-to-engine-ready interval. The two API images differed by 533 MB.
That size difference did not produce a consistent timing penalty after
the services had started and become healthy.

No Proxmox backup activity appeared in the sampled windows. CT128 CPU
pressure during cold entry ranged from 6.406% to 14.006%, with I/O full
pressure from 0.700% to 9.569%. Proxmox CPU usage in the wider cold windows
ranged from 42.018% to 66.690%. The two slowest runs coincided with the two
highest Proxmox CPU readings. The closest pair by Proxmox CPU use had
56.082% versus 57.045% and a 0.545 s increase on B. Load imbalance and five
samples per head limit conclusions about smaller changes. There is no
reproducible slowdown of the earlier magnitude to bisect.

The first attempt at sample 9 stopped with HTTP 502 from the standalone
ingress check after successful Enable. It did not submit a workflow. The
local relay emits 502 for upstream HTTP errors, and the old log omitted the
upstream error code. The final pair retained the original preflight and
deadlines, added transport error logging, and completed both measurements.
The first four pairs were retained. All ten projects, provider processes,
sample image tags and generated credential files were verified absent.

[Paired measurements and host samples](../../results/cloudflare-compose-cold-paired.json)

## Upgrade Receipt Ownership

The duplicate-revision bug was confined to the rehearsal driver. The
product drain passes the installation ID to previous-worker and resource
cleanup checks in `_drain_previous_revision`. The cleanup query starts
with `CloudflareOwnedResource.installation_id`, then applies the desired
revision filter. The script and inventory snapshots also filter by
installation ID before testing the revision. Account-wide script identity
readback serves usage accounting across retained installations.

[Upgrade drain](../../../../api/src/jobs/platform/cloudflare_enable.py)
· [Receipt queries](../../../../api/src/jobs/platform/cloudflare_capacity.py)

## Previous Replacement Settlement Timeout

Upgrade completed in 31.111 s on attempt one. The original workflow and
ACK passed, the successor took the scheduler lease, and the primary hold
was observable with 24 MiB free. The driver then timed out at the
38-second check for the replacement execution's terminal status and
released slot. Disconnect succeeded on attempt one and independently
verified nine script and twelve namespace 404s.

The retained provider log contains 15 workload terminal observations,
including one confirmed stop. The runtime logs each observation before
saving the terminal receipt and sending it, so the count alone cannot
establish dispatch, persistence or ACK for the replacement route. The
old log also lacks timestamps, successor API logs and the replacement
database readback at the deadline. Docker used the json-file driver, and
the previous teardown removed the containers and database volume. Those
records are unavailable for an exact replay of the historical failure.

The harness now captures the successor API log in CT128 and records
replacement status, pending dispatch, slot and per-route terminal and
ACK timings before the primary is released. It keeps the original
38-second settlement deadline. Missing or corrupt Pyodide assets fail
before the provider opens listeners, and workerd starts with an explicit
verified disk cache.

The full traced rehearsal passed all nine items, with the first rolling
worker pointed directly at the successor API. The replacement reached a
terminal event 7.068 s after dispatch, received its ACK at 7.587 s, and
read back a released revision-2 slot in 11.228 s. A Managed-only control
also stopped the interim primary while both APIs were still reachable.
It observed one completed terminal event at 6.450 s, one ACK at 6.564 s,
and a released slot in 10.144 s. Both controls kept the original route
intact and passed with the unchanged settlement deadline.

The post-readiness primary hold took 24.817 s in the full trace and
25.108 s in the stop control, compared with 5.231 s in the failed run.
These timings start after the worker health gate. Neither control
recreated the short hold from the failed run. The historical runtime
cause is blocked by missing evidence. The harness now preserves the
observations needed to distinguish a late dispatch, terminal delivery,
ACK or database settlement.

Both traced runs passed 19 offline packaging, relay and cache tests
each with networking disabled. The live provider used the pinned
bundle on an internal-only network. The full trace had a single backup
process scan match at 23:17:08 UTC. Proxmox task history showed zero
overlapping backup jobs. Independent CT128 readback verified the
provider, Compose resources, release tags and scratch absent after each
run. Only the test evidence was retained.

[Settlement and teardown evidence](../../results/cloudflare-compose-settlement-followup.json)
