# Hosted Containers Trial Plan

This trial requires owner approval for one disposable Workers Paid account
and one Bifrost installation using Existing HTTPS. The sequence passed
the [CT128 hosted-shaped rehearsal](../e2e/README.md) using
`combined-rehearsal.py --containers-hosted-shape` with an empty provider stub,
pinned Miniflare and the paired Docker guest. The hosted trial uses the
normal signed release verifier and provider adapters. From the trial checkout, run:

```sh
python3 spikes/cloudflare-pyodide/dev/e2e/combined-rehearsal.py \
  --containers-hosted-shape --hosted-containers-config /absolute/private/containers-trial.json
```

Keep the config private. It names the
test Compose project, network, Compose files, account, dedicated hostname,
authorized management token, and the three approved spend thresholds.
The runner uses the dedicated instance's operator account. By default its
provider reads target the public Cloudflare API.

## Owner approvals

Obtain explicit owner approval for each outward-facing effect before starting.

1. Push the exact release tag to the fork and run its release CI.
2. Publish the GitHub Release with `cloudflare-containers-manifest.json`,
   `cloudflare-containers-manifest.sigstore.json` and
   `cloudflare-containers-container-registry.mjs`.
3. Make the digest-pinned GHCR Linux image publicly readable.
4. Use keyless cosign signing. This writes a permanent public Rekor entry
   identifying the fork repository and release workflow.
5. Approve the exact policy and its combined monthly fleet floor from preflight.
6. Approve the calculated live target, live abort threshold, and settled cap.
   Record the signed rates and cleanup margin used for those calculations.

## Manual read-only checklist

- Before activation, record the expected signer identity, release repository,
  tag and source revision from the pinned API image and signed Linux manifest.
  Compare them with the images selected for the trial.
- Before activation and after Disconnect, record the provider usage window
  and attributed instance-seconds, outbound bytes, requests, and CPU. Keep
  the opening and closing samples together for the same account and window.
- Before activation, record DNS resolution for the alternate
  peer as seen from the API and whether the API can connect to the alternate
  worker origin on TCP 2053 and the probe peer on TCP 8443.

## Sequence and evidence

| Step | Action and evidence to retain | Hosted question |
| --- | --- | --- |
| 1. Preflight | Pin the API image and paired public GHCR Linux image to their release digests. Record the release repository, source revision and expected signer from the API image. Check the exact trial inventories are empty and capture the opening Bifrost ledger and provider usage baseline. Review Existing HTTPS activation with Containers opted in, `standard-1`, two instances total, one per tenant, one workflow per instance, 120 s idle, 20 s start window, 300 s workflow-instance lifetime and 250,000 bytes per direction. Record the calculated monthly fleet floor and obtain separate approval for that policy. | Real release inputs and attributable billing baseline. |
| 2. Activate and Enable | Submit Activate once and follow its Enable job. Save the real signature verification result, prepared digest, application and script/namespace receipts, and the settled canary. Require joint admission for that exact revision only after the canary passes. The canary must use the verified ingress, reject the wrong host, disallowed route and redirect, and witness the non-intercepted TCP denials on 2053 and 8443. Record how the alternate peer resolves and that the API can reach it before counting the guest connection failures as evidence. | Real release signatures, hosted Durable Object scheduling, DNS and TCP enforcement without local guest firewall rules, and independent placement readback. |
| 3. Run one overflow workflow | Propose and accept the Linux review through the public workflow approval API. Hold the primary at 24 MiB free and submit one 96 MiB, 30 s workflow with a native import, child process and two SDK calls. Require a successful result and a single Container claim in the scheduler ledger. | Hosted scheduling for real work and placement identity continuity. The API handoff remains a local-only test. |
| 4. Observe idle stop | The single-use guest starts draining after it reports the result. Wait up to 150 s for stop and settlement on that same incarnation. Confirm stopped placement through the exact actor read, then revoke the Linux approval. | A busy guest survives the exercised workflow and retires after the result. Unused-instance idle timing and a longer busy-work inactivity test remain open. |
| 5. Scale In and re-enable | Run Scale In once. Require closed admission, zero active or starting instances, and unchanged application, four scripts and six namespaces. Run Enable again with unchanged configuration. Require the same revision and resources, a second settled Container canary and fresh joint admission. Submit no second workflow. | Retained resources can schedule again, with fresh egress evidence and no duplicate provisioning. |
| 6. Disconnect and settle | Run Disconnect even after a failed step. Verify the teardown below and save the high-water live estimate, raw settled ledger and available attributed provider usage for the trial window. Compare runtime and egress along with the supporting Workers and Durable Object usage. | Verified placement and resource absence, and a first billing comparison. Delayed or unattributable provider usage keeps billing qualification open. |

## Spend and abort

The suggested shared limit is **$75/month**, including a **100 instance-hour**
Container budget. Two `standard-1` instances may run concurrently.
With the trial's signed rates, the modeled monthly Container floor is
**$12.27**, including $7.40 in compute and the fixed control and outbound
holds. The modeled Workers floor is **$32.13**, for a **$44.40** combined
minimum. Confirm both figures in activation preflight for the exact signed
release and policy.
The existing Workers Paid subscription is **$5/month**.
Compare provider instance-seconds, outbound bytes, requests and CPU against
the Bifrost ledger. Included allowances can make the incremental invoice $0.

The runner applies absolute thresholds to the ledger of its fresh database.
Calculate provider usage above the opening baseline. Calculate the approved
thresholds before activation from the signed manifest's instance-second
rate `r`, outbound GB rate `e`, per-start control `f`, and
`cleanup_seconds` `c`. Use at most two Container canaries and one workload
start, with 250,000 outbound bytes each.

| Component | Planning input |
| --- | --- |
| Containers, expected | Two 45 s canaries including stop, plus 30 s boot, 30 s work, 120 s idle and 5 s stop. Total `275r + 0.00075e + 3f`. |
| Containers, live reservation allowance | Two canary pins of `15 + 121 + c` seconds and one work pin of `20 + 300 + c` seconds. Total `(592 + 3c)r + 0.00075e + 3f`. |
| Workers runtime canaries, both Enables | Use $0.000431967 for the pair from the Managed-only hosted run, inclusive of its supporting control and ingress. |
| Supporting Workers requests and CPU | The earlier attributed Workers sample was $0.00006021396. Keep this as a usage comparison anchor. |
| Durable Objects and ingress | Earlier hosted runs did not isolate both costs. Carry $0.012847448 as a conservative combined supporting allowance from the settled Existing HTTPS Upgrade run. Keep the provider breakdown separate where available. |

Round the live reservation allowance plus both supporting allowances up to
the next cent for the **live target**. Set the **abort threshold** one
additional rounded live reservation allowance above it, reserved for cleanup.
Round the expected cost plus the supporting allowances up to the next cent
for the **settled cap**. For illustration, `c=60` and `f=$0.000001` with the
pinned rates give $0.03 live, $0.05 abort and $0.02 settled. Replace those
figures with the actual signed inputs before seeking approval. Write the
approved values to `live_target_usd`, `live_abort_usd`, and `settled_cap_usd`
in the private config. Check spend
at each transition and before a new start. Treat missing spend evidence as
a stop condition. [Prior hosted receipts](PHASE1-PILOT.md) · [Bifrost spend assumptions](SPEND-DEFAULTS.md)

The local hosted-shaped run with namespace lag completed its first Enable
on attempt two. The runner checks canary reservations per Enable and verifies
settlement before continuing. The run recorded two settled canaries, one
Container workflow claim, a $0.02400 peak ledger estimate and $0.00291 settled
before Disconnect. Those estimates used the local signed-release fixture rates.

- Hosted DNS resolution, API reachability on the probe ports and provider
  raw-TCP denial remain unqualified. If any witness fails, require admission
  to stay closed, skip steps 3–5 and go straight to Disconnect.
- The suggested monthly allowance is 30 Container canaries per Cloudflare
  account in this Bifrost database, about $0.15 at pilot rates. Other
  Bifrost databases have separate allowances. Confirm at least two remain
  before the trial.
- Treat `CF_CONTAINERS_CLEANUP_PENDING` and
  `CF_CONTAINERS_QUALIFICATION_BACKOFF` as stop conditions. Neither permits a
  third canary. Record Capacity lane state, the original dispatch/claim fence,
  and independent provider placement reads to observe the scheduler.
- Abort if the signer, image, account, ownership or revision differs from
  the recorded value. Also abort on an unexpected start or duplicate dispatch,
  a failed or ambiguous egress witness, capacity above the pilot caps,
  settlement beyond the rehearsed window, or uncertain placement or cleanup.
  Allow 20 minutes for the
  active sequence, then reserve 5 minutes for teardown. If verified
  absence is still pending, retain oversight without restarting capacity.

## Teardown verification

Use the saved identities for independent exact reads and matching inventory
checks. Require zero active or starting placements, absence of the
application, registry tag, four scripts and six namespaces, a retired
installation, and no live incarnations, held seats, unresolved effects or
retained credentials. Verify removal of any trial-owned token, ingress
resources and local stack using the [Workers teardown procedure](PHASE1-PILOT.md).
Release publication is tracked separately from Cloudflare teardown. The
public Rekor entry remains permanently even if the tag, assets and image are removed.
Keep sanitized timestamps, digests, usage and absence receipts. If a read
is inconclusive, preserve the ownership record and leave the trial open
for cleanup verification.

## Qualification remaining after a successful trial

- The trial exercises one workflow and two settled canaries on one account,
  one ingress, one release, and the pilot instance type. Higher concurrency,
  other instance types, different account or ingress configurations, and
  sustained load remain unqualified.
- The workflow covers its own busy interval and single-use retirement.
  Unused-instance idle timing, prolonged busy-work inactivity, and hosted
  restart and handoff remain unqualified.
- The observed egress results cover the exercised host, route, redirect, DNS
  and TCP probes. Other network paths and provider failure modes remain
  unqualified.
- One attributed trial window gives a billing comparison for its exercised
  usage. Meter completeness, attribution during concurrent workloads and
  steady-state monthly billing remain unqualified. If the provider reports
  delayed or unattributable usage, the trial's billing comparison remains open.
