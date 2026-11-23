# Hosted Containers Trial Plan

This is a proposed owner-approved trial for one disposable Workers Paid account
and one Bifrost installation using Existing HTTPS. The sequence comes from
the [CT128 Containers rehearsal](../e2e/README.md), which passed with an
empty provider stub, pinned Miniflare and the paired Docker guest. The
local driver has no hosted Containers mode. The public opt-in, Linux review
and hosted-shaped rehearsal are prerequisites. Use the normal signed release
verifier and provider adapters.

## Publication approvals

Obtain explicit owner approval for each outward-facing effect before starting.

1. Push the exact release tag to the fork and run its release CI.
2. Publish the GitHub Release with `cloudflare-containers-manifest.json`,
   `cloudflare-containers-manifest.sigstore.json` and
   `cloudflare-containers-container-registry.mjs`.
3. Make the digest-pinned GHCR Linux image publicly readable.
4. Use keyless cosign signing. This writes a permanent public Rekor entry
   identifying the fork repository and release workflow.

## Sequence and evidence

| Step | Action and evidence to retain | Hosted question |
| --- | --- | --- |
| 1. Preflight | Pin the API image and paired public GHCR Linux image to their release digests. Record the release repository, source revision and expected signer from the API image. Check the exact trial inventories are empty and capture the opening Bifrost ledger and provider usage baseline. Review Existing HTTPS activation with Containers opted in, `standard-1`, two instances total, one per tenant, one workflow per instance, 120 s idle, 20 s start window, 300 s workflow-instance lifetime and 250,000 bytes per direction. Record the calculated monthly fleet floor and obtain separate approval for that policy. | Real release inputs and attributable billing baseline. |
| 2. Activate and Enable | Submit Activate once and follow its Enable job. Save the real signature verification result, prepared digest, application and script/namespace receipts, and the settled canary. Require joint admission for that exact revision only after the canary passes. The canary must use the verified ingress, reject the wrong host, disallowed route and redirect, and witness the non-intercepted TCP denials on 2053 and 8443. Record how the alternate peer resolves and that the API can reach it before counting the guest connection failures as evidence. | Real release signatures, hosted Durable Object scheduling, DNS and TCP enforcement without local guest firewall rules, and independent placement readback. |
| 3. Run one overflow workflow | Use the rehearsal's approved Provider-org workflow, including its native import, child process and two SDK calls. Hold the primary at the rehearsed 24 MiB free and submit one 96 MiB, 30 s workflow. Require a successful native result and one Container dispatch. Match the application, actor, image and start witness in the independent placement readback. | Hosted scheduling for real work and placement identity continuity. The API handoff remains a local-only test. |
| 4. Observe idle stop | Wait for that same incarnation to settle without sending more work. Record run end, drain and stop observations, and verify active and starting instance counts both reach zero. Compare the observed idle timing with the 120 s policy, allowing up to 30 s for readback and settlement as in the rehearsal. | A busy guest survives the exercised work and handoff, idle retirement follows the configured window, and provider stop can be independently verified. A longer busy-work inactivity test is outside this trial. |
| 5. Scale In and re-enable | Run Scale In once, require closed admission and zero running instances, and match all four retained scripts, namespaces, the application and image to their first receipts. Run Enable again with unchanged configuration. Require the same revision and resources, a second settled Container canary and fresh joint admission. Submit no second workflow. | Retained resources can schedule again, with fresh egress evidence and no duplicate provisioning. |
| 6. Disconnect and settle | Run Disconnect even after a failed step. Verify the teardown below and save the high-water live estimate, raw settled ledger and available attributed provider usage for the trial window. Compare runtime and egress along with the supporting Workers and Durable Object usage. | Verified placement and resource absence, and a first billing comparison. Delayed or unattributable provider usage keeps billing qualification open. |

## Spend and abort

The provisional Containers-only monthly floor is approximately **$180**
for two `standard-1` instances. Add the approved Workers fleet floor.
Recompute the exact combined admission minimum from both signed releases and
the selected policy. The existing Workers Paid subscription is **$5/month**.
Compare provider instance-seconds, outbound bytes, requests and CPU against
the Bifrost ledger. Included allowances can make the incremental invoice $0.

Use incremental spend above the opening baseline. Calculate the approved
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
figures with the actual signed inputs before seeking approval. Check spend
at each transition and before a new start. Treat missing spend evidence as
a stop condition. [Prior hosted receipts](PHASE1-PILOT.md) · [Bifrost spend assumptions](SPEND-DEFAULTS.md)

- Hosted DNS resolution, API reachability on the probe ports and provider
  raw-TCP denial remain unqualified. If any witness fails, require admission
  to stay closed, skip steps 3–5 and go straight to Disconnect.
- Treat `CF_CONTAINERS_CLEANUP_PENDING` and
  `CF_CONTAINERS_QUALIFICATION_BACKOFF` as stop conditions. Neither permits a
  third canary. Record Capacity lane state, the original dispatch/claim fence,
  and independent provider placement reads to observe the scheduler.
- Abort on wrong signer, image, account, ownership or revision; an
  unexpected start or duplicate dispatch; a failed or ambiguous egress
  witness; capacity above the pilot caps; settlement beyond the rehearsed
  window; or uncertain placement or cleanup. Allow 20 minutes for the
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
