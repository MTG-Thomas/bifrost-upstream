# Hosted Containers Trial Plan

This is a proposed owner-approved trial for one disposable Workers Paid account
and one Bifrost installation using Existing HTTPS. The sequence comes from
the [CT128 Containers rehearsal](../e2e/README.md), which passed with an
empty provider stub, pinned Miniflare and the paired Docker guest. The
local driver has no hosted Containers mode. Its private opt-in and workflow
approval fixtures must have authorized equivalents on the trial installation
before starting. Use the normal signed release verifier and provider adapters.

## Sequence and evidence

| Step | Action and evidence to retain | Hosted question |
| --- | --- | --- |
| 1. Preflight | Pin the API image and paired public GHCR Linux image to their release digests. Record the release repository, source revision and expected signer from the API image. Check the exact trial inventories are empty and capture the opening Bifrost ledger and provider usage baseline. Review Existing HTTPS activation with Containers opted in, `standard-1`, two instances total, one per tenant, one workflow per instance, 120 s idle, 20 s start window, 300 s workflow-instance lifetime and 250,000 bytes per direction. Record the calculated monthly fleet floor and obtain separate approval for that policy. | Real release inputs and attributable billing baseline. |
| 2. Activate and Enable | Submit Activate once and follow its Enable job. Save the real signature verification result, prepared digest, application and script/namespace receipts, and the settled canary. Require joint admission for that exact revision only after the canary passes. The canary must use the verified ingress, reject the wrong host, disallowed route and redirect, and witness the non-intercepted TCP denials on 2053 and 8443. Record how the alternate peer resolves and that the API can reach it before counting the guest connection failures as evidence. | Real release signatures, hosted Durable Object scheduling, DNS and TCP enforcement without local guest firewall rules, and independent placement readback. |
| 3. Run one overflow workflow | Use the rehearsal's approved Provider-org workflow, including its native import, child process and two SDK calls. Hold the primary at the rehearsed 24 MiB free and submit one 96 MiB, 30 s workflow. At the in-flight SDK read, release the primary hold, hand the control connection to a healthy replacement API and let the read finish. Require a successful native result and one original Container dispatch on the same incarnation. Match the application, actor, image and start witness in the independent placement readback. | Hosted scheduling for real work, ingress through a runtime handoff, and placement identity continuity. |
| 4. Observe idle stop | Wait for that same incarnation to settle without sending more work. Record run end, drain and stop observations, and verify active and starting instance counts both reach zero. Compare the observed idle timing with the 120 s policy, allowing up to 30 s for readback and settlement as in the rehearsal. | A busy guest survives the exercised work and handoff, idle retirement follows the configured window, and provider stop can be independently verified. A longer busy-work inactivity test is outside this trial. |
| 5. Scale In and re-enable | Run Scale In once, require closed admission and zero running instances, and match all four retained scripts, namespaces, the application and image to their first receipts. Run Enable again with unchanged configuration. Require the same revision and resources, a second settled Container canary and fresh joint admission. Submit no second workflow. | Retained resources can schedule again, with fresh egress evidence and no duplicate provisioning. |
| 6. Disconnect and settle | Run Disconnect even after a failed step. Verify the teardown below and save the high-water live estimate, raw settled ledger and available attributed provider usage for the trial window. Compare runtime and egress along with the supporting Workers and Durable Object usage. | Verified placement and resource absence, and a first billing comparison. Delayed or unattributable provider usage keeps billing qualification open. |

## Spend and abort

Use incremental spend above the opening baseline. Two 45 s canaries plus
30 s boot, up to 30 s work, 120 s idle and 5 s stop for the workflow model
275 instance-seconds, about **$0.0057** at the conservative
`standard-1` rate of $0.00002056/s. Three 250,000-byte outbound allowances
add up to $0.0000375 at $0.05/GB, before the release's fixed control reserve.
Allow **$0.02 total expected settled** for Container and supporting Workers,
Durable Objects and ingress. Recompute with the exact signed release and
the opening account state. The separate monthly fleet floor is an admission
policy, and the following are trial stop thresholds. [Cloudflare Containers pricing](https://developers.cloudflare.com/containers/platform/pricing/) · [Bifrost spend assumptions](SPEND-DEFAULTS.md)

- **$0.10 live target.** Check at each transition and before any new start.
  Proceed only if the next step plus cleanup remains below the target.
- **$0.15 live abort, $0.03 settled cap.** Stop submitting work at the
  live abort, or sooner if projected settlement would exceed the settled
  cap. Continue teardown and account readback. Treat missing spend evidence
  as a stop condition. These operator thresholds do not replace the
  product's spend guard or cap charges at the provider.
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
Keep sanitized timestamps, digests, usage and absence receipts. If a read
is inconclusive, preserve the ownership record and leave the trial open
for cleanup verification.
