# Workers Upgrade and Repair

Bifrost compares the packaged Workers deployment digest to the saved release.
The digest covers source provenance, modules, bindings, compatibility settings,
and meter bounds. It excludes the release tag and CI run number. Promoting the
same commit and identical package from RC to final advances release precedence
without replacing Workers. A change to the source commit or deployment inputs
can produce a different digest even when the embedded Python runtime is identical.
The Phase 4 migration closes admission for an earlier admitted pilot and
fences its queued Enable jobs under the old revision.
Rebuild developer and qualification databases that applied an earlier Phase 4
migration before testing the folded migration.
An API with a different package closes new claims and sends. The scheduler
then closes admission under the installation lock, pins the package digest,
and enqueues Reconcile at the next revision. Reconcile and Upgrade both use
that job and return its shared status URL. The settings page shows the last
verified deployed revision and whether replacement is pending.

The old scope, credentials, and endpoints stay usable for reconnect, terminal
ACKs, and cleanup during drain. A dispatched attempt keeps its original claim.
Reconcile records the drain checkpoint after worker retirement and exact
script and namespace absence, then stages the replacement under a fresh
revision. It reprices the saved limits against the replacement release's
measured budget. Admission opens after the signed SDK canary, exact provider
readback, and spend check succeed.

Keep the previous worker-control, ingress, and credential protocols available
through retirement. If a release cannot serve the previous wire contract,
verify Scale In on the previous version before rolling the API. Run the
capacity scheduler and job runners from the same target Bifrost release.
The image build workflow records the Bifrost version and its run number.
SemVer precedence orders versions, with the run number breaking ties for
identical deployments at the same version. A newer version can take over
a pending replacement after verified retirement of any partial deployment.
A different digest at the same SemVer precedence returns
`CF_RELEASE_CONFLICT`. An older package returns `CF_RELEASE_DOWNGRADE`.
An operator can explicitly accept either through
`bifrost capacity cloudflare reconcile --reset-release-fence` with the current
installation ID and revision. Bifrost audits the previous and selected release
identities. A scaled-in installation stays paused after a reset.

New runners read version 1 jobs already in the queue. If a previous runner
rejects a version 2 job during rollout, the scheduler re-enqueues a pinned
upgrade with backoff. Remove the version 1 reader one release after version 2 ships.
Existing saved limits and consent apply to the replacement. If the new
release's measured envelope exceeds the saved budget, review the policy and
then reconcile. If Cloudflare rejects the management credential, replace it
for the same account and reconcile the pinned revision. Independent scope
credentials continue to serve bounded retirement while available.
The scheduler backs off from five minutes to one hour for drain timeouts,
pending cleanup, release mismatches, provider outages, and unavailable spend
evidence. An unavailable Managed Tunnel connector can recover when its
supervisor connects. Reconcile checks the connector before uploading
replacement Workers and applies the same backoff. A successful verification
resets the backoff. Admitted scopes
keep their periodic verification. Paused upgrades with other failures wait
for an operator action or a new desired revision.

## Pending Hosted Qualification

Queue pre-warming changes the packaged launcher bytes. The meter inventory
classifies the updated sources. The release builder still requires a measured
bundle pin for those bytes before publishing a qualified Workers package.

Use an approved disposable account and disposable Bifrost environment.
Record the image and manifest digests, job and claim identities, script and
namespace receipts, and provider-side runtime retirement witnesses.

1. Hold an approved no-effect workflow on the previous Workers release. Stage
   the replacement and verify that no new claim or send is admitted. Let
   the original attempt finish on its existing connection. The deliberate
   disconnect and terminal replay check runs only in the local fixture.
2. Verify the old Workers runtime, scripts and namespaces are absent before
   any replacement upload. Once the managed Containers lifecycle is
   integrated, hold work in both runtimes and require independent Container
   process and application retirement before either replacement is uploaded.
3. Fail the signed canary and verify that the replacement stays closed.
   Restore readiness and confirm the pinned revision opens after a successful
   canary. Check the primary continues to serve compatible work.
4. Revoke the management token after a journaled effect. Replace it with a
   validated same-account credential and resume. Verify the job reads the
   original effect before another mutation and that only one generation opens.
   Repeat the credential case after retirement and before the first upload.
5. Scale in, disconnect, and verify provider cleanup and retained usage
   evidence for the retired resources.

## Combined Hosted Qualification Disposition

The two prepared production images accepted their respective release
manifests and reported distinct deployment digests. The combined run
stopped at the Existing HTTPS Enable execution-class gate. It did not
activate the replacement or exercise Upgrade drain and terminal replay.

See the [sanitized combined hosted receipt](../../results/cloudflare-scaleout-combined-hosted.json).

## Combined Hosted Re-run Disposition

Both prepared release manifests passed the production image loader and
had distinct deployment digests. The Existing HTTPS worker preflight
stopped before Enable. The replacement image remained offline, and
Upgrade drain and terminal replay were not exercised.

See the [sanitized re-run receipt](../../results/cloudflare-scaleout-combined-hosted-rerun.json).

## Combined Hosted Run With HTTP Connection Reuse

The 2026-11-22 UTC run built both release images and verified their
distinct deployment digests. The first release completed Existing HTTPS
Enable. The local primary hold gate stopped the workload before the
replacement release was staged on the running stack. Upgrade drain and
terminal replay were not exercised. Both trial image tags were removed.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-connection-reuse.json).

## Observable-Gate Hosted Run

The 2026-11-22 UTC run verified both release images and their distinct
deployment digests. The first release passed Existing HTTPS Enable and
completed one cold workflow. The run stopped at the latency measurement
before the replacement image was staged on the running stack. Upgrade
drain and terminal replay were not exercised. Both image tags were removed.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-observable-gates.json).

## Hosted Cold and Warm Reuse Run

Both pinned releases passed offline loader verification from source
`788ffc641`. The run stopped at a post-latency harness gate before
the replacement image started. Hosted Upgrade drain and terminal
replay remain unqualified. Both local image tags were removed.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-latency-reuse.json).

### Combined Hosted Run at `db39a3698`

Both pinned packages passed the production image loader. Enable stopped
on the Existing HTTPS ownership gate, so the replacement never started
and hosted Upgrade drain and replay remain unqualified. Teardown verified
both trial image tags were gone.

See the [sanitized ownership-stop receipt](../../results/cloudflare-scaleout-combined-hosted-ownership-stop.json).

### Hosted Preset B Run at `e7c59788c`

Both requalified packages loaded in production API images. The managed
ingress ownership stop occurred before Upgrade submission. Hosted drain
and terminal replay remain unqualified. Both image tags were removed.

See the [sanitized preset B receipt](../../results/cloudflare-scaleout-preset-b-hosted.json).

The Managed-only run at `a9226cb62` verified both pinned packages offline.
The first Enable stopped at the version-zero tunnel configuration fence,
before any Worker upload. Hosted Upgrade remained unqualified.
Both local image tags were removed.
[Sanitized Managed-only receipt](../../results/cloudflare-scaleout-managed-hosted-a922.json)

### Hosted Managed Run 1 at `8712eb4c6`

Both pinned packages passed offline verification. Hosted Managed Enable,
rotation and re-Enable passed. Upgrade was unrun because the exact
control-socket interruption is available in the committed local fixture.
The hosted run closed through Scale In and verified Disconnect.
[Sanitized run receipt](../../results/cloudflare-scaleout-managed-hosted-8712.json)

### Uninterrupted Existing HTTPS Upgrade

Rehearse with the two pinned packages and the internal fixture:

```sh
python3 spikes/cloudflare-pyodide/dev/e2e/combined-rehearsal.py --upgrade-only
```

The hosted procedure uses the existing dedicated HTTPS lane. Start the
same ingress relay in `--hosted-ingress-relay` mode on loopback with its
separate authenticated control port. Point the trial connector at the
relay. Restrict destinations to the owned Compose bridge. Switch new
connections to the successor API after observing the persisted admission
fence. Each established socket keeps its original upstream until close.
Use the private operator configuration with `--hosted-upgrade-config`.

1. Verify both pins. Connect and Enable with the worker preflight and
   signed canary. Submit one approved, bounded no-effect workflow on
   the old revision. Retain the sent fence, launcher and original claim.
2. Stage the successor while the old API continues to serve its socket.
   Trigger Upgrade and observe admission closed while the saved claim
   and fence are still active. Let the workflow finish. Require one
   dispatch and the ACK of its matching successful terminal result.
3. Observe the durable `upgrade_drained_at` checkpoint with admission
   closed. Require every old script receipt to be absent at or before
   this checkpoint. The job verifies the old scripts and namespaces
   before the checkpoint and journals replacements only after it.
   Require the replacement observations to follow the checkpoint and
   independently confirm the saved old script and namespace IDs are
   absent.
4. Require the replacement's signed canary and inventory readback in
   the successful Upgrade job before admission reopens. After the
   original ACK, retire the original API and give the successor up to
   40 seconds to take the scheduler lease. Recreate the trial worker
   with the exact successor API URL and wait for it to become ready.
   Run a new workflow pinned to the replacement revision and
   verify its terminal ACK. Read spend, Disconnect, and verify all
   saved namespace IDs from the journal map values for both revisions.

Terminal replay after a deliberate socket interruption is a separate
local-only check. Hosted evidence from this procedure covers uninterrupted
drain and completion.
