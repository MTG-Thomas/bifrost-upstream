# Workers Upgrade and Repair

Bifrost compares the packaged Workers manifest digest to the saved release.
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
Complete a pending replacement before moving the scheduler to another build.
Existing saved limits and consent apply to the replacement. If the new
release's measured envelope exceeds the saved budget, review the policy and
then reconcile. If Cloudflare rejects the management credential, replace it
for the same account and reconcile the pinned revision. Independent scope
credentials continue to serve bounded retirement while available.

## Pending Hosted Qualification

Use an approved disposable account and disposable Bifrost environment.
Record the image and manifest digests, job and claim identities, script and
namespace receipts, and provider-side runtime retirement witnesses.

1. Hold an approved no-effect workflow on the previous Workers release. Stage
   the replacement and verify that no new claim or send is admitted. Break
   the control connection, let the original attempt finish, and verify its
   terminal replay and ACK under the original claim.
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
