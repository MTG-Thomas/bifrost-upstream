# rc04 Container Image Preparation and Cleanup

The fourth hosted attempt reached Activate and sent the registry copy. Its
receipt recorded one retryable Enable attempt and no Container canaries,
workflow reservations, or incarnations. The original preparation HTTP status
and response body were unavailable, so the receipt could not distinguish
registry visibility delay from a pending preparation.

## Registry and Preparation State

The registry copy and runtime preparation are separate effects. Cloudflare's
CLI push uses registry credentials and performs a Docker tag and push. Its
images list command enumerates registry repositories and tags. Tag visibility
can be probed independently of runtime preparation. [cf push source](https://github.com/cloudflare/cf/blob/main/packages/cli/src/commands/containers/push/index.ts),
[cf list source](https://github.com/cloudflare/cf/blob/main/packages/cli/src/commands/containers/images/list/index.ts).

The digest-pinned preparation POST starts or observes the same durable job.
HTTP 202 reports pending work. HTTP 200 reports ready or terminal error.
Wrangler likewise resolves or uploads a digest-pinned image and polls the
POST for up to 15 minutes before using it in Worker metadata. The result's
optional artifact digest describes generated runtime artifacts. The journal
retains the OCI image digest separately. [Preparation API](https://developers.cloudflare.com/api/resources/containers/subresources/images/methods/prepare/),
[Wrangler deployment helper](https://github.com/cloudflare/workers-sdk/blob/main/packages/deploy-helpers/src/deploy/helpers/durable-object-container-applications.ts),
[Wrangler image build](https://github.com/cloudflare/workers-sdk/blob/main/packages/wrangler/src/deployment-bundle/build-container-images.ts).

1. Commit an owned intent with `creation_sent` and the signed OCI digest
   before the copy. Copy only to the intended repository and release tag.
2. For up to 180 seconds after the copy, poll exact repository inventory and
   the tag's digest at five-second intervals. An absent tag or an OCI read
   that cannot confirm a listed tag is still settling. Unexpected tags or a
   different digest violate the ownership fence.
3. Once the exact digest is visible, send the idempotent preparation POST.
   Continue on pending and accept ready for that exact digest-pinned image.
   Terminal error or unexpected shape holds admission closed. Evidence records
   the observed tag state, HTTP status, preparation enum, JSON shape, and
   presence of known fields. It excludes provider messages, unknown keys,
   image URLs, and credentials.
4. The PlatformJob resource key excludes concurrent lifecycle jobs for the
   same installation while the attempt owns its lease. The registry calls and
   sleeps run outside database transactions. Task cancellation interrupts the
   wait and the OCI wrapper terminates an in-flight subprocess. Recheck the
   lease and journal before storing a receipt or proceeding to deployment.
5. An exhausted wait produces a retryable `CF_DEPLOYMENT_UNVERIFIED` with
   the last sanitized observation. Enable already has three attempts and
   delays of 15 and 45 seconds. Requeue clears the old lease and preserves
   the failure details. Each retry still needs an admitted
   PlatformJob executor.

## Disconnect and Claim Recovery

An image copy can leave a `creating` row with a sent effect and no provider
identity. Disconnect reads that exact tag and compares the digest to the
journal before committing the receipt and moving to `deleting`. Deletion
verifies the digest again. If the tag is absent, the existing settling
window must confirm absence before retirement. A different digest blocks
cleanup. The provider's tag deletion is a separate operation from readback,
so the runbook's ownership fence and pause on concurrent edits to trial
targets still apply. [Registry implementation](https://github.com/cloudflare/workers-sdk/blob/main/packages/containers-shared/src/registry-images.ts).

Workers scripts already observe and record both receipt fields before
deleting. Container applications bind the exact application and launcher
namespace first. Managed Tunnel cleanup binds its tunnel and DNS receipts
before their respective deletion transitions. The existing observation
constraint stays in force.

The fourth attempt had zero active PlatformJob resource-key holders while
Disconnect was queued. Its sole static launcher was lost after a worker
control WebSocket close with code 1008. Recreating only that worker restored
claim capacity, and Disconnect reached its first terminal result on attempt
one. This explains the queue delay separately from the image-journal
constraint violation after claim. The receipt does not establish the cause
of the WebSocket close. The worker availability requirement stays in force.
