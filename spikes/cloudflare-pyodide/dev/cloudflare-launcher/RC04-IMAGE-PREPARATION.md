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
   A transient copy error requeues the job. The next attempt rechecks the
   exact destination before it can copy again.
2. For up to 180 seconds after the copy, poll exact repository inventory and
   the tag's digest at five-second intervals. An absent tag in inventory
   is still settling. Transient and unclassified inventory or digest
   errors also settle within this window. When inventory lists the exact
   tag, an image-not-found digest response counts as registry lag. An
   image-not-found copy error with an empty destination remains terminal.
   Explicit authentication and tool startup failures stop immediately.
   Failure evidence records the last sanitized class and the failure
   count for each affected operation.
   Unexpected tags or a different digest violate the ownership fence.
   The source manifest and config are verified before copy. Destination
   readback uses the registry-reported digest because that digest pins the
   verified OCI manifest and its config and layer descriptors.
3. Once the exact digest is visible, send the idempotent preparation POST.
   Continue on pending and accept ready for that exact digest-pinned image.
   Terminal error or unexpected shape holds admission closed. Evidence records
   the observed tag state, HTTP status, preparation enum, JSON shape, and
   presence of known fields. It excludes provider messages, unknown keys,
   image URLs, and credentials.
4. The PlatformJob resource key excludes concurrent lifecycle jobs for the
   same installation while the attempt owns its lease. Check the lease
   before touching the registry, immediately before the copy, during the
   copy, and on every poll. The first two checks bracket the `crane version`
   check, signed source manifest and config reads, the first `cf` inventory
   read (including CLI version and account checks), and the credential
   mint. Each lease check uses a short
   transaction. The registry calls and sleeps run independently of those
   transactions. Lease loss cancels an in-flight copy and stops further
   provider work. A superseded job with a valid lease continues preparing
   within the same window. If the image becomes ready, it commits the exact
   receipt and exits before launching the deployment. If the wait expires
   or a retryable error ends the attempt, it reports a retryable failure
   and leaves the receipt at `creating` for unconfirmed-create cleanup.
   Confirmed terminal errors still require operator verification and
   leave the receipt available for that same cleanup.
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
Disconnect was queued. Its sole static worker was marked lost, and reconnects
received WebSocket close 1008. Recreating the worker gave it a new incarnation
and restored claim capacity.

The API-side executor writes a database heartbeat every five seconds on a
dedicated liveness connection, with a five-second bound per write. The
dispatch reconciler sweeps every 15 seconds when idle and can wake sooner. It
requires 30 seconds of steady database reachability before marking a static
executor lost, and then checks whether PostgreSQL has gone more than 30
seconds without that executor's heartbeat. The remote owner lease also
depends on fresh launcher telemetry and has a 20-second lifetime.

Enable's registry preparation runs in a launcher child. Its HTTP and OCI
operations do not run on the API event loop, and the preparation journal
does not lock the executor row. The retained receipt does not include
heartbeat timestamps, control telemetry, or renewal errors from the loss
window. It establishes the lost state and the reconnect refusal. The
initiating interruption and its timing relative to preparation remain unknown.

The old hello path found the existing incarnation, skipped registration,
and refused ownership because its row was lost. The launcher treated the
generic close as a transport error and kept presenting the same identity.
The host now sends an explicit loss close after authenticating a static
hello and matching its recorded launcher identity. A lost static row can
no longer renew its owner lease or authorize a new claim. The launcher
clears readiness and stops that incarnation's children before enrolling
under fresh incarnation and launcher IDs with empty inventory. A routine
reconnect still retains its identity and children. Managed workers keep
their existing provider-bound retirement paths. Disconnect still runs
through the ordinary fenced PlatformJob claim path on a ready executor.
An explicitly retired static row still receives a generic 1008 refusal.
Automatic re-enrollment applies to static rows the liveness sweep marked lost.
