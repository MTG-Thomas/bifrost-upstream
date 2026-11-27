# Managed Container guest initialization

Hosted attempt 6 reached signed hello and guest initialization on the first
session. The API received a signed `Exception/unexpected` fatal at
`guest-init`, the relay closed with 1011, and Cloudflare reported exit 1. The
canary settled after 37.612 seconds. Disconnect completed on its first attempt
and verified absence. Qualification timed out waiting for ACK.

The failure is narrowed to the child readiness wait, concurrent startup
control processing, or the post-readiness memory sample before inventory.
The exception category in the receipt cannot distinguish those paths. The
published image's offline fault rehearsal reached the injected post-hello
failure at `requirements`. That run did not reproduce a child startup failure.

| Boundary | Hosted evidence and local difference | rc06 evidence |
| --- | --- | --- |
| Entrypoint and supervisor privileges | The same baked entrypoint runs in both environments. Signed hello means the image init already spawned the child and became the restricted supervisor. | Supervisor UID/GID/groups, capability masks, NoNewPrivs, and PID-1 check. |
| Child identity and capabilities | Docker and the hosted runtime can supply different capability sets and procfs views. A nonzero inheritable mask already causes startup refusal. | Separate identity, prctl and capability outcomes. The fixture gives the Docker child an actual inherited capability and checks its refusal. |
| Interception CA | The supervisor completed control TLS. The workflow UID may have different file access from the supervisor UID. An unreadable CA would break the guest's HTTPS client when it tried to use it. | The child checks that it can open the required CA and reports existence, readability and size bucket. The fixture presents a root-owned mode-0600 file to the child probe while the supervisor uses the real local intercept CA. |
| Scratch and shared memory | The baked scratch directory is writable by the workflow UID in the release image. Runtime mounts can change access. The readiness protocol itself uses pipes. | Report scratch access and the shared-memory size bucket. The scratch fixture removes write permission. Shared memory is diagnostic because configuring the spawn method does not allocate it. |
| Runtime preload | A thrown dependency preload exception previously could send a pickled error to a one-byte reader. Other preload exceptions could close the pipe without a report. Neither outcome identified the failing phase. | One-byte outcomes distinguish runtime import, settings construction and hook installation. No traceback or exception text crosses the pipe. |
| Resources and process death | The offline launcher runs under Docker with the fixture's standard-1 memory limit. The hosted kernel, CPU scheduling and cgroup view can differ. The provider's exit code 1 does not identify an OOM or a signal in the child. | Record the memory limit bucket, CPU count, kernel major/minor and separate child exit code or signal if observed. Missing facts remain unknown. |
| Control and SDK egress | The local provider uses Workerd and Docker plus an isolated denial boundary. The hosted path uses the tunnel, the actual interception CA, and Cloudflare's DNS and socket policy. The attempt did not reach the SDK canary witness. | The existing signed control and SDK stages remain in place. Pipe outcomes distinguish readiness timeout, EOF, malformed frame, pipe I/O, child exit and child signal. |

Facts are a bounded, validated record containing fixed field names and scalar
values only. The API verifies the existing signed owner, session generation
and sequence before retaining them in the reservation journal and qualification
evidence. A fatal may carry supervisor facts alone if the child died before
reporting. This uses the existing event and storage shape. Capacity protocol 2,
SDK protocol 1 and the database schema are unchanged.

The local runner accepts `--containers-hosted-shape --exercise-guest-init-fault
<mode>` for identity, prctl, capabilities, ca-unreadable, scratch-unwritable,
runtime-import, settings, hook, ready-exit and ready-signal.
It checks signed evidence in both the job and Capacity status, confirms the
canary remains unqualified, then verifies Disconnect. The hook is mounted
read-only onto the selected local image. These switches are rejected with
hosted configuration.

Use a short private archive path for these fault modes. The Workerd proxy
needs its Unix socket pathname to fit the platform limit. If preparing a
reduced archive for the image-preparation journey, include
`preflight-compose-worker.sh` alongside the runner's other inputs.
