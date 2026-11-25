# rc05 Container Startup and Recovery

The published rc04 Linux image completed the offline hosted-shaped trial.
The hosted canary established generation 1 and then exited with code 1
before an SDK witness or qualification ACK. The offline result narrows the
investigation to hosted behavior or timing. The exit cause remains open.
The missing failed-placement settlement was a separate defect.

## Hosted Path Review

| Path | Hosted and offline inputs | Could explain exit 1 after hello? |
| --- | --- | --- |
| Launch and identity | The signed registry passes a digest-pinned image, an incarnation control key, pinned ingress, timing bounds, and an optional probe nonce to `ctx.container.start`. Miniflare maps that same start to the inspected Docker image and keeps the product environment. The supervisor removes the control key, rejects privileged variables, and gives the workflow child an explicit environment and distinct UID. | The accepted signed hello establishes that the identity, key, and first API WebSocket handshake worked. A guest process can still exit before its readiness pipe is read. `guest-init` separates that failure from bootstrap. |
| Entrypoint and requirements | Both run the image entrypoint as PID 1, split UIDs and capabilities, then validate the readiness pipe. The managed bootstrap gets explicit null requirements, because the image contains baked wheels. Neither path asks the guest to install packages or reach Redis. Docker uses local kernel and filesystem semantics. Cloudflare supplies the hosted runtime, writable layers and process limits. | Yes. An error in the requirements subprocess, readiness pipe or startup frame path sets the launcher failure flag. Track `requirements` and `guest-init`. Failures before hello cannot send a session diagnostic. |
| Welcome, lease, inventory and close | The same API state machine negotiates a generation and grants the relative runtime lease. Miniflare uses the local WebSocket relay into worker ingress. Hosted adds Cloudflare interception, edge forwarding and tunnel transport, with different latency and scheduling. | Yes. A frame or transport error during startup can fail initialization. `bootstrap`, `runtime-grant` and `inventory` separate those steps. Relay close forwarding previously flattened every close to 1000. It now preserves transferable codes and substitutes fixed text. A transport loss that has no sendable close code is categorized separately. |
| HTTPS CA and SDK witness | The guest uses the runtime intercept CA path for both the control socket and SDK. Cloudflare creates the ephemeral CA when interception is registered before start. Miniflare supplies its own intercept CA at the same path. The upstream hosted hop uses the public ingress origin and real edge/tunnel trust. The offline stub probes a private TLS endpoint and local ingress. [Cloudflare outbound interception](https://developers.cloudflare.com/containers/configuration/outbound-traffic/) | Initial CA and hostname validation had already worked at hello. SDK TLS, route forwarding, authorization or the expected HTTP 204 marker can still fail. The SDK command reports an error reply, so an SDK failure alone does not establish a process exit. `sdk-tls` and `sdk-witness` identify the attempt, including allowlisted TLS and DNS exception classes. |
| DNS, raw TCP and denial proof | Hosted starts with internet disabled and registers only the exact HTTPS origin. The API resolves and reaches the alternate peer on 2053 and 8443 before the guest challenges its IP. Locally the API uses an isolated Docker peer and the harness fences raw TCP before forwarding hello. Miniflare exercises the real HTTP denial and redirect relay. [Cloudflare outbound and DNS behavior](https://developers.cloudflare.com/containers/configuration/outbound-traffic/#non-http-traffic) | An unreachable alternate peer can prevent command dispatch. A denied SDK route or raw TCP mismatch can prevent qualification. These errors do not by themselves explain process exit 1. `canary-command` shows the guest waited for a command, while `egress-probes` proves it reached the denial checks after the SDK witness. Hosted DNS filtering and port reachability still need a live witness. |
| Qualification and signature | Both paths require matching current-generation SDK witness, denial set and ACK under the Enable lease. Local release material uses a synthetic trust verifier backed by the actual journal and image digest. The published rc04 release signature and source digest were verified separately. | A failed proof closes enable admission through the existing verdict. It does not force the guest to exit before an ACK. `ack` is set before emitting the reply. It is progress information and cannot grant readiness. |
| Instance resources | Hosted pins standard-1 with 0.5 vCPU, 4 GiB RAM and 8 GB disk. Docker reports 2.12 GB of image data for the published rc04 guest. Startup forks one guest, imports the baked runtime and runs the requirements helper with no runtime package writes. Miniflare runs the same image with Docker on a different kernel and scheduling budget. [Cloudflare instance limits](https://developers.cloudflare.com/containers/platform/limits/) | The image size leaves nominal disk room. It does not measure the hosted writable layer, memory peak, or I/O latency. CPU or I/O delay can expose startup/lease timing. The recorded exit 1 does not identify resource pressure, and the hard runtime alarm uses exit 124. |

## Diagnostic and Recovery Evidence

The existing managed control stream carries optional `guest_stage` and
`guest_fatal` events. Each uses the incarnation control key and binds the
generation, sequence and allowlisted payload. The launcher attempts one fatal
report with a half-second send bound. Older receivers can ignore the events.
Capacity protocol 2 and SDK protocol 1 stay unchanged. The API keeps the last
stage, fatal class/category, and close code/category in the reservation
envelope. Capacity and the qualification job expose the same sanitized
evidence. There is no traceback, exception text, peer close text, or control
key in the record. A hard kill, pre-hello failure, or failed transport may
leave only the last stage and provider terminal metadata.

The local failure rehearsal layers
`spikes/cloudflare-pyodide/dev/e2e/combined-guest-site/sitecustomize.py` at
`/app/sitecustomize.py` in its local guest image. Use a fresh `combined-local-*`
archive with mode `0700` and stage the hook under `api/` in that archive
before building. The root Docker build context excludes `spikes/`.
The hook runs the real guest through
signed hello and bootstrap, then injects a requirements failure. Use
`--containers-hosted-shape --exercise-guest-fatal` on the isolated local stack.
The runner checks the failure in Capacity and the job, settlement, and
Disconnect. The hook is excluded from all release image recipes.

The terminal-state repair keeps the original two witnesses. The retained rc04
installation can retry product Disconnect against the existing provider actor
and registry fence, once a separately authorized upgraded stack is available.
The new evidence is optional JSON in the reservation envelope and requires no
migration.
