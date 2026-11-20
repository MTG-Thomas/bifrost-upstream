# Phase 0 Cloudflare Provisioning

**2026-11-20 · source base `e5f385a20`**

The trusted-runner spike uses Python and `httpx`, both already in the API image. Python can build multipart requests and reconcile API responses in the process that owns Bifrost's credentials. The Containers lane adds the pinned daemonless `crane` executable. The qualification uses `cf` for the existing account session, temporary token administration, a read-only subscription and spend check, and managed-registry tag inventory and deletion.

The [fixture manifest](phase0-artifacts.json) pins every uploaded JavaScript part and a single-platform public BusyBox image by SHA-256. The Workers trial exercises a provider, SQLite launcher and separate runtime Worker with representative bindings. The Containers trial reuses the existing SQLite `ScopeRegistry`. The compiled Pyodide release bundle, Linux Bifrost release image, release signatures and product runner packaging require their own qualification.

## Envelope and tooling

- The run declared a 30-minute window and an estimated $0.10 ceiling. Each Containers trial was limited to a single `lite` instance and each Workers trial used one launcher. The Containers fixture has one physical DO, a persistent one-shot start fence, a two-minute alarm and inactivity timeout, a terminating process and Internet egress disabled. The account's existing $25 Workers notification was enabled. The trial controlled Container spend through size, count and lifetime. [Container limits](https://developers.cloudflare.com/containers/platform/limits/) · [Pricing](https://developers.cloudflare.com/containers/platform/pricing/)
- Python `3.13.17`, `httpx 0.28.1`, Node `26.13.1`, `cf 1.0.0-beta.31` and `crane v0.20.6`. Crane came from the version-pinned Go module. No Wrangler, `cf deploy`, Docker build, Bifrost stack, production canary, platform credential or LLM call was used.
- The Worker token held **Account Settings Read** and **Workers Scripts Write** on the selected account. The Containers token added **Workers Containers Write**. The account's existing `cf` session minted each account-owned trial token for 30 minutes and independently verified deletion by name. No billing write, DNS, tunnel or account policy mutation was required.

## Operation verdicts

| Operation | Path | Hosted evidence |
| --- | --- | --- |
| Account identity and feature entitlements | REST | The scope token returned HTTP 200 for the exact account and `/entitlements`. Active boolean allocations were present for `workers.enabled` and `containers.enabled`. |
| Workers Paid and spend policy | cf-fallback | Scoped REST `/subscriptions` returned HTTP 403. Cloudflare documents **Billing Read** for this endpoint. The account-owned permission group was visible, but issuing a separate Billing Read-only token through the existing session returned exit 1, and list readback found no matching token. The existing session read `rate_plan.id=workers_paid`, `state=Paid` and current period dates via `accounts subscriptions get`. `billing spend-policies list` confirmed the enabled notification. [Subscriptions API](https://developers.cloudflare.com/api/resources/accounts/subresources/subscriptions/methods/get/) |
| Module upload, SQLite exports and private bearer | REST | Multipart `PUT` created three module Workers. Declarative `exports` resolved SQLite namespaces. The provider returned 401 without its synthetic bearer and 200 with it. The authenticated call reached the launcher over the cross-script DO binding, reached the runtime over a service binding and round-tripped the receipt through SQLite. |
| Lost response and ownership conflict | REST | The runner discarded the runtime upload response, reconciled the exact name and owner marker and issued one PUT total. A second create for the occupied name was refused before upload. The distinct foreign marker is covered by a local unit test. |
| Registry credentials | REST | Short-lived push/pull credentials read back the exact account and managed registry host. The runner confined the credential file to its temporary directory. |
| Image copy and preparation | OCI and REST | `crane copy` transferred the pinned Linux/amd64 manifest. The destination tag resolved to the same SHA-256. The idempotent `POST /containers/image-preparations` reached `ready`. |
| Registry tag listing and deletion | cf-fallback | `crane ls` failed. Scoped `cf containers images list` saw the exact repository. `cf containers images delete` removed its digest-checked trial tag and a fresh list returned no matching repository. |
| DO-managed application | REST | Explicit `POST /containers/applications` accepted the SQLite namespace and `durable_object` scheduling. The provider's application ID was a 32-character opaque hex string. Using it verbatim resolved detail and instance reads. |
| Instance start, cap and stop | DO API and REST | The authenticated provider returned 202 after `ctx.container.start()` selected the prepared image and `lite` size. `GET …/instances-v2` reported one running instance. The SQLite ScopeRegistry returned 409 for a distinct second reservation and the control plane still reported one running instance. Stop returned HTTP 200 and the instance readback reached zero live instances before app deletion. |
| Teardown | REST and cf-fallback | The successful Worker and Container trials independently verified absence of scripts, namespaces, the application, registry tag and scoped API tokens. A prior Container attempt left a disabled Worker, app and tag when the malformed app ID prevented cleanup. Recovery matched the deployed entry bytes, ownership marker, namespace association and pinned digest, verified zero live instances, then removed and read back every residual resource. The local temporary credential directory was removed. The registry JWT expires after its 15-minute lifetime. |

The first Container upload sent `containers[].images` as an array and failed with HTTP 400, code 10021. The REST handler reported that the field takes a string map. The fixed request sends the named digest as a map and attaches the named Container to the SQLite export. A later attempt reformatted the application ID as a hyphenated UUID and got HTTP 404, code 1609. Exact-ID readback and cleanup succeeded after preserving the opaque identifier. The tests protect that spelling.

The managed registry's list and delete commands require their JSON output. The pinned `cf` build emits an empty stream for this custom command under `-q`. The runner uses regular output and parses JSON. A failed or ambiguous write never switches adapters.

Estimated aggregate incremental spend was below **$0.01**. The trial observed one running `lite` instance with a two-minute lifetime ceiling and a small number of Worker and DO requests. The invoice was unavailable at teardown. Cloudflare exposes registry tag absence and instance status. Provider-internal image preparation cache eviction and underlying blob garbage collection have no verified readback in this trial. [Cloudflare pricing](https://developers.cloudflare.com/containers/platform/pricing/) · [Image preparation REST API](https://developers.cloudflare.com/api/resources/containers/subresources/images/methods/prepare/)

The existing `cf` session independently read back zero trial Workers, Durable Object namespaces, Container applications, registry entries and account tokens.

## Phase 1 recommendation

Use the REST Worker adapter. Qualify the exact packaged launcher and runtime artifacts before enablement. Verify account identity and feature allocations with Account Settings Read, and use an independently authorized read-only Workers Paid check until a scoped subscription read is proven. Keep runtime authentication, the spend gate and durable cleanup inventory in the trusted Bifrost runner.

Keep Containers independently gated for Phase 3. Reuse the qualified REST application and image preparation operations, the daemonless OCI copy, and the pinned `cf` adapter for registry tag listing and deletion. Re-run against the signed release image and full provider lifecycle, including concurrent reserve and controller loss, before opening Containers admission.

## Reproduce

Use the selected trial account ID explicitly and run from the repository root. The default preflight only reads account state. An apply creates a fresh random prefix. It keeps a local intent receipt if the run fails so recovery can use exact IDs and ownership evidence.

```sh
root=spikes/cloudflare-pyodide/dev/cloudflare-launcher
python "$root/phase0.py" --account "$TRIAL_ACCOUNT"
python "$root/phase0.py" --account "$TRIAL_ACCOUNT" --scope workers --apply
python "$root/phase0.py" --account "$TRIAL_ACCOUNT" --scope containers --crane "$PINNED_CRANE" --apply
python -m unittest discover -s "$root" -p test_phase0.py -v
python -m ruff check "$root/phase0.py" "$root/phase0_adapter.py" "$root/test_phase0.py"
node --test "$root/capacity.test.mjs"
```

Focused validation passed seven Python unit tests, Ruff and four existing Node capacity tests. The Docker-backed Bifrost suites were outside this fixture run.
