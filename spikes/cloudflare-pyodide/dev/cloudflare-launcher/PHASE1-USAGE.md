# Phase 1 Hosted Usage Qualification

**2026-11-20 UTC, Phase 1b baseline `ecf05ec77`**

**Verdict.** GraphQL Analytics fell short of the usage writer requirements.
The operator `cf` session independently verified Workers Paid. Account Settings
Read alone verified the Containers entitlement.

## Trial

The existing `cf` session selected the trial account. Its read-only preflight
confirmed an active Workers Paid period, from 2026-10-30 00:00 UTC inclusive
through 2026-11-30 00:00 UTC exclusive, and an enabled $25 Workers spend
notification. The notification is an alert. The trial bounded spend separately.

- Envelope of 38 minutes starting 09:36:54 UTC, one trial-prefixed Worker, one
  SQLite Durable Object namespace, one object, no Containers and no Bifrost
  stack. Workers Observability and preview URLs were disabled.
- At 09:37:27 to 09:37:36 UTC, 96 sequential authenticated POSTs returned HTTP 200
  with the expected SQLite round-trip value. Each request ran 250,000 integer
  hash iterations and made one Durable Object call. The Worker CPU cap was
  10 ms. Fixed iterations held the work constant while measured CPU varied
  across invocations. The fixture performed one KV write and read per request,
  plus a single 64 KiB write on the first request.
- The Worker accepted the synthetic bearer for three minutes at most. The run
  disabled the endpoint at 09:37:37 UTC, immediately after load. The fixture
  used no alarm, external service, LLM, platform secret or production canary.
- Management used a temporary account-owned token with **Account Settings Read**
  and **Workers Scripts Write** on the selected account. GraphQL data queries
  used account-owned tokens with **Account Analytics Read** alone on the same
  account. A separate **Account Settings Read** token isolated the entitlement
  and permission checks. Token administration used `cf 1.0.0-beta.31`. Each
  token had an expiry at or before the trial deadline.

## Item 1. Attributable usage

GraphQL endpoint `POST /client/v4/graphql`. All data queries pinned
`viewer.accounts(filter: {accountTag: ...})`, used a half-open UTC window, and
filtered for identities recorded by the trial. The schedule sampled about 1 second
after the load, then at 1, 2, 5, 8, 12, 15 and 20 minutes. The Worker and DO invocation
queries kept the load window fixed. Periodic and storage queries extended the
end to the time of the sample so they could see later reports. A supplemental
analytics-only query measured the additive Worker CPU field and tested the
storage dataset's namespace-list membership filter. It generated no load.
[Workers metrics](https://developers.cloudflare.com/analytics/graphql-api/tutorials/querying-workers-metrics/)
[Durable Object metrics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/)

| Dataset | Identity and fields | Hosted outcome |
| --- | --- | --- |
| `workersInvocationsAdaptive` | `scriptName`, `sum { requests cpuTimeUs wallTime duration requestDuration }`, `avg { sampleInterval }` | Exact script dimension. The fixed load-window query estimated 96 requests, 71,952 µs of CPU, raw sums of 6,998,742 for `wallTime`, 6,993,714 for `requestDuration`, and 0.87484275 for `duration`. `avg.sampleInterval` was 3.84. |
| `durableObjectsInvocationsAdaptiveGroups` | `namespaceId`, `sum { requests wallTime }`, `avg { sampleInterval }` | Exact namespace dimension. The load-window query reported 1 request at 15 minutes and 21 at 20 minutes, of 96 verified calls. Final raw `wallTime` was 1,316,232 and `avg.sampleInterval` was 1.91. |
| `durableObjectsPeriodicGroups` | `namespaceId`, `sum { cpuTime activeTime duration rowsRead rowsWritten storageReadUnits storageWriteUnits }` | Exact namespace dimension. First row at 20 minutes. Raw `activeTime` was 5,273,979, `cpuTime` was 25,642 and `duration` was 0.675069312. It reported 96 rows read and 97 written, matching the fixture. The trial did not establish invoiced duration units. |
| `durableObjectsStorageGroups` | `namespaceIds_has`, `max { storedBytes }`, `dimensions { namespaceIds }` | The schema exposes namespace-list membership and a maximum stored-byte value. The membership query returned zero rows through 20 minutes despite the 64 KiB write. Neither a namespace-exclusive storage value nor a GB-month total was observed. |

The first Worker data appeared between the approximately 1-second and 60-second
samples. The earliest namespace-attributed invocation row appeared between 12
and 15 minutes. It reported one request at a sample interval of 1 after the
96 confirmed DO round trips. A wider exact-namespace and script-name control
at about 10 minutes had also returned empty results.

**Billing-period query.** Account dataset settings reported a 32-day maximum
range, 90-day retention and 10,000 rows per query for all four datasets. The
requested period to date fit within that range. The period query reproduced 96
Worker requests and 71,952 microseconds of Worker CPU. It returned 20 estimated DO requests versus 21 in the fixed load window,
and no DO periodic or storage rows even though the short-window periodic
query had data. The period can be queried, but the trial could not reconcile
complete request, duration and storage totals. Worker totals held at each
nonempty sample apart from floating-point serialization. The random nonexistent
script and namespace controls returned empty arrays.
[Dataset settings](https://developers.cloudflare.com/analytics/graphql-api/features/discovery/settings/)

**Grants and errors.** Account Analytics Read alone was sufficient for data
reads. The management credential also received a successful settings response.
A data query with only Account Settings Read returned HTTP 200 with
`authorization denied` and no data. The deliberate unknown-field query
returned HTTP 200 with an `errors` entry and no data. The response exposed
`ratelimit` and `ratelimit-policy` headers. The trial stayed below the shared
quota and observed no HTTP 429.
Cloudflare documents 300 queries per five minutes by default and HTTP 429 for
quota or query-resource limits. A reader must reject GraphQL errors even when
HTTP succeeds, preserve evidence age through transient failures and respect
backoff without writing replacement zeroes.
[Analytics API token](https://developers.cloudflare.com/analytics/graphql-api/getting-started/authentication/api-token-auth/)
[Error responses](https://developers.cloudflare.com/analytics/graphql-api/errors/)
[Query limits](https://developers.cloudflare.com/analytics/graphql-api/limits/)

**Disposition.** GraphQL Analytics did not qualify for the usage writer. The guard
requires a complete, attributable observation no more than 15 minutes old.
Admission therefore stays in
**Spend Data Stale**. Ask Cloudflare for a supported scoped metering source
with per-resource coverage and a usable completeness watermark, then qualify
it against the same guard. The restricted billable-usage API documents daily
account-level records. Even with access, its attribution and timing require
separate evidence. [Billable usage API](https://developers.cloudflare.com/api/resources/billing/subresources/usage/methods/get_account_usage_v2/)

## Item 2. Workers Paid and Containers entitlement

The independent Workers Paid preflight succeeded through the existing
operator session. Bind the command to the intended account and read it from a
directory without a project config.

```sh
CLOUDFLARE_ACCOUNT_ID='<account-id>' cf accounts subscriptions get -q
```

Require exactly one matching `rate_plan.id == "workers_paid"`,
`state == "Paid"`, and `current_period_start <= now < current_period_end`.
The subscription's timestamps give the observed billing period for Item 1.
Cloudflare documents **Billing Read** for the REST equivalent,
`GET /client/v4/accounts/{account_id}/subscriptions`. Phase 0 (`2dadd5f0e`) observed
HTTP 403 with the provisioning token. This run reproduced HTTP 403, code 10000, with
an Account Settings Read-only token. It resolved the account-scoped Billing
Read permission group, but account-owned token creation with that grant alone
returned HTTP 400 and the token list showed no matching creation. That leaves
the operator `cf` read verified and the isolated Billing Read product
credential unqualified. Enable still needs an independently authorized
authority that can provide the same current-account evidence.
[Account subscriptions API](https://developers.cloudflare.com/api/resources/accounts/subresources/subscriptions/methods/get/)

`GET /client/v4/accounts/{account_id}/entitlements` returned HTTP 200 with
active boolean-true `containers.enabled` and `workers.enabled` allocations
under a temporary token with **Account Settings Read alone**. For a Containers
feature check, require `containers.enabled`, a boolean-true allocation and an
empty deletion date. Cloudflare lists **Account Read** for this endpoint.
`workers.enabled` establishes feature access. Use the subscription read for
plan identity and period evidence.
[Account entitlements API](https://developers.cloudflare.com/api/resources/accounts/subresources/entitlements/methods/list/)

## Spend and teardown

Estimated incremental spend was under **$0.03**. The estimate used one object
at 128 MB across the whole 38-minute envelope, at most 960 Worker CPU-ms from
96 requests at the configured limit, 96 DO calls and a small SQLite footprint.
It used published usage rates with margin and excluded the existing subscription
fee. The invoice was unavailable at teardown.
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)
[Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)

Scoped REST verified Worker and namespace absence at 09:57:40 UTC. The
independent `cf` session had found both identities during the trial. At
09:57:42 UTC it found zero Worker search matches and no namespace under the
exact script or namespace ID. All five issued account tokens returned absence
on exact-ID readback after deletion and were absent from the independent token
list. The last token readback was 09:57:48 UTC. The rejected Billing Read token
also had zero matches. The trial created no other hosted resources.
