# Preset B spend calibration

The [hosted receipt](cloudflare-scaleout-preset-b-hosted.json) records
$0.061801363 of final Bifrost estimate above the opening balance.
The attributed Workers sample priced at $0.000060214, giving the
reported 1,026× ratio. That denominator covers only Worker requests
and CPU. It has no Durable Object rows or Dynamic Worker creation data.
The receipt retains launcher lifetimes and seal status, but not the
raw per-counter totals, complete route bookings, or Loader identity
keys. An exact invoice or exact per-component ledger split cannot
be recovered from it.

## What went into the Bifrost estimate

The four metered lifetimes sum to 331.105820 s. The preset pins a
$0.000072778 per-second allowance with zero separate root rate. Applied
to those lifetimes, this is **$0.024097219**. The formula below breaks
out that exact subtotal. The four fixed control and acknowledgement
allowances add **$0.000175800**. The remaining **$0.037528344** is a
combined residual for foreground pricing, dynamic identities, actual
launcher counters, slot-duration charges, and storage residue. The estimates below describe
parts of that residual and should not be summed as a reconciliation.
[Ledger pricing formulas](../../../api/src/services/worker_pool/cloudflare_spend.py)

| Component | Recoverable Bifrost amount or formula | Measured or reserved |
| --- | --- | --- |
| Dynamic Worker creation | $0.002 per booked daily identity. The nine simultaneous slots plus the fourth-tenant tenth imply at least ten distinct Loader identities. A scenario with 10–12 identity-days costs $0.020–$0.024. | Daily forecast and generation bookings deduplicate normal reuse and credit the matching forecast. The scenario assumes one generation per distinct slot identity and no extra rotation. This run's exact identity count is unavailable. |
| DO requests | $0.000149 of launcher cadence allowance. Fixed lifecycle and per-dispatch allowances also contain DO requests. | Actual launcher-local counters come from the four sealed receipts. The receipt export has no counter totals. Other control and dispatch counts remain modeled. |
| DO duration | $0.001589 for three 128 MB objects reserved for the full launcher lifetimes. Separately, 13 × 201 s of full workflow timeout-plus-linger pricing would be $0.004181. | The launcher clock is measured. Three resident objects, occupied-slot time, and booked TenantHost timeout and linger are duration proxies. Cloudflare billing depends on actual non-hibernating time. |
| SQLite rows and stored bytes | $0.003353 of recurring row allowances and $0.006622 of recurring storage allowance. Actual launcher writes, fixed control writes and time-prorated residue are separate. | Receipt-driven writes settle. Cadence and per-dispatch row budgets, storage multipliers, and the per-second storage allowance remain modeled. |
| Workers requests and CPU | Provider Analytics measured 196 requests and 70.698 CPU ms for the attributed Workers sample. Gross cost is $0.000060214. Bifrost also prices capacity request counts and qualified per-run gateway, startup and engine bookings. | Measured front-door counts and receipt-based credits coexist with qualified CPU estimates. The engine's workflow wall time is a conservative CPU proxy, especially for sleep-heavy fixtures. |
| Logs | $0.000795 of recurring log-event allowance plus $0.011589 of recurring ingestion and retention allowance. The Bifrost upload disables Workers observability and Logpush for the deployed scripts. | A sealed launcher log counter is internal evidence, not Cloudflare ingestion. The recurring byte and event allowances are fixed proxies. |
| Fixed allowances | $0.000175800 for four launcher setup, retirement and ACK bundles. For scale, 13 full foreground control allowances at $0.000062228 are another $0.000809, before receipt credits. | Released where supported. Capacity planning's full fleet fixed allowance is used in the policy envelope and is not an extra lump-sum invoice charge in this trial ledger. |
| Unreleased reserves | All four launcher meters sealed. No unresolved incarnation or route flags remained at final readback. | A seal replaces the full launcher counter ceiling, which is why the live $2.02 fell sharply. Foreground credits are narrower: a stopped or untrusted route can retain its booked CPU and requests even after retirement. The export cannot isolate that retained amount or establish the earlier transient route flags. |

About 39% of the final estimate is explained by the recurring
and fixed launcher allowances alone. Recurring log-byte plus SQLite
storage allowances account for about $0.01821 of it. The remaining
gap includes per-day engine charges, retained duration and foreground
proxies. A 1,026× comparison against a front-door Workers sample
therefore mixes different billing dimensions.

## Published rates and a realistic bill

Cloudflare Workers Paid includes 10 million monthly Worker requests
and 30 million CPU milliseconds, with gross overage of $0.30 and
$0.02 per million respectively. Dynamic Workers share those two
allocations and include 1,000 dynamic Worker creations per month,
with a $0.002 charge per additional daily identity. Reusing a Loader
identity during the day does not add a separate creation charge.
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/) · [Dynamic Workers pricing](https://developers.cloudflare.com/dynamic-workers/pricing/)

Durable Objects Paid includes 1 million requests and 400,000 GB-s
per month. The gross overage rates are $0.15 per million requests and
$12.50 per million GB-s, with monthly billing-unit rounding. Duration
uses the allocated 128 MB class and depends on actual billable object
residency. SQLite includes 25 billion reads, 50 million writes and
5 GB-month. Overage is $0.001 per million reads, $1 per million writes
and $0.20 per GB-month. Byte-month pricing integrates stored data over
time. Applying a byte-month allowance every launcher second is a
Bifrost backstop, not a provider meter.
[Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)

At the trial date, Workers Logs Paid included 20 million events per
month and charged $0.60 per additional million. Cloudflare documents a
December 1, 2026 migration to Observability pricing. The new Paid
allocation includes 50 GB of ingestion and 10 GB-month of storage per
billing cycle, then $0.25/GB and $0.10/GB-month. The model carries
both schedules as a conservative policy allowance. Only the original
event schedule applied at the time of this trial.
[Workers Logs pricing](https://developers.cloudflare.com/workers/observability/logs/workers-logs/) · [Observability pricing](https://developers.cloudflare.com/observability/pricing/)

The realistic **incremental invoice is $0 if the account has room in
the included allocations**. The monthly $5 subscription is fixed.
Allowance consumption and invoice rounding are account-wide and were
not observed for this run. For a gross, prorated comparison before
allowances and rounding, **roughly $0.02–$0.035** is a planning scenario,
not a billed amount. It combines the $0.020–$0.024 identity scenario,
the $0.000060214 measured front-door sample, and roughly $0.001–$0.011
for omitted isolated CPU and DO compute and storage at these rates.
The upper end accommodates as much as 3,600 object-seconds
($0.00576) and 250,000 additional CPU ms ($0.005), with small request
and SQLite allowances. Those are scenario inputs, not observations
from missing provider datasets. A trial crossing a monthly rounding
boundary can have a different invoice impact.
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/) · [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)

The ledger is about 1.8–3.1× that gross scenario. The missing
provider meters keep this from establishing the owner's 10× accuracy
criterion. A zero incremental invoice has no spend ratio.

## Recommendation

Keep the existing admission envelope, cleanup headroom, and treatment
of missing receipts. Display three separate figures: **live reserved
exposure**, **expected gross usage**, and **expected incremental
invoice** when remaining account allocations are known. Build the gross
figure from distinct daily Loader identities, attributable Worker CPU
and requests, billable DO duration, and measured storage/observability
usage. Mark unknown or delayed meters as provisional and show a range
rather than a precise total. Reconcile against a complete provider
window and test the 10× tolerance on gross attributable usage where the
denominator is nonzero. Verify expected zero-charge intervals separately
against the account's available allowances. The pricing code is unchanged.
