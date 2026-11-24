# Scale Out with Cloudflare Spend Defaults

Decision brief for Gap 4 in [ONE-BUTTON-GAP.md](ONE-BUTTON-GAP.md) and the open spend defaults in [PHASE3-CONTAINERS.md](PHASE3-CONTAINERS.md). Evidence is pinned to spike commit `83783a42e`. All figures are USD at the repository's gross rates. The **$5/month Workers Paid subscription** is billed separately by Cloudflare and is excluded from the forecasts and the admission limit. Forecasts model a 30-day month. The ledger uses UTC calendar months and the fleet validation allows for 31 days.

The Containers pilot shares the $75 monthly limit with Workers. The
suggested 100 instance-hours allow two concurrent `standard-1` instances
within a monthly allocation. The ledger books startup, lifetime and cleanup
seconds for each reservation, including canaries, then settles from receipts.
The budget resets at the UTC month boundary. Exhausting it pauses Container
workflow placement and routine refresh only. Qualification canaries still
count toward instance-hours but may exceed the hours budget to admit the
joint revision under the shared dollar limit, backoff and seat caps.
The suggested qualification allowance is 30 Container canary reservations
per Cloudflare account per UTC month in each Bifrost database, about $0.15
from the pilot release. Separate Bifrost databases track their own allowance. Qualification
and refresh both debit it. Exhaustion leaves Workers eligible on a verified
revision and defers Container qualification until the allowance has room.
The signed release rates price the Container allocation for
the joint fleet check. Compute for 100 hours is about $7.40 before outbound
and control holds.

## Per-Workflow Cost Model

These units model approved overflow from a Provider organization or Global. Global is one tenant. Cold starts are normal when bursts are separated or the relevant launcher or engine has expired. Warm reuse requires the same live engine and tenant within its 90-second linger and the launcher's own lifetime. Keep-warm and queue pre-warm are both off. Count only workflows that reach the overflow lane in the forecast.

| Run | Planning cost | Assumptions and calculation |
| --- | ---: | --- |
| Cold Workers | **$0.007** ($0.004–$0.012) | One new engine identity at $0.002. The meter reference has about 7.3 s of boot CPU, 4.85 s of cold exercise/startup proxy and 0.21 s of workflow CPU. At $0.02 per million ms, their combined cost is about $0.00025. Add residence, launcher and gateway work, and retirement. The hosted run's **$0.007125** settled increment anchors the all-in planning value. The range is a sensitivity assumption for varying isolation, work and retirement, with the upper end comparable to the trial's live estimate before verified deletion. |
| Warm Workers | **$0.001** ($0.0005–$0.002) | Reuse an existing identity. Assume 15 s of execution wall time, about 0.21 s of CPU, two SDK requests, and allocate up to 90 s of tail to this run. A 128 MB DO costs $0.0000016/s at gross marginal rates. Allocating up to four DO-equivalents (launcher, ledger, registry, tenant host) over 105 s gives $0.000672, with shared time apportioned across jobs. CPU for the workflow is about $0.000004. Requests at $0.30/million Worker calls and $0.15/million DO calls are small at this scale. Round up to about $0.001 to allow for gateway CPU, control, SQLite and logs. Shared tails can lower the allocation. This is a planning model with no warm-only hosted settlement in these receipts. |
| Containers (`standard-1`) | **About $0.0063** for an isolated 150 s run, or **about $0.0034/run** in a burst of ten | Full provisioned CPU, memory and disk cost $0.00002056/instance-second (about $0.074/hour). An isolated run uses `30 s boot + 150 s work + 120 s idle + 5 s stop = 305 s`, or $0.006271 before egress and control. Ten serial runs on one instance use `30 + 10 × 150 + 120 + 5 = 1,655 s`, or $0.034027 per burst before egress and control. Add `$0.05 × outbound GB + control reserve` per start, then divide by workflows served. Maximum two instances across the installation, one per tenant, one workflow at a time per instance. The timings and achieved reuse are assumptions for the pilot. |

Workers inputs come from [`WORKERS_RATES` and identity/residence pricing](../../../../api/src/services/worker_pool/cloudflare_spend.py) and the [meter bounds and CPU reference](../../../../api/src/services/worker_pool/cloudflare_meter_bounds.json). The [cold workflow receipt](../../results/cloudflare-scaleout-combined-hosted-observable-gates.json) includes Enable, the workflow and cleanup. Its $0.012356 live reading included outstanding usage before retirement. The [$0.000202 connection-reuse receipt](../../results/cloudflare-scaleout-combined-hosted-connection-reuse.json) covered Enable and cleanup without a hosted workflow. These receipts provide scenario anchors. Attributed Cloudflare Analytics samples were empty, so provider billing and the warm/cold cost ratio cannot be inferred from them. See [PHASE1-USAGE.md](PHASE1-USAGE.md), [PER-TENANT-SCRIPTS.md](PER-TENANT-SCRIPTS.md) and [the Containers rate derivation](PHASE3-CONTAINERS.md#spend-enforcement-and-estimate).

## Monthly Usage Profiles

Workers duty below means **executing slot-seconds ÷ available physical slot-seconds**. It assumes 15 s wall time per workflow and describes activity before idle linger. Workers cost comes from cold and warm workflow counts, including the allowances above. Containers duty means **billed instance-seconds ÷ two always-live instances**, including boot, idle and stop. These percentages use different denominators and should carry their labels in the preview. Workload and cold fractions are scenarios for activation, with no production frequency inferred from trial receipts.

| Profile | Default forecast assumptions | Expected Bifrost-metered usage/month | Illustrative range |
| --- | --- | ---: | ---: |
| Light | Four events per month, 10 workflows each. 80% cold. `40 × 15 / (2 × 2,592,000)` ≈ **0.012%** Workers duty with two physical slots. | `32 × $0.007 + 8 × $0.001 =` **$0.23** | ~$0.13–$0.40 |
| Moderate | 40 workflows/day in clustered bursts, 35% cold. `1,200 × 15 / (9 × 2,592,000)` ≈ **0.077%** Workers duty with nine slots. | `420 × $0.007 + 780 × $0.001 =` **$3.72** | ~$2–$7 |
| Heavy | 1,000 workflows/day in sustained overflow windows, 40% cold to allow for tenant changes and launcher rotation. `30,000 × 15 / (9 × 2,592,000)` ≈ **1.93%** Workers duty. | `12,000 × $0.007 + 18,000 × $0.001 =` **$102** | ~$57–$180 |
| Containers-inclusive | Moderate Workers plus 10 Container starts/day, 10 serial workflows per start, `300 × 1,655 = 496,500` billed instance-seconds (~**9.6%** Containers duty). Assume 100 kB outbound per start and use $0.000001/start as an illustrative fixed control reserve from the local test fixture. | `$3.72 + 496,500 × $0.00002056 + 0.03 GB × $0.05 + 300 × $0.000001 =` **$13.93** | ~$7–$27 |

Workers ranges vary the unit costs at fixed workflow counts and cold share. The Containers-inclusive range also varies Container starts from half to double, with the same cost per burst. These are sensitivity ranges for complete settlement, with bootstrap, cleanup and live reservation variability left visible in the ledger. Orphaned resources, missing receipts, more expensive work and rapid re-creation can raise estimated usage sooner. The Containers design's always-live reference is `2 × 30 × 24 × 3,600 × $0.00002056 = $106.58` for compute alone. Production control cost and egress assumptions must come from the selected release and workload. [Container rates fixture](../../../../api/tests/helpers/cloudflare_containers.py)

At one refresh per hour for 31 days of eligible demand, with a modeled
45 seconds of billed residence per canary, Container refresh adds about
`744 × 45 × $0.00002056 = $0.69` in compute plus egress and control.
Reconcile checks on its normal five-minute schedule and backs off failed
canary starts for 5, 10, 20, 40, then 60 minutes. A continuously failing
lane settles at at most one start an hour after the initial failures, or
about $0.69 per 31 days of compute at the same 45-second assumption.
Reconcile starts a canary only if eligible work cannot fit on the primary
and a spare seat is available. The ledger holds the full bounded envelope
until independent stop evidence arrives.

## Owner Options

Each limit is a suggested USD amount per UTC month for the **shared Bifrost estimate**, including unsettled usage. When the limit is reached, Bifrost closes new Cloudflare admission and new work stays on or queues for the primary. The ledger can close admission earlier when it cannot verify usage or safely reserve the next start. Existing admitted work and cleanup follow their own deadlines and settlement rules. Fresh headroom can come from settlement, the UTC month rollover or an explicit policy change.

| Option | Suggested Limit and Forecast | Save-Time Fit | Availability and Spend Tradeoffs |
| --- | --- | --- | --- |
| A | **$30**, Light, **0.012%** Workers duty. Workers only. | One launcher × two slots, 3,600 s lifetime, nine installation slots configured and two physically reachable. **$17.61** envelope using the checked-in release inputs. | Two slots may leave a concentrated burst on the primary before spend is material. Cold churn and unsettled usage can close admission later in the month. $30 is still much larger than the light forecast, but limits the authorized Bifrost exposure compared with the nine-slot options. |
| B | **$75**, Moderate, **0.077%** Workers duty. Workers only. | Three launchers × four slots capped at nine total, **3,600 s** lifetime. **$32.13** modeled envelope with the same inputs. This preset requires the requalified Workers bundle. | A fresh launcher can admit workflows with the default 1,800 s timeout. The claim window narrows as the launcher ages. TenantHost linger remains 90 s, and idle launchers retire under the worker pool's idle policy. High demand or churn can exhaust $75 during a month. |
| C | **$425**, Heavy, **1.93%** Workers duty. Workers only. | Three launchers × four slots capped at nine total, **120 s** lifetime. **$415.59** qualified envelope. | Preserves the existing pilot geometry and lifetime. Short incarnations can reduce warm reuse, and $425 can still close admission if usage and reserves exceed the modeled workload. It authorizes hundreds of dollars of estimated usage even if expected spend is much lower. |
| D | **$75 conditional**, Containers-inclusive pilot with **100 Container instance-hours**. | Preset B Workers with up to two concurrent `standard-1` instances. The verified Workers and Container envelopes must fit the combined limit. | Both lanes draw against the dollar limit. Exhausting instance-hours pauses Container workflows and demand-driven proof refresh while Workers remain eligible. Joint qualification canaries still run within the shared dollar and canary limits and count against instance-hours. |

### Preserve Fleet Validation

Preset B is covered by the requalified Workers bundle with enforced
incarnation deadlines and lifetime-scaled meters. The API suggests Preset B
for new installations when the installed, verified release includes those
meters and $75 covers its computed envelope. [CT128 qualification receipt](../../results/cloudflare-scaleout-meter-lifetime-timing.json)

The API suggestion verifies the monthly fleet policy. A hosted preset B
trial still requires a recalculated, owner-approved trial budget and a
passing rehearsal spend gate for that trial plan. [Preset B follow-up](../../results/cloudflare-scaleout-preset-b-followup.md)

Workers save rejects a monthly limit below the selected release's computed envelope. The formula prices the bounded launcher fleet at `launcher cap × (max lifetime + cleanup margin)` plus fixed meter allowances, then adds `$0.002 × reachable slots × 31 × (ceil(86,400 / max lifetime) + 1)` for identity rotation. It uses the manifest's qualified CPU inputs for the fleet price and rounds the result up to cents. In Option C, rotation alone is `$402.318` and the other bounded meters add about `$13.267`. The protected Connect test passes with `$50` against a `$45.26` fixture envelope generated from different fixture bounds and geometry. The relevant floor for any option comes from the exact selected release and policy. [Policy pricing](../../../../api/src/services/worker_pool/cloudflare_policy.py) · [Fleet calculation](../../../../api/src/services/worker_pool/cloudflare_spend.py) · [Hosted preparation receipt](../../results/cloudflare-scaleout-meter-hosted-prep.json) · [Protected Connect fixture](../../../../api/tests/e2e/platform/test_cloudflare_capacity_persistence.py)

Price the Container instance-hour allocation at the signed release rate.
The fleet check adds gross compute for the smaller of the month's instance
capacity and the purchased hours, plus a conservative allowance for control
and outbound holds on repeated starts. The dollar ledger independently
guards those fixed charges as usage accrues. Policy save requires the sum
of the Workers and Container envelopes to fit the configured limit.
[Container fleet formula](../../../../api/src/services/worker_pool/cloudflare_containers_policy.py) · [Combined admission check](../../../../api/src/services/worker_pool/cloudflare_containers_admission.py) · [Containers qualification](PHASE3-CONTAINERS.md)

## Activation Preview

Show the forecast, fee, fleet minimum and admission limit together before consent. Example for Option B after meter requalification:

- **Estimated Overflow Usage** — About $3.72/month at 40 workflows/day in clustered bursts, with 35% cold starts and 0.077% Workers duty. For Containers, show billed instance duty, expected outbound traffic and the lane subtotals.
- **Workers Paid Subscription** — $5/month billed separately by Cloudflare.
- **Minimum Limit for This Fleet** — $32.13/month for three four-slot launchers, nine installation slots, and a 3,600 s lifetime with the checked-in rates. Recalculate when the release, capacity or spend-relevant bounds change.
- **Monthly Estimated Usage Limit** — $75/month. “At the limit, new work stays on the primary. Bifrost can pause overflow earlier if the next start cannot be reserved or usage cannot be verified.”

After activation, show month-to-date settled and reserved usage, remaining headroom and UTC reset. Keep the forecast approximate and display the limit and fleet minimum to cents so policy failures can be explained. Describe the fleet minimum and admission limit as Bifrost backstops. Provider retries and overages, charge adjustments, delayed cleanup and traffic elsewhere in the account can affect the Cloudflare bill. Where attributed Analytics are available, display them separately as advisory.

## Selected Activation Preset

The owner selected **Option B** for new Workers installations once the
deadline-aware bundle clears local validation and meter requalification.
The $75 suggested limit covers the $32.13 modeled fleet envelope for three
four-slot launchers, capped at nine installation slots. Existing
installations keep their saved limits and can choose the new values in Edit
Policy after the requalified release is installed. Option D still requires explicit
opt-in and verification of its signed release rates and byte bounds.
