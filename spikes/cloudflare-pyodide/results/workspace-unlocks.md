# Cloudflare workflow eligibility at workspace revision `37581f6`

The census used 317 manifest entries, a 1,000 ms CPU budget, and the same
workspace snapshot at both allowances. Source was analyzed offline. A candidate
still needs operator approval, and the static estimate does not attest to SDK
forwarding, actual request count, or CPU consumption.

| Allowance | Before | After | Before with unresolved URL calls | After with unresolved URL calls |
| ---: | ---: | ---: | ---: | ---: |
| 50 | 119 / 317 | 124 / 317 | 31 | 36 |
| 12 | 73 / 317 | 78 / 317 | 17 | 22 |

## Ranked baseline refusals

At allowance 50, the analyzer first refused 110 for the source budget, 41 for
unresolved packages, 27 for dynamic operations, 18 for the subrequest estimate,
and 2 for an unsupported stdlib import. At allowance 12, the subrequest count
rose to 64. The other first-refusal counts were the same.

Each count pair below is for allowances **50 / 12**. “Alone” is the number of
extra candidates after removing just that specific blocker in an offline
counterfactual on the baseline analyzer. “Cumulative” removes the blockers
through that row in the displayed order. The experiments used an expanded
per-file limit within the existing bundle cap, assumed listed packages
importable, or waived the named operation or estimate check. They measure
overlap and downstream refusals without asserting runtime viability.

| First refusal | Workflows | Extra alone | Extra cumulative | Finding |
| --- | ---: | ---: | ---: | --- |
| 128 KiB parse-file cap | 110 / 110 | 0 / 0 | 0 / 0 | The fixed per-file guard stopped the pass. Relaxing it exposed other imports, dynamic operations, or bundle and request limits. |
| Computed `getattr` immediately invoked | 26 / 26 | 11 / 11 | 11 / 11 | Five entry-local literal-dispatch false positives fixed (c). Six computed names remained in imported code. The other 15 encountered more refusals. |
| `croniter` | 21 / 21 | 0 / 0 | 11 / 11 | Pure-Python runtime candidate (a). All 21 next require a computed `importlib.import_module` call. |
| Subrequest estimate | 18 / 64 | 18 / 64 | 29 / 75 | The allowance waiver includes calls that would hit the current launcher limit or target SDK routes beyond the current gateway. |
| `pypdf` | 7 / 7 | 0 / 0 | 29 / 75 | Runtime candidate (a), followed by `openrouter`, then the subrequest estimate. |
| `mammoth` | 4 / 4 | 0 / 0 | 29 / 75 | Runtime candidate (a), followed by `bs4` and `reportlab`. |
| `reportlab` | 4 / 4 | 0 / 0 | 29 / 75 | Runtime build work (a), including Pyodide Pillow, followed by the source cap. |
| `openrouter` | 3 / 3 | 3 / 0 | 39 / 85 | Runtime candidate (a). The compiled artifact grew by 2.81 MB gzip for three standalone candidates at 50. Deferred on cost. |
| `requests` | 2 / 2 | 0 / 0 | 39 / 85 | Would need a compatible Fetch transport (a). Its ordinary socket transport is Linux-only here (d). The next first refusal was `jwt`. |
| `subprocess` | 2 / 2 | 0 / 0 | 39 / 85 | Linux-only process creation (d). |
| `importlib.import_module` with a computed name | 1 / 1 | 0 / 0 | 39 / 85 | The runtime would need the actual imported module in the approved snapshot. Removing this check exposed another dynamic operation. |

The 26 computed attributes comprised 15 lookups on an attribute receiver
with a variable selector (zero candidates from waiving that pattern),
six local-receiver lookups with a concatenated selector (six candidates),
and five local-receiver lookups with a variable selector whose
call sites all passed string literals (five candidates). The two high-allowance budget groups
estimated 62 and 68 subrequests (nine each). At 12, the remaining estimates
were 13 (5), 14 (1), 15 (5), 17 (4), 19 (3), 20 (4), 21 (6), 22 (5),
23 (1), 25 (2), 33 (2), 35 (3), and 42 (5). Removing the file cap also
surfaced two `awscrt` imports, which require a native runtime build outside
this spike's package set.

SDK forwarding is a separate constraint (b). All 18 workflows in the
62/68 groups statically reference `bifrost.roles` and `bifrost.users`;
`/api/roles` and `/api/users` are outside the gateway allowlist. Other
observed SDK surfaces include `bifrost.workflows.execute` (dispatch is
parent-owned) and `bifrost.api` (forwarding depends on the requested route).
`bifrost.knowledge` uses `/api/sdk/`, which the gateway already forwards.
These are SDK observations over analyzed source, independent of the
first-refusal counts. Expanding credential or route scope would require a
separate decision.

## Implemented slice and runtime cost

The analyzer now recognizes immediate public-literal selector dispatch through
an entry-local private helper with direct, statically bounded callers.
It counts the possible request and flags its destination as unresolved.
The five new candidates all require URL review. Gateway rules, runtime
limits, and the compiled runtime package set are unchanged.
An operator can submit a new proposal for a workflow previously refused at
unchanged source pins.

The staging precompiler produced the same baseline runtime used for the
candidate, 19,205,628 bytes raw and 6,548,935 bytes gzip. The runtime size
delta is zero. A loopback Workerd `boot_probe` importing `runtime_runner`
measured 2,772 ms median across five fresh children (2,770–5,533 ms).
Both analyzer revisions use the same runtime artifact.
The separate OpenRouter packaging trial measured 29,667,891 bytes raw and
9,354,580 bytes gzip before supervisor code. That trial and other rejected
package experiments are excluded from the candidate.

Run the census using `dev/routing/census.mjs` against an archive of the
stated workspace revision with `--cpu 1000` and `--allowance 50` or
`--allowance 12`. Keep the raw reports local because they include source
paths and inferred hostnames.

## Installed Solution census

The same revision contains 35 Solution workspaces and 317 manifest entries.
The install-aware rerun uses the CLI's package source collector and the
server analyzer against each install's own tree. Four descriptors with
outbound access account for 36 excluded entries. Among the remaining entries,
71 are other executable types, leaving 210 Normal workflow entries to analyze.

| Subrequest allowance | Earlier static candidates | Before Solution routing | After Solution routing, conditional on approval | Unresolved URL calls among the new candidates |
| ---: | ---: | ---: | ---: | ---: |
| 12 | 78 | 0 | 52 | 7 |
| 50 | 124 | 0 | 83 | 11 |

The 52 and 83 are eligible to receive the dispatch marker once the exact
installed bytes have been published and approved. No approvals were granted
for this workspace. The archive cannot establish live install settings,
instance-vendored modules, actual SDK calls, or execution success. Reproduce
the aggregate-only count with `dev/routing/solution-census.py`.
