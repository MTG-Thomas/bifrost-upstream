# Workspace eligibility census

`census.mjs` discovers workflow entries in `.bifrost/workflows.yaml` or scans for
workflow-shaped Python files. `analyze-file.py` calls the server's
`cloudflare_analysis.py` over local source. `census-gate.mjs` compares the report
with a threshold and baseline. Use Node and Python 3.11 or newer.

```sh
node --test
node census.mjs --workspace ../../../.. --allowance 12 --cpu 1000 --out out.json
node census.mjs --repo ../../../.. --out out.json
```

Set `BIFROST_ANALYZER_PATH` to the directory containing `cloudflare_analysis.py`
when running outside the Bifrost checkout. The example Actions workflow
expects a copy of the analyzer from the policy revision being evaluated.
Regenerate the baseline when adopting this analyzer. Previous census reports
describe the earlier experimental build and detection rules.

The report includes transitive workspace pins, bundled packages, identifiable
literal HTTPS origins, and static CPU and subrequest estimates. It neither
executes source nor grants an approval. Review and accept deployed workspace
source using the operator command in [the launcher design](../cloudflare-launcher/DESIGN.md#phase-3-workspace-approval-and-placement).

Historical results: [workspace census](../../backend/WORKSPACE-CENSUS.md).

For installed Solutions, use `solution-census.py` against an archive of the
workspace commit. With Bifrost's CLI dependencies available, add `api` and
`api/src/services/execution` to `PYTHONPATH` and run:

```sh
python spikes/cloudflare-pyodide/dev/routing/solution-census.py /path/to/workspace-archive
```

This mode prints aggregate counts only. It applies the CLI's install-local
source collector and excludes outbound-enabled installs, non-workflow types
and explicit Fast Path entries. It models no instance-side vendoring or
overrides. It counts source candidates after the install and lane checks.
The real queue also requires a finalized install, current accepted approval
and an eligible Worker.
