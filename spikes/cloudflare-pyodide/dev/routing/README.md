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

## Operator Diagnostics

Set `BIFROST_CLOUDFLARE_DIAGNOSTICS_ENABLED=true` in the Bifrost API and
deploy worker to enable advice on editor saves, workflow validation, and
Solution deploy results. The default is off. These warnings leave Linux
validation and deployment valid whenever the existing platform checks pass.

- **Editor save** checks only the saved Python file. It reports unsupported
  process, transport, native or dynamic operations, known SDK gateway gaps,
  reviewable HTTP destinations, and source parse limits. It does not read
  dependencies or estimate the bundle cost.
- **Workflow validation** checks submitted or stored source and reports
  file, line, column, rule ID and fix hint when known. The enabled path
  inspects decorators and parameters statically. Resolving imports from
  the loose workspace requires platform repository read permission. Use
  `bifrost workflows validate PATH --from-file LOCAL_FILE` for proposed
  content. The CLI and MCP accept execution lane, inline and artifact workspace
  details for a placement check.
- **Solution deploy preflight** evaluates the actual install-local Python
  bundle and outbound setting, including vendored source. Read the per-workflow
  status and findings in `result.cloudflare_diagnostics` on the deploy job.
  Agent artifacts receive one note about agent execution and summaries.
  Apps and Forms use their existing UI paths and may invoke separately
  diagnosed workflows.
- **Workspace report** is an explicit offline opt-in. Point it at a manifest
  workspace, a Solution, or a tree of Solutions. The Solution census calls
  the same report code. Each artifact counts once per rule, regardless
  of how many times the rule appears.

```sh
bifrost workflows cloudflare-report /path/to/workspace --allowance 50 --cpu 1000
bifrost workflows cloudflare-report /path/to/workspace --aggregate-only --json
PYTHONPATH=api python spikes/cloudflare-pyodide/dev/routing/solution-census.py /path/to/workspace
```

The full report follows the approval analyzer through the first canonical
refusal and includes runtime observations found during that scan. Address
the first refusal to expose any deeper blockers. A `candidate` status still
requires source-bound operator approval and eligible Normal Worker capacity.
A `review` status requests an operator decision on unresolved HTTPS hosts.
The static estimate cannot measure how many calls a loop or dependent package
will make at runtime. The offline census cannot see install overrides or
shared source awaiting vendoring. Keep full reports local when source paths
or artifact names are private.

| Rule ID | Severity | Surfaces | Fix |
| --- | --- | --- | --- |
| CF_PROCESS | Warning | Save, validate, preflight, report | Replace native processes with Python or use Linux. |
| CF_TRANSPORT | Warning | Save, validate, preflight, report | Use HTTPX with HTTPS and an allowed host. |
| CF_PACKAGE | Warning | Bundle validate, preflight, report | Use a bundled package or qualify a compatible Pyodide build. |
| CF_NATIVE | Warning | Save for known denied imports, bundle validate, preflight, report | Use WebAssembly-compatible code or the native runner. |
| CF_DYNAMIC | Warning | Save, validate, preflight, report | Use explicit imports, direct calls and static dispatch. |
| CF_PARSE_BUDGET | Warning | Save for file, validate, preflight, report | Reduce file size or reduce imported bundle size. |
| CF_SUBREQUESTS | Warning | Bundle validate, preflight, report | Batch calls or review the allowance. |
| CF_CPU | Warning | Bundle validate, preflight, report | Trim code or review the CPU budget. |
| CF_HOST_BUDGET | Warning | Save, validate, preflight, report | Reduce distinct HTTPS hosts. |
| CF_SOURCE | Warning | Save, validate, preflight, report | Check the Python path, source, entry function or imports. |
| CF_SDK_GATEWAY | Warning | Save, validate, preflight, report | Use a forwarded SDK area or move the step to Linux. |
| CF_URL_REVIEW | Info | Save, validate, preflight, report | Identify or review and approve possible HTTPS hosts. |
| CF_INLINE | Warning | Validate with inline context | Save and register a Normal workflow. |
| CF_FAST_PATH | Warning | Validate, preflight, report | Use Normal where its scheduling fits. |
| CF_OUTBOUND | Warning | Preflight, report | Keep Linux or vendor dependencies and review install-local access. |
| CF_ARTIFACT_WORKSPACE | Warning | Validate with artifact workspace context | Keep Linux or remove the artifact workspace dependency. |
| CF_EXECUTABLE_TYPE | Warning | Validate, preflight, report | Use an independently approved Normal workflow for the Workers step. |
| CF_AGENT | Info | Preflight, report | Keep agent runs on Bifrost. Summaries can use Workers when enabled. |
