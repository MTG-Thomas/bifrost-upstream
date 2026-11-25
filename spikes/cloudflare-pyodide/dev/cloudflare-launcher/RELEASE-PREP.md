# Containers Trial Release Readiness

**Prepared locally. Publication is held for the frozen-target release review,
owner approval, and fork settings checks.**
The exact rc03 release candidate is `01393ec44c77ba509d05c7d0b258dc35e7a15571`
on local branch `t3/cloudflare-release-rc03`. It includes the shared-account
namespace fix, runner evidence fixes, and the Sigstore v0.3 verifier fix.
The approved release body is byte-identical to rc02. Push the pinned candidate to
`trial/cloudflare-containers-20261124` on `personal`
(`MTG-Thomas/bifrost-upstream`). Keep the fork's `main` unchanged. The tag
pins the commit before this documentation receipt.

## Fork, workflow and tag

- `git ls-remote --symref personal HEAD refs/heads/main` reported that the
  fork defaults to `main` at `27b7e39175769b2ab4ea8629b8a1f9dd81e529d5`.
  That commit is local and is outside the candidate's ancestry. The proposed
  trial branch was absent during that check. The first trial then used that
  branch for its failed release attempt.
- Comparing `.github/workflows/ci.yml` at fork main and the spike shows the
  spike adds qualified Workers packaging, signed Linux assets, and release
  staging followed by image promotion. **Both comparisons before the routing fix hard-code
  `ghcr.io/gobifrost/bifrost-api` and `ghcr.io/gobifrost/bifrost-client`.**
  On the spike the Linux image would also target
  `ghcr.io/gobifrost/bifrost-api-cloudflare-linux`. The tagged commit's
  workflow is what matters for this release.
- The release candidate derives package paths from the lowercased repository
  owner, checks it against the repository name, and gates all registry writers
  on that check. Fork destinations are
  `ghcr.io/mtg-thomas/bifrost-api`, `ghcr.io/mtg-thomas/bifrost-client`, and
  `ghcr.io/mtg-thomas/bifrost-api-cloudflare-linux`. Verify these paths cover
  digest pushes, cache writes, signatures, attestations and promotion.
  The guard rejects literal upstream destinations on tags and missing trial
  variables on fork tags. The ten-digit date-based RC namespace is reserved
  for fork trials and rejected on upstream. Ordinary upstream tags retain
  optional packaging. The workflow uses the fork's `GITHUB_TOKEN`.
- The fork published rc02 from `85e761039753a4421e872f40cda8c792e3fc5e4c`.
  Hosted preflight found that Cosign's legacy parser rejected its v0.3
  release bundle before any Cloudflare call. The new tag is **`v2.0.0-rc.2026112403`**.
  The exact tag was absent on both remotes during preparation. The fork trial
  branch pointed to rc02, an ancestor of this candidate. Recheck both refs
  at the approval gate. The ten-digit
  suffix reserves it for fork trials and keeps the prerelease off `:latest`.
- CI strips the leading `v` and sets both the API version and
  `workers.release_version` to `2.0.0-rc.2026112403`. The version exceeds
  the CLI floor of `1.4.3`, and the API reconstructs the exact tag from the
  packaged Workers version. The CLI tarball uses PEP 440 metadata
  `2.0.0rc2026112403`. The signer identity would be
  `https://github.com/MTG-Thomas/bifrost-upstream/.github/workflows/ci.yml@refs/tags/v2.0.0-rc.2026112403`.
- The three plugin manifests match the prerelease version. The tag workflow
  selects `product-updates/release-bodies/v2.0.0-rc.2026112403.md` by exact
  filename. Both rc02 and rc03 bodies are retained.
  The general `scripts/release-check.sh` preflight requires a separately
  rendered Product Updates body with reviewed inventory metadata. The fork
  workflow checks the exact-tag body and prepares approved in-app updates
  separately.

## Publication commands for owner review

These commands pin the current local candidate for review. If the approved
body or release inputs change its SHA, refresh this pin before approval and
publication. Recheck both remotes for the exact tag and the fork for the
destination branch. Use only the explicit `personal` refspecs below.
Never use `git push --tags` or `git push --follow-tags`. The commands remain unrun.

```sh
git tag -a v2.0.0-rc.2026112403 01393ec44c77ba509d05c7d0b258dc35e7a15571 -m "Cloudflare Containers Trial"
test "$(git rev-parse 'v2.0.0-rc.2026112403^{commit}')" = 01393ec44c77ba509d05c7d0b258dc35e7a15571 || exit 1
git -c push.followTags=false push personal 01393ec44c77ba509d05c7d0b258dc35e7a15571:refs/heads/trial/cloudflare-containers-20261124
git -c push.followTags=false push personal refs/tags/v2.0.0-rc.2026112403:refs/tags/v2.0.0-rc.2026112403
```

The tag runs the full CI test suite on the fork's GitHub Actions before
publication. Release outputs and signing are described below.

## Fork settings to verify

| Item | Checked in source | Owner verification |
| --- | --- | --- |
| `QUALIFIED_WORKERS_RUNTIME_URL` | CI downloads the exact qualified runtime over HTTPS without credentials. The builder pins SHA-256 `11ade0a1ea1b117d577803cff87d1c3c4074d793df5bcb17a9f473923e55f254`. | Confirm the fork repository variable and anonymously readable URL. Bringing up a new public runtime artifact would need to be included in the approval. |
| `QUALIFIED_WORKERS_METER_BOUNDS` | Use exact JSON from `api/src/services/worker_pool/cloudflare_meter_bounds.json` on the release commit. The builder checks the bounds and bundle fingerprints. | Confirm the value in Actions variables and match it to the release commit. |
| `CONTAINERS_RELEASE_RATES` | Required for Linux packaging when the qualified Workers build is enabled. Exact proposed JSON below. | Confirm the configured value or approve setting it. Absent optional packaging variables can allow a release without the Containers artifacts, so verify all three before pushing. |
| Workflow token | Uses GitHub's built-in `GITHUB_TOKEN`. No custom secret, Cloudflare token, PAT, or static signing key is referenced by the release workflow. | Confirm access to push the approved branch and tag, tag rules, and policies permitting the job-scoped token permissions. |
| Actions and GHCR | Image jobs request `packages: write`, `id-token: write` and `attestations: write`. Release staging requests `contents: write`, `id-token: write` and `attestations: write`. Promotion requests `packages: write` and `contents: write`. | Confirm Actions enabled on the fork, pinned actions and GitHub-hosted runners allowed, GitHub OIDC and attestations available, and package creation and writes permitted. Give the fork repository Actions access to any existing destination packages. |
| Public reads | The API fetches the three GitHub Release assets anonymously and checks the public Linux OCI digest. | Confirm the release assets can be made public. After the package exists, make only the Linux package public under the approved scope and verify an anonymous digest read. Arrange authorized pulls for API and client images if private. Retain the release, assets and Linux image after the trial. |

## Tag workflow, by job

1. `windows-credentials`, `lint`, `test-client-unit`, `test-unit`,
   `test-client-smoke`, the three `test-e2e` shards and
   `test-azure-object-storage` run the gates. `test-e2e-gate` requires the
   relevant results. Merge-queue candidate and main dev publication jobs
   skip this tag. The trial branch name alone does not match a publication push trigger.
2. `build-api` verifies the plugin versions, bundles approved product
   updates, and builds the qualified Workers release twice to compare output.
   It bakes the exact tag's cosign verifier identity into the amd64 API image,
   pushes by digest, and signs and attests that digest. It builds the matched
   Linux image from that API digest and pushes `sha-<full-release-commit>`.
   It signs the manifest as a blob, covering the OCI, wheel, module and Workers
   digests and rates. Its run artifact holds the manifest, Sigstore bundle and
   provider module. Linux authenticity is verified through that signed manifest.
3. `build-client` bundles approved product updates and builds the amd64
   production client with the same version. It pushes by digest, then signs
   and attests that digest.
4. `create-release` verifies the release body, downloads the Linux artifact,
   and stages a draft GitHub prerelease. It adds the source tarball, SHA-256
   and Sigstore bundle, optional type stubs and their signature when present,
   and `cloudflare-containers-manifest.json`,
   `cloudflare-containers-manifest.sigstore.json`, and
   `cloudflare-containers-container-registry.mjs`. It attests the source
   tarball and reads back and compares the three Linux asset bytes.
5. `promote-release-images` applies and checks the API and client prerelease
   and SHA tags against the immutable digests, then publishes the draft.
   Cosign operations are public transparency effects. Include the API and
   client image signatures, Linux manifest and source tarball signatures,
   and any optional stub signature in the Rekor approval.

## Proposed signed rates and caps

The fork variable values are owner checks. The following proposed
`CONTAINERS_RELEASE_RATES` uses the locally rehearsed values. The release
schema pins the instance-second rate and outbound rate. The control and cleanup
values are proposed inputs to approve and verify in the published manifest.

```json
{"version":"standard-1-v1","instance_second_usd":"0.00002056","outbound_gb_usd":"0.05","control_usd":"0.000001","cleanup_seconds":60}
```

For `r=0.00002056`, `e=0.05`, `f=0.000001`, and `c=60`, the live
Container reservation allowance is **$0.01591282**, the expected Container cost
is **$0.00569450**, and the two supporting allowances sum to **$0.013279415**.
Following the [trial formulas](PHASE3-CONTAINERS-HOSTED-TRIAL.md), round up to
get **$0.03 live target** and **$0.02 settled cap**. Add the independently
rounded **$0.02 cleanup allowance** to the live target to get **$0.05 abort**.
Confirm the exact signed inputs and combined monthly floor during preflight.

## Private runner config

Use a fresh, dedicated Compose installation with the approved images pinned
by digest, normal provider adapters, no host-published API or worker ports,
and an empty opening ledger. Prepare separate hosted Compose files with
names matching the private config.
Use the previously approved shared account and zone. Check the exact
trial script names, script attribution on namespaces, application name and
registry repository for collisions before activation. Save the foreign
inventory and verify it is unchanged after Disconnect. Confirm that every
namespace row reports a script for the trial runner's opening check.
For Existing HTTPS the zone is operator metadata and the
runner does not send it or mutate DNS. The `.env` operator credentials and
the runner config stay private. Set the config mode to `0600`.

```json
{
  "account": "<PRIOR_WORKERS_ACCOUNT_ID>",
  "zone": "<PRIOR_WORKERS_ZONE_ID>",
  "hostname": "<APPROVED_EXISTING_HTTPS_HOSTNAME>",
  "project": "bifrost-test-containers-hosted",
  "network": "<PRIVATE_COMPOSE_NETWORK>",
  "compose_files": ["trial-hosted-compose.yml", "trial-hosted-overlay.yml"],
  "managementToken": "<EXPIRING_ACCOUNT_SCOPED_TOKEN>",
  "live_target_usd": "0.03",
  "live_abort_usd": "0.05",
  "settled_cap_usd": "0.02"
}
```

After rc03 is published and approved for the trial, pin the Compose project
to its published API and client digests and verify the signed Linux digest.
From that project directory, call the runner from the separate candidate
checkout using the approved private config.

```sh
python3 /absolute/shared-account-harness/spikes/cloudflare-pyodide/dev/e2e/combined-rehearsal.py \
  --containers-hosted-shape --hosted-containers-config /absolute/private/containers-trial.json
```
