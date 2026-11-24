"""Static safety contracts for the shared dev-image CI topology.

These tests deliberately inspect workflow source rather than simulating GitHub.
They protect the repository-controlled half of the invariant: the complete suite
runs for an exact merge candidate, while dev images can only be published after
that candidate reaches ``main``. The live required-check settings are audited
separately because they are GitHub repository configuration, not source code.
"""

import inspect
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.verify_merge_candidate import REQUIRED_JOBS
from src.services.worker_pool import cloudflare_containers_release


def _repository_root() -> Path:
    # Host tests see <repo>/api/tests; the Dockerized test runner mounts the API
    # at /app and workflow sources at /app/.github.
    for candidate in Path(__file__).resolve().parents:
        if (candidate / ".github" / "workflows" / "ci.yml").is_file():
            return candidate
    raise RuntimeError("could not locate repository workflow sources")


REPO_ROOT = _repository_root()
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
CI_NOOP_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci-noop.yml"
NIGHTLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly.yml"
PLAYWRIGHT_CONFIG = REPO_ROOT / "client" / "playwright.config.ts"
TEST_SCRIPT = REPO_ROOT / "test.sh"
TEST_COMPOSE = REPO_ROOT / "docker-compose.test.yml"


def _load_workflow(path: Path) -> dict[str, Any]:
    # BaseLoader keeps the key ``on`` as a string instead of YAML 1.1's boolean.
    loaded = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def _normalized(expression: str) -> str:
    return " ".join(expression.split())


def test_container_release_signing_and_verification_use_the_same_bundle_contract() -> None:
    steps = _load_workflow(CI_WORKFLOW)["jobs"]["build-api"]["steps"]
    install, = [step for step in steps if step.get("name") == "Install cosign for the Linux release descriptor"]
    assert install["with"]["cosign-release"] == cloudflare_containers_release._COSIGN_VERSION
    assert any('install -Dm0755 "$(command -v cosign)"' in step.get("run", "") for step in steps)
    assert any("bash deploy/capacity/cloudflare/build-containers.sh" in step.get("run", "") for step in steps)
    signing = (REPO_ROOT / "deploy/capacity/cloudflare/build-containers.sh").read_text()
    assert re.search(r'^cosign sign-blob --yes --new-bundle-format --bundle "\$output/manifest\.sigstore\.json"', signing, re.M)
    verifier = inspect.getsource(cloudflare_containers_release.verify_containers_release)
    assert '"verify-blob", "--new-bundle-format", "--offline"' in verifier
    assert '"--bundle", "manifest.sigstore.json"' in verifier


def test_promotion_verifier_requires_every_merge_candidate_job() -> None:
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    shard_job = jobs["test-e2e"]
    shards = shard_job["strategy"]["matrix"]["shard"]
    name_template = shard_job["name"]
    assert "${{ matrix.shard }}" in name_template
    assert name_template.endswith(f"/{len(shards)})")

    expected = {
        jobs[job_id]["name"]
        for job_id in (
            "lint",
            "test-unit",
            "test-client-unit",
            "test-client-smoke",
            "build-dev-api-candidate",
            "build-dev-client-candidate",
            "test-e2e-gate",
        )
    }
    expected.update(
        name_template.replace("${{ matrix.shard }}", shard) for shard in shards
    )

    assert REQUIRED_JOBS == expected


def test_full_suite_gates_exact_merge_candidate_before_dev_image() -> None:
    workflow = _load_workflow(CI_WORKFLOW)
    triggers = workflow["on"]
    jobs = workflow["jobs"]

    assert {"pull_request", "merge_group", "push"} <= triggers.keys()

    full_e2e_condition = _normalized(jobs["test-e2e"]["if"])
    full_e2e_gate_condition = _normalized(jobs["test-e2e-gate"]["if"])
    for condition in (full_e2e_condition, full_e2e_gate_condition):
        assert "github.event_name == 'merge_group'" in condition
        assert "github.event_name == 'workflow_dispatch'" in condition
        assert "startsWith(github.ref, 'refs/tags/v')" in condition
    assert "pull_request" not in full_e2e_condition
    assert "github.event_name == 'pull_request'" in full_e2e_gate_condition

    assert set(jobs["test-e2e-gate"]["needs"]) == {
        "test-e2e",
        "test-azure-object-storage",
        "test-client-unit",
        "test-client-smoke",
        "windows-credentials",
        "build-dev-api-candidate",
        "build-dev-client-candidate",
    }
    gate_source = "\n".join(step.get("run", "") for step in jobs["test-e2e-gate"]["steps"])
    assert "needs.build-dev-api-candidate.result" in gate_source
    assert "needs.build-dev-client-candidate.result" in gate_source
    assert jobs["test-e2e-gate"]["name"] == "E2E Tests"
    assert [
        name for name, job in jobs.items() if job.get("name") == "E2E Tests"
    ] == ["test-e2e-gate"]

    for job_name in ("lint", "test-unit", "test-client-unit"):
        condition = _normalized(jobs[job_name]["if"])
        assert "github.event_name != 'push'" in condition
        assert "github.ref != 'refs/heads/main'" in condition

    build_dev = jobs["build-dev"]
    assert _normalized(build_dev["if"]) == (
        "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    )
    assert build_dev["needs"] == ["registry-route"]
    verification = next(
        step
        for step in build_dev["steps"]
        if step.get("name") == "Verify exact merge candidate gate"
    )
    assert "verify_merge_candidate.py" in verification["run"]
    assert '--sha "$GITHUB_SHA"' in verification["run"]


def test_critical_browser_smoke_strengthens_only_exact_candidate_events() -> None:
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    smoke = jobs["test-client-smoke"]
    condition = _normalized(smoke["if"])

    assert "github.event_name == 'merge_group'" in condition
    assert "github.event_name == 'workflow_dispatch'" in condition
    assert "startsWith(github.ref, 'refs/tags/v')" in condition
    assert "pull_request" not in condition
    assert set(smoke["needs"]) == {"registry-route", "build-dev-client-candidate"}

    run_steps = [step.get("run", "") for step in smoke["steps"]]
    assert any("./test.sh client smoke" in step for step in run_steps)

    client_build = next(
        step for step in smoke["steps"]
        if step.get("name") == "Build production client under test (cached)"
    )
    assert client_build["with"]["file"] == "./client/Dockerfile"
    assert client_build["with"]["target"] == "production"
    assert client_build["with"]["tags"] == "bifrost-test-client-e2e:latest"

    compose = yaml.safe_load(TEST_COMPOSE.read_text())
    client = compose["services"]["client"]
    assert client["image"] == "bifrost-test-client-e2e:${BIFROST_TEST_IMAGE_TAG:-latest}"
    assert client["build"]["dockerfile"] == "Dockerfile"
    assert client["build"]["target"] == "production"
    assert "volumes" not in client

    playwright_config = PLAYWRIGHT_CONFIG.read_text()
    assert "retries: 0" in playwright_config
    assert "retain-on-failure" in playwright_config
    assert "on-first-retry" not in playwright_config

    smoke_specs = {
        "setup/global.setup.ts",
        "auth.unauth.spec.ts",
        "forms-public.unauth.spec.ts",
        "apps-preview.admin.spec.ts",
    }
    for relative_path in smoke_specs:
        source = (REPO_ROOT / "client" / "e2e" / relative_path).read_text()
        assert 'tag: "@smoke"' in source, relative_path


def test_ci_jobs_use_command_owned_stacks() -> None:
    """Each CI test invocation owns startup, diagnostics and teardown."""
    ci_jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    nightly_jobs = _load_workflow(NIGHTLY_WORKFLOW)["jobs"]
    selected = [
        ci_jobs["test-unit"],
        ci_jobs["test-e2e"],
        ci_jobs["test-client-smoke"],
        nightly_jobs["product-browser"],
        nightly_jobs["slow-unit-contracts"],
        nightly_jobs["backend-coverage"],
    ]

    for job in selected:
        commands = "\n".join(step.get("run", "") for step in job["steps"])
        assert "./test.sh stack up" not in commands
        assert "./test.sh" in commands
        assert all(
            "BIFROST_TEST_USE_CLEAN_BOOT" not in step.get("env", {})
            for step in job["steps"]
        )


def test_azure_object_storage_lane_exercises_only_the_provider_neutral_contract() -> None:
    """Azure coverage is an exact-candidate gate, not a second full E2E suite."""
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    azure = jobs["test-azure-object-storage"]

    condition = _normalized(azure["if"])
    assert "github.event_name == 'merge_group'" in condition
    assert "github.event_name == 'workflow_dispatch'" in condition
    assert "startsWith(github.ref, 'refs/tags/v')" in condition
    assert "pull_request" not in condition
    assert azure["name"] == "Azure Object Storage E2E"

    build = next(
        step for step in azure["steps"]
        if step.get("name") == "Build API dev image (cached)"
    )
    assert build["with"]["file"] == "./api/Dockerfile.dev"
    assert build["with"]["tags"] == "bifrost-test-api-dev:latest"

    run = next(
        step for step in azure["steps"]
        if step.get("name") == "Run Azure object-storage contract"
    )
    assert run["env"] == {
        "BIFROST_SKIP_BUILD": "1",
        "BIFROST_TEST_OBJECT_STORAGE_PROVIDER": "azure_blob",
        "BIFROST_TEST_AZURITE": "1",
        "BIFROST_TEST_AZURE_BLOB_RESET": "1",
        "BIFROST_AZURE_BLOB_ACCOUNT_URL": "http://azurite:10000/bifrostcontract",
        "BIFROST_AZURE_BLOB_ACCOUNT_NAME": "bifrostcontract",
        "BIFROST_AZURE_BLOB_CONTAINER": "bifrost-test",
        "BIFROST_AZURE_BLOB_AUTH": "account_key",
        "BIFROST_AZURE_BLOB_ACCOUNT_KEY": "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
    }
    assert "./test.sh tests/e2e/platform/test_object_storage.py -v --durations=0" in run["run"]
    assert "./test.sh e2e" not in run["run"]

    gate = jobs["test-e2e-gate"]
    assert "test-azure-object-storage" in gate["needs"]
    gate_source = "\n".join(step.get("run", "") for step in gate["steps"])
    assert "needs.test-azure-object-storage.result" in gate_source


def test_dev_artifact_is_built_on_merge_candidate_and_promoted_without_rebuild() -> None:
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    api_candidate = jobs["build-dev-api-candidate"]
    client_candidate = jobs["build-dev-client-candidate"]
    promotion = jobs["build-dev"]

    for candidate in (api_candidate, client_candidate):
        assert _normalized(candidate["if"]) == "github.event_name == 'merge_group'"
        assert candidate["permissions"]["packages"] == "write"
        assert candidate["permissions"]["id-token"] == "write"
        assert candidate["permissions"]["attestations"] == "write"
        assert candidate["needs"] == ["registry-route"]
    assert api_candidate["name"] == "Build API Dev Candidate"
    assert client_candidate["name"] == "Build Client Dev Candidate"

    candidate_steps = api_candidate["steps"] + client_candidate["steps"]
    candidate_builds = [
        step for step in candidate_steps
        if str(step.get("uses", "")).startswith("docker/build-push-action@")
    ]
    assert len(candidate_builds) == 2
    assert all(step["with"]["push"] == "true" for step in candidate_builds)
    assert {
        step["with"]["tags"] for step in candidate_builds
    } == {
        "ghcr.io/${{ env.API_IMAGE }}:candidate-${{ github.sha }}",
        "ghcr.io/${{ env.CLIENT_IMAGE }}:candidate-${{ github.sha }}",
    }
    candidate_source = "\n".join(
        step.get("run", "") for step in candidate_steps
    )
    assert "docker run --rm" in candidate_source
    assert "from src.main import app" in candidate_source
    assert "get_version() == os.environ['EXPECTED_VERSION']" in candidate_source
    for candidate in (api_candidate, client_candidate):
        candidate_steps = candidate["steps"]
        assert any(step.get("name", "").startswith("Sign ") for step in candidate_steps)
        assert any(step.get("name", "").startswith("Attest ") for step in candidate_steps)

    assert promotion["name"] == "Promote Dev Images"
    assert not any(
        str(step.get("uses", "")).startswith("docker/build-push-action@")
        for step in promotion["steps"]
    )
    promotion_source = "\n".join(
        step.get("run", "") for step in promotion["steps"]
    )
    assert "candidate-${GITHUB_SHA}" in promotion_source
    assert "docker buildx imagetools create" in promotion_source
    assert "Digest mismatch" in promotion_source
    assert "fallback" not in promotion_source.lower()
    assert promotion["outputs"]["version"] == "${{ steps.version.outputs.version }}"


def test_candidate_caches_use_separate_registry_refs_and_keep_export_errors_fatal() -> None:
    """Keep candidate caches independent of GitHub Actions cache storage."""
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    for image in ("api", "client"):
        job = jobs[f"build-dev-{image}-candidate"]
        build = next(
            step for step in job["steps"]
            if str(step.get("uses", "")).startswith("docker/build-push-action@")
        )
        cache_ref = "ghcr.io/${{ env." + image.upper() + "_IMAGE }}:buildcache"
        assert build["with"]["cache-from"] == f"type=registry,ref={cache_ref}"
        assert build["with"]["cache-to"] == f"type=registry,ref={cache_ref},mode=max"
        assert "continue-on-error" not in build
        assert "ignore-error" not in build["with"]["cache-to"]


def test_ci_publication_does_not_depend_on_an_operator_cluster() -> None:
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    assert "deploy-dev" not in jobs
    assert "deploy-dry-run" not in jobs
    source = CI_WORKFLOW.read_text()
    assert "DIGITALOCEAN_ACCESS_TOKEN" not in source
    assert "DO_CLUSTER_NAME" not in source
    assert "kubectl" not in source
    for job in jobs.values():
        assert "deploy-dev" not in job.get("needs", [])
        assert "deploy-dry-run" not in job.get("needs", [])


def test_pull_request_reports_required_e2e_context_without_running_full_suite() -> None:
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]

    gate = jobs["test-e2e-gate"]
    assert gate["name"] == "E2E Tests"
    assert "github.event_name == 'pull_request'" in _normalized(gate["if"])
    source = "\n".join(step.get("run", "") for step in gate["steps"])
    assert 'github.event_name }}" = "pull_request"' in source
    assert "needs.test-client-unit.result" in source
    assert "Full E2E is deferred" in source


def test_docs_only_inverse_workflow_covers_the_same_paths_and_check_names() -> None:
    workflow = _load_workflow(CI_WORKFLOW)
    noop = _load_workflow(CI_NOOP_WORKFLOW)

    assert workflow["on"]["pull_request"]["paths-ignore"] == noop["on"][
        "pull_request"
    ]["paths"]

    required_names = {"Lint & Type Check", "Unit Tests", "E2E Tests"}
    workflow_names = {job["name"] for job in workflow["jobs"].values()}
    noop_names = {job["name"] for job in noop["jobs"].values()}
    assert required_names <= workflow_names
    assert required_names <= noop_names


def test_release_tags_still_require_the_complete_suite() -> None:
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    release_prerequisites = {"registry-route", "test-unit", "test-e2e-gate", "lint"}

    assert set(jobs["build-api"]["needs"]) == release_prerequisites
    assert set(jobs["build-client"]["needs"]) == release_prerequisites
    assert set(jobs["create-release"]["needs"]) == {"registry-route", "build-api", "build-client"}

    for job_name in ("build-api", "build-client"):
        condition = _normalized(jobs[job_name]["if"])
        assert "always()" in condition
        assert "startsWith(github.ref, 'refs/tags/v')" in condition
        for prerequisite in release_prerequisites:
            assert f"needs.{prerequisite}.result == 'success'" in condition

    release_condition = _normalized(jobs["create-release"]["if"])
    assert "always()" in release_condition
    assert "startsWith(github.ref, 'refs/tags/v')" in release_condition
    assert "needs.build-api.result == 'success'" in release_condition
    assert "needs.build-client.result == 'success'" in release_condition


def test_release_remains_draft_until_assets_and_images_are_verified() -> None:
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    stage = jobs["create-release"]
    create, verify = stage["steps"][-2:]
    assert create["id"] == "release" and ".draft = true" in create["run"]
    assert verify["env"]["RELEASE_ID"] == "${{ steps.release.outputs.id }}"
    promotion = jobs["promote-release-images"]
    assert "needs.create-release.result == 'success'" in _normalized(promotion["if"])
    promote, publish = promotion["steps"][-2:]
    assert promote["name"] == "Promote and verify the signed image digests"
    assert publish["env"]["RELEASE_ID"] == "${{ needs.create-release.outputs.release-id }}"
    assert "draft=false" in publish["run"]


_UPSTREAM_IMAGE = re.compile(r"(?:ghcr\.io/|(?<![\w/.-]))gobifrost/[a-z0-9_.-]+", re.IGNORECASE)
_SCRIPT_PATH = re.compile(r"(?<![\w/])((?:api/scripts|scripts|deploy)/[\w./-]+\.(?:sh|py|mjs))\b")
_SCRIPT_MODULE = re.compile(r"\bpython3?\s+-m\s+(scripts\.[\w.]+)")
_LOCAL_ACTION = re.compile(r"\./(\.github/actions/[\w/-]+)")


def _publication_script_sources(steps: list[dict[str, Any]]) -> dict[str, str]:
    """Follow checked-in action and command references from publication jobs."""
    sources: dict[str, str] = {}
    pending = [step.get("run", "") + "\n" + step.get("uses", "") for step in steps]
    while pending:
        source = pending.pop()
        refs = set(_SCRIPT_PATH.findall(source))
        refs.update(module.replace(".", "/") + ".py" for module in _SCRIPT_MODULE.findall(source))
        refs.update(action + "/action.yml" for action in _LOCAL_ACTION.findall(source))
        for ref in refs - sources.keys():
            path = REPO_ROOT / ref
            if REPO_ROOT == Path("/app"):
                if ref.startswith("scripts/"):
                    path = Path("/ci-root-scripts") / ref.removeprefix("scripts/")
                elif ref.startswith("api/scripts/"):
                    path = REPO_ROOT / ref.removeprefix("api/")
                elif ref.startswith("deploy/"):
                    path = Path("/") / ref
            sources[ref] = path.read_text(encoding="utf-8")
            pending.append(sources[ref])
    return sources


def test_ci_registry_writes_require_repository_routing() -> None:
    workflow = _load_workflow(CI_WORKFLOW)
    jobs = workflow["jobs"]
    assert not _UPSTREAM_IMAGE.search(CI_WORKFLOW.read_text())
    assert next(iter(jobs)) == "registry-route"
    assert "needs" not in jobs["registry-route"]
    assert "packages" not in jobs["registry-route"].get("permissions", {})
    writers = {name for name, job in jobs.items() if "write" in (
        job.get("permissions", {}).get("packages"), job.get("permissions", {}).get("contents"),
    )}
    assert writers == {"build-dev-api-candidate", "build-dev-client-candidate", "build-dev",
                       "build-api", "build-client", "create-release", "promote-release-images"}
    for name in writers:
        job = jobs[name]
        assert "registry-route" in job["needs"]
        if "always()" in job["if"]:
            assert "needs.registry-route.result == 'success'" in job["if"]
        for path, source in _publication_script_sources(job["steps"]).items():
            if path == "scripts/product_updates.py":
                # This is the GitHub inventory CLI's default repository.
                source = source.replace('default="gobifrost/bifrost"', "")
            assert not _UPSTREAM_IMAGE.search(source), path
        for image in ("API_IMAGE", "CLIENT_IMAGE"):
            if image in json.dumps(job["steps"]):
                assert job["env"][image] == "${{ needs.registry-route.outputs." + image.lower().replace("_", "-") + " }}"


_TRIAL_VARS = ("QUALIFIED_WORKERS_RUNTIME_URL", "QUALIFIED_WORKERS_METER_BOUNDS", "CONTAINERS_RELEASE_RATES")


@pytest.mark.parametrize(
    ("owner", "repository", "ref", "missing", "hardcoded", "error"),
    [
        ("MTG-Thomas", "MTG-Thomas/bifrost-upstream", "refs/tags/v2.0.0-rc.2026112402", None, False, None),
        ("gobifrost", "gobifrost/bifrost", "refs/tags/v2.0.0", "all", False, None),
        ("gobifrost", "gobifrost/bifrost", "refs/tags/v2.0.0-rc.2026112402", None, False, "require a fork owner"),
        ("MTG-Thomas", "gobifrost/bifrost", "refs/tags/v2.0.0-rc.2026112402", None, False, "mismatched"),
        ("MTG-Thomas", "MTG-Thomas/bifrost-upstream", "refs/tags/v2.0.0-rc.2026112402", None, True, "Hard-coded"),
        *[
            ("MTG-Thomas", "MTG-Thomas/bifrost-upstream", "refs/tags/v2.0.0-rc.2026112402",
             variable, False, f"Missing required release variable: {variable}")
            for variable in _TRIAL_VARS
        ],
    ],
)
def test_registry_route_before_tag_publication(
    tmp_path: Path, owner: str, repository: str, ref: str, missing: str | None, hardcoded: bool, error: str | None,
) -> None:
    route = _load_workflow(CI_WORKFLOW)["jobs"]["registry-route"]["steps"][1]
    assert route["env"]["REPOSITORY_OWNER"] == "${{ github.repository_owner }}"
    assert route["env"]["REPOSITORY"] == "${{ github.repository }}"
    env = {**os.environ, "REPOSITORY_OWNER": owner, "REPOSITORY": repository, "GITHUB_REF": ref,
           "REGISTRY": "ghcr.io", "GITHUB_OUTPUT": str(tmp_path / "output")}
    for variable in _TRIAL_VARS:
        assert route["env"][variable] == "${{ vars." + variable + " }}"
        env[variable] = " " if missing in (variable, "all") else "test-input"
    for path in (".github/workflows/ci.yml", ".github/actions/sign-image/action.yml",
                 "deploy/capacity/cloudflare/build-containers.sh"):
        fixture = tmp_path / path
        fixture.parent.mkdir(parents=True, exist_ok=True)
        fixture.write_text("GoBiFrOsT/unexpected" if hardcoded else "")
    result = subprocess.run(["bash", "-c", route["run"]], cwd=tmp_path, env=env, capture_output=True, text=True)
    output = tmp_path / "output"
    if error:
        assert result.returncode != 0 and error in result.stdout
        assert not output.exists()
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert output.read_text().splitlines() == [
            f"api-image={owner.lower()}/bifrost-api", f"client-image={owner.lower()}/bifrost-client",
            f"containers-required={int(owner.lower() != 'gobifrost')}",
        ]


@pytest.mark.parametrize(("required", "available", "success"), [("0", "", True), ("1", "", False), ("1", "1", True)])
def test_release_requires_fork_linux_artifacts(required: str, available: str, success: bool) -> None:
    steps = _load_workflow(CI_WORKFLOW)["jobs"]["create-release"]["steps"]
    index = next(i for i, step in enumerate(steps) if step.get("name") == "Require Linux release assets for fork tags")
    assert index < next(i for i, step in enumerate(steps) if step.get("id") == "release")
    step = steps[index]
    assert step["env"] == {
        "CONTAINERS_REQUIRED": "${{ needs.registry-route.outputs.containers-required }}",
        "CONTAINERS_RELEASE": "${{ needs.build-api.outputs.containers-release }}",
    }
    env = {**os.environ, "CONTAINERS_REQUIRED": required, "CONTAINERS_RELEASE": available}
    assert (subprocess.run(["bash", "-c", step["run"]], env=env).returncode == 0) == success


def test_nightly_owns_full_browser_slow_and_clean_build_discovery() -> None:
    workflow = _load_workflow(NIGHTLY_WORKFLOW)
    triggers = workflow["on"]
    jobs = workflow["jobs"]

    assert {"schedule", "workflow_dispatch"} <= triggers.keys()
    assert set(jobs) == {
        "product-browser",
        "slow-unit-contracts",
        "backend-coverage",
        "clean-production-build",
    }

    browser_steps = [
        step.get("run", "") for step in jobs["product-browser"]["steps"]
    ]
    assert any("./test.sh client nightly" in step for step in browser_steps)
    assert all("client docs" not in step for step in browser_steps)

    slow_steps = [
        step.get("run", "") for step in jobs["slow-unit-contracts"]["steps"]
    ]
    assert any("-m slow" in step for step in slow_steps)

    # Coverage is deliberately truthful and off the merge critical path. The
    # former PR upload pointed at a file pytest never generated and Codecov
    # reported success after finding zero reports.
    ci_jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    assert all(
        "codecov/codecov-action" not in step.get("uses", "")
        for step in ci_jobs["test-unit"]["steps"]
    )
    coverage_steps = jobs["backend-coverage"]["steps"]
    measure = next(
        step for step in coverage_steps if step.get("name") == "Measure backend unit coverage"
    )
    assert "--cov=src" in measure["run"]
    assert "--cov-report=xml:/tmp/bifrost/coverage.xml" in measure["run"]
    upload = next(
        step for step in coverage_steps if step.get("name") == "Upload measured coverage to Codecov"
    )
    assert upload["with"]["files"] == "./coverage.xml"
    assert upload["with"]["disable_search"] == "true"
    assert upload["with"]["fail_ci_if_error"] == "true"

    build_steps = jobs["clean-production-build"]["steps"]
    docker_builds = [step for step in build_steps if "with" in step]
    assert len(docker_builds) == 2
    assert all(step["with"]["no-cache"] == "true" for step in docker_builds)

    test_script = TEST_SCRIPT.read_text()
    assert "client_e2e --grep @smoke" in test_script
    assert "--project=docs" not in test_script.split("client_nightly()", 1)[1].split("}", 1)[0]


def test_windows_storage_runs_native_checks_and_blocks_the_shared_gate() -> None:
    jobs = _load_workflow(CI_WORKFLOW)["jobs"]
    windows = jobs["windows-credentials"]
    assert windows["runs-on"] == "windows-2025"
    assert windows["if"] == "github.event_name != 'push' || github.ref != 'refs/heads/main'"
    commands = "\n".join(step.get("run", "") for step in windows["steps"])
    assert "--require-hashes -r requirements-windows-credentials.lock" in commands
    assert "test_windows_credentials.py" in commands
    assert "python scripts/windows_credential_smoke.py" in commands
    source = "\n".join(step.get("run", "") for step in jobs["test-e2e-gate"]["steps"])
    # Native checks must gate even the fast PR path, before its early exit.
    assert source.index("needs.windows-credentials.result") < source.index("pull_request")
    assert 'needs.windows-credentials.result }}" != "success"' in source


SIGN_IMAGE_ACTION = REPO_ROOT / ".github" / "actions" / "sign-image" / "action.yml"


def test_all_image_publishers_use_the_verified_signer() -> None:
    consumers = []
    for path in (CI_WORKFLOW, REPO_ROOT / ".github/workflows/dev-worker-image.yml"):
        for job_id, job in _load_workflow(path)["jobs"].items():
            for step in job["steps"]:
                # Image signing shares one implementation; release-asset blob
                # signing retains its separate existing bundle contract.
                assert "cosign sign --" not in step.get("run", "")
                if step.get("uses") == "./.github/actions/sign-image":
                    consumers.append(job_id)
                    assert "@${{ steps." in step["with"]["image"]
                    assert job["permissions"]["id-token"] == "write"
    assert sorted(consumers) == sorted([
        "build-dev-api-candidate", "build-dev-client-candidate", "build-api", "build-client", "publish",
    ])
    installer = _load_workflow(SIGN_IMAGE_ACTION)["runs"]["steps"][0]
    assert installer["with"]["cosign-release"] == "v3.1.4"


@pytest.mark.parametrize("sign_status,verify_status", [(0, 0), (17, 0), (0, 23)])
def test_image_signing_requires_a_valid_signature_from_this_workflow(
    tmp_path: Path, sign_status: int, verify_status: int
) -> None:
    """Execute the real action shell: either signing or verification failure stops CI."""
    calls_file = tmp_path / "calls.jsonl"
    cosign = tmp_path / "cosign"
    cosign.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['CALLS_FILE'], 'a') as f: f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "sys.exit(int(os.environ['SIGN_STATUS' if sys.argv[1] == 'sign' else 'VERIFY_STATUS']))\n"
    )
    cosign.chmod(0o755)
    image = "ghcr.io/gobifrost/bifrost-api@sha256:" + "a" * 64
    workflow = "gobifrost/bifrost/.github/workflows/ci.yml@refs/heads/gh-readonly-queue/main/pr-1023"
    action = _load_workflow(SIGN_IMAGE_ACTION)
    command = action["runs"]["steps"][1]["run"]
    result = subprocess.run(
        ["bash", "-c", command],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "IMAGE": image,
             "GITHUB_WORKFLOW_REF": workflow, "CALLS_FILE": str(calls_file),
             "SIGN_STATUS": str(sign_status), "VERIFY_STATUS": str(verify_status)},
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == (sign_status or verify_status), result.stderr
    calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
    expected = [["sign", "--yes", image]]
    if sign_status == 0:
        expected.append([
            "verify", "--certificate-identity", f"https://github.com/{workflow}",
            "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com", image,
        ])
    assert calls == expected


def test_dependency_lock_regeneration_uses_production_python_image():
    """Resolve native dependencies with the same interpreter shipped to customers."""
    dockerfile = (REPO_ROOT / "api" / "Dockerfile").read_text()
    production_image = next(
        line.split()[1] for line in dockerfile.splitlines()
        if line.startswith("FROM python:")
    )
    workflow = _load_workflow(
        REPO_ROOT / ".github" / "workflows" / "dependabot-lockfile-regen.yml"
    )
    scripts = [
        step.get("run", "")
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
    ]
    regenerate = next(script for script in scripts if "pip-compile --generate-hashes" in script)
    assert f"{production_image} sh -c" in regenerate
    for command in regenerate.splitlines():
        if "pip-compile --generate-hashes" in command:
            assert "--allow-unsafe" in command
            assert "--no-reuse-hashes" in command
