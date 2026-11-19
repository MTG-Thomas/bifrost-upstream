"""Solution runtime source is eligible only after successful finalization."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from src.models.orm import Solution
from src.services.solutions.deploy import (
    SolutionBundle,
    SolutionDeployer,
    SolutionFinalizeIncomplete,
)


@pytest.mark.parametrize("fail", [False, True])
async def test_deploy_publishes_source_generation_only_after_finalize(db_session, monkeypatch, fail):
    previous_generation = uuid4()
    solution = Solution(
        id=uuid4(), slug=f"generation-{uuid4().hex}", name="Generation fixture",
        runtime_source_generation=previous_generation,
    )
    db_session.add(solution)
    await db_session.flush()
    deployer = SolutionDeployer(db_session)
    publish_source = AsyncMock()
    monkeypatch.setattr(deployer, "_write_source_artifact", AsyncMock())
    monkeypatch.setattr(deployer, "_write_python", publish_source)
    monkeypatch.setattr(
        deployer, "_upload_compiled_dists",
        AsyncMock(side_effect=OSError("fixture app upload failed") if fail else None),
    )
    monkeypatch.setattr(deployer, "_delete_stale_app_dist", AsyncMock())
    monkeypatch.setattr("src.services.solutions.deploy._FINALIZE_BACKOFF_S", 0)

    result = await deployer.deploy(SolutionBundle(solution=solution, version="2.0.0"))
    await db_session.flush()
    assert solution.runtime_source_generation is None
    publish_source.assert_not_awaited()

    if fail:
        with pytest.raises(SolutionFinalizeIncomplete):
            await result.finalize_s3()
        assert solution.runtime_source_generation is None
    else:
        await result.finalize_s3()
        assert solution.runtime_source_generation not in {None, previous_generation}
    publish_source.assert_awaited_once_with(solution.id, {})



@pytest.mark.parametrize("mode", ["report", "failure", "discard"])
async def test_deploy_defers_advisory_diagnostics_and_scans_shared_module_once(
    db_session, monkeypatch, caplog, mode,
):
    from bifrost import cloudflare_report
    from src import config

    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(cloudflare_diagnostics_enabled=True))
    scan = AsyncMock(wraps=cloudflare_report.diagnose)
    if mode == "failure":
        scan.side_effect = RuntimeError("fixture diagnostic failure")
    monkeypatch.setattr(cloudflare_report, "diagnose", scan)

    solution = Solution(id=uuid4(), slug=f"diagnostics-{uuid4().hex}", name="Diagnostics fixture")
    db_session.add(solution)
    await db_session.flush()
    path = "workflows/example.py"
    bundle = SolutionBundle(
        solution=solution,
        workflows=[
            {"id": str(uuid4()), "path": path, "function_name": name, "name": name}
            for name in ("first", "second")
        ],
        python_files={path: "import subprocess\ndef first(): pass\ndef second(): pass\n"},
    )
    result = await SolutionDeployer(db_session).deploy(
        bundle, include_cloudflare_diagnostics=mode != "discard",
    )
    assert result.workflows_upserted == 2
    scan.assert_not_awaited()

    if mode == "discard":
        assert result.collect_cloudflare_diagnostics is None
        return

    assert result.collect_cloudflare_diagnostics is not None
    diagnostics = await result.collect_cloudflare_diagnostics()
    scan.assert_awaited_once()
    if mode == "failure":
        assert diagnostics is None
        assert "Cloudflare diagnostics failed" in caplog.text
    else:
        assert diagnostics is not None
        assert [report["function_name"] for report in diagnostics] == ["first", "second"]
        assert [report["findings"][0]["rule_id"] for report in diagnostics] == ["CF_PROCESS"] * 2
