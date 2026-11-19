"""Solution runtime source is eligible only after successful finalization."""

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
