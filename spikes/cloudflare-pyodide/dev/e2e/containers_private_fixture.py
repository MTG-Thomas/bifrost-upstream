"""Disposable API entrypoint for the private Solution Git qualification."""

import asyncio
import json
import sys
from pathlib import Path
from uuid import UUID

from git import Repo
from shared.solution_git_sources import SolutionGitCredentials, save_verified_source
from sqlalchemy import func, update
from src.core.database import get_db_context, init_db
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.solutions import Solution
from src.services.worker_pool.liveness import (
    HEARTBEAT_LOST_AFTER,
    end_silent_static_executors,
)

REPO = "https://github.com/fixture-owner/container-update"
CONFIG = Path("/app/scripts/.containers-private.json")


def config():
    return json.loads(CONFIG.read_text())


async def seed(solution_id: UUID) -> None:
    await init_db()
    async with get_db_context() as db:
        solution = await db.get(Solution, solution_id)
        assert solution is not None
        source = await save_verified_source(
            db,
            {
                "purpose": "solution", "stage": "ready", "installation_id": 1,
                "app": {"id": 1, "name": "Local qualification"},
                "repository": {
                    "id": 1, "full_name": "fixture-owner/container-update",
                    "default_branch": "main", "private": True,
                },
            },
            solution.organization_id, "local-qualification",
        )
        solution.version = "1.0.0"
        solution.git_connected = True
        solution.git_repo_url = REPO
        solution.git_source_id = source.id
        solution.git_ref = "update"
        solution.repo_subpath = "solutions/demo"
        await db.commit()


async def retire_paused_workers() -> None:
    """Advance only the static fixtures past the canonical liveness threshold."""
    await init_db()
    async with get_db_context() as db:
        await db.execute(update(ExecutionWorker).where(
            ExecutionWorker.form == "static",
            ExecutionWorker.state.in_(("starting", "ready", "draining")),
        ).values(heartbeat_at=func.now() - 2 * HEARTBEAT_LOST_AFTER))
        await db.commit()
    assert not await end_silent_static_executors(), "Static fixtures must have no workflow claims"


if __name__ == "__main__":
    if sys.argv[1] == "retire-paused-workers":
        asyncio.run(retire_paused_workers())
    else:
        asyncio.run(seed(UUID(sys.argv[1])))
else:
    from src.main import app as api_app
    from src.worker.remote.platform_stage_host import PlatformStageBackend

    app = api_app
    verify = SolutionGitCredentials.verify
    clone = Repo.clone_from
    compute = PlatformStageBackend.compute

    async def verify_fixture(self):
        if self.repo_url != REPO:
            return await verify(self)
        await self._current()
        return config()["token"]

    def clone_fixture(url, *args, **kwargs):
        if url != REPO:
            return clone(url, *args, **kwargs)
        fixture = config()
        env = kwargs["env"].copy()
        assert env["GIT_CONFIG_KEY_0"] == "http.https://github.com/.extraheader"
        env.update(
            GIT_CONFIG_KEY_0=f"http.{fixture['url']}.extraheader",
            GIT_SSL_CAINFO="/app/.phase6-ca.pem",
        )
        return clone(fixture["url"], *args, **{**kwargs, "env": env})

    async def compute_fixture(self, claim, bundle):
        if bundle.get("kind") == "descriptor":
            assert set(bundle) == {"kind", "content"}
            assert config()["token"] not in bundle["content"]
        return await compute(self, claim, bundle)

    SolutionGitCredentials.verify = verify_fixture
    Repo.clone_from = staticmethod(clone_fixture)
    PlatformStageBackend.compute = compute_fixture
