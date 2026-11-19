"""API orchestration for the Container variant of the Solution update sweep."""

from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from uuid import UUID

from bifrost.solution_descriptor import DESCRIPTOR_FILENAME, is_solution_workspace
from fastapi import HTTPException
from pydantic import BaseModel, ValidationError
from shared.solution_git_sources import clone_solution_repository, load_source
from shared.solution_git_stage import (
    MAX_DESCRIPTOR_BYTES,
    DescriptorInput,
    GitInput,
    VersionResult,
    credential_free_https,
)
from sqlalchemy import select

from src.core.database import get_db_context
from src.jobs.platform.base import PlatformJobContext, PlatformJobFailure
from src.models.orm.config import SystemConfig
from src.models.orm.solutions import Solution
from src.services.events.builtins import emit_solution_update_available
from src.services.solutions.git_sync import resolve_repo_subpath
from src.services.solutions.update_check import compute_update_available
from src.worker.remote.platform_stage_host import lock_stage_job

if TYPE_CHECKING:
    from src.jobs.schedulers.platform_jobs import ClaimedPlatformJob
    from src.worker.remote.platform_stage_host import PlatformStageBackend

logger = logging.getLogger(__name__)


def _source(solution: Solution) -> tuple[str | None, str | None, str | None, UUID | None, UUID | None]:
    return (
        solution.git_repo_url, solution.git_ref, solution.repo_subpath,
        solution.git_source_id, solution.organization_id,
    )


async def _prepare(solution: Solution) -> tuple[GitInput | DescriptorInput, str | None]:
    url, ref, subpath, source_id, org = _source(solution)
    assert url is not None
    if source_id is None and credential_free_https(url):
        return GitInput(repo=url, ref=ref, subpath=subpath), None

    # App installation credentials and legacy transports stay on the API.
    # The stage only receives the selected descriptor, never Git metadata.
    generation = None
    with TemporaryDirectory(prefix="bifrost-update-stage-") as directory:
        checkout = Path(directory)
        async with get_db_context() as db:
            if source_id is not None:
                _, source = await load_source(db, source_id, org)
                generation = str(source["generation"])
            await clone_solution_repository(db, source_id, org, url, checkout, ref)
        root = resolve_repo_subpath(checkout, subpath)
        if not is_solution_workspace(root):
            raise ValueError("A Solution descriptor is required")
        with (root / DESCRIPTOR_FILENAME).open("rb") as descriptor:
            content = descriptor.read(MAX_DESCRIPTOR_BYTES + 1)
        return DescriptorInput(content=content.decode()), generation


async def _apply(
    claim: ClaimedPlatformJob, original: Solution, version: str | None, source_generation: str | None,
) -> bool:
    notify = False
    async with get_db_context() as db:
        await lock_stage_job(db, claim.id, claim.lease_token)
        solution = await db.scalar(select(Solution).where(
            Solution.id == original.id, Solution.git_connected.is_(True),
        ).with_for_update())
        if solution is None or _source(solution) != _source(original) or solution.version != original.version:
            return False
        if solution.git_source_id is not None:
            await db.scalar(select(SystemConfig.id).where(
                SystemConfig.id == solution.git_source_id,
            ).with_for_update(read=True))
            try:
                _, source = await load_source(db, solution.git_source_id, solution.organization_id)
            except HTTPException:
                return False
            if source["generation"] != source_generation:
                return False
        available = compute_update_available(installed=solution.version, remote=version)
        if solution.update_available_version != available:
            notify = available is not None and solution.update_available_version is None
            solution.update_available_version = available
        await db.commit()
    if notify and available is not None:
        await emit_solution_update_available(
            solution_id=solution.id, slug=solution.slug, organization_id=solution.organization_id,
            installed_version=solution.version, available_version=available,
        )
    return available is not None


async def run_solution_update_stage(
    backend: PlatformStageBackend, claim: ClaimedPlatformJob,
    context: PlatformJobContext, payload: BaseModel,
) -> dict:
    async with get_db_context() as db:
        solutions = (await db.scalars(select(Solution).where(
            Solution.git_connected.is_(True), Solution.git_repo_url.is_not(None),
        ).order_by(Solution.id))).all()

    updates_found = 0
    failed = 0
    for solution in solutions:
        try:
            bundle, generation = await _prepare(solution)
        except Exception:  # noqa: BLE001 - Git adapters raise different errors; details can contain credentials.
            failed += 1
            logger.warning("Could not prepare Solution update stage for %s", solution.id)
            continue
        try:
            raw = await backend.compute(claim, bundle.model_dump(mode="json"))
        except PlatformJobFailure as exc:
            if exc.code != "stage_compute_failed":
                raise
            failed += 1
            logger.warning("Container could not compute Solution version for %s", solution.id)
            continue
        try:
            result = VersionResult.model_validate(raw)
            if isinstance(bundle, GitInput) != (result.commit is not None):
                raise ValueError("Stage result does not match its input kind")
        except (ValidationError, ValueError):
            raise PlatformJobFailure("invalid_stage_result", "Container returned an invalid Solution version.") from None
        if await _apply(claim, solution, result.version, generation):
            updates_found += 1

    return {"checked": len(solutions), "updates_found": updates_found, "check_failures": failed}
