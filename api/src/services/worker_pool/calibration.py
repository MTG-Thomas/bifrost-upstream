"""A startup measurement belongs to the runtime that actually produced it."""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from src.core.database import get_db_context
from src.models.orm.execution_workers import MANAGED_WORKER_FORMS, ExecutionWorker
from src.services.memory_requests import MemoryBudget
from src.services.worker_pool.sizing import LIVE_STATES
from src.worker.remote.fencing import fence_remote_owner
from src.worker.remote.protocol import LINUX_RUNTIME_PROFILE


def measured_budget(worker: ExecutionWorker, environment: dict[str, Any]) -> MemoryBudget | None:
    calibration = worker.memory_calibration
    if not calibration or calibration.get("environment") != environment:
        return None
    sample = calibration.get("measurement")
    if not sample or sample.get("version") != environment["version"]:
        return None
    if sample.get("package_generation") != environment["package_generation"]:
        return None
    if sample.get("threshold") != environment["threshold"]:
        return None
    return MemoryBudget(
        limit_bytes=sample["limit_bytes"], baseline_bytes=sample["baseline_bytes"],
        threshold=sample["threshold"],
    )


async def load_memory_calibration(
    db: AsyncSession, scope: ColumnElement[bool], environment: dict[str, Any],
) -> list[MemoryBudget]:
    history = (await db.scalars(select(ExecutionWorker).where(
        scope,
        ExecutionWorker.runtime_profile == LINUX_RUNTIME_PROFILE,
        ExecutionWorker.state.in_((*LIVE_STATES, "retired")),
        ExecutionWorker.memory_calibration["environment"] == environment,
        ExecutionWorker.memory_calibration["measurement"].is_not(None),
    ).distinct(ExecutionWorker.memory_limit_bytes).order_by(
        ExecutionWorker.memory_limit_bytes, ExecutionWorker.created_at.desc(),
    ))).all()
    return [sample for worker in history if (sample := measured_budget(worker, environment)) is not None]


async def load_packing_baselines(db: AsyncSession, workers: list[ExecutionWorker]) -> dict[UUID, int]:
    """Forecast pending workers using calibration for their own launch environment."""
    baselines: dict[UUID, int] = {}
    cached: dict[tuple[str, str | None, str | None, str], int | None] = {}
    for worker in workers:
        if worker.form not in MANAGED_WORKER_FORMS or (
            worker.state not in ("reserved", "starting")
            and (worker.state != "ready" or worker.memory_budget_bytes is not None)
        ):
            continue
        environment = (worker.memory_calibration or {}).get("environment")
        if not environment:
            continue
        key = (worker.form, worker.kubernetes_namespace, worker.capacity_scope, json.dumps(environment, sort_keys=True))
        if key not in cached:
            scope = and_(
                ExecutionWorker.form == worker.form,
                ExecutionWorker.kubernetes_namespace == worker.kubernetes_namespace,
                ExecutionWorker.capacity_scope == worker.capacity_scope,
            )
            samples = await load_memory_calibration(db, scope, environment)
            cached[key] = max((sample.baseline_bytes for sample in samples), default=None)
        if (baseline := cached[key]) is not None:
            baselines[worker.id] = baseline
    return baselines


async def record_memory_calibration(
    worker_id: UUID, budget: MemoryBudget, package_generation: int,
) -> None:
    """Record once, before claims open; heartbeats cannot relabel this snapshot."""
    async with get_db_context() as db:
        if not await fence_remote_owner(db, for_update=True):
            return
        worker = await db.scalar(select(ExecutionWorker).where(
            ExecutionWorker.id == worker_id,
            ExecutionWorker.form.in_(MANAGED_WORKER_FORMS),
            ExecutionWorker.state.in_(("starting", "ready", "draining")),
        ).with_for_update())
        if worker is None or not worker.memory_calibration:
            return
        calibration = worker.memory_calibration
        if "measurement" in calibration:
            return
        environment = calibration["environment"]
        if (
            environment["version"] != worker.version
            or environment["package_generation"] != package_generation
            or environment["threshold"] != budget.threshold
            or worker.memory_limit_bytes != budget.limit_bytes
        ):
            return
        worker.memory_calibration = {
            **calibration,
            "measurement": {
                "version": worker.version, "package_generation": package_generation,
                "threshold": budget.threshold, "limit_bytes": budget.limit_bytes,
                "baseline_bytes": budget.baseline_bytes,
            },
        }
        await db.commit()
