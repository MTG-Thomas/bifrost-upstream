"""Managed Workers timing is telemetry only and never confers admission."""

import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, literal, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.models.orm.cloudflare_capacity import CloudflareWorkerReservation
from src.models.orm.executions import Execution

logger = logging.getLogger(__name__)


def timing_values(column: Any, **stages: datetime) -> Any:
    stamps = {key: value.isoformat() for key, value in stages.items()}
    # First observation wins across replays and independently arriving stages.
    return literal(stamps, JSONB).op("||")(func.coalesce(column, literal({}, JSONB)))


async def record_execution_timing(db: AsyncSession, execution_id: UUID, **stages: datetime) -> None:
    await db.execute(update(Execution).where(Execution.id == execution_id).values(
        cloudflare_timing=timing_values(Execution.cloudflare_timing, **stages),
    ))


async def record_transport_timing(
    *, execution_id: UUID | None = None, worker_id: UUID | None = None, **stages: datetime,
) -> None:
    try:
        async with get_db_context() as db:
            if execution_id is not None:
                await record_execution_timing(db, execution_id, **stages)
            else:
                assert worker_id is not None
                await db.execute(update(CloudflareWorkerReservation).where(
                    CloudflareWorkerReservation.worker_id == worker_id,
                ).values(launch_timing=timing_values(CloudflareWorkerReservation.launch_timing, **stages)))
    except Exception:
        logger.warning("Could not persist managed Workers timing", exc_info=True)


def worker_terminal_timing(value: object) -> dict[str, datetime]:
    if not isinstance(value, dict):
        return {}
    stages: dict[str, datetime] = {}
    for key in ("engine_ready_at", "entry_at"):
        stamp = value.get(key)
        # Accept only UTC epoch milliseconds in the supported date range.
        if type(stamp) is int and 0 < stamp < 253402300800000:
            stages[key] = datetime.fromtimestamp(stamp / 1000, UTC)
    return stages
