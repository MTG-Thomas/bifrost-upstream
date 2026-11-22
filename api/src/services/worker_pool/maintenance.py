"""Keep the executor registry free of dead rows."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, exists, or_

from src.core.database import get_db_context
from src.models.orm.cloudflare_capacity import CloudflareWorkerReservation
from src.models.orm.execution_workers import ExecutionWorker
from src.scheduler.registry import ScheduledTaskOutcome
from src.services.worker_pool.cloudflare_spend import pending_ledger_receipt

ENDED_RETENTION = timedelta(days=7)


async def prune_execution_workers() -> ScheduledTaskOutcome:
    """Delete executors that ended over ``ENDED_RETENTION`` ago (silent ones
    are ended by the control plane: ``worker_pool.liveness``)."""
    now = datetime.now(UTC)
    this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    previous_month = (this_month - timedelta(days=1)).replace(day=1)
    async with get_db_context() as db:
        pruned = (
            await db.execute(
                delete(ExecutionWorker).where(
                    ExecutionWorker.state.in_(("retired", "lost")),
                    ExecutionWorker.ended_at < now - ENDED_RETENTION,
                    (ExecutionWorker.form != "external")
                    | ExecutionWorker.capacity_resource_deleted_at.is_not(None),
                    # Keep the reservation's scope until WorkerLedger is settled.
                    ~pending_ledger_receipt(ExecutionWorker.id).exists(),
                    # The Container reserve carries unsettled exposure and
                    # UTC-month history beyond the executor retention window.
                    ~exists().where(
                        CloudflareWorkerReservation.worker_id == ExecutionWorker.id,
                        CloudflareWorkerReservation.container_reserved_at.is_not(None),
                        or_(
                            CloudflareWorkerReservation.container_stopped_at.is_(None),
                            CloudflareWorkerReservation.container_stopped_at >= previous_month,
                        ),
                    ),
                )
            )
        ).rowcount
    return ScheduledTaskOutcome(summary=f"Pruned {pruned} ended executors")


__all__ = ["prune_execution_workers"]
