"""Isolated child-process runner for one claimed platform job."""

from __future__ import annotations

import asyncio
import logging
import sys
from functools import partial
from typing import TypedDict
from uuid import UUID

from pydantic import ValidationError
from shared.app_build_errors import APP_BUILD_JOB_TYPES, app_build_memory_message
from sqlalchemy import select

from src.core.database import close_db, get_db_context, init_db
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobDeferred,
    PlatformJobFailure,
    PlatformJobHandler,
    PlatformJobRequiresAction,
)
from src.jobs.platform.policy import PlatformJobExecutor
from src.jobs.platform.registry import get_platform_job_definition
from src.models.orm.platform_jobs import PlatformJob
from src.services.audit_context import ActorContext, clear_actor, set_actor
from src.services.platform_jobs import (
    defer_platform_job,
    finish_platform_job,
    retry_platform_job_failure,
)

logger = logging.getLogger(__name__)


class _ReceiptArgs(TypedDict, total=False):
    result: dict[str, str]


async def run_claimed_platform_job(
    job_id: UUID, lease_token: UUID, *, stage_handler: PlatformJobHandler | None = None,
    executor: PlatformJobExecutor | None = None,
) -> bool:
    finish = finish_platform_job
    if stage_handler is not None:
        finish = partial(finish_platform_job, stage_owned=True)
    async with get_db_context() as db:
        if stage_handler is not None:
            from src.worker.remote.platform_stage_host import lock_stage_job

            try:
                await lock_stage_job(db, job_id, lease_token)
            except PlatformJobCancelled:
                return False
        job = (
            await db.execute(
                select(PlatformJob).where(
                    PlatformJob.id == job_id,
                    PlatformJob.lease_token == lease_token,
                    PlatformJob.status.in_(("running", "cancel_requested")),
                )
            )
        ).scalar_one_or_none()
        if job is None or job.status == "cancel_requested":
            return False
        definition = get_platform_job_definition(job.job_type)
        if definition is None:
            if stage_handler is not None:
                await db.rollback()
            await finish(
                job_id,
                lease_token,
                status="failed",
                error_code="unknown_job_type",
                error_message=f"No handler is registered for {job.job_type}.",
            )
            return False
        execution_receipt = (
            {"execution_class": executor.value}
            if executor is not None and definition.policy.record_execution_class
            else {}
        )
        receipt_args: _ReceiptArgs = {"result": execution_receipt} if execution_receipt else {}
        payload_model = definition.payload_model_for_version(job.payload_version)
        if payload_model is None:
            if stage_handler is not None:
                await db.rollback()
            await finish(
                job_id,
                lease_token,
                status="failed",
                error_code="unsupported_payload_version",
                error_message=(
                    f"{job.job_type} payload version {job.payload_version} "
                    f"is not supported."
                ),
                **receipt_args,
            )
            return False
        payload_data = job.payload
        if job.encrypted_payload is not None:
            from src.core.security import decrypt_secret

            payload_data = decrypt_secret(job.encrypted_payload)
        context = PlatformJobContext(
            job_id=job.id,
            lease_token=lease_token,
            organization_id=job.organization_id,
            requested_by_user_id=job.requested_by_user_id,
            requested_by_email=job.requested_by_email,
            requested_by_name=job.requested_by_name,
            impersonated_by_user_id=job.impersonated_by_user_id,
            impersonation_session_id=job.impersonation_session_id,
            checkpoint=job.result,
        )

    try:
        requested_by_user_id = UUID(context.requested_by_user_id)
    except ValueError:
        requested_by_user_id = None
    actor_token = set_actor(
        ActorContext(
            user_id=requested_by_user_id,
            organization_id=context.organization_id,
            email=context.requested_by_email,
            name=context.requested_by_name,
            source="platform_job",
            # Background system work, not a browser/CLI/MCP call.
            surface="service",
            impersonated_by_user_id=context.impersonated_by_user_id,
            impersonation_session_id=context.impersonation_session_id,
        )
    )

    try:
        payload = (
            payload_model.model_validate_json(payload_data)
            if job.encrypted_payload is not None
            else payload_model.model_validate(payload_data)
        )
        handler = definition.handler if stage_handler is None else stage_handler
        result = await handler(context, payload)
        return await finish(
            job_id,
            lease_token,
            status="succeeded",
            result={**(result or {}), **execution_receipt},
        )
    except PlatformJobCancelled:
        if stage_handler is not None:
            return False
        return await finish(
            job_id,
            lease_token,
            status="cancelled",
            **receipt_args,
        )
    except PlatformJobDeferred as exc:
        if stage_handler is not None:
            return await finish(
                job_id, lease_token, status="failed", error_code="unsupported_stage_deferral",
                error_message="A Container stage must settle or release its current PlatformJob attempt.",
            )
        return await defer_platform_job(
            job_id,
            lease_token,
            phase=exc.phase,
            result=exc.result,
        )
    except PlatformJobFailure as exc:
        if execution_receipt:
            exc.result = {**(exc.result or {}), **execution_receipt}
        if stage_handler is not None:
            return await finish(
                job_id, lease_token, status="failed", error_code=exc.code,
                error_message=exc.message, result=exc.result,
            )
        return await retry_platform_job_failure(
            job_id, lease_token, exc,
            enabled=definition.policy.retry_on_failure, retry_delay=definition.policy.retry_delay,
        )
    except PlatformJobRequiresAction as exc:
        return await finish(
            job_id,
            lease_token,
            status="requires_action",
            phase=exc.phase,
            result={**exc.result, **execution_receipt},
        )
    except ValidationError as exc:
        return await finish(
            job_id,
            lease_token,
            status="failed",
            error_code="invalid_payload",
            error_message=str(exc),
            **receipt_args,
        )
    except Exception as exc:
        logger.exception(
            "Unhandled platform-job handler failure", extra={"job_id": str(job_id)}
        )
        build_oom = isinstance(exc, MemoryError) and job.job_type in APP_BUILD_JOB_TYPES
        return await finish(
            job_id,
            lease_token,
            status="failed",
            error_code="memory_pressure" if build_oom else "handler_error",
            error_message=(
                app_build_memory_message()
                if build_oom
                else "Platform job failed unexpectedly; see server logs."
            ),
            error_retryable=False,
            **receipt_args,
        )
    finally:
        clear_actor(actor_token)


async def _main(job_id: str, lease_token: str, executor: PlatformJobExecutor | None = None) -> int:
    await init_db()
    try:
        completed = await run_claimed_platform_job(
            UUID(job_id),
            UUID(lease_token),
            executor=executor,
        )
        return 0 if completed else 1
    finally:
        await close_db()


def _run_cli() -> int:
    if len(sys.argv) not in (3, 4):
        raise SystemExit("usage: platform.runner JOB_ID LEASE_TOKEN [EXECUTOR]")
    from src.services.execution.oom_priority import volunteer_for_oom_kill

    volunteer_for_oom_kill()
    executor = PlatformJobExecutor(sys.argv[3]) if len(sys.argv) == 4 else None
    return asyncio.run(_main(sys.argv[1], sys.argv[2], executor))


if __name__ == "__main__":
    raise SystemExit(_run_cli())
