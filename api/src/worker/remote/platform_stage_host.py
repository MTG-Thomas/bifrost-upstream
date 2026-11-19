"""API-owned PlatformJob stages over the existing launcher control socket."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, suppress
from hashlib import sha256
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobFailure,
)
from src.jobs.schedulers.platform_jobs import ClaimedPlatformJob, _handle_runner_loss
from src.models.orm.platform_jobs import PlatformJob
from src.worker.remote.fencing import current_remote_lease_ref, fence_remote_owner
from src.worker.remote.platform_stage_protocol import (
    MAX_STAGE_INPUT_BYTES,
    StageBinding,
    encode_bundle,
)
from src.worker.remote.protocol import (
    CredentialRouteChanged,
    OpaqueDispatchInterrupted,
    ProtocolError,
)

if TYPE_CHECKING:
    from src.worker.remote.host import Bridge


async def lock_stage_job(db: AsyncSession, job_id: UUID, lease_token: UUID) -> PlatformJob:
    """Serialize mutations with both API-owner replacement and job lease loss."""
    if current_remote_lease_ref() is None or not await fence_remote_owner(db):
        raise PlatformJobCancelled
    job = await db.scalar(select(PlatformJob).where(
        PlatformJob.id == job_id, PlatformJob.lease_token == lease_token,
        PlatformJob.status == "running", PlatformJob.lease_expires_at > func.clock_timestamp(),
    ).with_for_update())
    if job is None:
        raise PlatformJobCancelled
    return job


StageHandler = Callable[
    ["PlatformStageBackend", ClaimedPlatformJob, PlatformJobContext, BaseModel],
    Awaitable[dict[str, Any]],
]


class PlatformStageBackend:
    """One stage at a time. The shared PlatformJob supervisor owns the lease."""

    def __init__(
        self, bridge: Bridge, handlers: Mapping[str, tuple[str, StageHandler]],
    ) -> None:
        self.bridge = bridge
        self.handlers = handlers
        self.active: StageBinding | None = None
        self.releasing = False
        self.busy = False

    def memory(self) -> tuple[int, int]:
        return self.bridge.memory()

    async def spawn(self, claim: ClaimedPlatformJob) -> ApiStageJobProcess:
        if self.busy or self.active is not None:
            raise RuntimeError("Platform stage admission is occupied")
        _, handler = self.handlers[claim.job_type]
        self.busy = True
        return ApiStageJobProcess(self, claim, handler)

    @asynccontextmanager
    async def _authorize(self, binding: StageBinding, generation: int, *, release: bool):
        # The bridge holds its send lock; the DB locks span only the bounded
        # command send, never the reply or a Container subprocess.
        async with get_db_context() as db:
            if release:
                if current_remote_lease_ref() is None or not await fence_remote_owner(db):
                    raise PlatformJobCancelled
            else:
                job = await lock_stage_job(db, UUID(binding.job_id), UUID(binding.lease_token))
                if job.remote_stage != self._attribution(binding, job.attempt):
                    raise PlatformJobCancelled
            owner = current_remote_lease_ref()
            if owner is None or owner.lease.session_generation != generation:
                raise ConnectionError("Platform stage launcher session changed")
            async with asyncio.timeout(3):
                yield

    async def _request(
        self, action: str, binding: StageBinding, *, release: bool = False, **payload: Any,
    ) -> dict[str, Any]:
        generation = self.bridge.lease.session_generation
        gate = self.bridge.inventory_ready if release else self.bridge.reconciliation_ready
        if not self.bridge.connected or not gate.is_set():
            raise ConnectionError("Platform stage launcher is reconciling")
        try:
            reply = await self.bridge.request(
                action, {**binding.wire(), **payload}, expected_generation=generation,
                authorization=self._authorize(binding, generation, release=release),
            )
        except (CredentialRouteChanged, OpaqueDispatchInterrupted) as exc:
            raise ConnectionError("Platform stage launcher session changed") from exc
        if StageBinding.parse(reply) != binding:
            raise ProtocolError("Platform stage reply binding changed")
        return reply

    def _attribution(self, binding: StageBinding, attempt: int) -> dict[str, Any]:
        return {
            "worker_id": str(self.bridge.lease.worker_id),
            "attempt": attempt,
            "stage_id": binding.stage_id,
            "capability": binding.capability,
            "input_sha256": binding.input_sha256,
        }

    async def compute(self, claim: ClaimedPlatformJob, bundle: dict[str, Any]) -> dict[str, Any]:
        if self.active is not None:
            raise RuntimeError("Previous platform stage has not been released")
        capability, _ = self.handlers[claim.job_type]
        binding = StageBinding(
            str(uuid4()), str(claim.id), str(claim.lease_token), capability,
            sha256(encode_bundle(bundle, MAX_STAGE_INPUT_BYTES)).hexdigest(),
        )
        async with get_db_context() as db:
            job = await lock_stage_job(db, claim.id, claim.lease_token)
            job.remote_stage = self._attribution(binding, job.attempt)
            await db.commit()
        self.active = binding
        try:
            while True:
                try:
                    status = await self._request("stage_status", binding)
                    match status.get("state"):
                        case "unknown":
                            await self._request("stage_start", binding, bundle=bundle)
                        case "succeeded":
                            if isinstance(result := status.get("result"), dict):
                                return result
                            raise ProtocolError("Platform stage result must be an object")
                        case "failed":
                            raise PlatformJobFailure("stage_compute_failed", "Container compute failed.")
                        case "running":
                            pass
                        case _:
                            raise ProtocolError("Invalid platform stage state")
                except (ConnectionError, TimeoutError):
                    # Read back the retained stage before another start.
                    pass
                await asyncio.sleep(0.25)
        finally:
            await self._cleanup(binding)

    async def _release(self, binding: StageBinding) -> None:
        async with asyncio.timeout(10):
            while True:
                reply = await self._request("stage_status", binding, release=True)
                if reply.get("state") in ("unknown", "released"):
                    return
                try:
                    reply = await self._request("stage_release", binding, release=True)
                    if reply.get("state") == "released":
                        return
                    raise ProtocolError("Platform stage release was not confirmed")
                except TimeoutError:
                    # Observe the retained tombstone before retrying a release.
                    continue

    async def _cleanup(self, binding: StageBinding) -> None:
        self.releasing = True
        try:
            await self._release(binding)
        except PlatformJobCancelled:
            # The successor reconciles the retained inventory under its fence.
            raise
        except (ConnectionError, TimeoutError, ProtocolError):
            # Do not reopen admission while physical cleanup is uncertain.
            if self.bridge.connected:
                with suppress(OSError, RuntimeError, TimeoutError):
                    async with asyncio.timeout(3):
                        await self.bridge.socket.close(code=1011, reason="platform stage cleanup pending")
            raise PlatformJobFailure("stage_cleanup_pending", "Container stage cleanup awaits reconnect.") from None
        else:
            self.active = None
            self.releasing = False

    async def reconcile(self, raw: object) -> None:
        if not isinstance(raw, list) or len(raw) > 1:
            raise ProtocolError("Invalid platform stage inventory")
        if not raw:
            if self.releasing:
                self.active = None
                self.releasing = False
            elif self.active is not None:
                # The socket may have dropped before the first start. The
                # status-before-start loop will determine whether to send it.
                return
            return
        binding = StageBinding.parse(raw[0])
        if binding.capability not in {capability for capability, _ in self.handlers.values()}:
            raise ProtocolError("Unknown inventoried platform stage capability")
        if self.active is not None and binding != self.active:
            raise ProtocolError("Platform stage inventory changed its binding")
        if self.active is not None and not self.releasing:
            return

        job_id, token = UUID(binding.job_id), UUID(binding.lease_token)
        async with get_db_context() as db:
            job = await db.get(PlatformJob, job_id)
            bound = (
                job is not None and job.lease_token == token
                and job.remote_stage == self._attribution(binding, job.attempt)
            )
        await self._release(binding)
        if bound:
            await _handle_runner_loss(
                job_id, token, error_code="runner_lost",
                error_message="The API-owned Container stage lost its orchestrator.",
                memory_sample=self.memory(),
            )
        self.active = None
        self.releasing = False


class ApiStageJobProcess:
    """Expose the API task through the same supervisor interface as a child."""

    def __init__(self, backend: PlatformStageBackend, claim: ClaimedPlatformJob, handler: StageHandler):
        self.returncode: int | None = None
        self.task = asyncio.create_task(self._run(backend, claim, handler), name=f"platform-stage:{claim.id}")

    async def _run(self, backend: PlatformStageBackend, claim: ClaimedPlatformJob, handler: StageHandler) -> int:
        from src.jobs.platform.runner import run_claimed_platform_job

        async def run(context: PlatformJobContext, payload: BaseModel) -> dict[str, Any]:
            return await handler(backend, claim, context, payload)

        code = 1
        try:
            done = await run_claimed_platform_job(claim.id, claim.lease_token, stage_handler=run)
            code = 0 if done else 1
            return code
        finally:
            self.returncode = code
            backend.busy = False

    async def wait(self) -> int:
        return await asyncio.shield(self.task)

    async def terminate(self) -> None:
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
