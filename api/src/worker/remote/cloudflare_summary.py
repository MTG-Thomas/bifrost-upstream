import asyncio
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.models.orm.agent_runs import AgentRun
from src.models.orm.execution_workers import ExecutionWorkerClaim
from src.models.orm.work_dispatches import WorkDispatch
from src.services.execution.summary_dispatch import (
    FAMILY,
    claim_summary,
    fail_remote_summary,
    release_summary,
)
from src.services.memory_requests import SUMMARY_REQUEST_BYTES
from src.services.worker_pool.claims import PoolClaim
from src.worker.remote.fencing import fence_remote_owner
from src.worker.remote.protocol import CredentialRouteChanged

_STATES = {"not_started", "completed", "uncertain"}


class SummaryTransport(Protocol):
    async def request(
        self, action: str, payload: dict[str, Any], *, expected_generation: int | None = None,
    ) -> dict[str, Any]: ...


@dataclass
class SummaryRoute:
    run_id: UUID
    claim_id: UUID
    memory_request_bytes: int
    deadline: datetime
    finished: asyncio.Future[None]
    reservation_key: str
    handle: str | None = None
    receipt: tuple[str, str] | None = None

    @property
    def id(self) -> str:
        return f"summary:{self.run_id}:{self.claim_id}"


def _handle(data: dict[str, Any]) -> str:
    handle = data.get("runtime_handle")
    if "pid" in data or not isinstance(handle, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{1,255}", handle):
        raise ValueError("Invalid opaque summary handle")
    return handle


class CloudflareSummaryRoutes:
    """The API owns the summary claim. The launcher owns a single bounded provider call."""

    def __init__(self, transport: SummaryTransport, worker_id: UUID, endpoint: str) -> None:
        self.transport = transport
        self.worker_id = worker_id
        self.endpoint = endpoint
        self.routes: dict[str, SummaryRoute] = {}

    async def run(self, claim: PoolClaim, generation: int) -> None:
        from src.services.execution.cloudflare_summary import (
            TIMEOUT_SECONDS,
            prepare_completion,
        )

        run_id = claim.subject_id
        claim_id = UUID(claim.message["claim_id"])
        deadline = datetime.now(UTC) + timedelta(seconds=TIMEOUT_SECONDS)
        route = SummaryRoute(
            run_id, claim_id, claim.memory_request_bytes, deadline,
            asyncio.get_running_loop().create_future(), str(claim.dispatch_id),
        )
        self.routes[route.id] = route
        try:
            async with get_db_context() as db:
                if not await fence_remote_owner(db, for_update=True):
                    raise ConnectionError("Summary owner changed before preparation")
                run = await db.scalar(select(AgentRun).where(
                    AgentRun.id == run_id, AgentRun.status == "completed", AgentRun.summary_status == "pending",
                ).with_for_update(of=AgentRun))
                owned = await db.scalar(select(WorkDispatch.id).join(
                    ExecutionWorkerClaim, ExecutionWorkerClaim.subject_id == WorkDispatch.subject_id,
                ).where(
                    WorkDispatch.id == claim.dispatch_id, WorkDispatch.family == FAMILY,
                    WorkDispatch.assigned_worker_id == self.worker_id, WorkDispatch.subject_id == run_id,
                    ExecutionWorkerClaim.id == claim_id, ExecutionWorkerClaim.worker_id == self.worker_id,
                    ExecutionWorkerClaim.family == FAMILY,
                ))
                if run is None or owned is None:
                    self._resolve(route)
                    return
                completion = await prepare_completion(db, run, self.endpoint)
                if not await claim_summary(db, run_id, claim.dispatch_id):
                    self._resolve(route)
                    return
                await db.commit()
        except ValueError:
            await release_summary(
                worker_id=self.worker_id, run_id=run_id, dispatch_id=claim.dispatch_id,
                claim_id=claim_id, linux_only=True,
            )
            self._resolve(route)
            return

        try:
            reply = await self.transport.request("cloudflare_summary", {
                "route_id": route.id,
                "run_id": str(run_id),
                "claim_id": str(claim_id),
                "deadline": deadline.isoformat(),
                "memory_request_bytes": route.memory_request_bytes,
                "context": completion,
            }, expected_generation=generation)
        except (ConnectionError, TimeoutError):
            # A lost receipt leaves a provider call possible. Inventory or its terminal decides.
            pass
        except (CredentialRouteChanged, MemoryError, RuntimeError):
            await self._fail(route, "The Cloudflare summary request did not start")
            return
        else:
            try:
                handle = _handle(reply)
                if route.handle is None:
                    route.handle = handle
            except ValueError:
                # Treat a malformed reply as ambiguous, like a lost one.
                pass
        await self.wait(route)

    async def wait(self, route: SummaryRoute) -> None:
        from src.services.execution.cloudflare_summary import UNCERTAIN_ERROR

        seconds = max(0, (route.deadline - datetime.now(UTC)).total_seconds()) + 15
        try:
            async with asyncio.timeout(seconds):
                await asyncio.shield(route.finished)
        except TimeoutError:
            try:
                await self.transport.request("terminate", {
                    "route_id": route.id, "error_type": "TimeoutError", "error": "Summary deadline elapsed",
                })
            except (ConnectionError, TimeoutError, CredentialRouteChanged):
                pass
            await self._fail(route, UNCERTAIN_ERROR)

    async def _fail(self, route: SummaryRoute, reason: str) -> None:
        await fail_remote_summary(
            worker_id=self.worker_id, run_id=route.run_id, claim_id=route.claim_id, reason=reason,
        )
        self._resolve(route)

    def _resolve(self, route: SummaryRoute) -> None:
        if not route.finished.done():
            route.finished.set_result(None)
        self.routes.pop(route.id, None)

    async def reconcile(self, live: list[Any], terminals: list[Any]) -> list[SummaryRoute]:
        from src.services.execution.cloudflare_summary import (
            TIMEOUT_SECONDS,
            UNCERTAIN_ERROR,
        )

        observed: set[str] = set()
        adopted = []
        for terminal, entries in ((False, live), (True, terminals)):
            for entry in entries:
                run_id, claim_id = UUID(entry["subject_id"]), UUID(entry["claim_id"])
                deadline = datetime.fromisoformat(entry["deadline"])
                size = entry.get("memory_request_bytes")
                handle = _handle(entry)
                route_id = f"summary:{run_id}:{claim_id}"
                if (
                    entry.get("route_id") != route_id or route_id in observed or deadline.utcoffset() is None
                    or deadline > datetime.now(UTC) + timedelta(seconds=TIMEOUT_SECONDS)
                    or type(size) is not int or size != SUMMARY_REQUEST_BYTES
                ):
                    raise ValueError("Invalid opaque summary inventory")
                observed.add(route_id)
                receipt = (entry["event_id"], entry["attempt_state"]) if terminal else None
                if receipt is not None and (not isinstance(receipt[0], str) or not isinstance(receipt[1], str) or receipt[1] not in _STATES):
                    raise ValueError("Invalid opaque summary receipt")
                route = self.routes.get(route_id)
                if route is None:
                    async with get_db_context() as db:
                        if not await fence_remote_owner(db, for_update=True):
                            raise ConnectionError("Summary owner changed during reconciliation")
                        run = await self._current_run(db, run_id, claim_id)
                    if run is None:
                        if not terminal:
                            raise ValueError("Live summary has no generating claim")
                        continue  # ACK an already settled or superseded receipt.
                    route = SummaryRoute(
                        run_id, claim_id, size, deadline,
                        asyncio.get_running_loop().create_future(), route_id,
                    )
                    self.routes[route_id] = route
                    adopted.append(route)
                elif (
                    route.deadline != deadline or route.memory_request_bytes != size
                    or route.handle not in (None, handle) or route.receipt not in (None, receipt)
                ):
                    raise ValueError("Opaque summary route metadata changed")
                route.handle, route.receipt = handle, receipt

        async with get_db_context() as db:
            if not await fence_remote_owner(db, for_update=True):
                raise ConnectionError("Summary owner changed during reconciliation")
            generating = (await db.execute(select(ExecutionWorkerClaim.subject_id, ExecutionWorkerClaim.id).join(
                AgentRun, AgentRun.id == ExecutionWorkerClaim.subject_id,
            ).where(
                ExecutionWorkerClaim.worker_id == self.worker_id, ExecutionWorkerClaim.family == FAMILY,
                AgentRun.summary_status == "generating",
            ))).all()
        for run_id, claim_id in generating:
            route_id = f"summary:{run_id}:{claim_id}"
            if route_id not in observed:
                await fail_remote_summary(
                    worker_id=self.worker_id, run_id=run_id, claim_id=claim_id, reason=UNCERTAIN_ERROR,
                )
                if route := self.routes.get(route_id):
                    self._resolve(route)
        return adopted

    async def _current_run(self, db: AsyncSession, run_id: UUID, claim_id: UUID) -> AgentRun | None:
        return await db.scalar(select(AgentRun).join(
            ExecutionWorkerClaim, ExecutionWorkerClaim.subject_id == AgentRun.id,
        ).where(
            AgentRun.id == run_id, AgentRun.status == "completed", AgentRun.summary_status == "generating",
            ExecutionWorkerClaim.id == claim_id, ExecutionWorkerClaim.worker_id == self.worker_id,
            ExecutionWorkerClaim.family == FAMILY,
        ).with_for_update(of=AgentRun))

    async def handle_result(self, event: dict[str, Any]) -> None:
        from src.services.execution.cloudflare_summary import settle_completion
        from src.services.execution.summary_dispatch import report_unclaimed_summary

        route = self.routes.get(str(event.get("route_id")))
        if route is None:
            return
        handle = _handle(event)
        state, result = event.get("attempt_state"), event.get("result")
        receipt = (event.get("_event_id"), state)
        if (
            route.handle not in (None, handle) or route.receipt not in (None, receipt)
            or not isinstance(receipt[0], str) or not isinstance(state, str) or state not in _STATES
            or not isinstance(result, dict)
            or type(result.get("success")) is not bool or (result["success"] and state != "completed")
        ):
            raise ValueError("Invalid opaque summary result")
        route.handle, route.receipt = handle, receipt
        async with get_db_context() as db:
            if not await fence_remote_owner(db, for_update=True):
                raise ConnectionError("Summary owner changed before settlement")
            run = await self._current_run(db, route.run_id, route.claim_id)
            if run is not None:
                await settle_completion(db, run, result, state)
        await report_unclaimed_summary(route.run_id, {}, "")
        self._resolve(route)
