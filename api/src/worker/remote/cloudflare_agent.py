"""Opaque autonomous agent commands over the existing launcher control socket."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select

from src.core.database import get_db_context
from src.models.orm.agent_runs import AgentRun, CloudflareAgentAttempt
from src.models.orm.execution_workers import ExecutionWorkerClaim
from src.models.orm.work_dispatches import WorkDispatch
from src.services.authorization.callers import CallerUnavailable
from src.services.authorization.enforce import PermissionDenied
from src.services.execution import cloudflare_agent
from src.services.execution.cloudflare_chat_outbox import has_projection_loss
from src.services.memory_requests import AGENT_RUN_REQUEST_BYTES
from src.services.worker_pool.claims import PoolClaim
from src.worker.remote.agent_route import run_timeout_seconds
from src.worker.remote.protocol import CredentialRouteChanged

logger = logging.getLogger(__name__)


class AgentTransport(Protocol):
    async def request(
        self, action: str, payload: dict[str, Any], *, expected_generation: int | None = None,
    ) -> dict[str, Any]: ...


@dataclass
class CloudflareAgentRoute:
    run_id: UUID
    claim_id: UUID
    deadline: datetime
    finished: asyncio.Event
    reservation_key: str
    handle: str | None = None

    @property
    def id(self) -> str:
        return f"agent:{self.run_id}:{self.claim_id}"


class CloudflareAgentRoutes:
    def __init__(self, transport: AgentTransport, worker_id: UUID, endpoint: str) -> None:
        self.transport = transport
        self.worker_id = worker_id
        self.endpoint = endpoint
        self.routes: dict[str, CloudflareAgentRoute] = {}

    async def run(self, claim: PoolClaim, generation: int) -> None:
        route = CloudflareAgentRoute(
            claim.subject_id, UUID(claim.message["claim_id"]),
            datetime.now(UTC), asyncio.Event(),
            str(claim.dispatch_id),
        )
        self.routes[route.id] = route
        try:
            try:
                seconds = min(cloudflare_agent.ATTEMPT_SECONDS, await run_timeout_seconds(claim.subject_id))
                route.deadline += timedelta(seconds=seconds)
                prepared = await cloudflare_agent.prepare_run(claim, route.deadline, self.endpoint)
            except Exception:  # noqa: BLE001 - No launch command was sent.
                logger.warning("Cloudflare agent preparation failed for run %s", route.run_id)
                prepared = False
            if not prepared:
                await cloudflare_agent.settle_run(
                    route.run_id, route.claim_id, "not_started", {}, dispatch_id=claim.dispatch_id,
                )
                return
            try:
                reply = await self.transport.request("cloudflare_agent", {
                    "route_id": route.id, "run_id": str(route.run_id), "claim_id": str(route.claim_id),
                    "deadline": route.deadline.isoformat(), "memory_request_bytes": claim.memory_request_bytes,
                }, expected_generation=generation)
            except (CredentialRouteChanged, MemoryError, RuntimeError):
                await cloudflare_agent.settle_run(route.run_id, route.claim_id, "not_started", {})
                return
            except (ConnectionError, TimeoutError):
                pass
            else:
                try:
                    self._check_handle(route, reply)
                except ValueError:
                    # A broker request, inventory or terminal can still identify the route.
                    logger.warning("Invalid Cloudflare agent launch reply for run %s", route.run_id)
            await self.supervise(route)
        finally:
            self.routes.pop(route.id, None)

    @staticmethod
    def _check_handle(route: CloudflareAgentRoute, event: dict[str, Any]) -> None:
        handle = event.get("runtime_handle") if isinstance(event, dict) else None
        if not isinstance(handle, str) or not 0 < len(handle) <= 255 or route.handle not in (None, handle):
            raise ValueError("Invalid opaque agent handle")
        route.handle = handle

    async def handle_request(self, event: dict[str, Any], generation: int) -> None:
        route = self.routes.get(str(event.get("route_id")))
        data, request_id = event.get("data"), event.get("request_id")
        if route is None or route.finished.is_set() or not isinstance(request_id, str) or not 0 < len(request_id) <= 255:
            return
        result: dict[str, Any] = {}
        ok = False
        try:
            if not isinstance(data, dict):
                raise TypeError("Invalid Cloudflare agent request")
            self._check_handle(route, event)
            if event["operation"] == "grant":
                turn = data.get("turn")
                if type(turn) is not int:
                    raise ValueError("Invalid Cloudflare agent turn")
                result = await cloudflare_agent.grant_turn(
                    route.run_id, route.claim_id, request_id, turn, data.get("execution_id"), self.endpoint,
                )
            elif event["operation"] == "commit":
                result = await cloudflare_agent.commit_event(route.run_id, route.claim_id, data)
            else:
                raise ValueError("Unknown Cloudflare agent request")
            ok = True
        except (HTTPException, PermissionDenied, CallerUnavailable):
            result = {"refused": True}
        except (ValueError, PermissionError, TypeError, KeyError, TimeoutError, ConnectionError):
            pass
        await self.transport.request("cloudflare_agent_reply", {
            "request_id": request_id, "route_id": route.id, "ok": ok, "result": result,
        }, expected_generation=generation)

    async def handle_result(self, event: dict[str, Any]) -> None:
        route = self.routes.get(str(event.get("route_id")))
        if route is None:
            return  # Reconciliation has already settled or rejected this route.
        self._check_handle(route, event)
        state, result = event.get("attempt_state"), event.get("result")
        if state not in ("completed", "uncertain", "not_started") or not isinstance(result, dict):
            raise ValueError("Invalid opaque agent terminal")
        await cloudflare_agent.settle_run(route.run_id, route.claim_id, state, result)
        route.finished.set()

    async def supervise(self, route: CloudflareAgentRoute) -> None:
        stop_sent = False
        while not route.finished.is_set():
            async with get_db_context() as db:
                status = await db.scalar(select(AgentRun.status).where(AgentRun.id == route.run_id))
                projection_lost = await has_projection_loss(db, route.claim_id)
            if status not in {"running", "cancelling"}:
                return
            now = datetime.now(UTC)
            if not stop_sent and (status == "cancelling" or projection_lost or now >= route.deadline):
                try:
                    await self.transport.request("terminate", {
                        "route_id": route.id,
                        "error_type": (
                            "Cancelled" if status == "cancelling"
                            else "ChatProjectionLost" if projection_lost else "Timeout"
                        ),
                        "error": "Cloudflare agent stopped",
                    })
                except (TimeoutError, ConnectionError, CredentialRouteChanged, RuntimeError):
                    pass
                else:
                    stop_sent = True
            if now >= route.deadline + timedelta(seconds=10):
                await cloudflare_agent.settle_run(route.run_id, route.claim_id, "uncertain", {})
                return
            try:
                await asyncio.wait_for(route.finished.wait(), timeout=0.5)
            except TimeoutError:
                pass

    async def supervise_adopted(self, route: CloudflareAgentRoute) -> None:
        try:
            await self.supervise(route)
        finally:
            self.routes.pop(route.id, None)

    async def reconcile(self, entries: list[dict[str, Any]]) -> list[CloudflareAgentRoute]:
        """Restore routes from the intersection of inventory and active journals."""
        async with get_db_context() as db:
            unprepared = (await db.execute(select(
                ExecutionWorkerClaim.subject_id, ExecutionWorkerClaim.id, WorkDispatch.id,
            ).join(AgentRun, AgentRun.id == ExecutionWorkerClaim.subject_id).join(
                WorkDispatch, WorkDispatch.subject_id == AgentRun.id,
            ).where(
                ExecutionWorkerClaim.worker_id == self.worker_id,
                ExecutionWorkerClaim.family == "agent_run",
                WorkDispatch.family == "agent_run",
                WorkDispatch.assigned_worker_id == self.worker_id,
                AgentRun.status == "queued",
            ))).all()
            owned = (await db.execute(select(
                CloudflareAgentAttempt.run_id, CloudflareAgentAttempt.claim_id, CloudflareAgentAttempt.data,
            ).join(ExecutionWorkerClaim, ExecutionWorkerClaim.id == CloudflareAgentAttempt.claim_id).join(
                AgentRun, AgentRun.id == CloudflareAgentAttempt.run_id,
            ).where(
                ExecutionWorkerClaim.worker_id == self.worker_id,
                ExecutionWorkerClaim.family == "agent_run",
                AgentRun.status.in_(("running", "cancelling")),
            ))).all()
        inventory = {entry["route_id"]: entry for entry in entries}
        if len(inventory) != len(entries):
            raise ValueError("Duplicate Cloudflare agent inventory")
        for run_id, claim_id, dispatch_id in unprepared:
            if f"agent:{run_id}:{claim_id}" not in self.routes:
                await cloudflare_agent.settle_run(run_id, claim_id, "not_started", {}, dispatch_id=dispatch_id)
        adopted = []
        for run_id, claim_id, data in owned:
            key = f"agent:{run_id}:{claim_id}"
            entry = inventory.get(key)
            if entry is None:
                await cloudflare_agent.settle_run(run_id, claim_id, "uncertain", {})
                if existing := self.routes.get(key):
                    existing.finished.set()
                continue
            if (
                entry.get("subject_id") != str(run_id) or entry.get("claim_id") != str(claim_id)
                or entry.get("memory_request_bytes") != AGENT_RUN_REQUEST_BYTES
                or entry.get("deadline") != data["deadline"]
            ):
                raise ValueError("Cloudflare agent inventory does not match the claim")
            route = self.routes.get(key)
            if route is None:
                route = CloudflareAgentRoute(
                    run_id, claim_id, datetime.fromisoformat(data["deadline"]), asyncio.Event(), data["dispatch_id"],
                )
                self.routes[key] = route
                adopted.append(route)
            self._check_handle(route, entry)
        return adopted
