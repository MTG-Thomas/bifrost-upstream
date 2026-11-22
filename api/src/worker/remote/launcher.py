"""Lightweight physical runner: OS supervision without application imports.

Only the API can issue commands. Durable claims, SDK authorization and result
commits stay in the ordinary consumers hosted by ``remote.host``.
"""
from __future__ import annotations

import asyncio
import contextlib
import ctypes
import json
import logging
import os
import re
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from queue import Empty
from typing import Any, NamedTuple
from uuid import UUID, uuid4

from shared.solution_git_stage import SOLUTION_GIT_CAPABILITY
from shared.version import get_version

from src.jobs.platform.policy import PlatformJobExecutor
from src.services.execution.container_result import InvalidContainerResult
from src.services.execution.cpu_sampler import CPUSampler, get_clock_ticks
from src.services.execution.memory_monitor import (
    effective_memory_sample,
    memory_pressure_ratio,
)
from src.services.execution.template_process import TemplateProcess
from src.worker.remote.container_guest import ContainerGuest
from src.worker.remote.control_connection import connect_control, load_control_headers
from src.worker.remote.identity import LauncherIdentity, load_launcher_identity
from src.worker.remote.platform_stage_protocol import StageBinding
from src.worker.remote.platform_stages import PlatformStages, StageProgram
from src.worker.remote.protocol import (
    CLOUDFLARE_WORKFLOW_HANDLER,
    CREDENTIAL_PROTOCOL,
    EXECUTION_HANDLERS,
    LINUX_RUNTIME_PROFILE,
    SUMMARY_CLAIM_PROTOCOL,
    ControlEnvelope,
    FragmentAssembler,
    Hello,
    PayloadSerializationError,
    fragment,
    new_nonce,
    sign_hello,
)

logger = logging.getLogger(__name__)
LAUNCHER_DRAIN_GRACE_SECONDS = 300.0
CONTAINER_FIRST_GRANT_SECONDS = 15
CONTAINER_MAX_RUNTIME_GRANT_MS = 20_000
PR_SET_DUMPABLE = 4


def _load_containers_control_secret() -> str:
    """Remove the child environment copy and protect the initial /proc image."""
    if os.geteuid() == 0:
        raise RuntimeError("Containers launcher requires a non-root user")
    for name in os.environ:
        normalized = name.upper().removeprefix("BIFROST_")
        if (
            "DATABASE_URL" in normalized
            or normalized in {"SECRET_KEY", "REDIS_URL", "CAPACITY_TOKEN", "WORKER_CAPACITY_TOKEN",
                              "CLOUDFLARE_CAPACITY_TOKEN", "CLOUDFLARE_CONTAINERS_TOKEN", "WORKER_LAUNCHER_ID"}
            or normalized.startswith(("S3_", "AZURE_BLOB_", "WORKER_NETWORK"))
        ):
            raise RuntimeError("Containers launcher received a forbidden environment variable")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0:
        raise RuntimeError("Containers launcher could not protect its process environment")
    return os.environ.pop("BIFROST_WORKER_CONTROL_SECRET")


def _load_container_lifetime() -> tuple[float, int]:
    """Read the two non-renewable timing pins before starting the supervisor."""
    try:
        deadline_ms = int(os.environ.pop("BIFROST_CONTAINER_DEADLINE_MS"))
        idle_seconds = int(os.environ.pop("BIFROST_CONTAINER_IDLE_SECONDS"))
        remaining = deadline_ms / 1000 - time.time()
        if not 0 < remaining <= 93_600 or not 1 <= idle_seconds <= 120:
            raise ValueError
    except (KeyError, OverflowError, ValueError):
        raise RuntimeError("Invalid managed Container lifetime") from None
    return remaining, idle_seconds


def _exit_at_container_deadline(_signal: int, _frame: Any) -> None:
    os._exit(124)


async def _next_control_frame(
    socket: Any,
    stopping: asyncio.Event,
    retired: asyncio.Event | None = None,
    drain_task: asyncio.Task[None] | None = None,
) -> str | None:
    """Receive one frame while still observing process shutdown signals."""
    receive = asyncio.create_task(socket.recv())
    wake_tasks = {asyncio.create_task(stopping.wait())}
    if retired is not None:
        wake_tasks.add(asyncio.create_task(retired.wait()))
    waiters = {receive, *wake_tasks}
    if drain_task is not None:
        waiters.add(drain_task)
    done, pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        if task is not drain_task:
            task.cancel()
    await asyncio.gather(*(task for task in pending if task is not drain_task), return_exceptions=True)
    if receive not in done:
        return None
    return receive.result()


async def _receive_control_commands(
    launcher: Launcher,
    socket: Any,
    stopping: asyncio.Event,
    drain_task: asyncio.Task[None] | None = None,
) -> asyncio.Task[None] | None:
    """Continue receiving ACKs and retirement while physical drain runs."""
    assembler = FragmentAssembler()
    while not launcher.retired.is_set():
        raw = await _next_control_frame(socket, stopping, launcher.retired, drain_task)
        if raw is None:
            if launcher.retired.is_set() or (drain_task is not None and drain_task.done()):
                return drain_task
            if drain_task is None:
                drain_task = asyncio.create_task(launcher.begin_drain())
            continue
        frame = ControlEnvelope.from_json(raw)
        if frame.session_generation != launcher.session_generation:
            raise RuntimeError("stale API control session")
        if frame.sequence <= launcher._last_command_sequence:
            raise RuntimeError("stale API control command")
        launcher._last_command_sequence = frame.sequence
        command = assembler.feed(frame)
        if command is None:
            continue
        if command.kind == "ack":
            event_id = command.payload.get("event_id")
            if isinstance(event_id, str):
                launcher.pending_terminal.pop(event_id, None)
                launcher.pending_terminal_inventory.pop(event_id, None)
            continue
        if command.kind == "runtime_lease":
            launcher.grant_runtime_lease(command.payload.get("ttl_ms"))
            continue
        if command.kind != "command":
            raise RuntimeError("unexpected API control message")
        launcher.task(launcher.command(command.payload, generation=launcher.session_generation))
    return drain_task


async def _await_container_startup(launcher: Launcher, setup: asyncio.Task[None]) -> None:
    """Require continuous authorization until the guest can advertise readiness.

    Losing the startup transport fails the physical boot before any dispatch
    can reach it. An enrolled guest uses the normal fenced reconnect path.
    """
    while not setup.done():
        receive = asyncio.create_task(launcher.socket.recv())
        try:
            done, _ = await asyncio.wait({setup, receive}, timeout=5, return_when=asyncio.FIRST_COMPLETED)
            if receive in done:
                frame = ControlEnvelope.from_json(receive.result())
                if (
                    frame.kind != "runtime_lease" or frame.session_generation != launcher.session_generation
                    or frame.sequence <= launcher._last_command_sequence
                ):
                    raise RuntimeError("Invalid Container startup control frame")
                launcher._last_command_sequence = frame.sequence
                launcher.grant_runtime_lease(frame.payload.get("ttl_ms"))
            elif not setup.done():
                current, limit = effective_memory_sample()
                await launcher.send({
                    "event": "sample", "current": current, "limit": limit,
                    "pressure_ratio": memory_pressure_ratio(), "runs": {},
                })
        finally:
            if not receive.done():
                receive.cancel()
                await asyncio.gather(receive, return_exceptions=True)
    await setup


async def _shutdown_launcher(
    launcher: Launcher,
    drain_task: asyncio.Task[None] | None,
    stopping: asyncio.Event,
) -> None:
    """Honor one bounded physical drain before stopping local children."""
    if stopping.is_set() and drain_task is None:
        drain_task = asyncio.create_task(launcher.begin_drain())
    if drain_task is not None:
        await asyncio.gather(drain_task, return_exceptions=True)
    await launcher.stop()


class ProcStat(NamedTuple):
    state: str
    ppid: int
    start_ticks: int


def proc_stat(pid: int) -> ProcStat | None:
    """State, parent and start time of ``pid``, or None once it is gone."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    # The command name may contain spaces; fields after it are fixed.
    fields = stat.rsplit(")", 1)[1].split()
    return ProcStat(state=fields[0], ppid=int(fields[1]), start_ticks=int(fields[19]))


@dataclass
class Child:
    route_id: str
    execution_id: str
    pid: int
    work: Any
    result: Any
    started: float
    timeout: float
    service: dict[str, Any] | None = None
    memory_kill_growth_bytes: int = 0
    grace: float = 5
    rss_start: int = 0
    rss_peak: int = 0
    growth_peak: int = 0
    current_growth: int = 0
    stopping: bool = False
    sampler: CPUSampler | None = None
    terminal_reason: tuple[str, str] | None = None
    active_execution: dict[str, Any] | None = None
    started_at: str | None = None
    memory_request_bytes: int | None = None
    agent_run: bool = False
    agent_handler: str = "agent.run"
    summary_claim_id: str | None = None
    # Start time of the forked process, so a reused PID is never mistaken
    # for it when the launcher reaps an orphaned agent child.
    start_ticks: int | None = None

    @property
    def kind(self) -> str:
        if self.agent_run:
            return "agent_run" if self.agent_handler == "agent.run" else "agent_summary"
        return "service" if self.service is not None else "workflow"


class Launcher:
    def __init__(
        self,
        socket: Any,
        *,
        identity: LauncherIdentity,
        secret: str,
        on_ready: Callable[[], None] | None = None,
        on_stop: Callable[[], None] | None = None,
        authenticated_control: bool = False,
        containers: bool = False,
        container_release: str | None = None,
        container_probe_nonce: str | None = None,
        container_idle_seconds: int | None = None,
        container_deadline: float | None = None,
        container_guest: ContainerGuest | None = None,
    ) -> None:
        self.socket = socket
        self.identity = identity
        self.secret = secret
        self._authenticated_control = authenticated_control
        self.containers = containers
        self.container_release = container_release
        self.container_probe_nonce = container_probe_nonce
        self.container_idle_seconds = container_idle_seconds
        self.container_deadline = container_deadline
        self.container_guest = container_guest
        self._guest_finished = False
        self._container_lease_until: float | None = None
        self._idle_since = time.monotonic()
        self._idle_draining = False
        self.platform_stages = (
            PlatformStages(
                {SOLUTION_GIT_CAPABILITY: StageProgram("src.worker.remote.solution_git_stage", 45)},
            )
            if containers and container_release is None else None
        )
        self.session_generation = 0
        self._sequence = 0
        self._last_command_sequence = 0
        self._hello_sequence = 0
        self.on_ready = on_ready or (lambda: None)
        self.on_stop = on_stop or (lambda: None)
        self.requirements: dict[str, Any] = {}
        self.requirements_content: str | None = None
        self.pending_terminal: dict[str, dict[str, Any]] = {}
        self.pending_terminal_inventory: dict[str, dict[str, Any]] = {}
        self.template = TemplateProcess()
        # Started with the launcher and kept, like the workflow template.
        self.agent_template = None if containers else TemplateProcess(profile="agent")
        # Finished agent children by PID -> start time. If their template
        # dies they re-parent to the launcher (PID 1 in a pod), which must
        # reap them.
        self.exited_agent_children: dict[int, int] = {}
        self.children: dict[str, Child] = {}
        self.jobs: dict[str, asyncio.subprocess.Process] = {}
        self.job_claims: dict[str, dict[str, str | int]] = {}
        self._template_lock = asyncio.Lock()
        # Separate from the workflow template's lock: rebuilding the agent
        # template takes seconds and must not stall workflow forks.
        self._agent_template_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._monitor_task: asyncio.Task[Any] | None = None
        self.failed = False
        self.pressure_threshold: float | None = None
        self.dangerous_memory_ratio: float | None = None
        self.baseline_bytes: int | None = None
        self.retired = asyncio.Event()

    async def send(self, payload: dict[str, Any]) -> None:
        async with self._send_lock:
            if self.session_generation < 1:
                raise ConnectionError("launcher has no fenced API session")
            envelope = ControlEnvelope(
                session_generation=self.session_generation,
                sequence=self._sequence + 1,
                kind="event",
                payload=payload,
            )
            sent = 0
            for frame in fragment(envelope):
                raw = frame.to_json()
                try:
                    await self.socket.send(raw)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Background task logging must not recover credentials
                    # echoed in a transport exception or WebSocket close reason.
                    raise ConnectionError("remote control transport is unavailable") from (
                        None if self._authenticated_control else exc
                    )
                sent += 1
            self._sequence += sent

    async def send_terminal(
        self, payload: dict[str, Any], *, inventory: dict[str, Any]
    ) -> None:
        """Keep terminal results until the current fenced API owner ACKs them."""
        event_id = str(uuid4())
        event = {**payload, "_event_id": event_id}
        self.pending_terminal[event_id] = event
        self.pending_terminal_inventory[event_id] = {"event_id": event_id, **inventory}
        try:
            await self.send(event)
        except PayloadSerializationError:
            # Encoding fails before fragment() yields any frames. This event
            # cannot be replayed; transport failures still retain it for ACK.
            self.pending_terminal.pop(event_id, None)
            self.pending_terminal_inventory.pop(event_id, None)
            raise

    def task(self, coroutine: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self.task_finished)
        return task

    def task_finished(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            if task is self._monitor_task:
                self.failed = True
                self.on_stop()
            logger.error("Launcher supervision failed", exc_info=task.exception())
            asyncio.create_task(self.socket.close(code=1011, reason="Launcher supervision failed"))

    async def start(self) -> None:
        # This existing helper owns requirements IO and exits before ready.
        module = "src.services.execution.requirements_setup_helper"
        if self.containers:
            if os.environ.get("BIFROST_BAKED_WHEELS_ONLY") == "1" and self.requirements_content:
                raise RuntimeError("The Container release cannot install runtime requirements")
            result = await self.helper(module, "--bootstrap", input_payload={"content": self.requirements_content})
        else:
            result = await self.helper(module)
        self.requirements = result
        if self.container_guest is not None:
            await asyncio.to_thread(self.container_guest.wait_ready)
        elif self.agent_template is None:
            await asyncio.to_thread(self.template.start)
        else:
            await asyncio.gather(
                asyncio.to_thread(self.template.start), asyncio.to_thread(self.agent_template.start)
            )
        current, limit = effective_memory_sample()
        self.baseline_bytes = current
        self._idle_since = time.monotonic()
        self._monitor_task = self.task(self.monitor())

    async def hello(self, requirements: dict[str, Any]) -> None:
        """Authenticate each new WebSocket; the physical process stays alive."""
        async with self._send_lock:
            await self._hello(requirements)

    async def reconnect(self, socket: Any, requirements: dict[str, Any]) -> None:
        """Fence a replacement transport before another event can use it."""
        async with self._send_lock:
            self.socket = socket
            await self._hello(requirements)

    def grant_runtime_lease(self, ttl_ms: object) -> None:
        """Arm hard expiry from a fenced API grant without extending the pinned lifetime."""
        if (
            self.container_release is None or self.container_deadline is None
            or type(ttl_ms) is not int or not 0 < ttl_ms <= CONTAINER_MAX_RUNTIME_GRANT_MS
        ):
            raise RuntimeError("Invalid managed Container runtime lease")
        now = time.monotonic()
        if self._container_lease_until is not None and now >= self._container_lease_until:
            raise RuntimeError("Managed Container runtime lease already expired")
        deadline = min(self.container_deadline, now + ttl_ms / 1000)
        if deadline <= now:
            raise RuntimeError("Managed Container runtime lease is expired")
        signal.setitimer(signal.ITIMER_REAL, deadline - now)
        self._container_lease_until = deadline

    async def _hello(self, requirements: dict[str, Any]) -> None:
        current, limit = effective_memory_sample()
        self._hello_sequence += 1
        nonce = self.container_probe_nonce or new_nonce()
        self.container_probe_nonce = None
        hello = Hello(
            launcher_id=self.identity.launcher_id,
            incarnation_id=self.identity.incarnation_id,
            job_uid=self.identity.job_uid,
            pod_uid=self.identity.pod_uid,
            hostname=self.identity.hostname,
            form=self.identity.form,
            resource_uid=self.identity.resource_uid,
            issued_at=int(time.time()),
            nonce=nonce,
            hello_sequence=self._hello_sequence,
            execution_lane=os.environ.get("BIFROST_POOL_WORKER_EXECUTION_LANE", "normal"),
            runtime_profile=LINUX_RUNTIME_PROFILE if self.containers else None,
            container_release=self.container_release,
        )
        await self.socket.send(json.dumps({
            "event": "hello",
            "hello": sign_hello(self.secret, hello).to_dict(),
            "sample": {
                "current": current, "limit": limit,
                "pressure_ratio": memory_pressure_ratio(),
            },
            "requirements": requirements,
            "version": get_version(),
            "credential_protocol": CREDENTIAL_PROTOCOL,
            "handlers": (
                (CLOUDFLARE_WORKFLOW_HANDLER, (SOLUTION_GIT_CAPABILITY, 1))
                if self.platform_stages is not None else
                (CLOUDFLARE_WORKFLOW_HANDLER,) if self.containers else EXECUTION_HANDLERS
            ),
            "summary_claim_protocol": SUMMARY_CLAIM_PROTOCOL,
        }, separators=(",", ":")))
        welcome = json.loads(await self.socket.recv())
        if welcome.get("event") != "welcome" or not isinstance(welcome.get("session_generation"), int):
            raise RuntimeError("remote host refused launcher session")
        self.session_generation = welcome["session_generation"]
        self._sequence = 0
        self._last_command_sequence = 0
        if self.containers:
            assembler = FragmentAssembler()
            while True:
                frame = ControlEnvelope.from_json(await self.socket.recv())
                if frame.session_generation != self.session_generation or frame.sequence <= self._last_command_sequence:
                    raise RuntimeError("Invalid worker bootstrap session")
                self._last_command_sequence = frame.sequence
                startup = assembler.feed(frame)
                if startup is None:
                    continue
                if startup.kind != "bootstrap" or "requirements_content" not in startup.payload:
                    raise RuntimeError("Worker bootstrap inputs are required")
                content = startup.payload["requirements_content"]
                if content is not None and not isinstance(content, str):
                    raise RuntimeError("Invalid worker requirements content")
                if self.container_release is not None:
                    self.grant_runtime_lease(startup.payload.get("ttl_ms"))
                self.requirements_content = content
                break

    def inventory(self) -> dict[str, Any]:
        """Describe live physical routes for fenced API-owner adoption.

        Context and authoritative claim state are intentionally not copied over
        the wire: the adopting API reconstructs them from canonical DB claims.
        """
        routes: list[dict[str, Any]] = []
        for child in self.children.values():
            if child.agent_run:
                routes.append(self.agent_route(child, pid=child.pid))
                continue
            routes.append({
                "route_id": child.route_id,
                "kind": child.kind,
                "subject_id": child.execution_id,
                "pid": child.pid,
                "service": child.service,
                "active_execution": child.active_execution,
                "started_at": child.started_at,
                "timeout_seconds": child.timeout,
                "memory_request_bytes": child.memory_request_bytes,
            })
        for route_id, process in self.jobs.items():
            claim = self.job_claims.get(route_id, {})
            routes.append({
                "route_id": route_id,
                "kind": "platform_job",
                "subject_id": claim.get("job_id"),
                "pid": process.pid,
                "lease_token": claim.get("lease_token"),
                "memory_request_bytes": claim.get("memory_request_bytes"),
            })
        return {
            "version": 1,
            "baseline_bytes": self.baseline_bytes,
            "routes": routes,
            "terminal_routes": list(self.pending_terminal_inventory.values()),
            **({"compute_stages": self.platform_stages.inventory()} if self.platform_stages is not None else {}),
        }

    async def helper(self, module: str, *args: str, input_payload: dict[str, Any] | None = None) -> dict[str, Any]:
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", module, *args,
            stdin=asyncio.subprocess.PIPE if input_payload is not None else None,
            stdout=asyncio.subprocess.PIPE, start_new_session=True,
        )
        try:
            data = json.dumps(input_payload).encode() if input_payload is not None else None
            stdout, _ = await process.communicate(data)
        except BaseException:
            await self.kill_process(process)
            raise
        if process.returncode:
            raise RuntimeError(f"{module} failed with exit code {process.returncode}")
        return json.loads(stdout)

    async def command(self, request: dict[str, Any], *, generation: int) -> None:
        """Execute one command only while its fenced session remains current."""
        if generation != self.session_generation:
            return
        action, payload = request["action"], request["payload"]
        try:
            response: dict[str, Any] = {}
            if self.containers and action in ("service", "agent_run", "agent_summary", "platform_job"):
                raise ValueError("Work family is unavailable on Containers")
            if self.platform_stages is not None and action in ("stage_status", "stage_start", "stage_release"):
                binding = StageBinding.parse(payload)
                if action == "stage_start":
                    status = self.platform_stages.status(binding)
                    if status["state"] == "unknown" and (
                        self.pressure_threshold is None or memory_pressure_ratio() >= self.pressure_threshold
                    ):
                        raise MemoryError("Launcher memory pressure rejects platform stage")
                    response = self.platform_stages.start(binding, payload["bundle"])
                elif action == "stage_release":
                    response = await self.platform_stages.release(binding)
                else:
                    response = self.platform_stages.status(binding)
            elif action in ("workflow", "service"):
                if self._idle_draining:
                    raise MemoryError("Container is draining after its idle deadline")
                self.check_fork_pressure()
                route_id = payload["route_id"]
                if route_id in self.children or route_id in self.jobs:
                    raise ValueError("Duplicate route")
                if generation != self.session_generation:
                    return
                async with self._template_lock:
                    if generation != self.session_generation:
                        return
                    if self.container_release is not None:
                        if self.container_guest is None:
                            raise MemoryError("Container workflow isolation is unavailable")
                        pid, work, result = self.container_guest.take()
                    else:
                        pid, work, result = await asyncio.to_thread(
                            self.template.fork, worker_id=route_id, sdk_socket_path=None,
                        )
                self._idle_since = time.monotonic()
                context = payload["context"]
                timeout = context.get("timeout_seconds", 300) if action == "workflow" else 0
                child = Child(route_id, payload["execution_id"], pid, work, result,
                              time.monotonic(), timeout,
                              service=payload.get("service"),
                              memory_kill_growth_bytes=payload.get("memory_kill_growth_bytes", 0),
                              grace=payload.get("graceful_shutdown_seconds", 5),
                              active_execution=payload.get("active_execution"),
                              started_at=payload.get("started_at"),
                              memory_request_bytes=payload.get("memory_request_bytes"))
                self.track_child(child)
                work.put_nowait((child.execution_id, context))
                response["pid"] = pid
            elif action in ("agent_run", "agent_summary"):
                agent_template = self.agent_template
                assert agent_template is not None  # Container launchers reject agent work above.
                handler = "agent.run" if action == "agent_run" else payload["handler"]
                if handler not in ("agent.run", "agent.summarize", "agent.summarize_backfill"):
                    raise ValueError("Unknown agent handler")
                if action == "agent_summary" and handler == "agent.run":
                    raise ValueError("Summary command requires a summary handler")
                if action == "agent_summary" and payload.get("worker_id") != str(self.identity.incarnation_id):
                    raise ValueError("Summary worker identity does not match this launcher")
                claim_id = str(UUID(payload["message"]["claim_id"])) if action == "agent_summary" else None
                self.check_fork_pressure()
                route_id = payload["route_id"]
                if route_id in self.children or route_id in self.jobs:
                    raise ValueError("Duplicate route")
                if generation != self.session_generation:
                    return
                async with self._agent_template_lock:
                    if generation != self.session_generation:
                        return
                    pid, work, result = await asyncio.to_thread(
                        agent_template.fork, worker_id=route_id
                    )
                child = Child(route_id, str(payload["run_id"]), pid, work, result,
                              time.monotonic(), 0,
                              memory_request_bytes=int(payload["memory_request_bytes"]),
                              agent_run=True, agent_handler=handler, summary_claim_id=claim_id)
                self.track_child(child)
                work.put_nowait({
                    "message": payload["message"],
                    **({"handler": handler, "worker_id": payload["worker_id"]} if action == "agent_summary" else {}),
                })
                response["pid"] = pid
            elif action == "platform_job":
                if self.pressure_threshold is None:
                    raise RuntimeError("Launcher resource policy has not been configured")
                if memory_pressure_ratio() >= self.pressure_threshold:
                    raise MemoryError("Launcher memory pressure rejects platform job")
                route_id = payload["route_id"]
                if route_id in self.jobs or route_id in self.children:
                    raise ValueError("Duplicate route")
                if generation != self.session_generation:
                    return
                process = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", "src.jobs.platform.runner",
                    payload["job_id"], payload["lease_token"], PlatformJobExecutor.REMOTE_LAUNCHER.value,
                    start_new_session=True,
                )
                self.jobs[route_id] = process
                self.job_claims[route_id] = {
                    "job_id": str(payload["job_id"]),
                    "lease_token": str(payload["lease_token"]),
                    "memory_request_bytes": int(payload["memory_request_bytes"]),
                }
                self.task(self.wait_job(route_id, process))
            elif action == "terminate_job":
                process = self.jobs.get(payload["route_id"])
                if process is not None:
                    await self.kill_process(process)
            elif action == "terminate":
                await self.terminate(payload["route_id"], payload.get("error_type", "CancelledError"),
                                     payload.get("error", "Cancelled"), payload.get("grace_seconds"))
            elif action == "recycle":
                if self.container_release is not None:
                    raise ValueError("The Container release cannot recycle its workflow guest")
                # Recycling picks up newly installed workflow packages. Agent
                # children never import customer packages, so the agent
                # template and its children are left alone.
                while any(not child.agent_run for child in self.children.values()):
                    await asyncio.sleep(.05)
                async with self._template_lock:
                    await asyncio.to_thread(self.template.shutdown)
                    await asyncio.to_thread(self.template.start)
            elif action == "package_tool":
                if self.containers:
                    if os.environ.get("BIFROST_BAKED_WHEELS_ONLY") == "1":
                        raise ValueError("The Container release cannot run package tools")
                    if payload.get("operation") == "requirements" and "content" not in payload:
                        raise ValueError("Requirements operation needs explicit content")
                    response = await self.helper("src.worker.remote.package_helper", "--bootstrap", input_payload=payload)
                else:
                    response = await self.helper("src.worker.remote.package_helper", json.dumps(payload))
            elif action == "configure":
                pressure = float(payload["pressure_threshold"])
                dangerous = float(payload["dangerous_memory_ratio"])
                if not 0 < pressure < dangerous < 1:
                    raise ValueError("Invalid launcher memory policy")
                self.pressure_threshold, self.dangerous_memory_ratio = pressure, dangerous
            elif action == "ready":
                self.on_ready()
            elif action == "retired":
                self.retired.set()
            else:
                raise ValueError("Unknown launcher command")
            if generation == self.session_generation:
                await self.send({"reply": request["id"], "result": response})
        except Exception as exc:
            if generation == self.session_generation:
                await self.send({"reply": request["id"], "error_type": type(exc).__name__,
                                 "error": str(exc)})

    def check_fork_pressure(self) -> None:
        """Refuse a fork while the launcher's memory is under pressure."""
        current, limit = effective_memory_sample()
        if self.pressure_threshold is None:
            raise RuntimeError("Launcher resource policy has not been configured")
        if (
            current < 0
            or limit <= 0
            or memory_pressure_ratio() >= self.pressure_threshold
        ):
            raise MemoryError("Launcher memory pressure rejects fork")

    def track_child(self, child: Child) -> None:
        """Sample a forked child and watch its result pipe."""
        child.sampler = CPUSampler(pid=child.pid, clock_ticks=get_clock_ticks())
        child.sampler.set_baseline(time.monotonic())
        child.rss_start = child.sampler.start_process_rss_bytes or 0
        if child.agent_run:
            stat = proc_stat(child.pid)
            child.start_ticks = stat.start_ticks if stat is not None else None
        self.children[child.route_id] = child
        asyncio.get_running_loop().add_reader(child.result.fileno(), self.result_ready, child.route_id)

    def reap_exited_agent_children(self) -> None:
        """Reap finished agent children that were orphaned to this process.

        Only the known PIDs, and only while the start time still matches:
        ``waitpid(-1)`` would steal the exit status of asyncio-managed
        subprocesses such as platform jobs.
        """
        for pid, start_ticks in list(self.exited_agent_children.items()):
            stat = proc_stat(pid)
            if stat is None or stat.start_ticks != start_ticks:
                del self.exited_agent_children[pid]
                continue
            if stat.ppid != os.getpid():
                # Still the template's child; the template reaps it.
                continue
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                reaped = pid
            if reaped:
                del self.exited_agent_children[pid]

    @staticmethod
    def agent_route(child: Child, **extra: Any) -> dict[str, Any]:
        """Inventory description of an agent child (live or terminal)."""
        return {
            "route_id": child.route_id,
            "kind": child.kind,
            **({"handler": child.agent_handler, "claim_id": child.summary_claim_id} if child.kind == "agent_summary" else {}),
            "subject_id": child.execution_id,
            **extra,
            "memory_request_bytes": child.memory_request_bytes,
        }

    async def wait_job(self, route_id: str, process: asyncio.subprocess.Process) -> None:
        code = await process.wait()
        try:
            await self.send_terminal(
                {"event": "job_exit", "route_id": route_id, "returncode": code},
                inventory={
                    "route_id": route_id,
                    "kind": "platform_job",
                    "subject_id": self.job_claims.get(route_id, {}).get("job_id"),
                    "lease_token": self.job_claims.get(route_id, {}).get("lease_token"),
                    "memory_request_bytes": self.job_claims.get(route_id, {}).get("memory_request_bytes"),
                },
            )
        finally:
            self.jobs.pop(route_id, None)
            self.job_claims.pop(route_id, None)

    @staticmethod
    async def kill_process(process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), 5)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()

    def result_ready(self, route_id: str, *, guest_exited: bool = False) -> None:
        child = self.children.get(route_id)
        if child is None or (self.container_guest is not None and child.stopping):
            return
        try:
            result = child.result.get_nowait()
            if self.container_release is not None and result.get("execution_id") != child.execution_id:
                raise InvalidContainerResult("Container result does not match the active execution")
        except Empty:
            if not guest_exited:
                return
            result = self.failure(child, "ProcessCrashError", "Child exited without a result")
        except InvalidContainerResult:
            result = self.failure(child, "ProcessCrashError", "Child sent an invalid Container result")
        except (EOFError, OSError):
            result = self.failure(child, "ProcessCrashError", "Child exited without a result")
        if asyncio.get_running_loop().remove_reader(child.result.fileno()):
            self.task(self.finish(child, result))

    @staticmethod
    def failure(child: Child, error_type: str, error: str) -> dict[str, Any]:
        return {"execution_id": child.execution_id, "success": False, "status": "Failed",
                "error": error, "error_message": error, "error_type": error_type,
                "duration_ms": int((time.monotonic() - child.started) * 1000)}

    async def finish(self, child: Child, result: dict[str, Any], *, guest_grace: float = 0) -> None:
        if self.children.pop(child.route_id, None) is None:
            return
        asyncio.get_running_loop().remove_reader(child.result.fileno())
        if self.container_guest is not None:
            self._idle_draining = True
            if not await self.container_guest.stop(grace=guest_grace):
                # The supervisor exits the namespace. Only independently
                # witnessed provider stop can resolve this terminal outcome.
                self.failed = True
                self.retired.set()
                return
        if child.terminal_reason is not None:
            result = self.failure(child, *child.terminal_reason)
        if child.sampler is not None:
            child.sampler.sample(time.monotonic())
            child.rss_peak = max(child.rss_peak, child.sampler.peak_process_rss_bytes or 0)
            child.growth_peak = max(child.growth_peak, child.rss_peak - child.rss_start)
        child.work.close()
        child.result.close()
        if child.agent_run:
            if child.start_ticks is not None:
                self.exited_agent_children[child.pid] = child.start_ticks
            await self.send_terminal(
                {"event": "agent_exit", "route_id": child.route_id, "ok": result.get("ok") is True,
                 "error_type": result.get("error_type"), "error": result.get("error")},
                inventory=self.agent_route(child),
            )
            return
        metrics = result.setdefault("metrics", {})
        if isinstance(metrics, dict):
            metrics["peak_memory_growth_bytes"] = max(metrics.get("peak_memory_growth_bytes") or 0,
                                                       child.growth_peak)
            if child.terminal_reason is not None and child.terminal_reason[0] == "MemoryLimitExceeded":
                metrics["peak_memory_growth_bytes"] = max(
                    metrics["peak_memory_growth_bytes"], child.memory_kill_growth_bytes or 0,
                )
            metrics["peak_process_rss_bytes"] = max(metrics.get("peak_process_rss_bytes") or 0,
                                                     child.rss_peak)
            if child.sampler is not None and child.sampler.peak_cpu_cores is not None:
                metrics["peak_cpu_cores"] = child.sampler.peak_cpu_cores
        inventory = {
            "route_id": child.route_id,
            "kind": child.kind,
            "subject_id": child.execution_id,
            "service": child.service,
            "active_execution": child.active_execution,
            "started_at": child.started_at,
            "timeout_seconds": child.timeout,
            "memory_request_bytes": child.memory_request_bytes,
        }
        try:
            await self.send_terminal(
                {"event": "result", "route_id": child.route_id, "result": result},
                inventory=inventory,
            )
        except PayloadSerializationError:
            logger.warning("Execution result could not be serialized; sending a failed result")
            await self.send_terminal(
                {"event": "result", "route_id": child.route_id,
                 "result": self.failure(child, "SerializationError", "Execution result could not be serialized")},
                inventory=inventory,
            )
        finally:
            self._guest_finished = self.container_guest is not None

    async def terminate(self, route_id: str, error_type: str, error: str,
                        grace: float | None = None) -> None:
        child = self.children.get(route_id)
        if child is None or child.stopping:
            return
        child.stopping = True
        if child.service is None or error_type == "MemoryLimitExceeded":
            child.terminal_reason = (error_type, error)
        if self.container_guest is not None:
            await self.finish(
                child, self.failure(child, error_type, error), guest_grace=child.grace if grace is None else grace,
            )
            return
        with contextlib.suppress(ProcessLookupError):
            os.kill(child.pid, signal.SIGTERM)
        deadline = time.monotonic() + (child.grace if grace is None else grace)
        while route_id in self.children and time.monotonic() < deadline:
            stat = proc_stat(child.pid)
            if stat is None or stat.state == "Z":
                # Gone, or exited and awaiting its reaper (an orphaned child
                # waits for the launcher's monitor).
                break
            await asyncio.sleep(.05)
        if route_id in self.children:
            with contextlib.suppress(ProcessLookupError):
                os.kill(child.pid, signal.SIGKILL)
            await self.finish(child, self.failure(child, error_type, error))

    def _container_idle_is_due(self, now: float) -> bool:
        guest = self.container_guest
        if guest is not None and (self._guest_finished or (not guest.used and guest.exited)):
            return True
        if self.container_idle_seconds is None:
            return False
        if self.children or self.pending_terminal or any(
            task is not self._monitor_task and not task.done() for task in self._tasks
        ):
            self._idle_since = now
            return False
        return now - self._idle_since >= self.container_idle_seconds

    async def monitor(self) -> None:
        tick = 0
        while True:
            if self._container_idle_is_due(time.monotonic()):
                self._idle_draining = True
                await self.begin_drain()
                self.retired.set()
                return
            # A package recycle intentionally stops the template while holding
            # this lock. Observe liveness only outside that replacement window.
            async with self._template_lock:
                if self.container_guest is None and not self.template.is_alive():
                    # Preserve the canonical pool's template-crash recovery;
                    # live children retain their existing pipes and identities.
                    logger.warning("Execution template died; replacing it")
                    await asyncio.to_thread(self.template.start)
            async with self._agent_template_lock:
                if self.agent_template is not None and not self.agent_template.is_alive():
                    # Its live children keep running; reap_exited_agent_children
                    # reaps those that re-parent to the launcher.
                    logger.warning("Agent template died; replacing it")
                    await asyncio.to_thread(self.agent_template.start)
            current, limit = effective_memory_sample()
            samples = {}
            for child in list(self.children.values()):
                if self.container_guest is not None and self.container_guest.exited:
                    self.result_ready(child.route_id, guest_exited=True)
                    continue
                assert child.sampler is not None
                child.sampler.sample(time.monotonic())
                rss = child.sampler.last_process_rss_bytes or 0
                child.rss_peak = max(child.rss_peak, rss)
                child.growth_peak = max(child.growth_peak, rss - child.rss_start)
                child.current_growth = max(0, rss - child.rss_start)
                samples[child.route_id] = {"growth_bytes": child.current_growth,
                                          "memory_peak_bytes": child.growth_peak,
                                          "peak_cpu_cores": child.sampler.peak_cpu_cores,
                                          "rss_bytes": rss}
                if child.stopping:
                    continue
                reason = None
                if child.timeout and time.monotonic() - child.started > child.timeout:
                    reason = ("TimeoutError", "Execution timed out")
                if reason:
                    # Once observed, a deadline/resource violation owns the
                    # terminal outcome even if the result pipe becomes ready.
                    if child.service is None:
                        child.terminal_reason = reason
                    self.task(self.terminate(child.route_id, *reason, grace=0))
            self.stop_largest_when_dangerous(
                current, limit,
                dangerous=memory_pressure_ratio() >= (self.dangerous_memory_ratio or 1),
            )
            self.reap_exited_agent_children()
            if tick % 4 == 0:
                async with self._template_lock:
                    if self.container_guest is None:
                        await asyncio.to_thread(self.template.collect_child_exit_statuses)
                async with self._agent_template_lock:
                    if self.agent_template is not None:
                        await asyncio.to_thread(self.agent_template.collect_child_exit_statuses)
                with contextlib.suppress(ConnectionError):
                    await self.send({"event": "sample", "current": current, "limit": limit,
                                     "pressure_ratio": memory_pressure_ratio(), "runs": samples})
            tick += 1
            await asyncio.sleep(.25)

    def stop_largest_when_dangerous(
        self, current: int, limit: int, *, dangerous: bool | None = None
    ) -> None:
        """Mirror the canonical pool: stop one largest-growing child per tick."""
        if self.dangerous_memory_ratio is None or dangerous is False or (
            dangerous is None and current < limit * self.dangerous_memory_ratio
        ):
            return
        candidates = [child for child in self.children.values()
                      if not child.stopping and child.terminal_reason is None]
        if not candidates:
            return
        child = max(candidates, key=lambda item: item.current_growth)
        reason = ("MemoryLimitExceeded", "Runner memory pressure")
        child.terminal_reason = reason
        self.task(self.terminate(child.route_id, *reason, grace=0))

    async def stop(self) -> None:
        self.on_stop()
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for child in self.children.values():
            asyncio.get_running_loop().remove_reader(child.result.fileno())
        if self.container_guest is not None:
            if not await self.container_guest.stop():
                self.failed = True
            self.container_guest.close()
        for child in self.children.values():
            if self.container_guest is None:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(child.pid, signal.SIGKILL)
            child.work.close()
            child.result.close()
        self.children.clear()
        await asyncio.gather(*(self.kill_process(p) for p in self.jobs.values()))
        if self.platform_stages is not None:
            await self.platform_stages.close()
        if self.container_guest is None:
            await asyncio.to_thread(self.template.shutdown)
        if self.agent_template is not None:
            await asyncio.to_thread(self.agent_template.shutdown)

    async def begin_drain(self) -> None:
        """Ask the API to drain and retain physical children for its grace."""
        with contextlib.suppress(ConnectionError):
            await self.send({"event": "draining"})
        deadline = time.monotonic() + LAUNCHER_DRAIN_GRACE_SECONDS
        # A child leaves ``children`` before its terminal result is ACKed.
        # Do not cancel that replay task merely because the process itself
        # exited; the canonical owner must persist the terminal outcome.
        while (self.children or self.jobs or self.pending_terminal) and time.monotonic() < deadline:
            await asyncio.sleep(0.1)


async def run_launcher(
    *,
    on_ready: Callable[[], None] | None = None,
    on_stop: Callable[[], None] | None = None,
) -> int:
    """Keep optional private networking alive through physical worker drain."""
    from src.core.storage import close_shared_storage_clients
    from src.worker.network import worker_network

    try:
        async with worker_network():
            return await _run_launcher(on_ready=on_ready, on_stop=on_stop)
    finally:
        await close_shared_storage_clients()


async def _run_launcher(
    *,
    on_ready: Callable[[], None] | None = None,
    on_stop: Callable[[], None] | None = None,
) -> int:
    """Run one physical launcher, reconnecting without killing live children."""
    api_url = os.environ.get("BIFROST_API_URL", "http://api:8000").rstrip("/")
    control_url = api_url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    control_url += "/api/worker-control"
    control_headers = load_control_headers(control_url)
    identity = load_launcher_identity()
    containers = identity.form == "external" and "BIFROST_WORKER_CONTROL_SECRET" in os.environ
    container_release = None
    probe_nonce = None
    idle_seconds = None
    remaining = None
    container_deadline = None
    container_guest = None
    if os.environ.get("BIFROST_BAKED_WHEELS_ONLY") == "1":
        container_release = os.environ.pop("BIFROST_CONTAINER_RELEASE", "")
        probe_nonce = os.environ.pop("BIFROST_CONTAINER_PROBE_NONCE", None)
        if not containers or not api_url.startswith("https://") or not container_release:
            raise RuntimeError("The Container release requires explicit incarnation, release and HTTPS ingress pins")
        if probe_nonce is not None and re.fullmatch(r"[A-Za-z0-9_-]{43}", probe_nonce) is None:
            raise RuntimeError("Invalid Container qualification challenge")
        remaining, idle_seconds = _load_container_lifetime()
        container_deadline = time.monotonic() + remaining
        container_guest = ContainerGuest.start_image(api_url)
    secret = _load_containers_control_secret() if containers else os.environ["BIFROST_SECRET_KEY"]
    launcher: Launcher | None = None
    candidate: Launcher | None = None
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)
    # The provider alarm also enforces the absolute bound independently.
    if remaining is not None:
        signal.signal(signal.SIGALRM, _exit_at_container_deadline)
        signal.setitimer(signal.ITIMER_REAL, min(remaining, CONTAINER_FIRST_GRANT_SECONDS))
    try:
        drain_task: asyncio.Task[None] | None = None
        while launcher is None or (not launcher.retired.is_set() and not launcher.failed):
            try:
                connection = (
                    connect_control(control_url, control_headers, container_intercept=True)
                    if container_release is not None else connect_control(control_url, control_headers)
                )
                async with connection as socket:
                    if launcher is None:
                        if candidate is None:
                            candidate = Launcher(
                                socket, identity=identity, secret=secret,
                                on_ready=on_ready, on_stop=on_stop,
                                authenticated_control=bool(control_headers),
                                containers=containers,
                                container_release=container_release,
                                container_probe_nonce=probe_nonce,
                                container_idle_seconds=idle_seconds,
                                container_deadline=container_deadline,
                                container_guest=container_guest,
                            )
                        else:
                            # A lost welcome may follow a committed enrollment.
                            # Keep the physical identity's monotonic hello sequence.
                            candidate.socket = socket
                        # Enroll before the potentially minute-long requirements
                        # install. The API's ordinary worker heartbeat can then
                        # keep the canonical row alive while it waits for the
                        # inventory below; no work is ready to route yet.
                        await candidate.hello({})
                        launcher = candidate
                        try:
                            setup = asyncio.create_task(candidate.start())
                            if container_guest is not None:
                                try:
                                    await _await_container_startup(candidate, setup)
                                finally:
                                    if not setup.done():
                                        setup.cancel()
                                        await asyncio.gather(setup, return_exceptions=True)
                            else:
                                while not setup.done():
                                    try:
                                        await asyncio.wait_for(asyncio.shield(setup), timeout=5)
                                    except TimeoutError:
                                        current, limit = effective_memory_sample()
                                        with contextlib.suppress(ConnectionError):
                                            await candidate.send({
                                                "event": "sample", "current": current,
                                                "limit": limit,
                                                "pressure_ratio": memory_pressure_ratio(), "runs": {},
                                            })
                                await asyncio.gather(setup)
                        except Exception:
                            # Initialization is not a transport failure. Exit so
                            # the provider can replace this failed physical worker.
                            launcher.failed = True
                            raise
                    else:
                        await launcher.reconnect(socket, launcher.requirements)
                    await launcher.send({
                        "event": "inventory", "inventory": launcher.inventory(),
                        "requirements": launcher.requirements,
                    })
                    for event in launcher.pending_terminal.values():
                        await launcher.send(event)
                    drain_task = await _receive_control_commands(
                        launcher, socket, stopping, drain_task
                    )
                    if stopping.is_set() and drain_task is not None and drain_task.done():
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if launcher is not None and launcher.failed:
                    message = "Remote launcher initialization or supervision failed"
                    level = logging.ERROR
                else:
                    message = "Remote control connection lost; preserving local children"
                    level = logging.WARNING
                if control_headers:
                    # Transport exceptions can retain request headers or echoed secrets.
                    logger.log(level, "%s (%s)", message, type(exc).__name__)
                else:
                    logger.log(level, message, exc_info=True)
                if stopping.is_set() or (launcher is not None and launcher.failed):
                    break
                await asyncio.sleep(1)
    finally:
        try:
            if launcher is not None:
                await _shutdown_launcher(launcher, drain_task, stopping)
            elif container_guest is not None:
                await container_guest.stop()
                container_guest.close()
        finally:
            if remaining is not None:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, signal.SIG_DFL)
    return 1 if launcher is not None and launcher.failed else 0


async def main() -> int:
    return await run_launcher()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(asyncio.run(main()))
