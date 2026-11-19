"""Launcher-side PlatformJob compute ledger. It has no platform data access."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from src.worker.remote.platform_stage_protocol import (
    MAX_STAGE_RESULT_BYTES,
    StageBinding,
    encode_bundle,
)

MAX_STAGE_RECORDS = 4096


@dataclass(frozen=True)
class StageProgram:
    module: str
    timeout_seconds: int


@dataclass
class _Stage:
    binding: StageBinding
    task: asyncio.Task[None] | None = None
    process: asyncio.subprocess.Process | None = None
    result: dict[str, Any] | None = None
    state: str = "running"
    stopping: bool = False


class PlatformStages:
    """Hold a single active or retained stage and identity-only tombstones."""

    def __init__(
        self,
        programs: Mapping[str, StageProgram],
        kill: Callable[[asyncio.subprocess.Process], Awaitable[None]],
    ) -> None:
        self.programs = programs
        self.kill = kill
        self.active: _Stage | None = None
        self.released: dict[str, StageBinding] = {}

    def inventory(self) -> list[dict[str, str]]:
        return [self.active.binding.wire()] if self.active is not None else []

    def _lookup(self, binding: StageBinding) -> _Stage | None:
        stage = self.active
        if stage is not None and stage.binding.stage_id == binding.stage_id:
            if stage.binding != binding:
                raise ValueError("Platform stage binding changed")
            return stage
        tombstone = self.released.get(binding.stage_id)
        if tombstone is not None and tombstone != binding:
            raise ValueError("Released platform stage binding changed")
        return None

    def status(self, binding: StageBinding) -> dict[str, Any]:
        stage = self._lookup(binding)
        if stage is None:
            return {**binding.wire(), "state": "released" if binding.stage_id in self.released else "unknown"}
        return {**binding.wire(), "state": stage.state, "result": stage.result}

    def start(self, binding: StageBinding, bundle: dict[str, Any]) -> dict[str, Any]:
        program = self.programs[binding.capability]
        encoded = binding.verify(bundle)
        status = self.status(binding)
        if status["state"] != "unknown":
            return status
        if self.active is not None or len(self.released) >= MAX_STAGE_RECORDS:
            raise ValueError("Platform stage capacity exhausted")
        stage = self.active = _Stage(binding)
        stage.task = asyncio.create_task(self._run(stage, program, encoded))
        return self.status(binding)

    async def release(self, binding: StageBinding) -> dict[str, Any]:
        stage = self._lookup(binding)
        if stage is not None:
            stage.stopping = True
            if stage.process is not None:
                await self.kill(stage.process)
            # The task also observes stopping if release won the spawn race.
            assert stage.task is not None
            await asyncio.shield(stage.task)
            if self.active is stage:
                self.active = None
                self.released[binding.stage_id] = binding
        return self.status(binding)

    async def close(self) -> None:
        if self.active is not None:
            await self.release(self.active.binding)

    async def _run(self, stage: _Stage, program: StageProgram, encoded: bytes) -> None:
        try:
            with TemporaryDirectory(prefix="bifrost-platform-stage-") as directory:
                root = Path(directory)
                (root / "input.json").write_bytes(encoded)
                # Whitelist only execution paths and CA settings. In particular
                # Git configuration, proxies, SDK tokens and control keys are absent.
                env = {
                    name: os.environ[name]
                    for name in ("PATH", "PYTHONPATH", "LANG", "SSL_CERT_FILE", "SSL_CERT_DIR")
                    if name in os.environ
                }
                env.update(HOME=directory, TMPDIR=directory, GIT_CONFIG_NOSYSTEM="1",
                           GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0")
                if stage.stopping:
                    return
                process = stage.process = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", program.module, directory, env=env,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                    start_new_session=True, limit=MAX_STAGE_RESULT_BYTES + 1,
                )
                try:
                    if stage.stopping:
                        return
                    assert process.stdout is not None
                    async with asyncio.timeout(program.timeout_seconds):
                        try:
                            await process.stdout.readexactly(MAX_STAGE_RESULT_BYTES + 1)
                        except asyncio.IncompleteReadError as exc:
                            output = exc.partial
                        else:
                            raise ValueError("Platform stage output exceeds its limit")
                        if await process.wait() != 0:
                            raise ValueError("Platform stage process failed")
                    result = json.loads(output)
                    if not isinstance(result, dict):
                        raise TypeError("Platform stage result must be an object")
                    encode_bundle(result, MAX_STAGE_RESULT_BYTES)
                    stage.result = result
                    stage.state = "succeeded"
                finally:
                    await self.kill(process)
        except (OSError, ValueError, TypeError, TimeoutError, RecursionError):
            stage.state = "failed"
        finally:
            if stage.state == "running":
                stage.state = "failed"
