"""A bounded data-only result pipe for a managed Container workflow."""

import json
import math
import os
from multiprocessing.connection import Connection
from queue import Empty
from struct import Struct
from typing import Any

MAX_RESULT_BYTES = 8 * 1024 * 1024
_HEADER = Struct("!I")
_READ_BYTES = 64 * 1024


class InvalidContainerResult(ValueError):
    """The guest sent a malformed or oversized result."""


def _finite(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise InvalidContainerResult("Nonfinite Container result value")
    return number


class ContainerResultWriter:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def send(self, result: dict[str, Any]) -> None:
        data = json.dumps(result, default=str, allow_nan=False, separators=(",", ":")).encode()
        if len(data) > MAX_RESULT_BYTES:
            raise InvalidContainerResult("Container result exceeds its byte limit")
        # Connection uses a four-byte network-order length for frames this size.
        self._conn.send_bytes(data)


class ContainerResultReader:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._buffer = bytearray()
        self._length: int | None = None
        os.set_blocking(conn.fileno(), False)

    def fileno(self) -> int:
        return self._conn.fileno()

    def close(self) -> None:
        self._conn.close()

    def get_nowait(self) -> dict[str, Any]:
        # A guest can leave a partial header or body. Never block the
        # supervisor, allocate from an unchecked length, or unpickle bytes.
        try:
            chunk = os.read(self.fileno(), _READ_BYTES)
        except BlockingIOError:
            raise Empty from None
        if not chunk:
            raise EOFError
        self._buffer.extend(chunk)
        if self._length is None:
            if len(self._buffer) < _HEADER.size:
                raise Empty
            self._length, = _HEADER.unpack_from(self._buffer)
            if not 0 < self._length <= MAX_RESULT_BYTES:
                raise InvalidContainerResult("Container result has an invalid frame length")
            del self._buffer[:_HEADER.size]
        if len(self._buffer) < self._length:
            raise Empty
        if len(self._buffer) != self._length:
            raise InvalidContainerResult("Container sent more than one result")
        try:
            result = json.loads(self._buffer.decode(), parse_constant=_finite, parse_float=_finite)
        except (ValueError, RecursionError):
            raise InvalidContainerResult("Container result is not valid JSON") from None
        if not isinstance(result, dict) or type(result.get("success")) is not bool:
            raise InvalidContainerResult("Container result has an invalid outcome")
        duration = result.get("duration_ms")
        if type(duration) is not int or not 0 <= duration < 2**31:
            raise InvalidContainerResult("Container result has an invalid duration")
        if any(result.get(key) is not None and not isinstance(result[key], dict) for key in (
            "roi", "variables", "execution_context",
        )):
            raise InvalidContainerResult("Container result has invalid execution metadata")
        if any(result.get(key) is not None and not isinstance(result[key], str) for key in (
            "status", "error", "error_type",
        )):
            raise InvalidContainerResult("Container result has an invalid status or error")
        # Process metrics used by the supervisor are measured locally.
        result.pop("metrics", None)
        return result
