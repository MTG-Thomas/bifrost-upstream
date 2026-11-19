"""Small, immutable PlatformJob bundles carried by the worker control socket."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any
from uuid import UUID

MAX_STAGE_INPUT_BYTES = 96 * 1024
MAX_STAGE_RESULT_BYTES = 4 * 1024


def encode_bundle(bundle: dict[str, Any], limit: int) -> bytes:
    data = json.dumps(
        bundle, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    ).encode()
    if len(data) > limit:
        raise ValueError("Platform stage bundle exceeds its size limit")
    return data


@dataclass(frozen=True)
class StageBinding:
    stage_id: str
    job_id: str
    lease_token: str
    capability: str
    input_sha256: str

    @classmethod
    def parse(cls, payload: dict[str, Any]) -> StageBinding:
        identifiers = [str(UUID(payload[key])) for key in ("stage_id", "job_id", "lease_token")]
        capability, digest = payload["capability"], payload["input_sha256"]
        if not isinstance(capability, str) or not re.fullmatch(r"[a-z_][a-z_.0-9]{0,95}", capability):
            raise ValueError("Invalid platform stage capability")
        if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ValueError("Invalid platform stage digest")
        return cls(*identifiers, capability, digest)

    def wire(self) -> dict[str, str]:
        return asdict(self)

    def verify(self, bundle: dict[str, Any]) -> bytes:
        encoded = encode_bundle(bundle, MAX_STAGE_INPUT_BYTES)
        if hashlib.sha256(encoded).hexdigest() != self.input_sha256:
            raise ValueError("Platform stage input digest mismatch")
        return encoded
