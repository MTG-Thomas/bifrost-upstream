"""Incarnation-scoped external-worker control keys; no application imports.

The installation key remains in the API. The API and provider derive a worker
key from their existing dedicated capacity credential and the prepared resource.
Canonical admission and ownership revoke terminal incarnations independently.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from uuid import UUID

_EXTERNAL_CONTROL_DOMAIN = b"bifrost.external-worker.control.v1\0"


def external_worker_control_key(
    provider_key: str, worker_id: UUID, resource_uid: str,
) -> str:
    """Derive a key that cannot authenticate another incarnation or resource."""
    if re.fullmatch(r"[a-zA-Z0-9._:-]{1,255}", resource_uid) is None:
        raise ValueError("Invalid external worker resource identity")
    binding = json.dumps([str(worker_id), resource_uid], separators=(",", ":")).encode()
    return hmac.new(
        provider_key.encode(), _EXTERNAL_CONTROL_DOMAIN + binding, hashlib.sha256,
    ).hexdigest()
