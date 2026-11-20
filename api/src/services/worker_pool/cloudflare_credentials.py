"""Encrypt Cloudflare credentials with their installation and purpose binding."""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import SecretStr

from src.core.security import decrypt_secret, encrypt_secret
from src.models.orm.cloudflare_capacity import CloudflareCredential

CredentialPurpose = Literal["account_token", "control_root", "scope_bearer"]


@dataclass(frozen=True, slots=True)
class CredentialRef:
    installation_id: UUID
    account_id: str
    purpose: CredentialPurpose
    scope: str
    version: int

    def __post_init__(self) -> None:
        if (self.purpose == "scope_bearer") != bool(self.scope) or self.version < 1:
            raise ValueError("Invalid Cloudflare credential reference")

    def binding(self) -> list[str | int]:
        return [str(self.installation_id), self.account_id, self.purpose, self.scope, self.version]


def seal_credential(
    ref: CredentialRef, value: SecretStr, *, actor: str, now: datetime
) -> CloudflareCredential:
    return CloudflareCredential(
        installation_id=ref.installation_id,
        account_id=ref.account_id,
        purpose=ref.purpose,
        scope=ref.scope,
        version=ref.version,
        ciphertext=encrypt_secret(json.dumps([ref.binding(), value.get_secret_value()])),
        recorded_by=actor,
        recorded_at=now,
    )


def open_credential(ref: CredentialRef, row: CloudflareCredential) -> SecretStr:
    if (
        row.ciphertext is None
        or ref.binding() != [str(row.installation_id), row.account_id, row.purpose, row.scope, row.version]
    ):
        raise ValueError("Cloudflare credential is unavailable for this reference")
    binding, value = json.loads(decrypt_secret(row.ciphertext))
    if binding != ref.binding() or not isinstance(value, str):
        raise ValueError("Cloudflare credential binding does not match")
    return SecretStr(value)
