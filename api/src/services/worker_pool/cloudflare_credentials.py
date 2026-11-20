"""Encrypt Cloudflare credentials with their installation and purpose binding."""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import SecretStr
from shared.worker_control_auth import managed_cloudflare_control_key
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import decrypt_secret, encrypt_secret
from src.models.orm.cloudflare_capacity import (
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareWorkerReservation,
)

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


async def load_account_token(db: AsyncSession, installation: CloudflareInstallation) -> SecretStr:
    version = installation.api_token_version
    if version is None:
        raise ValueError("Cloudflare management credential is unavailable")
    row = await db.get(CloudflareCredential, (installation.id, "account_token", "", version))
    if row is None:
        raise ValueError("Cloudflare management credential is unavailable")
    return open_credential(CredentialRef(installation.id, installation.account_id, "account_token", "", version), row)


async def load_managed_control_key(db: AsyncSession, pin: CloudflareWorkerReservation, resource_uid: str) -> str:
    version = pin.control_root_version
    if version is None:
        raise ValueError("Cloudflare control root is unavailable")
    row = await db.get(CloudflareCredential, (pin.installation_id, "control_root", "", version))
    if row is None:
        raise ValueError("Cloudflare control root is unavailable")
    root = open_credential(CredentialRef(pin.installation_id, row.account_id, "control_root", "", version), row)
    return managed_cloudflare_control_key(
        root.get_secret_value(), pin.installation_id, version, pin.scope, pin.worker_id, resource_uid,
    )
