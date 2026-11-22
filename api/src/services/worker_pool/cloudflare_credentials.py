"""Encrypt Cloudflare credentials with their installation and purpose binding."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

from pydantic import SecretStr
from shared.worker_control_auth import managed_cloudflare_control_key
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import decrypt_secret, encrypt_secret
from src.models.orm.cloudflare_capacity import (
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareStagedToken,
    CloudflareWorkerReservation,
)

CredentialPurpose = Literal["account_token", "control_root", "scope_bearer", "tunnel_secret", "connector_token"]
StagePurpose = Literal["connect", "rotate"]


class StagedCloudflareCredentialUnavailable(ValueError):
    """A one-time credential was missing, expired, or bound to another request."""


def _stage_binding(
    ref: UUID, actor_id: UUID, account_id: str, purpose: StagePurpose, installation_id: UUID | None,
) -> list[str]:
    binding = ["cloudflare-staged-token-v1", str(ref), str(actor_id), account_id, purpose]
    if installation_id is not None:
        return [*binding, str(installation_id)]
    return binding


async def purge_expired_staged_tokens(db: AsyncSession) -> None:
    await db.execute(delete(CloudflareStagedToken).where(
        CloudflareStagedToken.expires_at <= func.statement_timestamp(),
    ))


async def stage_account_token(
    db: AsyncSession, *, actor_id: UUID, account_id: str, purpose: StagePurpose, token: SecretStr,
    installation_id: UUID | None = None,
) -> tuple[UUID, datetime]:
    if (purpose == "rotate") != (installation_id is not None):
        raise ValueError("Cloudflare rotation requires an installation binding")
    await purge_expired_staged_tokens(db)
    ref = uuid4()
    expires_at = datetime.now(UTC) + timedelta(minutes=10)
    binding = _stage_binding(ref, actor_id, account_id, purpose, installation_id)
    db.add(CloudflareStagedToken(
        id=ref, actor_id=actor_id, account_id=account_id, purpose=purpose, expires_at=expires_at,
        ciphertext=encrypt_secret(json.dumps([binding, token.get_secret_value()])),
    ))
    return ref, expires_at


async def consume_staged_token(
    db: AsyncSession, ref: UUID, *, actor_id: UUID, account_id: str, purpose: StagePurpose,
    installation_id: UUID | None = None,
) -> SecretStr:
    row = await db.scalar(delete(CloudflareStagedToken).where(
        CloudflareStagedToken.id == ref,
        CloudflareStagedToken.actor_id == actor_id,
        CloudflareStagedToken.account_id == account_id,
        CloudflareStagedToken.purpose == purpose,
        CloudflareStagedToken.expires_at > func.statement_timestamp(),
    ).returning(CloudflareStagedToken.ciphertext))
    if row is not None:
        binding, value = json.loads(decrypt_secret(row))
        if binding == _stage_binding(ref, actor_id, account_id, purpose, installation_id) and isinstance(value, str):
            return SecretStr(value)
    raise StagedCloudflareCredentialUnavailable("Invalid or expired Cloudflare credential reference")


async def read_staged_connect_token(db: AsyncSession, ref: UUID, *, actor_id: UUID) -> tuple[str, SecretStr, datetime]:
    staged = CloudflareStagedToken
    row = (await db.execute(select(staged.account_id, staged.ciphertext, staged.expires_at).where(
        staged.id == ref, staged.actor_id == actor_id, staged.purpose == "connect",
        staged.expires_at > func.statement_timestamp(),
    ))).one_or_none()
    if row is not None:
        account_id, ciphertext, expires_at = row
        binding, value = json.loads(decrypt_secret(ciphertext))
        if binding == _stage_binding(ref, actor_id, account_id, "connect", None) and isinstance(value, str):
            return account_id, SecretStr(value), expires_at
    raise StagedCloudflareCredentialUnavailable("Invalid or expired Cloudflare credential reference")


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
