"""Installation-local desired state and Cloudflare resource journal."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    Text,
    UniqueConstraint,
    text,
    true,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.models.orm.base import Base


class CloudflareInstallation(Base):
    __tablename__ = "cloudflare_installations"
    __table_args__ = (
        CheckConstraint("singleton", name="ck_cf_installation_singleton"),
        CheckConstraint("desired_revision > 0", name="ck_cf_installation_revision"),
        CheckConstraint("desired_state IN ('scaled_in', 'enabled', 'disconnected')", name="ck_cf_desired_state"),
        CheckConstraint(
            "admitted_revision IS NULL OR (desired_state = 'enabled' AND admitted_revision = desired_revision)",
            name="ck_cf_admitted_revision",
        ),
        UniqueConstraint("id", "account_id", name="uq_cf_installation_account"),
        Index("uq_cf_active_installation", "singleton", unique=True, postgresql_where=text("retired_at IS NULL")),
        CheckConstraint("api_token_purpose = 'account_token' AND api_token_scope = ''", name="ck_cf_account_token_binding"),
        CheckConstraint("retired_at IS NOT NULL OR api_token_version IS NOT NULL", name="ck_cf_active_has_account_token"),
        CheckConstraint(
            "retired_at IS NULL OR (desired_state = 'disconnected' AND api_token_version IS NULL)",
            name="ck_cf_retired_installation",
        ),
        ForeignKeyConstraint(
            ["id", "api_token_purpose", "api_token_scope", "api_token_version"],
            [
                "cloudflare_capacity_credentials.installation_id", "cloudflare_capacity_credentials.purpose",
                "cloudflare_capacity_credentials.scope", "cloudflare_capacity_credentials.version",
            ],
            name="fk_cf_active_account_token", deferrable=True, initially="DEFERRED", use_alter=True,
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    singleton: Mapped[bool] = mapped_column(Boolean, server_default=true())
    account_id: Mapped[str] = mapped_column(String(32))
    worker_ingress_origin: Mapped[str] = mapped_column(String(255))
    consent_version: Mapped[str] = mapped_column(String(64))
    consent_actor: Mapped[str] = mapped_column(String(255))
    consent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    policy: Mapped[dict[str, Any]] = mapped_column(JSONB)
    desired_revision: Mapped[int]
    desired_state: Mapped[str] = mapped_column(String(16))
    admitted_revision: Mapped[int | None]
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    api_token_purpose: Mapped[str] = mapped_column(String(16), server_default="account_token")
    api_token_scope: Mapped[str] = mapped_column(String(64), server_default="")
    api_token_version: Mapped[int | None]


class CloudflareCredential(Base):
    __tablename__ = "cloudflare_capacity_credentials"
    __table_args__ = (
        ForeignKeyConstraint(
            ["installation_id", "account_id"],
            ["cloudflare_installations.id", "cloudflare_installations.account_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint("version > 0", name="ck_cf_credential_version"),
        CheckConstraint(
            "(purpose IN ('account_token', 'control_root') AND scope = '') OR "
            "(purpose = 'scope_bearer' AND scope <> '')",
            name="ck_cf_credential_scope",
        ),
    )

    installation_id: Mapped[UUID] = mapped_column(primary_key=True)
    purpose: Mapped[str] = mapped_column(String(16), primary_key=True)
    scope: Mapped[str] = mapped_column(String(64), primary_key=True)
    version: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[str] = mapped_column(String(32))
    ciphertext: Mapped[str | None] = mapped_column(Text)
    recorded_by: Mapped[str] = mapped_column(String(255))
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class CloudflareCapacityScope(Base):
    __tablename__ = "cloudflare_capacity_scopes"
    __table_args__ = (
        ForeignKeyConstraint(
            ["installation_id", "bearer_purpose", "scope", "bearer_version"],
            [
                "cloudflare_capacity_credentials.installation_id",
                "cloudflare_capacity_credentials.purpose",
                "cloudflare_capacity_credentials.scope",
                "cloudflare_capacity_credentials.version",
            ],
            ondelete="RESTRICT",
        ),
        CheckConstraint("bearer_purpose = 'scope_bearer'", name="ck_cf_scope_bearer_purpose"),
        CheckConstraint("deployment_revision > 0", name="ck_cf_scope_deployment_revision"),
    )

    installation_id: Mapped[UUID] = mapped_column(
        ForeignKey("cloudflare_installations.id", ondelete="RESTRICT"), primary_key=True
    )
    scope: Mapped[str] = mapped_column(String(64), primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(255))
    deployment_revision: Mapped[int]
    bearer_purpose: Mapped[str] = mapped_column(String(16), server_default="scope_bearer")
    bearer_version: Mapped[int]


class CloudflareWorkerReservation(Base):
    __tablename__ = "cloudflare_worker_reservations"
    __table_args__ = (
        CheckConstraint("deployment_revision > 0", name="ck_cf_reservation_revision"),
        Index("ix_cf_reservations_installation", "installation_id"),
    )

    worker_id: Mapped[UUID] = mapped_column(
        ForeignKey("execution_workers.id", ondelete="CASCADE"), primary_key=True,
    )
    installation_id: Mapped[UUID] = mapped_column(ForeignKey("cloudflare_installations.id", ondelete="RESTRICT"))
    scope: Mapped[str] = mapped_column(String(64))
    deployment_revision: Mapped[int]


class CloudflareOwnedResource(Base):
    __tablename__ = "cloudflare_owned_resources"
    __table_args__ = (
        ForeignKeyConstraint(
            ["installation_id", "account_id"],
            ["cloudflare_installations.id", "cloudflare_installations.account_id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint("installation_id", "account_id", "kind", "target_name", name="uq_cf_resource_target"),
        UniqueConstraint("account_id", "kind", "provider_id", name="uq_cf_resource_provider"),
        CheckConstraint("desired_revision > 0", name="ck_cf_resource_revision"),
        UniqueConstraint("effect_id", name="uq_cf_resource_effect"),
        CheckConstraint(
            "state IN ('creating', 'present', 'deleting', 'absent') AND "
            "(state NOT IN ('present', 'deleting') OR "
            "(provider_id IS NOT NULL AND provider_version IS NOT NULL))",
            name="ck_cf_resource_observation",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    installation_id: Mapped[UUID] = mapped_column(index=True)
    account_id: Mapped[str] = mapped_column(String(32))
    scope: Mapped[str] = mapped_column(String(64))
    kind: Mapped[str] = mapped_column(String(32))
    target_name: Mapped[str] = mapped_column(String(255))
    desired_revision: Mapped[int]
    artifact_digest: Mapped[str] = mapped_column(String(64))
    effect_id: Mapped[UUID]
    state: Mapped[str] = mapped_column(String(16))
    provider_id: Mapped[str | None] = mapped_column(String(255))
    provider_version: Mapped[str | None] = mapped_column(String(255))
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
