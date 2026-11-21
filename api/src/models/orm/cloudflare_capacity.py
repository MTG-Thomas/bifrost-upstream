"""Installation-local desired state and Cloudflare resource journal."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    SmallInteger,
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
        CheckConstraint(
            "control_root_purpose = 'control_root' AND control_root_scope = ''",
            name="ck_cf_installation_control_root",
        ),
        ForeignKeyConstraint(
            ["id", "control_root_purpose", "control_root_scope", "control_root_version"],
            [
                "cloudflare_capacity_credentials.installation_id", "cloudflare_capacity_credentials.purpose",
                "cloudflare_capacity_credentials.scope", "cloudflare_capacity_credentials.version",
            ],
            name="fk_cf_installation_control_root", deferrable=True, initially="DEFERRED", use_alter=True,
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
    usage_incomplete_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    usage_advisory: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    api_token_purpose: Mapped[str] = mapped_column(String(16), server_default="account_token")
    api_token_scope: Mapped[str] = mapped_column(String(64), server_default="")
    api_token_version: Mapped[int | None]
    control_root_purpose: Mapped[str] = mapped_column(String(16), server_default="control_root")
    control_root_scope: Mapped[str] = mapped_column(String(64), server_default="")
    control_root_version: Mapped[int | None]


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
    deployment_revision: Mapped[int] = mapped_column(primary_key=True)
    bearer_purpose: Mapped[str] = mapped_column(String(16), server_default="scope_bearer")
    bearer_version: Mapped[int]
    capacity_cpu_ms: Mapped[int] = mapped_column(Integer, server_default="30000")
    gateway_required: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))


class CloudflareWorkerReservation(Base):
    __tablename__ = "cloudflare_worker_reservations"
    __table_args__ = (
        CheckConstraint("deployment_revision > 0", name="ck_cf_reservation_revision"),
        CheckConstraint("control_root_version IS NOT NULL", name="ck_cf_reservation_has_root", postgresql_not_valid=True),
        Index("ix_cf_reservations_installation", "installation_id"),
        CheckConstraint(
            "(canary_job_id IS NULL) = (canary_lease_token IS NULL) AND "
            "(canary_sdk_at IS NULL OR canary_job_id IS NOT NULL) AND "
            "(canary_ack_at IS NULL OR canary_sdk_at IS NOT NULL)",
            name="ck_cf_canary_binding",
        ),
        CheckConstraint(
            "canary_sdk_generation IS NULL OR (canary_sdk_generation > 0 AND canary_sdk_at IS NOT NULL)",
            name="ck_cf_canary_sdk_generation",
        ),
        CheckConstraint(
            "control_root_purpose = 'control_root' AND control_root_scope = ''",
            name="ck_cf_reservation_control_root",
        ),
        ForeignKeyConstraint(
            ["installation_id", "control_root_purpose", "control_root_scope", "control_root_version"],
            [
                "cloudflare_capacity_credentials.installation_id", "cloudflare_capacity_credentials.purpose",
                "cloudflare_capacity_credentials.scope", "cloudflare_capacity_credentials.version",
            ],
            name="fk_cf_reservation_control_root",
        ),
    )

    worker_id: Mapped[UUID] = mapped_column(
        ForeignKey("execution_workers.id", ondelete="CASCADE"), primary_key=True,
    )
    installation_id: Mapped[UUID] = mapped_column(ForeignKey("cloudflare_installations.id", ondelete="RESTRICT"))
    scope: Mapped[str] = mapped_column(String(64))
    deployment_revision: Mapped[int]
    control_root_purpose: Mapped[str] = mapped_column(String(16), server_default="control_root")
    control_root_scope: Mapped[str] = mapped_column(String(64), server_default="")
    control_root_version: Mapped[int | None]
    canary_job_id: Mapped[UUID | None]
    canary_lease_token: Mapped[UUID | None]
    canary_sdk_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    canary_sdk_generation: Mapped[int | None] = mapped_column(Integer)
    canary_ack_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


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
    namespace_ids: Mapped[dict[str, str]] = mapped_column(JSONB, server_default="{}")
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CloudflareStagedToken(Base):
    __tablename__ = "cloudflare_staged_tokens"
    __table_args__ = (
        CheckConstraint("purpose IN ('connect', 'rotate')", name="ck_cf_staged_token_purpose"),
        Index("ix_cf_staged_token_expiry", "expires_at"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    actor_id: Mapped[UUID]
    account_id: Mapped[str] = mapped_column(String(32))
    purpose: Mapped[str] = mapped_column(String(16))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ciphertext: Mapped[str] = mapped_column(Text)


class CloudflareEngineUsageKey(Base):
    __tablename__ = "cloudflare_engine_usage_keys"

    installation_id: Mapped[UUID] = mapped_column(ForeignKey("cloudflare_installations.id"), primary_key=True)
    usage_key: Mapped[str] = mapped_column(String(256), primary_key=True)
    booked_by: Mapped[UUID | None]
    booked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CloudflareDispatchMeter(Base):
    __tablename__ = "cloudflare_dispatch_meters"

    worker_id: Mapped[UUID] = mapped_column(ForeignKey("cloudflare_usage_incarnations.worker_id"), primary_key=True)
    route_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    tenant_id: Mapped[str] = mapped_column(String(32))
    workflow_cpu_ms: Mapped[int | None]


class CloudflareUsageActivity(Base):
    __tablename__ = "cloudflare_usage_activity"
    __table_args__ = (
        CheckConstraint("dispatched_runs >= 0 AND sdk_requests >= 0", name="ck_cf_usage_activity_nonnegative"),
    )

    installation_id: Mapped[UUID] = mapped_column(ForeignKey("cloudflare_installations.id"), primary_key=True)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    dispatched_runs: Mapped[int] = mapped_column(BigInteger, server_default="0")
    sdk_requests: Mapped[int] = mapped_column(BigInteger, server_default="0")
    tenant_host_seconds: Mapped[int] = mapped_column(BigInteger, server_default="0")
    tenant_dispatches: Mapped[int] = mapped_column(BigInteger, server_default="0")
    inventory_tenant_seconds: Mapped[int] = mapped_column(BigInteger, server_default="0")
    inventory_dispatches: Mapped[int] = mapped_column(BigInteger, server_default="0")
    inventory_warm_pings: Mapped[int] = mapped_column(BigInteger, server_default="0")
    capacity_requests: Mapped[int] = mapped_column(BigInteger, server_default="0")
    capacity_cpu_ms: Mapped[int] = mapped_column(BigInteger, server_default="0")
    qualified_cpu_ms: Mapped[int] = mapped_column(BigInteger, server_default="0")
    qualified_dispatches: Mapped[int] = mapped_column(BigInteger, server_default="0")
    qualified_warm_pings: Mapped[int] = mapped_column(BigInteger, server_default="0")
    qualified_creations: Mapped[int] = mapped_column(BigInteger, server_default="0")
    qualified_legacy_events: Mapped[int] = mapped_column(BigInteger, server_default="0")
    generation_control_requests: Mapped[int] = mapped_column(BigInteger, server_default="0")
    released_cpu_us: Mapped[int] = mapped_column(BigInteger, server_default="0")
    released_engine_requests: Mapped[int] = mapped_column(BigInteger, server_default="0")


class CloudflareUsageIncarnation(Base):
    __tablename__ = "cloudflare_usage_incarnations"
    __table_args__ = (
        CheckConstraint("slots BETWEEN 1 AND 4", name="ck_cf_usage_slots"),
        CheckConstraint("deleted_at >= started_at", name="ck_cf_usage_lifetime"),
        Index("ix_cf_usage_incarnation_installation", "installation_id", "deleted_at"),
        Index("ix_cf_usage_incarnation_deleted", "deleted_at"),
    )

    worker_id: Mapped[UUID] = mapped_column(primary_key=True)
    installation_id: Mapped[UUID] = mapped_column(ForeignKey("cloudflare_installations.id"))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    slots: Mapped[int | None] = mapped_column(SmallInteger)
    meter_terminal: Mapped[dict[str, int] | None] = mapped_column(JSONB)
    meter_final: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    meter_uncertain: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    meter_release_digest: Mapped[str | None] = mapped_column(String(100))
    meter_rate_version: Mapped[str | None] = mapped_column(String(64))
    meter_bounds: Mapped[dict | None] = mapped_column(JSONB)
    deployment_revision: Mapped[int | None]


class CloudflareStorageResidue(Base):
    """Estimated storage from archived incarnations, grouped by deployment."""

    __tablename__ = "cloudflare_storage_residue"

    installation_id: Mapped[UUID] = mapped_column(ForeignKey("cloudflare_installations.id"), primary_key=True)
    release_digest: Mapped[str] = mapped_column(String(100), primary_key=True)
    deployment_revision: Mapped[int] = mapped_column(primary_key=True)
    launcher_bytes: Mapped[int] = mapped_column(BigInteger)
    ledger_bytes: Mapped[int] = mapped_column(BigInteger)
    rate_versions: Mapped[dict[str, bool]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
