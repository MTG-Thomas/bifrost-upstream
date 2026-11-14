"""Administrator-configured trust for inbound workload token exchange."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.models.orm.base import Base


class WorkloadTrustRule(Base):
    """One trusted issuer and the BiFrost identity it may act as."""

    __tablename__ = "workload_trust_rules"
    __table_args__ = (
        UniqueConstraint("name", name="uq_workload_trust_rules_name"),
        CheckConstraint(
            "(jwks_uri IS NOT NULL) <> (pinned_jwks IS NOT NULL)",
            name="ck_workload_trust_rules_one_key_source",
        ),
        CheckConstraint(
            "token_lifetime_seconds BETWEEN 60 AND 3600",
            name="ck_workload_trust_rules_lifetime",
        ),
        Index("ix_workload_trust_rules_lookup", "enabled", "subject_token_type"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    issuer: Mapped[str] = mapped_column(String(512), nullable=False)
    subject_token_type: Mapped[str] = mapped_column(String(128), nullable=False)
    audiences: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    jwks_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    pinned_jwks: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    claim_conditions: Mapped[list[dict[str, str]]] = mapped_column(
        JSONB, nullable=False
    )
    identity_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    token_lifetime_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    created_by: Mapped[UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
