"""Operator-reviewed workspace workflow revisions."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Index, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.models.orm.base import Base


class CloudflareApproval(Base):
    __tablename__ = "cloudflare_workflow_approvals"
    __table_args__ = (
        CheckConstraint("NOT (is_review AND is_current)", name="ck_cloudflare_review_inactive"),
        Index("uq_cloudflare_pending_review", "workflow_id", unique=True, postgresql_where=text("is_review")),
        Index("uq_cloudflare_approval_current", "workflow_id", unique=True, postgresql_where=text("is_current")),
    )

    revision: Mapped[str] = mapped_column(String(64), primary_key=True)
    workflow_id: Mapped[UUID] = mapped_column(ForeignKey("workflows.id", ondelete="CASCADE"), index=True)
    # Approval-time owner, including NULL for global workflows. Preserve this
    # witness when an organization is deleted or a workflow is relocated.
    organization_id: Mapped[UUID | None]
    policy_version: Mapped[str] = mapped_column(String(64))
    declaration: Mapped[dict[str, Any]] = mapped_column(JSONB)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)
    is_review: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))
    accepted_by: Mapped[str | None] = mapped_column(String(255))
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    derived_from: Mapped[str | None] = mapped_column(ForeignKey("cloudflare_workflow_approvals.revision"))
    pin_diff: Mapped[dict[str, dict[str, str | None]] | None] = mapped_column(JSONB(none_as_null=True))
