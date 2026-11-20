"""Pin Enable's reserved runtime to the active PlatformJob lease."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261120_cf_runtime_canary"
down_revision = "20261120_cf_scope_generations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "cloudflare_worker_reservations"
    op.add_column(table, sa.Column("canary_job_id", sa.Uuid()))
    op.add_column(table, sa.Column("canary_lease_token", sa.Uuid()))
    op.add_column(table, sa.Column("canary_sdk_at", sa.DateTime(timezone=True)))
    op.add_column(table, sa.Column("canary_ack_at", sa.DateTime(timezone=True)))
    op.add_column("cloudflare_owned_resources", sa.Column("namespace_ids", JSONB(), server_default="{}", nullable=False))
    op.create_check_constraint(
        "ck_cf_canary_binding", table,
        "(canary_job_id IS NULL) = (canary_lease_token IS NULL) "
        "AND (canary_sdk_at IS NULL OR canary_job_id IS NOT NULL) "
        "AND (canary_ack_at IS NULL OR canary_sdk_at IS NOT NULL)",
    )


def downgrade() -> None:
    table = "cloudflare_worker_reservations"
    op.drop_constraint("ck_cf_canary_binding", table, type_="check")
    op.drop_column("cloudflare_owned_resources", "namespace_ids")
    for name in ("canary_ack_at", "canary_sdk_at", "canary_lease_token", "canary_job_id"):
        op.drop_column(table, name)
