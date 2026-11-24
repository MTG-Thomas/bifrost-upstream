"""Pin managed Workers reservations to their installation and deployment."""

import sqlalchemy as sa
from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261120_cf_reservation_pin"
down_revision = "20261120_cf_resource_tombstone"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloudflare_worker_reservations",
        sa.Column("worker_id", sa.Uuid(), sa.ForeignKey("execution_workers.id", ondelete="CASCADE"), primary_key=True),
        sa.Column(
            "installation_id", sa.Uuid(),
            sa.ForeignKey("cloudflare_installations.id", ondelete="RESTRICT"), nullable=False,
        ),
        sa.Column("scope", sa.String(64), nullable=False),
        sa.Column("deployment_revision", sa.Integer(), nullable=False),
        sa.CheckConstraint("deployment_revision > 0", name="ck_cf_reservation_revision"),
    )
    op.create_index("ix_cf_reservations_installation", "cloudflare_worker_reservations", ["installation_id"])


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_worker_reservations")
    op.drop_table("cloudflare_worker_reservations")
