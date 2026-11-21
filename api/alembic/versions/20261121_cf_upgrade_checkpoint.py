"""Checkpoint managed Workers release replacement."""

import sqlalchemy as sa
from alembic import op

revision = "20261121_cf_upgrade_checkpoint"
down_revision = "20261121_cf_cleanup_window"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_installations", sa.Column("deployed_revision", sa.Integer()))
    op.add_column("cloudflare_installations", sa.Column("upgrade_release", sa.String(64)))
    op.add_column("cloudflare_installations", sa.Column("upgrade_drained_at", sa.DateTime(timezone=True)))
    op.create_check_constraint(
        "ck_cf_upgrade_checkpoint", "cloudflare_installations",
        "(upgrade_release IS NULL OR (desired_state = 'enabled' AND admitted_revision IS NULL)) "
        "AND (upgrade_drained_at IS NULL OR upgrade_release IS NOT NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_cf_upgrade_checkpoint", "cloudflare_installations", type_="check")
    op.drop_column("cloudflare_installations", "upgrade_drained_at")
    op.drop_column("cloudflare_installations", "upgrade_release")
    op.drop_column("cloudflare_installations", "deployed_revision")
