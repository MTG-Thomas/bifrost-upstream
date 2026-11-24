"""Checkpoint managed Workers replacement and establish the release fence."""

import sqlalchemy as sa
from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261121_cf_workers_upgrade"
down_revision = "20261121_cf_queue_prewarm"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_installations", sa.Column("deployed_revision", sa.Integer()))
    op.add_column("cloudflare_installations", sa.Column("upgrade_release", sa.String(64)))
    op.add_column("cloudflare_installations", sa.Column("upgrade_drained_at", sa.DateTime(timezone=True)))
    op.add_column("cloudflare_installations", sa.Column("workers_release_version", sa.String(128)))
    op.add_column("cloudflare_installations", sa.Column("workers_release_sequence", sa.BigInteger()))
    op.add_column("cloudflare_installations", sa.Column("workers_release_digest", sa.String(64)))
    op.add_column(
        "cloudflare_installations",
        sa.Column("drain_required", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_check_constraint(
        "ck_cf_upgrade_checkpoint", "cloudflare_installations",
        "(upgrade_release IS NULL OR (desired_state = 'enabled' AND admitted_revision IS NULL)) "
        "AND (upgrade_drained_at IS NULL OR upgrade_release IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_cf_workers_release_identity", "cloudflare_installations",
        "num_nonnulls(workers_release_version, workers_release_sequence, workers_release_digest) IN (0, 3)",
    )
    # Columns begin NULL, including the sequence for pilots without a known version.
    # Preserve the deployed revision while closing admission until replacement is verified.
    op.execute("""
        UPDATE cloudflare_installations
        SET deployed_revision = admitted_revision,
            admitted_revision = NULL,
            desired_revision = desired_revision + 1,
            drain_required = TRUE
        WHERE admitted_revision IS NOT NULL
    """)


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_installations")
    op.drop_constraint("ck_cf_workers_release_identity", "cloudflare_installations", type_="check")
    op.drop_constraint("ck_cf_upgrade_checkpoint", "cloudflare_installations", type_="check")
    op.drop_column("cloudflare_installations", "drain_required")
    op.drop_column("cloudflare_installations", "workers_release_digest")
    op.drop_column("cloudflare_installations", "workers_release_sequence")
    op.drop_column("cloudflare_installations", "workers_release_version")
    op.drop_column("cloudflare_installations", "upgrade_drained_at")
    op.drop_column("cloudflare_installations", "upgrade_release")
    op.drop_column("cloudflare_installations", "deployed_revision")
