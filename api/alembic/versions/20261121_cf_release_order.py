"""Mark cutover drains and bind numbered releases to SemVer precedence."""

import sqlalchemy as sa
from alembic import op

revision = "20261121_cf_release_order"
down_revision = "20261121_cf_release_sequence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_installations", sa.Column("workers_release_version", sa.String(128)))
    op.add_column("cloudflare_installations", sa.Column("workers_release_digest", sa.String(64)))
    op.add_column(
        "cloudflare_installations",
        sa.Column("drain_required", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    # Capture the cutover reason at the same time as closing admission. A
    # later revision gap alone could belong to Scale In or a policy edit.
    op.execute("""
        UPDATE cloudflare_installations
        SET deployed_revision = admitted_revision,
            admitted_revision = NULL,
            desired_revision = desired_revision + 1,
            drain_required = TRUE
        WHERE admitted_revision IS NOT NULL
    """)


def downgrade() -> None:
    op.drop_column("cloudflare_installations", "drain_required")
    op.drop_column("cloudflare_installations", "workers_release_digest")
    op.drop_column("cloudflare_installations", "workers_release_version")
