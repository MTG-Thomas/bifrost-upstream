"""Keep attempt uncertainty on the foreground booking."""

import sqlalchemy as sa
from alembic import op

revision = "20261122_cf_route_uncertainty"
down_revision = "20261122_cf_workers_deadline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_dispatch_meters", sa.Column(
        "attempt_uncertain", sa.Boolean(), nullable=False, server_default=sa.false(),
    ))


def downgrade() -> None:
    op.execute("""
        UPDATE cloudflare_usage_incarnations i SET meter_uncertain = true
        WHERE EXISTS (
            SELECT 1 FROM cloudflare_dispatch_meters d
            WHERE d.worker_id = i.worker_id AND d.attempt_uncertain
        )
    """)
    op.drop_column("cloudflare_dispatch_meters", "attempt_uncertain")
