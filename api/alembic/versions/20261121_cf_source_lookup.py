"""Index the execution-to-Container source authority lookup."""

import sqlalchemy as sa
from alembic import op

revision = "20261121_cf_source_lookup"
down_revision = "20261121_cf_containers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_cf_container_execution",
        "cloudflare_worker_reservations",
        [sa.text("(container_envelope ->> 'execution_id')")],
        postgresql_where=sa.text("container_reserved_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_cf_container_execution", table_name="cloudflare_worker_reservations")
