"""Add the Cloudflare chat event outbox."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20261119_cf_chat_outbox"
down_revision = "20261119_pjob_remote_stage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloudflare_chat_outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("claim_id", sa.Uuid(), sa.ForeignKey("cloudflare_agent_attempts.claim_id", ondelete="CASCADE"), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), sa.ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("chunk_sequence", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("envelope", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("redis_floor", sa.BigInteger()),
        sa.Column("redis_sequence", sa.BigInteger()),
        sa.Column("projection_lost", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.UniqueConstraint("claim_id", "chunk_sequence"),
    )
    op.create_index(
        "ix_cf_chat_outbox_pending", "cloudflare_chat_outbox", ["conversation_id", "id"],
        postgresql_where=sa.text("redis_sequence IS NULL AND projection_lost IS FALSE"),
    )
    op.create_index("ix_cf_chat_outbox_history", "cloudflare_chat_outbox", ["claim_id", "redis_sequence"])
    op.create_index("ix_cf_chat_outbox_lost", "cloudflare_chat_outbox", ["claim_id"],
                    postgresql_where=sa.text("projection_lost IS TRUE"))
    op.create_index("ix_cf_chat_outbox_receipts", "cloudflare_chat_outbox", ["id"], postgresql_where=sa.text(
        "redis_floor IS NOT NULL AND (redis_sequence IS NOT NULL OR projection_lost IS TRUE)",
    ))
    op.create_index("ix_cf_chat_outbox_prune", "cloudflare_chat_outbox", ["created_at"], postgresql_where=sa.text(
        "redis_floor IS NULL AND (redis_sequence IS NOT NULL OR projection_lost IS TRUE)",
    ))


def downgrade() -> None:
    op.drop_table("cloudflare_chat_outbox")
