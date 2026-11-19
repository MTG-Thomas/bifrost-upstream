"""Preserve unreported optional token usage for Cloudflare summaries."""

from alembic import op

revision = "20261118_cf_summary_usage"
down_revision = "20261118_cf_solution_source"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        UPDATE work_dispatches
        SET payload = payload || '{"linux_only": true}'::jsonb, cloudflare_revision = NULL
        WHERE family = 'agent_summary'
          AND cloudflare_revision = 'b3a451a62d1d77deb45f05a897c4bc48d81635368b23aa6ae152c4e60a57e5b1'
    """)
    op.alter_column("ai_usage", "cache_read_tokens", nullable=True)
    op.alter_column("ai_usage", "reasoning_tokens", nullable=True)


def downgrade() -> None:
    op.execute("""
        UPDATE work_dispatches
        SET payload = payload - 'linux_only',
            cloudflare_revision = 'b3a451a62d1d77deb45f05a897c4bc48d81635368b23aa6ae152c4e60a57e5b1'
        WHERE family = 'agent_summary' AND payload @> '{"linux_only": true}'::jsonb
    """)
    op.execute("UPDATE ai_usage SET cache_read_tokens = 0 WHERE cache_read_tokens IS NULL")
    op.execute("UPDATE ai_usage SET reasoning_tokens = 0 WHERE reasoning_tokens IS NULL")
    op.alter_column("ai_usage", "cache_read_tokens", nullable=False)
    op.alter_column("ai_usage", "reasoning_tokens", nullable=False)
