"""Exclude the Workers Paid subscription from existing fleet envelopes."""

from decimal import Decimal

from alembic import op
from sqlalchemy import text

revision = "20261120_cf_usage_only_envelope"
down_revision = "20261120_cf_usage_lookup"
branch_labels = None
depends_on = None


def _adjust_envelope(amount: Decimal) -> None:
    op.execute(text("""
        UPDATE cloudflare_installations
        SET policy = jsonb_set(
            policy, '{fleet_envelope_usd}',
            to_jsonb(((policy->>'fleet_envelope_usd')::numeric + :amount)::text)
        )
        WHERE policy->>'rate_table_version' = 'workers-paid-2026-11-20'
          AND policy ? 'fleet_envelope_usd'
    """).bindparams(amount=amount))


def upgrade() -> None:
    _adjust_envelope(Decimal("-5.00"))


def downgrade() -> None:
    _adjust_envelope(Decimal("5.00"))
