"""Persist disabled Containers policy and jointly fenced Linux reservations."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261121_cf_containers"
down_revision = "20261121_cf_workers_upgrade"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_installations", sa.Column(
        "containers_policy", JSONB(), nullable=False, server_default=sa.literal_column(
            """'{"enabled":false,"instance_type":"standard-1","installation_instances":2,
                 "tenant_instances":1,"workflow_slots":1,"idle_seconds":120}'::jsonb"""
        ),
    ))
    op.add_column("cloudflare_installations", sa.Column("containers_ready_revision", sa.Integer()))
    op.add_column("cloudflare_installations", sa.Column("containers_release_digest", sa.String(64)))
    op.add_column("cloudflare_worker_reservations", sa.Column("container_tenant", sa.String(36)))
    op.add_column("cloudflare_worker_reservations", sa.Column("container_release", sa.String(64)))
    op.add_column("cloudflare_worker_reservations", sa.Column("container_envelope", JSONB()))
    op.add_column("cloudflare_worker_reservations", sa.Column(
        "container_reserved_at", sa.DateTime(timezone=True),
    ))
    op.add_column("cloudflare_worker_reservations", sa.Column(
        "container_stopped_at", sa.DateTime(timezone=True),
    ))
    op.create_check_constraint(
        "ck_cf_containers_readiness", "cloudflare_installations",
        "num_nonnulls(containers_ready_revision, containers_release_digest) IN (0, 2) "
        "AND (containers_ready_revision IS NULL OR containers_ready_revision > 0)",
    )
    op.create_check_constraint(
        "ck_cf_container_reservation", "cloudflare_worker_reservations",
        "num_nonnulls(container_tenant, container_release, container_envelope, container_reserved_at) IN (0, 4) "
        "AND (container_stopped_at IS NULL OR "
        "(container_reserved_at IS NOT NULL AND container_stopped_at >= container_reserved_at))",
    )


def downgrade() -> None:
    # Dropping this revision with reserved instances would lose their physical and spend fences.
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM cloudflare_worker_reservations WHERE container_reserved_at IS NOT NULL) THEN
                RAISE EXCEPTION 'Container reservation history must be retained';
            END IF;
        END $$
    """)
    op.drop_constraint("ck_cf_container_reservation", "cloudflare_worker_reservations", type_="check")
    op.drop_constraint("ck_cf_containers_readiness", "cloudflare_installations", type_="check")
    for name in (
        "container_stopped_at", "container_reserved_at", "container_envelope", "container_release", "container_tenant",
    ):
        op.drop_column("cloudflare_worker_reservations", name)
    for name in ("containers_release_digest", "containers_ready_revision", "containers_policy"):
        op.drop_column("cloudflare_installations", name)
