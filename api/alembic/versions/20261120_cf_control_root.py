"""Bind managed Cloudflare incarnations to an installation-owned root."""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_control_root"
down_revision = "20261120_cf_scope_writer_fence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table, installation_column in (
        ("cloudflare_installations", "id"),
        ("cloudflare_worker_reservations", "installation_id"),
    ):
        name = "installation" if installation_column == "id" else "reservation"
        op.add_column(table, sa.Column("control_root_version", sa.Integer()))
        op.add_column(table, sa.Column("control_root_purpose", sa.String(16), server_default="control_root", nullable=False))
        op.add_column(table, sa.Column("control_root_scope", sa.String(64), server_default="", nullable=False))
        op.create_check_constraint(
            f"ck_cf_{name}_control_root", table, "control_root_purpose = 'control_root' AND control_root_scope = ''",
        )
        op.create_foreign_key(
            f"fk_cf_{name}_control_root", table, "cloudflare_capacity_credentials",
            [installation_column, "control_root_purpose", "control_root_scope", "control_root_version"],
            ["installation_id", "purpose", "scope", "version"], deferrable=name == "installation",
            initially="DEFERRED" if name == "installation" else None,
        )
    # Older pilot reservations cannot re-authenticate. Their last connection
    # can settle before retirement; a reconnect requires a root-pinned worker.
    op.execute("""
        ALTER TABLE cloudflare_worker_reservations
        ADD CONSTRAINT ck_cf_reservation_has_root CHECK (control_root_version IS NOT NULL) NOT VALID
    """)


def downgrade() -> None:
    op.drop_constraint("ck_cf_reservation_has_root", "cloudflare_worker_reservations", type_="check")
    for table, name in (
        ("cloudflare_worker_reservations", "reservation"),
        ("cloudflare_installations", "installation"),
    ):
        op.drop_constraint(f"fk_cf_{name}_control_root", table, type_="foreignkey")
        op.drop_constraint(f"ck_cf_{name}_control_root", table, type_="check")
        for column in ("control_root_scope", "control_root_purpose", "control_root_version"):
            op.drop_column(table, column)
