"""Protect migration-owned records during downgrade and autogenerate.

Cloudflare approvals and pending reviews are audit records. Preserve them
along with the execution attempt and chat delivery journals on downgrade.
"""

from alembic import op
from sqlalchemy.sql.schema import SchemaItem


NON_ORM_TABLES = frozenset({
    "schedules",  # Workflow schedules
    "solution_file_jobs",  # Solution file jobs
    "cloudflare_capacity_usage",  # Retained historical receipts
})


def include_object(
    object_: SchemaItem,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: SchemaItem | None,
) -> bool:
    """Leave reflected, migration-owned tables outside ORM autogenerate."""
    return not (type_ == "table" and reflected and compare_to is None and name in NON_ORM_TABLES)


def refuse_populated_downgrade(*tables: str, non_null: tuple[str, str] | None = None) -> None:
    """Fence concurrent writes, then refuse to discard protected data.

    Keep this v1 contract for historical revisions. Future policies belong in
    a separately versioned helper.

    Identifiers come from the revision's schema literals. Emit SQL through
    Alembic so these checks also run when an offline downgrade script is applied.
    """
    checks = {table: "" for table in tables}
    if non_null is not None:
        table, column = non_null
        checks[table] = f" WHERE {column} IS NOT NULL"
    op.execute(f"LOCK TABLE {', '.join(sorted(checks))} IN ACCESS EXCLUSIVE MODE")
    for table, predicate in checks.items():
        op.execute(f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM {table}{predicate}) THEN
                    RAISE EXCEPTION 'Cloudflare downgrade would discard data from {table}{predicate}'
                        USING ERRCODE = 'object_not_in_prerequisite_state';
                END IF;
            END $$;
        """)
