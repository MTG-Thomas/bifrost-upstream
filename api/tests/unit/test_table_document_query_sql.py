"""Generated SQL for table document queries must stay generic-plan safe.

asyncpg prepares and caches statements, so after five executions PostgreSQL
may reuse a generic plan that cannot see bound values. On a production copy,
``documents.data @> $n::JSONB`` with a broad containment led that plan to
BitmapAnd ``ix_documents_table_id`` with a GIN scan of ``ix_documents_data_gin``
reading ~114k index pages (44 s). These tests pin the SQL shape: no ``@>`` for
equality filters, and no bound boolean for compile-time policy facts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import asyncpg as pg_asyncpg

from shared import table_documents
from shared.policies.probe import compile_read_filter, make_seed_admin_bypass
from src.models.contracts.policies import TablePolicies
from src.models.contracts.tables import DocumentQuery
from src.models.orm.tables import Table

DIALECT = pg_asyncpg.dialect()


@dataclass
class FakeUser:
    user_id: UUID = field(default_factory=uuid4)
    email: str = "reader@example.com"
    organization_id: UUID | None = None
    role_ids: list[UUID] = field(default_factory=list)
    role_names: list[str] = field(default_factory=list)
    policy_authorities: tuple[str, ...] = ()


class _Result:
    def scalar(self):
        return 0

    def scalars(self):
        return self

    def all(self):
        return []


class CapturingSession:
    def __init__(self) -> None:
        self.statements: list = []

    async def execute(self, statement):
        self.statements.append(statement)
        return _Result()


def _table() -> Table:
    return Table(id=uuid4(), name="docs", organization_id=uuid4(), created_by="t@example.com")


def _render(statement) -> tuple[str, list]:
    compiled = statement.compile(dialect=DIALECT)
    return str(compiled), list(compiled.params.values())


def _seed_read_filter(*, admin: bool):
    policies = TablePolicies.model_validate(make_seed_admin_bypass())
    user = FakeUser(policy_authorities=("platform_admin",) if admin else ())
    return compile_read_filter(policies, user)


async def _query_sql(query: DocumentQuery, extra_where) -> list[tuple[str, list]]:
    session = CapturingSession()
    repo = table_documents.DocumentRepository(session, _table())  # type: ignore[arg-type]
    await repo.query(query, extra_where=extra_where)
    assert len(session.statements) == 2  # count + page
    return [_render(statement) for statement in session.statements]


@pytest.mark.asyncio
@pytest.mark.parametrize("where", [None, {}])
async def test_empty_filter_emits_no_containment_or_bound_boolean(where):
    statements = await _query_sql(
        DocumentQuery(where=where, order_by="name", order_dir="desc"),
        _seed_read_filter(admin=True),
    )
    for sql, params in statements:
        assert "@>" not in sql
        assert "BOOLEAN" not in sql.upper()
        assert " true" not in sql.lower()
        assert not any(isinstance(value, bool) for value in params)
        assert "WHERE documents.table_id = $1::UUID" in sql


@pytest.mark.asyncio
async def test_scalar_equality_uses_jsonb_compare_not_containment():
    where = {
        "status": "open",
        "active": True,
        "priority": 3,
        "score": {"eq": 1.5},
    }
    statements = await _query_sql(DocumentQuery(where=where), _seed_read_filter(admin=True))
    for sql, params in statements:
        assert "@>" not in sql
        assert "(documents.data ->> $2::TEXT) = $3::VARCHAR" in sql
        # One JSONB comparison per scalar filter, values serialized as JSON.
        assert sql.count("AS JSONB)") == 3
        assert {"true", "3", "1.5"} <= set(params)
        assert not any(isinstance(value, bool) for value in params)


@pytest.mark.asyncio
async def test_denied_seed_policy_compiles_to_constant_false():
    statements = await _query_sql(DocumentQuery(), _seed_read_filter(admin=False))
    for sql, params in statements:
        assert "WHERE false" in sql
        assert not any(isinstance(value, bool) for value in params)


@pytest.mark.asyncio
async def test_count_with_empty_filter_emits_no_containment():
    session = CapturingSession()
    repo = table_documents.DocumentRepository(session, _table())  # type: ignore[arg-type]
    await repo.count(where={}, extra_where=_seed_read_filter(admin=True))
    ((sql, params),) = [_render(statement) for statement in session.statements]
    assert "@>" not in sql
    assert sql.endswith("WHERE documents.table_id = $1::UUID")
    assert not any(isinstance(value, bool) for value in params)


@pytest.mark.parametrize(
    ("value", "json_text"),
    [(True, "true"), (False, "false"), (0, "0"), (2.5, "2.5")],
)
def test_scalar_equals_binds_json_text_not_python_scalar(value, json_text):
    sql, params = _render(select(table_documents._jsonb_scalar_equals("flag", value)))
    assert "@>" not in sql
    assert "CAST($2::VARCHAR AS JSONB)" in sql
    assert params == ["flag", json_text]
