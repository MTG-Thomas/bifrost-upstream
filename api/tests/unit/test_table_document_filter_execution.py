"""Scalar equality filters must match exactly what JSONB containment matched.

``_jsonb_scalar_equals`` replaced ``data @> {field: value}`` for bool/int/float
equality so that generic plans never pick a GIN containment scan. Executing
both forms against real PostgreSQL pins the row semantics: JSON type,
numeric equality, arrays, nulls and missing keys.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.table_documents import _jsonb_scalar_equals
from src.models.orm.tables import Document, Table

ROWS = [
    {"v": True},
    {"v": False},
    {"v": "true"},
    {"v": [True]},
    {"v": {"v": True}},
    {"v": 1},
    {"v": 1.0},
    {"v": 1.5},
    {"v": "1"},
    {"v": [1]},
    {"v": 0},
    {"v": None},
    {"other": 1},
    {},
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("value", "expected_rows"),
    [
        (True, {0}),
        (False, {1}),
        (1, {5, 6}),
        (1.0, {5, 6}),
        (1.5, {7}),
        (0, {10}),
        (-2, set()),
    ],
)
async def test_scalar_equality_matches_containment(
    db_session: AsyncSession, value, expected_rows
):
    table = Table(id=uuid4(), name=f"t_{uuid4().hex[:8]}", organization_id=None)
    db_session.add(table)
    await db_session.flush()
    for index, data in enumerate(ROWS):
        db_session.add(Document(id=f"row-{index}", table_id=table.id, data=data))
    await db_session.flush()

    async def matched(predicate) -> set[str]:
        result = await db_session.execute(
            select(Document.id).where(Document.table_id == table.id).where(predicate)
        )
        return set(result.scalars().all())

    expected = {f"row-{index}" for index in expected_rows}
    assert await matched(Document.data.contains({"v": value})) == expected
    assert await matched(_jsonb_scalar_equals("v", value)) == expected
