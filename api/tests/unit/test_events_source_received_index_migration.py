"""The events composite index is built concurrently with bounded DDL."""

import importlib.util
from contextlib import contextmanager
from pathlib import Path

import pytest

from src.models.orm.events import Event

INDEX_NAME = "ix_events_event_source_id_received_at"


def load_migration():
    path = (
        Path(__file__).resolve().parents[2]
        / "alembic/versions/20261118_events_source_received.py"
    )
    spec = importlib.util.spec_from_file_location("events_source_received_migration", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
@pytest.mark.parametrize("fail_ddl", [False, True])
def test_concurrent_index_bounds_and_resets_even_after_failure(monkeypatch, direction, fail_ddl):
    module = load_migration()
    calls = []

    class Recorder:
        @contextmanager
        def autocommit_block(self):
            calls.append("enter")
            try:
                yield
            finally:
                calls.append("exit")

        def get_context(self):
            return self

        def execute(self, sql):
            calls.append(sql)
            if fail_ddl and sql.startswith(("CREATE INDEX", "DROP INDEX")):
                raise RuntimeError("simulated DDL failure")

    monkeypatch.setattr(module, "op", Recorder())
    operation = getattr(module, direction)
    if fail_ddl:
        with pytest.raises(RuntimeError, match="simulated DDL failure"):
            operation()
    else:
        operation()

    assert calls[:3] == ["enter", "SET lock_timeout = '5s'", "SET statement_timeout = '30min'"]
    assert calls[-3:] == ["RESET statement_timeout", "RESET lock_timeout", "exit"]
    if direction == "upgrade":
        assert calls[3] == (
            f"CREATE INDEX CONCURRENTLY {INDEX_NAME} "
            "ON events (event_source_id, received_at)"
        )
    else:
        assert calls[3] == f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}"
    assert module.down_revision == "20261117_integ_secret_encrypt"
    assert len(module.revision) <= 32


def test_orm_declares_the_migrated_index():
    """Keep the model aligned so autogenerate never proposes dropping it."""
    indexes = {index.name: index for index in Event.__table__.indexes}
    assert [column.name for column in indexes[INDEX_NAME].columns] == [
        "event_source_id",
        "received_at",
    ]
