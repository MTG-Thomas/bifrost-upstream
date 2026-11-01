"""A cached client connection is observed without exposing identity in HTTP."""
import logging

from shared.runtime_database_identity import RuntimeDatabaseIdentityObserver

ROW = {"session_user": "bifrostadmin", "current_user": "bifrost", "current_database": "bifrost"}


def test_waits_for_same_client_connection_and_reports_once(caplog):
    observer = RuntimeDatabaseIdentityObserver()
    first, different = {}, {}
    with caplog.at_level(logging.INFO):
        observer.observe(ROW, first)
        observer.observe(ROW, different)
        assert not caplog.records
        observer.observe(ROW, first)
        observer.observe(ROW, first)
        observer.observe(ROW, different)
    assert len(caplog.records) == 1
    assert "session_user=bifrostadmin current_user=bifrost current_database=bifrost" in caplog.text
    assert "client_connection_previously_observed=true" in caplog.text


def test_new_physical_connection_info_does_not_count_as_reuse(caplog):
    observer = RuntimeDatabaseIdentityObserver()
    with caplog.at_level(logging.INFO):
        observer.observe(ROW, {})
        observer.observe(ROW, {})
    assert not caplog.records


def test_unsafe_or_extra_values_never_enter_logs(caplog):
    for row in [dict(ROW, session_user="sensitive\nvalue"), dict(ROW, password="sensitive-value")]:
        observer = RuntimeDatabaseIdentityObserver()
        info = {}
        with caplog.at_level(logging.INFO):
            observer.observe(row, info)
            observer.observe(row, info)
            observer.observe(ROW, info)
        assert "sensitive" not in caplog.text
        assert caplog.records[-1].getMessage().endswith("unsafe_identity_result")
        caplog.clear()
