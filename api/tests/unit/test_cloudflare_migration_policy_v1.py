"""Freeze the SQL contract imported by historical Cloudflare revisions."""

from migration_policy_v1 import op, refuse_populated_downgrade


def test_v1_locks_all_tables_before_checking_rows_in_priority_order(monkeypatch):
    statements: list[str] = []
    monkeypatch.setattr(op, "execute", statements.append)

    refuse_populated_downgrade("z_child", "a_parent", non_null=("middle", "receipt"))

    assert statements[0] == "LOCK TABLE a_parent, middle, z_child IN ACCESS EXCLUSIVE MODE"
    assert [" ".join(statement.split()) for statement in statements[1:]] == [
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM z_child) THEN "
        "RAISE EXCEPTION 'Cloudflare downgrade would discard data from z_child' "
        "USING ERRCODE = 'object_not_in_prerequisite_state'; END IF; END $$;",
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM a_parent) THEN "
        "RAISE EXCEPTION 'Cloudflare downgrade would discard data from a_parent' "
        "USING ERRCODE = 'object_not_in_prerequisite_state'; END IF; END $$;",
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM middle WHERE receipt IS NOT NULL) THEN "
        "RAISE EXCEPTION 'Cloudflare downgrade would discard data from middle WHERE receipt IS NOT NULL' "
        "USING ERRCODE = 'object_not_in_prerequisite_state'; END IF; END $$;",
    ]
