from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import SecretStr
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    open_credential,
    seal_credential,
)


def test_ciphertext_is_bound_to_scope_and_version() -> None:
    ref = CredentialRef(uuid4(), "a" * 32, "scope_bearer", "workers", 2)
    value = SecretStr("fake-scope-bearer")
    row = seal_credential(ref, value, actor="admin", now=datetime.now(UTC))

    assert row.ciphertext is not None and value.get_secret_value() not in row.ciphertext
    assert open_credential(ref, row) == value

    # Copying ciphertext and metadata into another scope/version must fail.
    row.scope = "other"
    row.version = 3
    with pytest.raises(ValueError, match="binding does not match"):
        open_credential(replace(ref, scope="other", version=3), row)


def test_purged_credential_leaves_a_non_secret_receipt() -> None:
    ref = CredentialRef(uuid4(), "a" * 32, "account_token", "", 1)
    now = datetime.now(UTC)
    row = seal_credential(ref, SecretStr("fake-api-token"), actor="operator", now=now)
    row.ciphertext = None

    with pytest.raises(ValueError, match="unavailable"):
        open_credential(ref, row)

    assert (row.recorded_by, row.recorded_at, row.version) == ("operator", now, 1)
