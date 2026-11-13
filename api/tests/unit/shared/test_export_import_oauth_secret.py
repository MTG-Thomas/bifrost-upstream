"""OAuth client secrets survive export from one instance and import into another."""

import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet

from shared import export_import as service
from src.core.security import decrypt_secret, derive_fernet_key
from src.models.contracts.export_import import ImportResult, OAuthProviderExportItem

SOURCE_KEY = "source-instance-secret-key"


def _stored_on_source_instance(plaintext: str) -> bytes:
    """``OAuthProvider.encrypted_client_secret`` as the source instance wrote it:
    ``encrypt_secret(plaintext).encode()`` under the source instance's key."""
    token = Fernet(derive_fernet_key(SOURCE_KEY)).encrypt(plaintext.encode())
    return base64.urlsafe_b64encode(token)


def _exported_item(stored: bytes) -> OAuthProviderExportItem:
    return OAuthProviderExportItem(
        provider_name="partner-api",
        display_name="Partner API",
        oauth_flow_type="client_credentials",
        client_id="client-123",
        encrypted_client_secret=base64.b64encode(stored).decode(),
        authorization_url=None,
        token_url="https://login.example.test/token",
        token_url_defaults={},
        redirect_uri=None,
        scopes=[],
        organization_id=None,
        organization_name=None,
    )


@pytest.fixture
def import_seams(monkeypatch):
    monkeypatch.setattr(service, "_resolve_org_id", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "require_permission", lambda *_args, **_kwargs: None)


@pytest.mark.asyncio
async def test_imported_oauth_secret_is_readable_under_the_destination_key(import_seams):
    provider = SimpleNamespace(organization_id=None)
    integration = SimpleNamespace(id=uuid4(), name="Partner", oauth_provider=provider)

    await service._import_oauth_provider(
        AsyncMock(),
        SimpleNamespace(),
        integration,  # type: ignore[arg-type]
        _exported_item(_stored_on_source_instance("client-secret-value")),
        SOURCE_KEY,
        ImportResult(entity_type="integrations"),
    )

    assert decrypt_secret(provider.encrypted_client_secret.decode()) == "client-secret-value"


@pytest.mark.asyncio
async def test_wrong_source_key_refuses_the_oauth_secret(import_seams):
    provider = SimpleNamespace(organization_id=None)
    integration = SimpleNamespace(id=uuid4(), name="Partner", oauth_provider=provider)

    with pytest.raises(ValueError, match="re-encrypt"):
        await service._import_oauth_provider(
            AsyncMock(),
            SimpleNamespace(),
            integration,  # type: ignore[arg-type]
            _exported_item(_stored_on_source_instance("client-secret-value")),
            "a-different-key",
            ImportResult(entity_type="integrations"),
        )

    assert not hasattr(provider, "encrypted_client_secret")
