"""Operator integration secrets stay within their owning organization."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest


pytestmark = pytest.mark.e2e


@pytest.fixture
def scoped_readers(e2e_client, platform_admin, org1_user, org2_user, async_session_factory):
    response = e2e_client.post("/api/roles", headers=platform_admin.headers,
                               json={"name": f"Package Reader {uuid4().hex[:8]}"})
    assert response.status_code == 201, response.text
    role_id = response.json()["id"]

    async def grant():
        from src.services.role_permissions import set_role_permissions
        async with async_session_factory() as db:
            await set_role_permissions(db, role_id=UUID(role_id),
                permissions=frozenset({"integrations.read", "secrets.read", "events.read"}))
            await db.commit()

    asyncio.run(grant())
    assigned = e2e_client.post(f"/api/roles/{role_id}/users", headers=platform_admin.headers,
                              json={"user_ids": [str(org1_user.user_id), str(org2_user.user_id)]})
    assert assigned.status_code == 204, assigned.text
    try:
        yield org1_user, org2_user
    finally:
        deleted = e2e_client.delete(f"/api/roles/{role_id}", headers=platform_admin.headers)
        assert deleted.status_code == 204, deleted.text


def test_operator_org_secret_does_not_resolve_for_tenant_secret_reader(
    e2e_client, platform_admin, org1, org2, scoped_readers, async_session_factory,
):
    operator, tenant = scoped_readers
    name = f"operator_package_{uuid4().hex[:8]}"
    secret = "synthetic-operator-key-" + uuid4().hex
    response = e2e_client.post("/api/integrations", headers=platform_admin.headers, json={
        "name": name, "config_schema": [{"key": "private_key", "type": "secret"}],
    })
    assert response.status_code == 201, response.text
    integration_id = response.json()["id"]
    try:
        for org, config in [(org1, {"private_key": secret}), (org2, {})]:
            saved = e2e_client.post(f"/api/integrations/{integration_id}/mappings",
                headers=platform_admin.headers, json={"organization_id": org["id"],
                "entity_id": str(uuid4()), "config": config})
            assert saved.status_code == 201, saved.text
        operator_read = e2e_client.post("/api/sdk/integrations/get", headers=operator.headers,
                                       json={"name": name})
        assert operator_read.status_code == 200, operator_read.text
        assert operator_read.json()["config"]["private_key"] == secret
        tenant_read = e2e_client.post("/api/sdk/integrations/get", headers=tenant.headers,
                                     json={"name": name})
        assert tenant_read.status_code == 200, tenant_read.text
        assert secret not in tenant_read.text
        assert not tenant_read.json()["config"].get("private_key")
        other_scope = e2e_client.post("/api/sdk/integrations/get", headers=tenant.headers,
            json={"name": name, "scope": org1["id"]})
        assert other_scope.status_code == 403, other_scope.text

        async def encrypted_storage():
            from sqlalchemy import select
            from src.models.orm.config import Config

            async with async_session_factory() as db:
                stored = (await db.execute(select(Config).where(
                    Config.integration_id == UUID(integration_id), Config.key == "private_key"))).scalar_one()
                assert stored.organization_id == UUID(org1["id"])
                assert stored.config_type.value == "secret"
                assert secret not in json.dumps(stored.value)

        asyncio.run(encrypted_storage())
        catalog = e2e_client.get(f"/api/integrations/{integration_id}", headers=platform_admin.headers)
        assert catalog.status_code == 200, catalog.text
        assert secret not in catalog.text
    finally:
        deleted = e2e_client.delete(f"/api/integrations/{integration_id}", headers=platform_admin.headers)
        assert deleted.status_code == 204, deleted.text
