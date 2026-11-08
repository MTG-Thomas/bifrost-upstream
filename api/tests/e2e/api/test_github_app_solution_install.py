"""Two real ZIP installs, health and a signed cross-org receipt through workers.

Eight jobs have distinct contracts: two installs prove portable source/definition
deployment; an activation redeploy proves retained secret/configuration ownership;
one health execution proves cold setup; ingress and receipt executions
prove provider identity, SDK routing and tenant data-plane isolation.
One repeated tenant publication proves atomic receipt behavior after uncertain
topic handoff, independently of ingress deduplication. Provider
inventory is a synthetic fixture; this never contacts GitHub or registers an App.
A test-only diagnostic workflow at activation proves encrypted token cache
write/read using the same restricted operator identity and requested permissions.
"""

import asyncio
import hashlib
import hmac
import importlib.util
import io
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4
from zipfile import ZipFile, ZIP_DEFLATED

import pytest

from tests.e2e.conftest import execute_workflow_sync, poll_until
from tests.e2e.platform.conftest import wait_for_deploy, wait_for_install

pytestmark = pytest.mark.e2e
PROVIDER = "00000000-0000-0000-0000-000000000002"


def test_zip_installs_route_one_signed_delivery_into_only_the_bound_tenant(
    e2e_client, platform_admin, org1, org2_user, async_session_factory, tmp_path,
):
    spec = importlib.util.spec_from_file_location("github_package_build", Path("/app/examples/github-app/build.py"))
    assert spec and spec.loader
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    headers, installs, identities = platform_admin.headers, {}, []
    integration_id = None
    try:
        for kind, oid, archive in zip(("operator", "consumer"), (PROVIDER, org1["id"]),
                                      builder.build(tmp_path), strict=True):
            upload_headers = {key: value for key, value in headers.items() if key.lower() != "content-type"}
            response = e2e_client.post("/api/solutions/install", headers=upload_headers,
                data={"organization_id": oid}, files={"file": (archive.name, archive.read_bytes(), "application/zip")})
            installed = wait_for_install(e2e_client, response, headers)
            assert installed.status_code == 201, installed.text
            installs[kind] = installed.json()["id"]

        async def configured_operator_fixture():
            """Synthetic install-owner decisions only in this disposable test DB.

            No live roles, identities or review approvals are changed. Preserve
            runtime enforcement: real restricted powers, explicit reach and
            append-only table policy apply to the resulting worker requests.
            """
            from sqlalchemy import select, update

            from src.models.orm.solution_workflow_permission_requests import SolutionWorkflowPermissionRequest
            from src.models.orm.tables import Document, Table
            from src.models.orm.users import Role, User
            from src.models.orm.workflows import Workflow
            from src.services.authorization.context import Boundary, BoundaryKind
            from src.services.role_permissions import set_role_permissions
            from src.services.solutions.deploy import solution_entity_id
            from src.services.user_role_assignments import insert_assignment

            async with async_session_factory() as db:
                for kind, oid, role_name in (("operator", PROVIDER, "GitHub App Operators"),
                                             ("consumer", org1["id"], "GitHub App Consumers")):
                    role = (await db.execute(select(Role).where(Role.name == role_name))).scalar_one()
                    grants = frozenset({"tables.readwrite", "events.readwrite", "integrations.read",
                                        "configs.read", "configs.readwrite", "secrets.read", "solutions.read"} if kind == "operator" else {"tables.readwrite"})
                    await set_role_permissions(db, role_id=role.id, permissions=grants)
                    user = User(id=uuid4(), email=f"package-{uuid4()}@identities.bifrost.internal",
                                name="Package Test Identity", organization_id=UUID(oid), identity_kind="custom",
                                is_active=True, is_verified=True, is_registered=True)
                    db.add(user)
                    await db.flush()
                    identities.append(user.id)
                    boundaries = [Boundary(BoundaryKind.ORGANIZATION, UUID(oid))]
                    if kind == "operator":
                        boundaries.append(Boundary(BoundaryKind.ORGANIZATION, UUID(org1["id"])))
                    await insert_assignment(db, user_id=user.id, role_id=role.id,
                                            boundaries=boundaries, assigned_by="synthetic-package-fixture")
                    await db.execute(update(Workflow).where(Workflow.solution_id == UUID(installs[kind]))
                                     .values(run_identity_id=user.id))
                    requests = (await db.execute(select(SolutionWorkflowPermissionRequest).where(
                        SolutionWorkflowPermissionRequest.solution_id == UUID(installs[kind])))).scalars().all()
                    for request in requests:
                        request.status, request.approved_digest = "approved", request.request_digest
                tid = solution_entity_id(UUID(installs["operator"]), UUID(builder.identity("operator/github_app_state")))
                assert await db.get(Table, tid)
                binding = {"kind": "binding", "installation": "70001", "organization": org1["id"],
                           "solution": installs["consumer"], "account": "90001", "app_id": "123", "order": time.time()}
                db.add(Document(table_id=tid, id="binding:70001", data=binding))
                await db.commit()
                return {name: str(solution_entity_id(UUID(installs["operator"]), UUID(builder.identity(f"operator/{name}"))))
                        for name in ("manage", "ingress", "form")}

        ids = asyncio.run(configured_operator_fixture())
        integration = e2e_client.get("/api/integrations", headers=headers)
        assert integration.status_code == 200, integration.text
        rows = integration.json()["items"]
        integration_id = next(row["id"] for row in rows if row["name"] == "GitHub App Operator")
        key = "synthetic-local-app-key-" + uuid4().hex
        configured = e2e_client.post(f"/api/integrations/{integration_id}/mappings", headers=headers,
            json={"organization_id": PROVIDER, "entity_id": "123", "config": {"app_id": "123", "private_key": key}})
        assert configured.status_code == 201, configured.text
        form = e2e_client.get(f"/api/forms/{ids['form']}", headers=headers)
        assert form.status_code == 200, form.text
        assert form.json()["name"] == "GitHub App Setup And Health"
        health = execute_workflow_sync(e2e_client, headers, ids["manage"], {"action": "health"}, org_id=PROVIDER)
        assert health["status"] == "Success", health
        assert health["result"]["connection_configured"] is True
        assert key not in json.dumps(health)

        source = e2e_client.get(f"/api/events/sources/{ids['ingress']}", headers=headers)
        assert source.status_code == 200, source.text
        assert source.json()["is_active"] is False
        secret = "synthetic-signature-" + uuid4().hex
        rotated = e2e_client.post(f"/api/events/sources/{ids['ingress']}/rotate-secret", headers=headers,
                                  json={"secret": secret})
        assert rotated.status_code == 200, rotated.text
        # Active state is definition-owned; redeploy rather than bypassing its guard.
        activated_zip = builder.build(tmp_path / "activated", enable_ingress=True)[0]
        # Keep the shipped code intact; add one test-only caller of Store.secret
        # so this synthetic stack can prove token custody without a live App.
        probe_id = str(uuid4())
        with ZipFile(activated_zip) as archive:
            members = {name: archive.read(name) for name in archive.namelist()}
        definitions = json.loads(members[".bifrost/workflows.yaml"])
        original = next(iter(definitions["workflows"].values()))
        definitions["workflows"][probe_id] = {**original, "id": probe_id, "name": "Package Token Cache Proof",
            "function_name": "cache_probe", "path": "workflows/cache_probe.py"}
        members[".bifrost/workflows.yaml"] = json.dumps(definitions).encode()
        events = json.loads(members[".bifrost/events.yaml"])
        ingress = next(item for item in events["events"].values() if item["source_type"] == "webhook")
        ingress["subscriptions"].append({"id": str(uuid4()), "workflow_id": probe_id})
        members[".bifrost/events.yaml"] = json.dumps(events).encode()
        members["workflows/cache_probe.py"] = b'''from bifrost import workflow
from modules.github_app import Store
from uuid import uuid4
@workflow(name="Package Token Cache Proof")
async def cache_probe():
    token = "synthetic-cache-token-" + uuid4().hex
    value = await Store().secret("github_app_probe_" + uuid4().hex, token)
    return {"encrypted_cache_round_trip": value == token}
'''
        fixture_zip = io.BytesIO()
        with ZipFile(fixture_zip, "w", ZIP_DEFLATED) as archive:
            for name, content in members.items():
                archive.writestr(name, content)
        deployed = wait_for_deploy(e2e_client, e2e_client.post(
            f"/api/solutions/{installs['operator']}/deploy", headers=upload_headers,
            files={"file": (activated_zip.name, fixture_zip.getvalue(), "application/zip")}), headers)
        assert deployed.status_code == 200, deployed.text
        activated = e2e_client.get(f"/api/events/sources/{ids['ingress']}", headers=headers)
        assert activated.status_code == 200 and activated.json()["is_active"], activated.text
        assert activated.json()["webhook"]["secret_set"] is True

        async def verified_inventory_fixture():
            from sqlalchemy import select, update
            from src.models.orm.solution_workflow_permission_requests import SolutionWorkflowPermissionRequest
            from src.models.orm.tables import Document
            from src.models.orm.workflows import Workflow
            from src.services.solutions.deploy import solution_entity_id

            async with async_session_factory() as db:
                tid = solution_entity_id(UUID(installs["operator"]), UUID(builder.identity("operator/github_app_state")))
                db.add(Document(table_id=tid, id="inventory:synthetic", data={
                    "kind": "inventory", "installation": "70001", "epoch": "initial",
                    "repositories": [{"id": "80001", "name": "synthetic/repo"}], "order": time.time(),
                    "verified_at": time.time()}))
                probe_workflow = solution_entity_id(UUID(installs["operator"]), UUID(probe_id))
                await db.execute(update(Workflow).where(Workflow.id == probe_workflow).values(run_identity_id=identities[0]))
                request = (await db.execute(select(SolutionWorkflowPermissionRequest).where(
                    SolutionWorkflowPermissionRequest.workflow_id == probe_workflow))).scalar_one()
                request.status, request.approved_digest = "approved", request.request_digest
                await db.commit()

        asyncio.run(verified_inventory_fixture())
        path = urlparse(activated.json()["webhook"]["callback_url"]).path
        body = json.dumps({"action": "opened", "installation": {"id": 70001},
                           "repository": {"id": 80001}, "organization_id": str(org2_user.organization_id)}).encode()
        delivery = str(uuid4())
        signed = {"Content-Type": "application/json", "X-GitHub-Event": "pull_request",
            "X-GitHub-Delivery": delivery, "X-Hub-Signature-256": "sha256=" +
            hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()}
        with ThreadPoolExecutor(max_workers=3) as pool:
            responses = list(pool.map(lambda _: e2e_client.post(path, content=body, headers=signed), range(3)))
        assert [response.status_code for response in responses] == [202] * 3

        def receipt():
            response = e2e_client.post("/api/tables/github_app_receipts/documents/query",
                params={"scope": org1["id"], "solution": installs["consumer"]}, headers=headers, json={})
            assert response.status_code == 200, response.text
            docs = response.json()["documents"]
            if not docs:
                observed = e2e_client.get(f"/api/events/sources/{ids['ingress']}/events", headers=headers)
                assert observed.status_code == 200, observed.text
                for event in observed.json()["items"]:
                    if event["failed_count"]:
                        failed = e2e_client.get(f"/api/events/{event['id']}/deliveries", headers=headers)
                        raise AssertionError(f"Intake delivery failed: {failed.text}")
            return docs if docs else None

        docs = poll_until(receipt, max_wait=30, interval=0.25)
        assert docs is not None, "No receipt arrived within the existing worker observation window"
        assert len(docs) == 1
        assert docs[0]["id"] == f"70001:{delivery}"
        assert docs[0]["data"]["repository_id"] == "80001"

        def cache_proven():
            history = e2e_client.get(f"/api/events/sources/{ids['ingress']}/events", headers=headers).json()
            items = e2e_client.get(f"/api/events/{history['items'][0]['id']}/deliveries", headers=headers).json()["items"]
            proof = next(item for item in items if item["workflow_name"] == "Package Token Cache Proof")
            assert proof["status"] != "failed", proof
            if proof["status"] != "success":
                return None
            execution = e2e_client.get(f"/api/executions/{proof['execution_id']}", headers=headers)
            assert "synthetic-cache-token-" not in execution.text
            return execution.json()["result"]

        assert poll_until(cache_proven, max_wait=30, interval=0.25) == {"encrypted_cache_round_trip": True}
        repeated = e2e_client.post("/api/events/emit", headers=headers, json={
            "topic": "github.app.event", "data": docs[0]["data"], "scope": org1["id"],
            "solution": installs["consumer"]})
        assert repeated.status_code == 200, repeated.text

        def replay_complete():
            deliveries = e2e_client.get(f"/api/events/{repeated.json()['event_id']}/deliveries", headers=headers)
            assert deliveries.status_code == 200, deliveries.text
            items = deliveries.json()["items"]
            assert not any(item["status"] == "failed" for item in items), items
            return items if len(items) == 1 and items[0]["status"] == "success" else None

        poll_until(replay_complete, max_wait=30, interval=0.25)
        assert len(receipt()) == 1
        denied = e2e_client.post("/api/tables/github_app_receipts/documents/query",
            params={"scope": org1["id"], "solution": installs["consumer"]}, headers=org2_user.headers, json={})
        assert denied.status_code in (403, 404)
        history = e2e_client.get(f"/api/events/sources/{ids['ingress']}/events", headers=headers).json()
        assert len(history["items"]) == 1
        assert key not in json.dumps(history)
    finally:
        for kind, sid in installs.items():
            deleted = e2e_client.delete(f"/api/solutions/{sid}", headers=headers,
                                        params={"confirm": f"github-app-{kind}"})
            assert deleted.status_code in (200, 204), deleted.text
        if integration_id:
            e2e_client.delete(f"/api/integrations/{integration_id}", headers=headers)

        async def remove_identities():
            from sqlalchemy import delete
            from src.models.orm.users import User

            async with async_session_factory() as db:
                await db.execute(delete(User).where(User.id.in_(identities)))
                await db.commit()

        asyncio.run(remove_identities())
