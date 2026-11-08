"""Manual setup, private signed intake, health and periodic reconciliation."""

from bifrost import api, context, events, integrations, workflow
from modules.github_app import Broker, CONNECTION, GitHub, PackageError, Store
from uuid import NAMESPACE_URL, UUID, uuid5


async def validate_destination(organization, solution):
    response = await api.get(f"/api/solutions/{solution}")
    if response.status_code != 200:
        raise PackageError("Consumer Solution is not accessible")
    target = response.json()
    if (target["organization_id"] != organization or target["status"] != "active"
            or not target["allow_inbound_access"] or target["slug"] != "github-app-consumer"):
        raise PackageError("Destination must be an active, inbound-enabled consumer install in the selected organization")
    # Solutions' documented portable entity identity is uuid5(install, template).
    template = uuid5(NAMESPACE_URL, "https://github.com/MTG-Thomas/bifrost-upstream/github-app/consumer/topic")
    source = str(uuid5(UUID(solution), str(template)))
    topic = await api.get(f"/api/events/sources/{source}")
    if topic.status_code != 200:
        raise PackageError("Consumer topic is not accessible")
    definition = topic.json()
    if (definition["organization_id"] != organization or definition["event_type"] != "github.app.event"
            or definition["source_type"] != "topic" or not definition["is_active"]):
        raise PackageError("Consumer topic is not active in the selected organization")


def operator_context():
    if not context.organization or not context.organization.is_provider or not context.solution_id:
        raise PackageError("Run this installed Solution in the provider organization")


async def broker():
    operator_context()
    await require_delivery_identity()
    connection = await operator_connection()
    store = Store()
    github = GitHub(connection.config["app_id"], connection.config["private_key"], store)
    return Broker(store, github, events.emit, validate_destination)


async def require_delivery_identity():
    response = await api.get("/api/events/adapters")
    if response.status_code != 200:
        raise PackageError("Cannot verify webhook capabilities with this run identity")
    adapters = response.json().get("adapters", [])
    if not any(item["name"] == "generic" and "delivery_id_header" in
               item.get("config_schema", {}).get("properties", {}) for item in adapters):
        raise PackageError("This deployment lacks signed webhook delivery identity; requires upstream PR #1285")


async def operator_connection():
    connection = await integrations.get(CONNECTION)
    if not connection or not connection.config.get("private_key") or not connection.config.get("app_id"):
        raise PackageError("Configure the operator-org App connection before enabling ingress")
    # The public catalog returns secret placeholders, not decrypted keys. Check
    # custody provenance before using the SDK's normally cascaded connection.
    response = await api.get(f"/api/integrations/{connection.integration_id}")
    if response.status_code != 200:
        raise PackageError("Cannot verify App key custody")
    metadata = response.json()
    if {"private_key", "app_id"} & set(metadata.get("config_defaults") or {}):
        raise PackageError("App credentials must be operator-org overrides; clear global defaults")
    mapping = next((item for item in metadata.get("mappings", [])
                    if item["organization_id"] == context.organization.id), None)
    if not mapping or not all(mapping.get("config", {}).get(key) for key in ("private_key", "app_id")):
        raise PackageError("App credentials must be explicitly configured in the operator organization")
    return connection


@workflow(name="GitHub App Operator", category="GitHub App")
async def manage(action: str = "health", installation_id: str = "", organization_id: str = "",
                 consumer_solution_id: str = "", provider_delivery_id: str = "") -> dict:
    """Run Health, Bind, Reconcile or Request Redelivery. Never returns credentials."""
    operator_context()
    if action == "health":
        # Health remains available before key setup; it makes no GitHub request.
        health = await Broker(Store(), None, events.emit, validate_destination).health()
        health["setup_errors"] = []
        for check in (require_delivery_identity, operator_connection):
            try:
                await check()
            except PackageError as exc:
                health["setup_errors"].append(str(exc))
        health["connection_configured"] = not health["setup_errors"]
        return health
    client = await broker()
    if action == "bind":
        return await client.bind(installation_id, organization_id, consumer_solution_id)
    if action == "reconcile":
        return await client.reconcile(installation_id)
    if action == "redeliver":
        return await client.redeliver(installation_id, provider_delivery_id)
    if action == "deliveries":
        return await client.deliveries(installation_id)
    raise PackageError("Unsupported operator action")


@workflow(name="GitHub App Signed Intake", category="GitHub App")
async def receive(headers: dict) -> dict:
    """Private source only. Routing comes from verified bindings, never payload org IDs."""
    operator_context()
    event = context.event
    if not event or event.organization_id != context.organization.id:
        raise PackageError("Signed intake requires an event in the operator organization")
    return await (await broker()).receive(event, headers)


@workflow(name="GitHub App Reconciliation", category="GitHub App")
async def reconcile_all() -> dict:
    """Bounded scheduled inventory. Failed entries remain visible for recovery."""
    client = await broker()
    bindings = await client.store.query(kind="binding")
    if len(bindings) > 50:
        raise PackageError("More than 50 bindings: partition the reconciliation schedule explicitly")
    results = []
    for binding in bindings:
        iid = binding["installation"]
        try:
            results.append({"installation": iid, "result": await client.reconcile(iid)})
        except PackageError as exc:
            results.append({"installation": iid, "error": str(exc)})
    return {"results": results, "failed": sum("error" in row for row in results)}
