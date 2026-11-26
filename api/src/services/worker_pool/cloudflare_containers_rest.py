"""Exact Container inventory and journal-bound registry effects."""

import asyncio
import base64
import json
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Any, Literal

import httpx

from pydantic import SecretStr

from src.services.worker_pool.cloudflare_containers_auth import container_scope_public_key
from src.services.worker_pool.cloudflare_containers_policy import CONTAINER_CLOCK_SKEW, ContainersPolicy
from src.services.worker_pool.cloudflare_containers_release import (
    OCIToolError,
    OCIToolFailure,
    VerifiedContainerRelease,
    run_oci_tool,
)
from src.services.worker_pool.cloudflare_provisioning import (
    DeletionSettling,
    ObservationCheck,
    ObservationPending,
    ObservationSettling,
    OwnershipCheck,
    OwnershipConflict,
    OwnershipResource,
    ResourceIntent,
    ResourceObservation,
    observe_owned,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    OWNER_BINDING,
    CloudflareRestObservation,
    ConfiguredWorkersScript,
    ScriptRestProvisioner,
    WorkersRestCleanup,
    worker_owner_value,
)

REGISTRY = "registry.cloudflare.com"
_HEX32 = re.compile(r"[a-f0-9]{32}")
_ACTOR_ID = re.compile(r"[a-f0-9]{64}")
_APP_ID = re.compile(r"[a-f0-9]{32}|[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")
# The exact actor API also reports provisioning, running, stopping,
# unhealthy and unknown. Only these three states witness a placement exit.
# https://developers.cloudflare.com/api/resources/containers/subresources/applications/subresources/instances/methods/get/
_TERMINAL_INSTANCE_STATES = ("failed", "stopped", "inactive")
_INVENTORY_PAGE_SIZE = 100
_MAX_INVENTORY_PAGES = 100
# Two settlement inventory reads per second per shared adapter leaves room
# for other requests under the published account/user limit of 1,200/5 min.
# https://developers.cloudflare.com/fundamentals/api/reference/limits/
_PLACEMENT_READ_INTERVAL = 0.5
_IMAGE_SETTLEMENT_SECONDS = 180
_IMAGE_SETTLEMENT_INTERVAL = 5
_IMAGE_COPY_LEASE_INTERVAL = 5


@dataclass(frozen=True, slots=True)
class InstanceStop:
    started_at: datetime | None
    stopped_at: datetime
    terminal_state: str
    exit_code: int | None


class ContainerStartUnverified(ObservationPending):
    """The exact actor read was absent or lacked a start timestamp."""


def _name(intent: ResourceIntent, kind: str) -> str:
    target = intent.target
    name = f"bifrost-{target.installation_id.hex}-r{intent.desired_revision}"
    if (
        target.scope != "containers" or target.kind != kind or target.name != name
        or intent.desired_revision < 1 or not _HEX32.fullmatch(target.account_id)
    ):
        raise OwnershipConflict("Container target does not match its installation and revision",
                                OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.TARGET_BINDING)
    return name


class ContainersRest(WorkersRestCleanup):
    def __init__(self, http: httpx.AsyncClient) -> None:
        super().__init__(http)
        self._next_placement_read = 0.0

    def _take_placement_reads(self, count: int) -> bool:
        # Reserve the entire check before sending any reads. The caller can
        # wait on settling after its transaction releases the row locks.
        now = monotonic()
        if now < self._next_placement_read:
            return False
        self._next_placement_read = now + count * _PLACEMENT_READ_INTERVAL
        return True

    async def verify_launcher_namespace(
        self, token: SecretStr, expected: ResourceObservation, namespace: str,
    ) -> None:
        if not _HEX32.fullmatch(namespace):
            raise OwnershipConflict("Invalid Container launcher namespace",
                                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, OwnershipCheck.NAMESPACE_CLASS)
        if await observe_owned(self, api_token=token, intent=expected.intent, recorded=expected) is None:
            raise ObservationPending("The owned Container launcher is absent",
                                      OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_RECORDED_ABSENT)
        namespaces = await self._script_namespaces(token, expected.intent.target, namespace_ids=frozenset({namespace}))
        launchers = [row for row in namespaces if row.get("class") == "ContainerLauncher" or row["id"] == namespace]
        if (
            len(launchers) != 1 or launchers[0]["id"] != namespace
            or launchers[0].get("class") != "ContainerLauncher"
            or launchers[0].get("script") != expected.intent.target.name
        ):
            raise OwnershipConflict("Container launcher namespace differs from the deployment receipt",
                                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, OwnershipCheck.RECEIPT_MISMATCH)

    async def _inventory(
        self, token: SecretStr, account: str, path: str, **filters: str,
    ) -> list[dict[str, Any]]:
        # Containers lists document result_info.page_token (echoed request),
        # next_page_token and per_page. An envelope with page/total_pages
        # instead is checked as numbered pagination, never mixed with cursors.
        # https://developers.cloudflare.com/api/resources/containers/subresources/applications/methods/list/
        params: dict[str, int | str] = {"per_page": _INVENTORY_PAGE_SIZE, **filters}
        rows: dict[str, dict[str, Any]] = {}
        cursors: set[str] = set()
        total = None
        page_count = None
        page_size = _INVENTORY_PAGE_SIZE
        for page in range(1, _MAX_INVENTORY_PAGES + 1):
            envelope = await self._request(token, account, "GET", path, params=params)
            assert envelope is not None
            batch, info = envelope["result"], envelope.get("result_info", {})
            if not isinstance(batch, list) or not isinstance(info, dict):
                break
            count = info.get("count", len(batch))
            reported_size = info.get("per_page", page_size)
            if (type(count) is not int or count != len(batch)
                    or type(reported_size) is not int or not 1 <= reported_size <= _INVENTORY_PAGE_SIZE
                    or (page > 1 and reported_size != page_size) or len(batch) > reported_size):
                break
            page_size = reported_size
            if any(not isinstance(row, dict) or not isinstance(row.get("id"), str) for row in batch):
                break
            ids = {row["id"] for row in batch}
            if len(ids) != len(batch) or ids & rows.keys():
                break
            rows.update((row["id"], row) for row in batch)
            if "total_count" in info:
                reported = info["total_count"]
                if type(reported) is not int or reported < len(rows) or (total is not None and total != reported):
                    break
                total = reported
            if total is not None and len(rows) > total:
                break

            cursor = info.get("next_page_token")
            echoed_cursor = info.get("page_token")
            if echoed_cursor is not None and echoed_cursor != params.get("page_token", ""):
                break
            if page_count is not None or "page" in info or "total_pages" in info:
                reported_page, reported_pages = info.get("page"), info.get("total_pages")
                if (cursors or cursor not in (None, "") or type(reported_page) is not int or reported_page != page
                        or type(reported_pages) is not int or not 0 <= reported_pages <= _MAX_INVENTORY_PAGES
                        or (page_count is not None and page_count != reported_pages)):
                    break
                page_count = reported_pages
                if page < page_count:
                    if len(batch) != page_size or (total is not None and len(rows) >= total):
                        break
                    params["page"] = page + 1
                    continue
                if ((page > page_count and not (page_count == 0 and page == 1 and not batch))
                        or (page > 1 and not batch)):
                    break
            elif cursor not in (None, ""):
                if not isinstance(cursor, str) or cursor in cursors or not batch or total == len(rows):
                    break
                cursors.add(cursor)
                params["page_token"] = cursor
                continue
            elif total is None and len(batch) == page_size:
                # A full page without a continuation or declared total does
                # not establish absence, even when other metadata is present.
                break

            if total is None or total == len(rows):
                return list(rows.values())
            break
        raise ObservationPending("Container inventory is incomplete",
                                 OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)

    @staticmethod
    def _application(row: object, account: str, name: str, namespace: str) -> str:
        if (
            not isinstance(row, dict) or not isinstance(row.get("id"), str) or not _APP_ID.fullmatch(row["id"])
            or row.get("name") != name or row.get("account_id") != account
            or row.get("scheduling_policy") != "durable_object"
            or not isinstance(row.get("durable_objects"), dict)
            or row["durable_objects"].get("namespace_id") != namespace
        ):
            raise OwnershipConflict("Container application identity or namespace differs from its journal",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        return row["id"]

    async def observe_application(
        self, token: SecretStr, intent: ResourceIntent, namespace: str, *,
        recorded_id: str | None = None, for_deletion: bool = False,
    ) -> ResourceObservation | None:
        settling = DeletionSettling if for_deletion else ObservationSettling
        name = _name(intent, "container_application")
        account = intent.target.account_id
        if not _HEX32.fullmatch(namespace) or (recorded_id is not None and not _APP_ID.fullmatch(recorded_id)):
            raise OwnershipConflict("Container journal lacks a valid application identity",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        path = "containers/applications"
        inventory = await self._inventory(token, account, path, name=name)
        if any(not isinstance(row.get("name"), str) for row in inventory):
            raise ObservationPending("Container inventory is missing application names",
                                     OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)
        rows = [row for row in inventory if row["name"] == name]
        if len(rows) > 1:
            raise OwnershipConflict("Multiple Container applications match the name",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.MULTIPLE_OCCUPANTS)
        if not rows:
            if recorded_id is not None:
                receipt = await self._request(token, account, "GET", f"{path}/{recorded_id}", allow_missing=True)
                if receipt is not None:
                    self._application(receipt["result"], account, name, namespace)
                    raise settling("Container application inventory differs from ID lookup",
                                              OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)
            return None
        app_id = self._application(rows[0], account, name, namespace)
        if recorded_id is not None and app_id != recorded_id:
            raise OwnershipConflict("Container application ID differs from its journal",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        receipt = await self._request(token, account, "GET", f"{path}/{app_id}", allow_missing=True)
        if receipt is None:
            raise settling("Container application has no matching ID readback",
                                      OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)
        if self._application(receipt["result"], account, name, namespace) != app_id:
            raise OwnershipConflict("Container application ID changed",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        if for_deletion and "health" in receipt["result"]:
            health = receipt["result"]["health"]
            counts = health.get("instances") if isinstance(health, dict) and health.get("summary") != "pending" else None
            if not isinstance(counts, dict) or any(
                type(value := counts.get(state)) is not int or value != 0 for state in ("active", "starting")
            ):
                # The aggregate can veto deletion. A zero count is not a
                # substitute for the independent placement inventory.
                raise DeletionSettling("Container application still has activity or an incomplete health sweep",
                                       OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_PLACEMENTS)
        return ResourceObservation(intent, app_id, namespace)

    async def delete_application(self, token: SecretStr, expected: ResourceObservation) -> None:
        intent = expected.intent
        observed = await self.observe_application(
            token, intent, expected.provider_version, recorded_id=expected.provider_id, for_deletion=True,
        )
        if observed is None:
            return
        account = intent.target.account_id
        path = f"containers/applications/{expected.provider_id}"
        # The documented active filter includes provisioning, running and
        # stopping actors. A single empty, terminal page proves no active
        # placements even when stopped history fills an exact page.
        # https://developers.cloudflare.com/api/resources/containers/subresources/applications/subresources/instances/methods/list/
        if not self._take_placement_reads(1):
            raise DeletionSettling("Waiting to read active Container placements",
                                   OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_PLACEMENTS)
        occupied = await self._request(
            token, account, "GET", f"{path}/instances-v2", params={"state": "active", "per_page": 1},
        )
        assert occupied is not None
        info = occupied.get("result_info", {})
        if (
            occupied["result"] != [] or not isinstance(info, dict)
            or any(info.get(key) not in (None, "") for key in ("page_token", "next_page_token"))
            or any(type(info.get(key, 0)) is not int or info.get(key, 0) != 0
                   for key in ("count", "total_count", "total_pages"))
        ):
            raise DeletionSettling("Container application still has live or unverified placements",
                                   OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_PLACEMENTS)
        await self._request(token, account, "DELETE", path, allow_missing=True)
        if await self.observe_application(
            token, intent, expected.provider_version, recorded_id=expected.provider_id, for_deletion=True,
        ):
            raise DeletionSettling("Container application is still visible",
                                   OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)

    async def create_application(self, token: SecretStr, intent: ResourceIntent, namespace: str) -> ResourceObservation:
        """Send the already-journaled POST. The caller independently observes the result."""
        name = _name(intent, "container_application")
        account = intent.target.account_id
        if not _HEX32.fullmatch(namespace):
            raise OwnershipConflict("Container namespace is invalid",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.NAMESPACE_CLASS)
        try:
            result = await self._request(token, account, "POST", "containers/applications", json_body={
                "name": name, "scheduling_policy": "durable_object",
                "durable_objects": {"namespace_id": namespace},
            })
        except CloudflareRestObservation as exc:
            # The OpenAPI schema describes 400 as a general input or limit
            # error and supplies no duplicate-name discriminator. Resolve
            # validation and conflict responses by exact inventory.
            # https://developers.cloudflare.com/api/resources/containers/subresources/applications/methods/create/
            if exc.status_code in (400, 409):
                raise ObservationSettling("Waiting to read back the Container application name",
                                          OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_CREATE) from None
            raise
        assert result is not None
        return ResourceObservation(intent, self._application(result["result"], account, name, namespace), namespace)

    async def observe_instance_stop(
        self, token: SecretStr, application: ResourceObservation, *, instance_id: str,
        image: str, not_before: datetime,
    ) -> InstanceStop:
        """Read the exact application and actor without waking the Durable Object."""
        observed = await self.observe_instance_outcome(
            token, application, instance_id=instance_id, image=image, not_before=not_before,
        )
        if observed is None:
            raise ContainerStartUnverified("Missing instance readback does not witness a stopped placement")
        if observed.started_at is None:
            raise ContainerStartUnverified("Missing start readback does not witness a stopped placement")
        return observed

    async def observe_instance_outcome(
        self, token: SecretStr, application: ResourceObservation, *, instance_id: str,
        image: str, not_before: datetime,
    ) -> InstanceStop | None:
        """Read a stopped, failed-before-start, or missing exact actor."""
        placement = await self._instance_inventory(token, application, instance_id)
        if placement is None:
            return None
        if placement.get("image") != image:
            raise OwnershipConflict("Container placement differs from its journal")
        try:
            status = placement["status"]
            state = status["state"]
            if state not in _TERMINAL_INSTANCE_STATES:
                raise ValueError
            stopped_at = datetime.fromisoformat(status["updated_at"])
            earliest = not_before - CONTAINER_CLOCK_SKEW
            if not earliest <= stopped_at <= datetime.now(UTC) + CONTAINER_CLOCK_SKEW:
                raise ValueError
            started_at = placement.get("started_at")
            if started_at is not None or state != "failed":
                started_at = datetime.fromisoformat(started_at)
                if not earliest <= started_at <= stopped_at:
                    raise ValueError
        except (KeyError, TypeError, ValueError):
            raise ObservationPending("Provider has not witnessed a bounded stop for this placement") from None
        code = status.get("exit_code")
        exit_code = code if type(code) is int and -(2**31) <= code < 2**31 else None
        return InstanceStop(
            started_at.astimezone(UTC) if started_at is not None else None,
            stopped_at.astimezone(UTC), state, exit_code,
        )

    async def observe_no_placement(self, token: SecretStr, application: ResourceObservation, instance_id: str) -> None:
        if await self._instance_inventory(token, application, instance_id) is not None:
            raise ObservationPending("A void Container still has a provider placement")

    async def _instance_inventory(
        self, token: SecretStr, application: ResourceObservation, instance_id: str,
    ) -> dict[str, Any] | None:
        name = _name(application.intent, "container_application")
        account = application.intent.target.account_id
        if (
            not _APP_ID.fullmatch(application.provider_id) or not _HEX32.fullmatch(application.provider_version)
            or not _ACTOR_ID.fullmatch(instance_id)
        ):
            raise OwnershipConflict("Invalid Container instance observation target")
        path = f"containers/applications/{application.provider_id}"
        if not self._take_placement_reads(2):
            raise ObservationSettling("Waiting to read the exact Container placement",
                                      OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_PLACEMENTS)
        app = await self._request(token, account, "GET", path)
        assert app is not None
        if self._application(app["result"], account, name, application.provider_version) != application.provider_id:
            raise OwnershipConflict("Container application ID changed")
        # The exact actor endpoint includes named actors and does not depend
        # on the size or lifecycle state of the application's history.
        # https://developers.cloudflare.com/api/resources/containers/subresources/applications/subresources/instances/methods/get/
        observed = await self._request(token, account, "GET", f"{path}/instances/{instance_id}", allow_missing=True)
        if observed is None:
            return None
        placement = observed["result"]
        if (
            not isinstance(placement, dict) or placement.get("application_id") != application.provider_id
            or placement.get("id") != instance_id
        ):
            raise OwnershipConflict("Container placement differs from the exact actor or application",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
        return placement


class ContainerLauncherRest(ScriptRestProvisioner):
    """Provision the launcher for one verified release and prepared digest."""

    def __init__(
        self, http: httpx.AsyncClient, release: VerifiedContainerRelease, image: ResourceObservation,
        *, origin: str, policy: ContainersPolicy, signing_secret: SecretStr,
    ) -> None:
        super().__init__(http)
        self.release = release
        self.image = image
        self.origin = origin
        self.policy = policy
        self.public_key = container_scope_public_key(signing_secret.get_secret_value())

    def _require_target(self, intent: ResourceIntent) -> str:
        name = _name(self.image.intent, "container_image")
        target = intent.target
        image_target = self.image.intent.target
        if (
            target.scope != "containers" or target.kind != "worker_script" or target.name != f"{name}-containers"
            or target.account_id != image_target.account_id or target.installation_id != image_target.installation_id
            or intent.desired_revision != self.image.intent.desired_revision
            or intent.artifact_digest != self.release.digest or intent.artifact_digest != self.image.intent.artifact_digest
            or self.image.provider_id != f"{REGISTRY}/{target.account_id}/{name}:release"
            or self.image.provider_version != self.release.manifest.image_digest
        ):
            raise OwnershipConflict("Container launcher differs from its prepared release",
                                    OwnershipResource.WORKER_SCRIPT, OwnershipCheck.RELEASE_INTENT)
        return f"{self.image.provider_id.removesuffix(':release')}@{self.image.provider_version}"

    async def create(
        self, *, api_token: SecretStr, intent: ResourceIntent, scope_bearer: SecretStr | None,
    ) -> None:
        image = self._require_target(intent)
        if scope_bearer is not None:
            raise OwnershipConflict("Container launcher upload does not take a bearer",
                                    OwnershipResource.WORKER_SCRIPT, OwnershipCheck.SCOPE_BEARER)
        module = "container-registry.mjs"
        settings = {
            "CONTAINERS_INSTALLATION": str(intent.target.installation_id),
            "CONTAINERS_REVISION": str(intent.desired_revision),
            "CONTAINERS_PUBLIC_KEY": self.public_key,
            "CONTAINERS_ORIGIN": self.origin,
            "CONTAINERS_CAP": str(self.policy.installation_instances),
            "CONTAINERS_IDLE_SECONDS": str(self.policy.idle_seconds),
            "CONTAINERS_INGRESS_BYTES": str(self.policy.ingress_bytes),
            "CONTAINERS_OUTBOUND_BYTES": str(self.policy.outbound_bytes),
            "CONTAINERS_IMAGE": self.image.provider_version,
            "CONTAINERS_PREPARED_IMAGE": image,
            "CONTAINERS_RELEASE": self.release.digest,
            "CONTAINERS_INGRESS_POLICY": (
                Path(__file__).resolve().parents[1] / "worker_ingress_routes.json"
            ).read_text(encoding="utf-8"),
        }
        metadata = {
            "main_module": module,
            "compatibility_date": "2026-10-30",
            "exports": {
                "ContainerLauncher": {"type": "durable-object", "storage": "sqlite", "container": "linux"},
                "ContainerScopeRegistry": {"type": "durable-object", "storage": "sqlite"},
                "ContainerIngress": {"type": "worker"},
            },
            # The raw upload uses the prepared reference string. Wrangler
            # config objects are resolved before the multipart upload.
            # https://github.com/cloudflare/workers-sdk/blob/657179d3d707a244038168675292b288e0a1f4ef/packages/deploy-helpers/tests/container-metadata.test.ts
            "containers": [{"name": "linux", "class_name": "ContainerLauncher", "images": {"bifrost-linux": image}}],
            "bindings": [
                {"name": OWNER_BINDING, "type": "plain_text", "text": worker_owner_value(intent)},
                *({"name": name, "type": "plain_text", "text": value} for name, value in settings.items()),
                {"name": "CONTAINER_INSTANCE", "type": "durable_object_namespace", "class_name": "ContainerLauncher"},
                {"name": "CONTAINER_REGISTRY", "type": "durable_object_namespace", "class_name": "ContainerScopeRegistry"},
            ],
            "keep_bindings": [],
            "observability": {"enabled": False},
            "logpush": False,
        }
        await self._upload_script(api_token, intent, {
            "metadata": ("metadata.json", json.dumps(metadata), "application/json"),
            module: (module, self.release.launcher_module, "application/javascript+module"),
        })

    async def configure_endpoint(
        self, *, api_token: SecretStr, expected: ResourceObservation,
    ) -> ConfiguredWorkersScript:
        self._require_target(expected.intent)
        return await self._configure_script_endpoint(
            api_token, expected, exports={"ContainerLauncher", "ContainerScopeRegistry"}, public_endpoint=True,
        )


def _registry_tool_observation(
    exc: OCIToolError, operation: Literal["inventory", "source", "copy", "digest"], *,
    evidence: dict[str, str] | None = None, tag_listed: bool = False,
) -> ObservationPending:
    details = {} if evidence is None else evidence
    count_key = f"{operation}_failure_count"
    details[f"{operation}_failure_class"] = exc.failure.value
    details[count_key] = str(int(details.get(count_key, "0")) + 1)
    retryable = exc.retryable_readback or (tag_listed and exc.failure is OCIToolFailure.IMAGE_NOT_FOUND)
    error = ObservationSettling if retryable else ObservationPending
    return error(
        f"Container registry {operation} could not be confirmed",
        OwnershipResource.CONTAINER_IMAGE, ObservationCheck.CONTAINER_IMAGE_PREPARATION, evidence=details,
    )


class ContainerRegistry:
    """Use the qualified crane and cf paths with isolated credential files."""

    def __init__(
        self, rest: ContainersRest, *,
        crane: Path = Path("/app/artifacts/cloudflare/bin/crane"),
        cf: Path = Path("/app/artifacts/cloudflare/node_modules/cf/bin/cf"),
    ) -> None:
        self.rest = rest
        self.crane = crane
        self.cf = cf

    async def _tags(self, token: SecretStr, account: str, name: str, cwd: Path) -> frozenset[str]:
        env = {"CLOUDFLARE_ACCOUNT_ID": account, "CLOUDFLARE_API_TOKEN": token.get_secret_value()}
        if (await run_oci_tool(self.cf, "--version", cwd=cwd)).strip() != b"1.0.0-beta.31":
            raise ObservationPending(
                "Expected the pinned registry CLI",
                OwnershipResource.CONTAINER_IMAGE, ObservationCheck.CONTAINER_IMAGE_PREPARATION,
            )
        try:
            owner = json.loads(await run_oci_tool(self.cf, "accounts", "get", "-q", cwd=cwd, env=env))
            if owner["id"] != account:
                raise OwnershipConflict("Registry CLI selected another account")
            rows = json.loads(await run_oci_tool(
                self.cf, "containers", "images", "list", "--filter",
                f"^(?:{account}/)?{name}$", cwd=cwd, env=env,
            ))
        except (ValueError, TypeError, KeyError):
            raise ObservationPending("Registry tag inventory is malformed") from None
        if not isinstance(rows, list) or len(rows) > 1 or any(
            not isinstance(row, dict) or row.get("name") not in {name, f"{account}/{name}"}
            or not isinstance(row.get("tags"), list)
            or any(not isinstance(tag, str) or not re.fullmatch(r"[\w][\w.-]{0,127}", tag, re.ASCII)
                   for tag in row["tags"])
            or len(set(row["tags"])) != len(row["tags"])
            for row in rows
        ):
            raise ObservationPending("Registry tag inventory is unconfirmed")
        return frozenset(rows[0]["tags"]) if rows else frozenset()

    async def _deployment_tags(self, token: SecretStr, account: str, name: str, cwd: Path) -> frozenset[str]:
        try:
            return await self._tags(token, account, name, cwd)
        except OCIToolError as exc:
            raise _registry_tool_observation(exc, "inventory") from None

    async def require_empty(self, token: SecretStr, intent: ResourceIntent) -> None:
        name = _name(intent, "container_image")
        with TemporaryDirectory(prefix="bifrost-oci-") as temp:
            tags = await self._deployment_tags(token, intent.target.account_id, name, Path(temp))
        if tags:
            raise OwnershipConflict("Container registry repository is occupied",
                                    OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.TARGET_OCCUPIED)

    async def _authenticate(
        self, token: SecretStr, account: str, cwd: Path, *, push: bool,
    ) -> None:
        credentials = await self.rest._request(
            token, account, "POST", f"containers/registries/{REGISTRY}/credentials",
            json_body={"expiration_minutes": 15, "permissions": ["pull", "push"] if push else ["pull"]},
        )
        assert credentials is not None
        row = credentials["result"]
        if (
            not isinstance(row, dict) or row.get("account_id") != account or row.get("registry_host") != REGISTRY
            or any(not isinstance(row.get(key), str) or not row[key] for key in ("username", "password"))
        ):
            raise OwnershipConflict("Registry credentials have a different account or host",
                                    OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.RECEIPT_MISMATCH)
        encoded = base64.b64encode(f"{row['username']}:{row['password']}".encode()).decode()
        with os.fdopen(os.open(cwd / "config.json", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as config:
            json.dump({"auths": {REGISTRY: {"auth": encoded}}}, config)

    async def copy(
        self, token: SecretStr, intent: ResourceIntent, release: VerifiedContainerRelease, *, previously_sent: bool,
        hold_lease: Callable[[], Awaitable[None]],
    ) -> ResourceObservation:
        """Replay only the journaled digest into an empty owned repository."""
        name = _name(intent, "container_image")
        account = intent.target.account_id
        if release.digest != intent.artifact_digest:
            raise OwnershipConflict("Container image release differs from its journal")
        destination = f"{REGISTRY}/{account}/{name}:release"
        with TemporaryDirectory(prefix="bifrost-oci-") as temp:
            cwd = Path(temp)
            await hold_lease()
            try:
                if (await run_oci_tool(self.crane, "version", cwd=cwd)).strip() != b"0.20.6":
                    raise ObservationPending(
                        "Expected the pinned OCI copy tool",
                        OwnershipResource.CONTAINER_IMAGE, ObservationCheck.CONTAINER_IMAGE_PREPARATION,
                    )
                await self._verify_image(release.manifest.image, release, cwd)
            except OCIToolError as exc:
                if exc.failure is OCIToolFailure.IMAGE_NOT_FOUND:
                    raise  # The immutable source image is missing.
                raise _registry_tool_observation(exc, "source") from None
            tags = await self._deployment_tags(token, account, name, cwd)
            visible = "release" in tags
            if tags and (not previously_sent or tags != {"release"}):
                raise OwnershipConflict("Container registry repository is occupied",
                                        OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.TARGET_OCCUPIED)
            await self._authenticate(token, account, cwd, push=not visible)
            if not visible:
                try:
                    await self._copy_while_current(release.manifest.image, destination, cwd, hold_lease)
                except OCIToolError as exc:
                    raise _registry_tool_observation(exc, "copy") from None
            digest = release.manifest.image_digest
            await self._wait_for_prepared_image(token, account, name, destination, digest, cwd, hold_lease)
        return ResourceObservation(intent, destination, digest)

    async def _copy_while_current(
        self, source: str, destination: str, cwd: Path, hold_lease: Callable[[], Awaitable[None]],
    ) -> None:
        stopped = asyncio.Event()

        async def watch_lease() -> None:
            while not stopped.is_set():
                try:
                    await asyncio.wait_for(stopped.wait(), timeout=_IMAGE_COPY_LEASE_INTERVAL)
                except TimeoutError:
                    if not stopped.is_set():
                        await hold_lease()

        await hold_lease()
        watcher = asyncio.create_task(watch_lease())
        copy = asyncio.create_task(run_oci_tool(self.crane, "copy", source, destination, cwd=cwd, timeout=90))
        try:
            await asyncio.wait({copy, watcher}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stopped.set()
            copy.cancel()
            await asyncio.gather(watcher, copy, return_exceptions=True)
        watcher.result()
        copy.result()

    async def _wait_for_prepared_image(
        self, token: SecretStr, account: str, name: str, tag: str, digest: str, cwd: Path,
        hold_lease: Callable[[], Awaitable[None]],
    ) -> None:
        evidence = {"registry_tag": "unobserved"}
        try:
            async with asyncio.timeout(_IMAGE_SETTLEMENT_SECONDS):
                while True:
                    await hold_lease()
                    operation: Literal["inventory", "digest"] = "inventory"
                    evidence["registry_tag"] = "unobserved"
                    try:
                        tags = await self._tags(token, account, name, cwd)
                        if tags - {"release"}:
                            raise OwnershipConflict(
                                "Container registry repository acquired an unexpected tag",
                                OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.TARGET_OCCUPIED,
                            )
                        evidence["registry_tag"] = "absent"
                        if tags:
                            operation = "digest"
                            evidence["registry_tag"] = "unconfirmed_digest"
                            await self._require_tag_digest(tag, digest, cwd)
                    except OCIToolError as exc:
                        failure = _registry_tool_observation(
                            exc, operation, evidence=evidence, tag_listed=operation == "digest",
                        )
                        if not isinstance(failure, ObservationSettling):
                            raise failure from None
                    else:
                        if tags:
                            evidence["registry_tag"] = "exact_digest"
                            await hold_lease()
                            if await self._prepare_image(token, account, name, digest, evidence):
                                return
                    await asyncio.sleep(_IMAGE_SETTLEMENT_INTERVAL)
        except TimeoutError:
            raise ObservationSettling(
                "Waiting for the exact Container image to be prepared",
                OwnershipResource.CONTAINER_IMAGE, ObservationCheck.CONTAINER_IMAGE_PREPARATION,
                evidence=evidence,
            ) from None

    async def _prepare_image(
        self, token: SecretStr, account: str, name: str, digest: str, evidence: dict[str, str],
    ) -> bool:
        # The digest-pinned POST idempotently starts or observes preparation.
        # https://developers.cloudflare.com/api/resources/containers/subresources/images/methods/prepare/
        image = f"{REGISTRY}/{account}/{name}@{digest}"
        response = await self.rest._request(
            token, account, "POST", "containers/image-preparations", json_body={"image": image}, include_http_status=True,
        )
        assert response is not None
        preparation = response["result"]
        row = preparation if isinstance(preparation, dict) else {}
        status = row.get("status")
        evidence.update({
            "preparation_http_status": str(response["_http_status"]),
            "preparation_shape": {"dict": "object", "list": "array", "NoneType": "null"}.get(
                type(preparation).__name__, "scalar",
            ),
            "preparation_fields": ",".join(key for key in ("image", "status", "artifact_digest", "reason") if key in row),
            "preparation_status": status if status in ("pending", "ready", "error") else "unknown",
        })
        if "image" in row and row["image"] != image:
            raise OwnershipConflict(
                "Container preparation refers to a different image",
                OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.RECEIPT_MISMATCH,
            )
        if row.get("image") != image or status not in ("pending", "ready"):
            raise ObservationPending(
                "Container image preparation did not return ready",
                OwnershipResource.CONTAINER_IMAGE, ObservationCheck.CONTAINER_IMAGE_PREPARATION,
                evidence=evidence,
            )
        return status == "ready"

    async def _require_tag_digest(self, tag: str, digest: str, cwd: Path) -> None:
        observed_digest = await run_oci_tool(self.crane, "digest", tag, cwd=cwd)
        if observed_digest.strip() != digest.encode():
            raise OwnershipConflict("Container registry tag has a different digest",
                                    OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.RECEIPT_MISMATCH)

    async def _observe_tag(
        self, token: SecretStr, intent: ResourceIntent, digest: str, cwd: Path,
    ) -> ResourceObservation | None:
        name = _name(intent, "container_image")
        account = intent.target.account_id
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            raise OwnershipConflict("Container image journal lacks a digest",
                                    OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.RECEIPT_MISSING)
        tags = await self._tags(token, account, name, cwd)
        if not tags:
            return None
        if tags != {"release"}:
            raise OwnershipConflict("Container registry repository is occupied",
                                    OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.TARGET_OCCUPIED)
        await self._authenticate(token, account, cwd, push=False)
        tag = f"{REGISTRY}/{account}/{name}:release"
        await self._require_tag_digest(tag, digest, cwd)
        return ResourceObservation(intent, tag, digest)

    async def observe_tag(self, token: SecretStr, intent: ResourceIntent, digest: str) -> ResourceObservation | None:
        with TemporaryDirectory(prefix="bifrost-oci-") as temp:
            return await self._observe_tag(token, intent, digest, Path(temp))

    async def delete(self, token: SecretStr, intent: ResourceIntent, digest: str) -> bool:
        """Return False for an empty read; never delete an unowned tag."""
        account = intent.target.account_id
        with TemporaryDirectory(prefix="bifrost-oci-") as temp:
            cwd = Path(temp)
            if await self._observe_tag(token, intent, digest, cwd) is None:
                return False
            name = _name(intent, "container_image")
            await run_oci_tool(
                self.cf, "containers", "images", "delete", f"{name}:release", "--force", cwd=cwd,
                env={"CLOUDFLARE_ACCOUNT_ID": account, "CLOUDFLARE_API_TOKEN": token.get_secret_value()},
            )
            return True

    async def _verify_image(self, image: str, release: VerifiedContainerRelease, cwd: Path) -> None:
        manifest = await run_oci_tool(self.crane, "manifest", image, cwd=cwd)
        config = await run_oci_tool(self.crane, "config", image, cwd=cwd)
        release.verify_image(manifest.removesuffix(b"\n"), config.removesuffix(b"\n"))
