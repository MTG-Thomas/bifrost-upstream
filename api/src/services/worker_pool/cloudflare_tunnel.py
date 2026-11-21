"""Managed Tunnel journal and connector credential lifecycle."""

import base64
import secrets
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from hmac import compare_digest
from uuid import uuid4

import httpx
from pydantic import SecretStr
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.models.orm.cloudflare_capacity import (
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialPurpose,
    CredentialRef,
    open_credential,
    seal_credential,
)
from src.services.worker_pool.cloudflare_provisioning import (
    DeletionSettling,
    ObservationPending,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
)
from src.services.worker_pool.cloudflare_tunnel_rest import (
    TunnelRest,
    connector_secret,
    dedicated_hostname,
)

TUNNEL_SCOPE = "reachability"
TUNNEL = "tunnel"
DNS = "tunnel_dns"


class TunnelUnavailable(ObservationPending):
    """Readback confirmed a required tunnel resource is unavailable."""


def tunnel_origin() -> str:
    channel = get_settings().cloudflare_tunnel_channel
    if channel == "compose":
        return "http://api:8001"
    if channel == "kustomize":
        return "http://worker-ingress:8001"
    raise ValueError("Install the Managed Tunnel connector before selecting this reachability mode")


def adapter(http: httpx.AsyncClient, row: CloudflareInstallation, token: SecretStr) -> TunnelRest:
    if row.reachability_mode != "managed_tunnel" or row.zone_id is None:
        raise ValueError("This installation does not have a managed tunnel zone")
    return TunnelRest(http, account_id=row.account_id, zone_id=row.zone_id,
                      hostname=dedicated_hostname(row.worker_ingress_origin), token=token)


async def resource(
    db: AsyncSession, row: CloudflareInstallation, kind: str,
) -> CloudflareOwnedResource | None:
    return await db.scalar(select(CloudflareOwnedResource).where(
        CloudflareOwnedResource.installation_id == row.id,
        CloudflareOwnedResource.scope == TUNNEL_SCOPE,
        CloudflareOwnedResource.kind == kind,
    ).with_for_update())


def _intent(item: CloudflareOwnedResource) -> ResourceIntent:
    return ResourceIntent(
        ResourceTarget(item.installation_id, item.account_id, item.scope, item.kind, item.target_name),
        item.effect_id, item.desired_revision, item.artifact_digest,
    )


def _recorded(item: CloudflareOwnedResource) -> ResourceObservation | None:
    if item.provider_id is None or item.provider_version is None:
        if item.state != "creating" and item.state != "absent":
            raise OwnershipConflict("Managed Tunnel receipt is incomplete")
        return None
    return ResourceObservation(_intent(item), item.provider_id, item.provider_version)


def _present(item: CloudflareOwnedResource, observation: ResourceObservation) -> None:
    item.provider_id = observation.provider_id
    item.provider_version = observation.provider_version
    item.state = "present"
    item.observed_at = datetime.now(UTC)


def _ref(row: CloudflareInstallation, purpose: CredentialPurpose, version: int) -> CredentialRef:
    return CredentialRef(row.id, row.account_id, purpose, "", version)


async def _credential(db: AsyncSession, ref: CredentialRef) -> SecretStr:
    entry = await db.get(CloudflareCredential, (ref.installation_id, ref.purpose, ref.scope, ref.version))
    if entry is None:
        raise OwnershipConflict("Managed Tunnel credential receipt is missing")
    return open_credential(ref, entry)


def _random_secret() -> SecretStr:
    return SecretStr(base64.b64encode(secrets.token_bytes(32)).decode("ascii"))


async def prepare(db: AsyncSession, row: CloudflareInstallation, *, actor: str) -> None:
    """Commit both intents and the one-time creation credential before external effects."""
    origin = tunnel_origin()
    if await resource(db, row, TUNNEL) is not None:
        return
    digest = sha256(f"{row.zone_id}\n{row.worker_ingress_origin}\n{origin}".encode()).hexdigest()
    for kind in (TUNNEL, DNS):
        effect = uuid4()
        name = f"bifrost-{effect.hex}" if kind == TUNNEL else dedicated_hostname(row.worker_ingress_origin)
        db.add(CloudflareOwnedResource(
            id=uuid4(), installation_id=row.id, account_id=row.account_id, scope=TUNNEL_SCOPE,
            kind=kind, target_name=name, desired_revision=row.desired_revision,
            artifact_digest=digest, effect_id=effect, state="creating",
        ))
    db.add(seal_credential(_ref(row, "tunnel_secret", 1), _random_secret(), actor=actor, now=datetime.now(UTC)))


async def provision_tunnel(db: AsyncSession, row: CloudflareInstallation, client: TunnelRest) -> None:
    item = await resource(db, row, TUNNEL)
    if item is None:
        raise OwnershipConflict("Managed Tunnel has no creation intent")
    await client.verify_zone()
    receipt = _recorded(item)
    if receipt is None:
        receipt = await client.create_tunnel(_intent(item), await _credential(db, _ref(row, "tunnel_secret", 1)))
    elif await client.tunnel(receipt.intent, receipt) is None:
        raise ObservationPending("The recorded Cloudflare tunnel is absent")
    await client.configure_ingress(receipt, tunnel_origin())
    await client.verify_private_routes(receipt.provider_id)
    _present(item, receipt)


async def provision_dns(
    db: AsyncSession, row: CloudflareInstallation, client: TunnelRest, *, actor: str,
) -> None:
    tunnel = await _owned_tunnel(db, row, client)
    record = await resource(db, row, DNS)
    if record is None:
        raise OwnershipConflict("Managed Tunnel has no DNS creation intent")
    receipt = _recorded(record)
    if receipt is None:
        receipt = await client.create_dns(_intent(record), tunnel)
    elif await client.dns(receipt.intent, receipt, tunnel.provider_id) is None:
        raise ObservationPending("The recorded Cloudflare DNS record is absent")
    _present(record, receipt)
    version = row.tunnel_token_version or 1
    token = await client.connector_token(tunnel.provider_id)
    expected = await _credential(db, _ref(row, "tunnel_secret", version))
    if not compare_digest(
        connector_secret(token.get_secret_value(), row.account_id, tunnel.provider_id), expected.get_secret_value(),
    ):
        raise OwnershipConflict("The Cloudflare tunnel credential changed outside Bifrost")
    if row.tunnel_token_version is None:
        db.add(seal_credential(_ref(row, "connector_token", version), token, actor=actor, now=datetime.now(UTC)))
        row.tunnel_token_version = version


async def _owned_tunnel(db: AsyncSession, row: CloudflareInstallation, client: TunnelRest) -> ResourceObservation:
    item = await resource(db, row, TUNNEL)
    receipt = _recorded(item) if item is not None else None
    if receipt is None or await client.tunnel(receipt.intent, receipt) is None:
        raise TunnelUnavailable("The recorded Cloudflare tunnel is absent")
    return receipt


async def verify(db: AsyncSession, row: CloudflareInstallation, client: TunnelRest) -> None:
    await client.verify_zone()
    tunnel = await _owned_tunnel(db, row, client)
    record = await resource(db, row, DNS)
    receipt = _recorded(record) if record is not None else None
    if receipt is None or await client.dns(receipt.intent, receipt, tunnel.provider_id) is None:
        raise TunnelUnavailable("The recorded Cloudflare DNS record is absent")
    await client.verify_ingress(tunnel, tunnel_origin())
    await client.verify_private_routes(tunnel.provider_id)
    if row.tunnel_token_version is None:
        raise ObservationPending("Managed Tunnel has not published a connector credential")
    expected = await _credential(db, _ref(row, "tunnel_secret", row.tunnel_token_version))
    if not compare_digest(await client.secret(tunnel.provider_id), expected.get_secret_value()):
        raise OwnershipConflict("The Cloudflare tunnel credential changed outside Bifrost")
    if not await connector_ready(db, row, client):
        raise TunnelUnavailable("Cloudflare has no connected tunnel supervisor")


async def repair_ingress(db: AsyncSession, row: CloudflareInstallation, client: TunnelRest) -> None:
    await client.write_ingress(await _owned_tunnel(db, row, client), tunnel_origin(), repair=True)


async def verify_management_writes(
    db: AsyncSession, row: CloudflareInstallation, client: TunnelRest, *, repair: bool = False,
) -> None:
    await client.verify_zone()
    tunnel = await resource(db, row, TUNNEL)
    receipt = _recorded(tunnel) if tunnel is not None else None
    observed = None
    if tunnel is not None and tunnel.state != "absent":
        secret = await _credential(db, _ref(row, "tunnel_secret", 1)) if receipt is None else None
        observed = await client.tunnel(_intent(tunnel), receipt, secret=secret)
        if observed is not None:
            await client.write_ingress(observed, tunnel_origin(), repair=repair, prove_write=True)
    record = await resource(db, row, DNS)
    if record is None or record.state == "absent":
        return
    owner = observed or receipt
    if owner is None:
        if record.provider_id is not None:
            raise OwnershipConflict("Managed Tunnel DNS has no owning tunnel receipt")
        return
    dns = await client.dns(
        _intent(record), _recorded(record), owner.provider_id,
        ignore_foreign=row.desired_state == "disconnected",
    )
    if dns is not None:
        await client.verify_dns_write(dns, owner.provider_id)


async def connector_ready(db: AsyncSession, row: CloudflareInstallation, client: TunnelRest) -> bool:
    item = await resource(db, row, TUNNEL)
    if item is None or item.provider_id is None:
        raise ObservationPending("The recorded Cloudflare tunnel is absent")
    connectors = await client.connectors(item.provider_id)
    rotating = item.provider_details.get("rotation_version") == row.tunnel_token_version
    excluded = set(item.provider_details.get("rotation_previous_connectors", [])) if rotating else set()
    if rotating and excluded and row.tunnel_token_projected_version == row.tunnel_token_version:
        receipt = _recorded(item)
        if receipt is None:
            raise OwnershipConflict("Managed Tunnel rotation has no exact receipt")
        if connectors & excluded:
            await client.evict_connectors(receipt, excluded)
            connectors = await client.connectors(item.provider_id)
    ready = (
        bool(connectors) and not (connectors & excluded)
        and row.tunnel_token_version is not None
        and row.tunnel_token_projected_version == row.tunnel_token_version
    )
    item.provider_details = {**item.provider_details, "connectors": sorted(connectors)}
    if ready and rotating:
        item.provider_details = {
            key: value for key, value in item.provider_details.items()
            if key not in ("rotation_version", "rotation_previous_connectors")
        }
    return ready


async def cleanup(db: AsyncSession, row: CloudflareInstallation, client: TunnelRest) -> bool:
    """Advance one durable cleanup step. Return true only after verified absence."""
    if row.tunnel_token_version is not None:
        row.tunnel_token_version = None
        return False
    if row.tunnel_token_projected_version != 0 and await db.get(
        CloudflareCredential, (row.id, "connector_token", "", 1),
    ) is not None:
        raise DeletionSettling("Waiting for the connector token projection to be withdrawn")
    tunnel = await resource(db, row, TUNNEL)
    if tunnel is None or tunnel.state == "absent":
        return True
    receipt = _recorded(tunnel)
    if receipt is None:
        receipt = await client.tunnel(
            _intent(tunnel), None, secret=await _credential(db, _ref(row, "tunnel_secret", 1)),
        )
        if receipt is not None:
            _present(tunnel, receipt)
            return False
    record = await resource(db, row, DNS)
    if record is None:
        raise OwnershipConflict("Managed Tunnel has no DNS cleanup intent")
    await client.verify_zone()
    if record.state != "absent":
        if receipt is None:
            # DNS cannot be requested until a tunnel receipt is committed.
            if record.provider_id is not None:
                raise OwnershipConflict("Managed Tunnel DNS has no owning tunnel receipt")
        else:
            dns = await client.dns(
                _intent(record), _recorded(record), receipt.provider_id, ignore_foreign=True,
            )
            if dns is not None:
                if record.state != "deleting":
                    _present(record, dns)
                    record.state = "deleting"
                    return False
                await client.remove_dns(dns, receipt.provider_id)
        if not _settle_absence(record):
            return False
    if receipt is not None:
        if tunnel.state != "deleting":
            tunnel.state = "deleting"
            return False
        await client.remove_tunnel(receipt)
    tunnel.provider_details = {"connectors": []}
    return _settle_absence(tunnel)


def _settle_absence(item: CloudflareOwnedResource) -> bool:
    now = datetime.now(UTC)
    if item.state == "creating":
        if item.observed_at is None:
            item.observed_at = now
            return False
        if now - item.observed_at < timedelta(minutes=1):
            raise DeletionSettling("Waiting for a possible late Managed Tunnel write")
    item.state = "absent"
    item.observed_at = now
    return True


async def stage_rotation(db: AsyncSession, row: CloudflareInstallation, *, version: int, actor: str) -> None:
    ref = _ref(row, "tunnel_secret", version)
    if await db.get(CloudflareCredential, (row.id, ref.purpose, "", version)) is None:
        db.add(seal_credential(ref, _random_secret(), actor=actor, now=datetime.now(UTC)))


async def rotate(
    db: AsyncSession, row: CloudflareInstallation, client: TunnelRest, *, version: int, actor: str,
) -> None:
    current = row.tunnel_token_version
    if current == version:
        return
    if current is None or current + 1 != version:
        raise OwnershipConflict("Cloudflare tunnel rotation revision changed")
    tunnel = await _owned_tunnel(db, row, client)
    token = await client.rotate(
        tunnel, await _credential(db, _ref(row, "tunnel_secret", current)),
        await _credential(db, _ref(row, "tunnel_secret", version)),
    )
    item = await resource(db, row, TUNNEL)
    if item is None:
        raise OwnershipConflict("Managed Tunnel rotation has no exact receipt")
    excluded = (
        set(item.provider_details.get("rotation_previous_connectors", []))
        | set(item.provider_details.get("rotation_candidates", []))
        | await client.connectors(tunnel.provider_id)
    )
    item.provider_details = {
        "rotation_version": version, "rotation_previous_connectors": sorted(excluded),
    }
    db.add(seal_credential(_ref(row, "connector_token", version), token, actor=actor, now=datetime.now(UTC)))
    row.tunnel_token_version = version
    await db.execute(update(CloudflareCredential).where(
        CloudflareCredential.installation_id == row.id,
        CloudflareCredential.purpose.in_(("tunnel_secret", "connector_token")),
        CloudflareCredential.version < version,
    ).values(ciphertext=None))


async def record_rotation_intent(
    db: AsyncSession, row: CloudflareInstallation, client: TunnelRest, *, version: int,
) -> None:
    item = await resource(db, row, TUNNEL)
    if item is None or item.provider_id is None:
        raise ObservationPending("The recorded Cloudflare tunnel is absent")
    if item.provider_details.get("rotation_intent_version") == version:
        return
    receipt = await _owned_tunnel(db, row, client)
    item.provider_details = {
        **item.provider_details,
        "rotation_intent_version": version,
        "rotation_candidates": sorted(await client.connectors(receipt.provider_id)),
    }


async def active_connector_token(db: AsyncSession, row: CloudflareInstallation) -> SecretStr | None:
    if row.retired_at is not None or row.reachability_mode != "managed_tunnel" or row.tunnel_token_version is None:
        return None
    return await _credential(db, _ref(row, "connector_token", row.tunnel_token_version))
