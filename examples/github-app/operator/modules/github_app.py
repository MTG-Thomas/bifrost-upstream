"""Provider-neutral SDK storage plus a GitHub.com read-only App client.

No platform internals, database access, PATs, logging of provider bodies or tokens.
All durable work is invoked by ordinary Bifrost workflows.
"""

import asyncio
import hashlib
import re
import time
from datetime import datetime
from uuid import UUID, uuid4

import httpx
import jwt

TABLE = "github_app_state"
CONNECTION = "GitHub App Operator"
TOPIC = "github.app.event"
READ_EVENTS = frozenset({"repository", "pull_request", "pull_request_review",
    "pull_request_review_comment", "issues", "issue_comment", "check_run",
    "check_suite", "release", "push", "create", "delete"})
LIFECYCLE = frozenset({"installation", "installation_repositories", "installation_target"})


class PackageError(Exception):
    """Sanitized operator-facing failure; never include a provider body or key."""


def provider_id(value):
    # Preserve GitHub's integer identities losslessly, including 19-digit IDs.
    if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]{0,19}", str(value)):
        raise PackageError("Invalid provider ID")
    return str(value)


def org_id(value):
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        raise PackageError("Expected an organization or Solution UUID") from None


class Store:
    """Public SDK only. Insert conflict is an outcome, not permission to upsert."""

    async def insert(self, key, data):
        from bifrost import api, context
        # Omit attribution: the server assigns the authenticated caller. The
        # tables.insert convenience method supplies an override automatically,
        # which would require unrelated tableattribution.readwrite authority.
        response = await api.post(f"/api/tables/{TABLE}/documents", params={
            "scope": context.organization.id, "solution": context.solution_id,
            "caller_solution": context.solution_id,
        }, json={"id": key, "data": data})
        if response.status_code == 201:
            return True
        if response.status_code == 409:
            return False
        raise PackageError("Package state write failed")

    async def get(self, key):
        from bifrost import tables
        row = await tables.get(TABLE, key)
        return row.data if row else None

    async def query(self, **where):
        from bifrost import tables
        result = await tables.query(TABLE, where=where, order_by="order", order_dir="desc", limit=1000)
        if result.total > 1000:
            raise PackageError("Package state query exceeds its bounded result window; archive old records")
        return [row.data for row in result.documents]

    async def secret(self, key, value=None):
        from bifrost import config
        if value is not None:
            await config.set(key, value, is_secret=True)
        return await config.get(key)  # Registers the exact plaintext for runtime redaction.


class GitHub:
    """Fixed origin and operation whitelist; no caller URLs or redirects."""

    def __init__(self, app_id, private_key, store, *, clock=time.time, sleep=asyncio.sleep, transport=None):
        self.app_id, self.private_key = provider_id(app_id), private_key
        self.store, self.clock, self.sleep, self.transport = store, clock, sleep, transport

    async def gate(self, bucket):
        # One durable claim per 250ms slot, shared by every workflow process.
        # No expired-lock deletion/reacquisition race. Failed callers spend a slot.
        deadline = self.clock() + 20
        while self.clock() < deadline:
            now = self.clock()
            if await self.store.insert(f"slot:{bucket}:{int(now * 4)}", {
                "kind": "slot", "bucket": bucket, "order": now,
            }):
                return
            await self.sleep(0.26)
        raise PackageError("Request scheduler busy; retry through workflow recovery")

    async def call(self, operation, installation=None, *, token=None, page=1, delivery=None):
        iid = provider_id(installation) if installation is not None else None
        did = provider_id(delivery) if delivery is not None else None
        if operation == "installation" and iid:
            method, path = "GET", f"/app/installations/{iid}"
        elif operation == "mint" and iid:
            method, path = "POST", f"/app/installations/{iid}/access_tokens"
        elif operation == "repositories" and iid and token:
            method, path = "GET", "/installation/repositories"
        elif operation == "deliveries":
            method, path = "GET", "/app/hook/deliveries"
        elif operation == "delivery" and did:
            method, path = "GET", f"/app/hook/deliveries/{did}"
        elif operation == "redeliver" and did:
            method, path = "POST", f"/app/hook/deliveries/{did}/attempts"
        else:
            raise PackageError("Unsupported GitHub operation")
        if not isinstance(page, int) or isinstance(page, bool) or not 1 <= page <= 100:
            raise PackageError("Invalid inventory page")
        # Installation-token repository calls share the installation's quota.
        # App-JWT calls, including minting, coordinate in the App bucket.
        bucket = iid if iid and operation == "repositories" else "app"
        cooldowns = await self.store.query(kind="cooldown", bucket=bucket)
        if any(row["until"] > self.clock() for row in cooldowns):
            raise PackageError("GitHub quota cooldown is active")
        await self.gate(bucket)
        if token is None:
            now = int(self.clock())
            try:
                token = jwt.encode({"iss": self.app_id, "iat": now - 60, "exp": now + 540},
                                   self.private_key, algorithm="RS256")
            except Exception:
                raise PackageError("App signing key is invalid") from None
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                   "X-GitHub-Api-Version": "2022-11-28"}
        kwargs = {}
        if operation in {"repositories", "deliveries"}:
            kwargs["params"] = {"per_page": 100, "page": page}
        if operation == "mint":
            kwargs["json"] = {"permissions": {"metadata": "read"}}
        try:
            async with httpx.AsyncClient(base_url="https://api.github.com", timeout=15,
                                        follow_redirects=False, transport=self.transport) as client:
                response = await client.request(method, path, headers=headers, **kwargs)
        except httpx.HTTPError:
            raise PackageError("GitHub request outcome is uncertain; inspect before repeating writes") from None
        remaining = response.headers.get("x-ratelimit-remaining")
        # A secondary limit can omit both Retry-After and a depleted primary
        # counter. Conservatively cool down any 403; never churn tokens on it.
        limited = response.status_code in {403, 429}
        if limited:
            now = self.clock()
            try:
                until = max(now + 60, float(response.headers.get("x-ratelimit-reset", now)),
                            now + float(response.headers.get("retry-after", 0)))
            except ValueError:
                until = now + 60
            await self.store.insert(f"cooldown:{uuid4()}", {
                "kind": "cooldown", "bucket": bucket, "until": until, "order": now,
            })
        await self.store.insert(f"request:{uuid4()}", {"kind": "request", "bucket": bucket,
            "operation": operation, "status": response.status_code, "remaining": remaining,
            "order": self.clock()})
        if not 200 <= response.status_code < 300:
            raise PackageError(f"GitHub {operation} returned HTTP {response.status_code}")
        if operation == "redeliver":
            return {"accepted": True}
        try:
            return response.json()
        except ValueError:
            raise PackageError("GitHub returned an invalid response") from None


class Broker:
    def __init__(self, store, github, emit, validate_target, *, clock=time.time, sleep=asyncio.sleep):
        self.store, self.github, self.emit = store, github, emit
        self.validate_target = validate_target
        self.clock, self.sleep = clock, sleep

    async def binding(self, installation):
        iid = provider_id(installation)
        binding = await self.store.get(f"binding:{iid}")
        if binding is None:
            raise PackageError("Installation has no verified destination binding")
        if binding.get("app_id") != self.github.app_id:
            raise PackageError("Installation binding belongs to another App")
        return binding

    async def epoch(self, iid):
        fences = await self.store.query(kind="fence", installation=iid)
        # Every newly observed invalidation changes the epoch. API and worker
        # clocks may differ, so ordering by their timestamps is not a fence.
        return hashlib.sha256("\0".join(sorted(row["identity"] for row in fences)).encode()).hexdigest() if fences else "initial"

    async def bind(self, installation, organization, solution):
        iid, oid, sid = provider_id(installation), org_id(organization), org_id(solution)
        await self.validate_target(oid, sid)
        verified = await self.github.call("installation", iid)
        self.check_installation(verified, iid)
        binding = {"kind": "binding", "installation": iid, "organization": oid, "app_id": self.github.app_id,
                   "solution": sid, "account": provider_id(verified["account"]["id"]), "order": self.clock()}
        if not await self.store.insert(f"binding:{iid}", binding):
            existing = await self.binding(iid)
            if any(existing[key] != binding[key] for key in ("organization", "solution", "account", "app_id")):
                raise PackageError("Installation is already bound; automatic reassignment is forbidden")
        return await self.reconcile(iid)

    @staticmethod
    def check_installation(value, iid):
        if provider_id(value.get("id")) != iid or value.get("suspended_at"):
            raise PackageError("Installation is unavailable or suspended")

    async def fence(self, iid, identity, observed_at):
        if isinstance(observed_at, str) and "T" in observed_at:
            observed_at = datetime.fromisoformat(observed_at.replace("Z", "+00:00")).timestamp()
        await self.store.insert(f"fence:{iid}:{identity}", {"kind": "fence", "installation": iid,
            "identity": identity, "order": f"{float(observed_at):020.6f}:{identity}"})

    async def token(self, iid, epoch):
        deadline = self.clock() + 30
        while self.clock() < deadline:
            if await self.epoch(iid) != epoch:
                raise PackageError("Installation changed during token refresh")
            cached = await self.store.query(kind="token", installation=iid, epoch=epoch)
            for row in cached:
                if row["expires"] > self.clock() + 60:
                    token = await self.store.secret(row["secret_key"])
                    if token:
                        return token
            # A fixed 30-second claim bounds crash recovery without stealing locks.
            claim = f"refresh:{iid}:{epoch}:{int(self.clock() // 30)}"
            if await self.store.insert(claim, {"kind": "refresh", "order": self.clock()}):
                minted = await self.github.call("mint", iid)
                try:
                    expiry = datetime.fromisoformat(minted["expires_at"].replace("Z", "+00:00")).timestamp()
                    token = minted["token"]
                    if not isinstance(token, str) or not token or expiry <= self.clock() + 60:
                        raise ValueError()
                except (KeyError, ValueError, TypeError):
                    raise PackageError("GitHub returned an invalid installation token") from None
                if await self.epoch(iid) != epoch:
                    raise PackageError("Installation changed during token refresh")
                key = f"github_app_token_{uuid4().hex}"
                await self.store.secret(key, token)
                await self.store.insert(f"token:{uuid4()}", {"kind": "token", "installation": iid,
                    "epoch": epoch, "expires": expiry, "secret_key": key, "order": self.clock()})
                return await self.store.secret(key)
            await self.sleep(0.25)
        raise PackageError("Token refresh is pending; recover this workflow after inspection")

    async def reconcile(self, installation):
        iid = provider_id(installation)
        binding = await self.binding(iid)
        epoch = await self.epoch(iid)
        started = self.clock()
        try:
            verified = await self.github.call("installation", iid)
            self.check_installation(verified, iid)
            if provider_id(verified["account"]["id"]) != binding["account"]:
                raise PackageError("Installation account no longer matches its binding")
            token = await self.token(iid, epoch)
            repos = []
            for page in range(1, 101):
                inventory = await self.github.call("repositories", iid, token=token, page=page)
                items = inventory["repositories"]
                repos.extend({"id": provider_id(item["id"]), "name": item["full_name"]} for item in items)
                if len(items) < 100:
                    break
            else:
                raise PackageError("Inventory exceeds the bounded 100-page scan")
        except PackageError:
            # A failed live verification invalidates previous inventory too.
            await self.fence(iid, str(uuid4()), f"{self.clock():020.6f}")
            raise
        if await self.epoch(iid) != epoch:
            raise PackageError("Installation changed during inventory; reconcile again")
        snapshot = {"kind": "inventory", "installation": iid, "epoch": epoch,
                    "repositories": repos, "order": started, "verified_at": self.clock()}
        await self.store.insert(f"inventory:{uuid4()}", snapshot)
        return {"installation": iid, "organization": binding["organization"],
                "repository_count": len(repos), "verified_at": snapshot["verified_at"]}

    async def inventory(self, iid):
        epoch = await self.epoch(iid)
        rows = await self.store.query(kind="inventory", installation=iid, epoch=epoch)
        if not rows or rows[0]["verified_at"] < self.clock() - 60:
            await self.reconcile(iid)
            return await self.inventory(iid)
        return rows[0]

    async def receive(self, event, headers):
        """Only called with trusted EventContext from the private signed source."""
        if event.type == "ping":
            return {"state": "ping"}
        payload = event.data
        iid = provider_id(payload.get("installation", {}).get("id"))
        delivery = headers.get("x-github-delivery", "")
        if not re.fullmatch(r"[0-9a-fA-F-]{36}", delivery):
            raise PackageError("Missing GitHub delivery GUID")
        binding = await self.binding(iid)
        if event.type in LIFECYCLE:
            await self.fence(iid, event.id, str(event.received_at))
            return {"state": "reconciliation_required", "installation": iid}
        if event.type == "repository" and payload.get("action") in {"deleted", "transferred"}:
            await self.fence(iid, event.id, str(event.received_at))
            return {"state": "reconciliation_required", "installation": iid}
        if event.type not in READ_EVENTS:
            return {"state": "ignored", "reason": "event_type"}
        repo = provider_id(payload.get("repository", {}).get("id"))
        snapshot = await self.inventory(iid)
        if repo not in {item["id"] for item in snapshot["repositories"]}:
            raise PackageError("Repository is outside this installation's verified inventory")
        envelope = {"delivery_id": delivery, "event_id": event.id, "installation_id": iid,
                    "repository_id": repo, "event_type": event.type, "payload": payload}
        if snapshot["epoch"] != await self.epoch(iid):
            raise PackageError("Installation changed before routing")
        await self.validate_target(binding["organization"], binding["solution"])
        if snapshot["epoch"] != await self.epoch(iid):
            raise PackageError("Installation changed before routing")
        result = await self.emit(TOPIC, envelope, scope=binding["organization"], solution=binding["solution"])
        await self.store.insert(f"routed:{event.id}", {"kind": "routed", "installation": iid,
            "delivery": delivery, "event_id": event.id, "destination_event": result["event_id"], "order": self.clock()})
        return {"state": "routed", "destination_event": result["event_id"]}

    async def redeliver(self, installation, delivery):
        iid, did = provider_id(installation), provider_id(delivery)
        await self.binding(iid)
        verified = await self.github.call("delivery", iid, delivery=did)
        if provider_id(verified.get("installation_id")) != iid:
            raise PackageError("Delivery belongs to another installation")
        # Verify exact ID without JS-number conversion; no caller-supplied GUID shortcut.
        if provider_id(verified.get("id")) != did:
            raise PackageError("Provider delivery identity mismatch")
        claim = f"redelivery:{did}"
        if not await self.store.insert(claim, {"kind": "redelivery", "installation": iid,
            "delivery": did, "guid": verified["guid"], "order": self.clock()}):
            raise PackageError("Redelivery already requested or uncertain; inspect GitHub before another attempt")
        await self.github.call("redeliver", iid, delivery=did)
        return {"state": "requested", "delivery_id": did, "guid": verified["guid"]}

    async def deliveries(self, installation):
        iid = provider_id(installation)
        await self.binding(iid)
        recent = await self.github.call("deliveries", iid)
        return {"page": 1, "bounded_to": 100, "deliveries": [{
            "id": provider_id(row["id"]), "guid": row["guid"], "status_code": row["status_code"],
            "delivered_at": row["delivered_at"], "redelivery": row["redelivery"],
        } for row in recent if str(row.get("installation_id")) == iid]}

    async def health(self):
        bindings = await self.store.query(kind="binding")
        rows = []
        for binding in bindings:
            iid = binding["installation"]
            epoch = await self.epoch(iid)
            inventory = await self.store.query(kind="inventory", installation=iid, epoch=epoch)
            latest = inventory[0] if inventory else None
            rows.append({"installation": iid, "organization": binding["organization"],
                "solution": binding["solution"], "repository_count": len(latest["repositories"]) if latest else 0,
                "verified_at": latest["verified_at"] if latest else None,
                "ready": bool(latest and latest["verified_at"] >= self.clock() - 60)})
        return {"installations": rows, "cooldowns": await self.store.query(kind="cooldown"),
                "deduplication": "retained Event only", "external_writes": False}
