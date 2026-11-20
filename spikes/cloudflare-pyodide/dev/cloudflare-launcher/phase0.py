"""Bounded hosted qualification. By default performs only read-only preflight."""

import argparse
import contextlib
import datetime as dt
import json
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
from phase0_adapter import (
    ACCOUNT_ID,
    Cloudflare,
    QualificationError,
    RegistryCopy,
    active_entitlement,
    application_path,
    application_spec,
    file_digest,
    observe_owned,
    pinned_registry_image,
    require_absent,
)

ROOT = Path(__file__).resolve().parent
CF_VERSION = "1.0.0-beta.31"
ARTIFACTS = json.loads((ROOT / "phase0-artifacts.json").read_text(encoding="utf-8"))
FIXTURE_DIGEST = ARTIFACTS["image"]["digest"]


def evidence(operation, **fields):
    print(json.dumps({"operation": operation, **fields}), flush=True)


class Cf:
    def __init__(self, account, directory, token=None):
        self.directory = directory
        self.env = {**os.environ, "NO_COLOR": "1", "CLOUDFLARE_ACCOUNT_ID": account}
        for key in ("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_API_KEY", "CLOUDFLARE_ZONE_ID"):
            self.env.pop(key, None)
        if token:
            self.env["CLOUDFLARE_API_TOKEN"] = token
        self.executable = shutil.which("cf.cmd" if os.name == "nt" else "cf")
        if not self.executable or self.run("--version").strip() != CF_VERSION:
            raise QualificationError(f"cf {CF_VERSION} is required")
        account_read = self.json("accounts", "get")
        if account_read["id"] != account:
            raise QualificationError("cf selected a different account")

    def run(self, *args):
        result = subprocess.run(
            [self.executable, *args], cwd=self.directory, env=self.env,
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            stdin=subprocess.DEVNULL, timeout=30, check=False,
        )
        if result.returncode:
            status = re.search(r"\b(?:400|401|403|404|409|429|500|502|503)\b", result.stderr)
            raise QualificationError(
                f"cf {' '.join(args[:3])} failed"
                + (f" (HTTP {status[0]})" if status else "")
            )
        return result.stdout

    def json(self, *args, quiet=True):
        output = self.run(*args, *(("-q",) if quiet else ()))
        try:
            return json.loads(output)
        except ValueError:
            if not output.strip():
                raise QualificationError(f"cf {' '.join(args[:3])} returned empty output") from None
            raise QualificationError(f"cf {' '.join(args[:3])} returned non-JSON") from None

    def preflight(self):
        safeguards = self.json("billing", "spend-policies", "list")
        if not any(row["enabled"] and row["product_id"] == "workers" for row in safeguards):
            raise QualificationError("existing Workers spend guard is disabled")
        subscriptions = self.json("accounts", "subscriptions", "get")
        now = dt.datetime.now(dt.UTC)
        if not any(
            row.get("rate_plan", {}).get("id") == "workers_paid" and row.get("state") == "Paid"
            and dt.datetime.fromisoformat(row["current_period_start"]) <= now
            < dt.datetime.fromisoformat(row["current_period_end"])
            for row in subscriptions
        ):
            raise QualificationError("Workers Paid subscription could not be verified")
        evidence("account-preflight", workers_paid=True, workers_spend_guard=True)

    @contextlib.contextmanager
    def scoped_token(self, account, name, containers):
        names = {"Account Settings Read", "Workers Scripts Write"}
        if containers:
            names.add("Workers Containers Write")
        groups = self.json("accounts", "tokens", "permission-groups", "list")
        ids = {row["name"]: row["id"] for row in groups if row["name"] in names}
        if ids.keys() != names:
            raise QualificationError("required token grants could not be resolved")
        token_name = name + "-auth"
        policy = self.directory / "policy.json"
        policy.write_text(json.dumps([{
            "effect": "allow",
            "permission_groups": [{"id": ids[grant]} for grant in sorted(names)],
            "resources": {f"com.cloudflare.api.account.{account}": "*"},
        }]), encoding="utf-8")
        expires = dt.datetime.now(dt.UTC) + dt.timedelta(minutes=30)
        try:
            created = self.json(
                "accounts", "tokens", "create", "--name", token_name,
                "--expires-on", expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "--policies", "@policy.json",
            )
            evidence("scoped-token", grants=sorted(names), expiry_minutes=30)
            yield created["value"]
        finally:
            tokens = self.json("accounts", "tokens", "list")
            owned = [item for item in tokens if item["name"] == token_name]
            if len(owned) > 1:
                raise QualificationError("ambiguous trial token inventory")
            if owned:
                self.json("accounts", "tokens", "delete", owned[0]["id"], "--force")
            if any(item["name"] == token_name for item in self.json("accounts", "tokens", "list")):
                raise QualificationError("trial token remains")
            evidence("token-teardown", absent=True)


def do_binding(name, cls, script=None):
    binding = {"type": "durable_object_namespace", "name": name, "class_name": cls}
    if script is not None:
        binding["script_name"] = script
    return binding


def secret_binding(name, value):
    return {"type": "secret_text", "name": name, "text": value}


def wait_until(read, label, seconds=30):
    deadline = time.monotonic() + seconds
    while True:
        result = read()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise QualificationError(f"readback did not confirm {label}")
        time.sleep(1)


class Trial:
    def __init__(self, cf, api, prefix, record):
        self.cf = cf
        self.api = api
        self.prefix = prefix
        self.owner = secrets.token_hex(16)
        self.worker_intents = []
        self.record = record
        self.mutation_deadline = time.monotonic() + 18 * 60

    def check_deadline(self):
        if time.monotonic() >= self.mutation_deadline:
            raise QualificationError("mutation window expired")

    def settings(self, name):
        return self.api.optional(f"workers/scripts/{name}/settings")

    def owned_worker(self, name):
        return observe_owned(self.settings(name), self.owner)

    def upload(self, suffix, main, modules, bindings, classes=(), *, lose_response=False,
               container_images=()):
        self.check_deadline()
        name = self.prefix + "-" + suffix
        require_absent(self.settings(name))
        parts = {module: (ROOT / module).read_bytes() for module in (main, *modules)}
        hashes = {module: file_digest(body) for module, body in parts.items()}
        if any(digest != f"sha256:{ARTIFACTS['javascript_sha256'][module]}"
               for module, digest in hashes.items()):
            raise QualificationError("Worker module does not match the Phase 0 artifact manifest")
        metadata = {
            "main_module": main,
            "compatibility_date": "2026-10-30",
            "bindings": [
                *bindings, {"type": "plain_text", "name": "PHASE0_OWNER", "text": self.owner},
            ],
            "keep_bindings": [],
        }
        if classes:
            metadata["exports"] = {cls: {"type": "durable-object", "storage": "sqlite"} for cls in classes}
        if container_images:
            metadata["exports"]["WorkerContainer"]["container"] = "trial"
            metadata["containers"] = [{"name": "trial", "class_name": "WorkerContainer", "images": container_images}]
        self.worker_intents.append(name)
        self.record("worker-intent", name=name, owner=self.owner, parts=hashes)
        try:
            result = self.api.upload(name, parts, **metadata)
        except httpx.TransportError:
            # The PUT may have committed. Observe first and issue no second PUT.
            lose_response = True
        if lose_response:
            # Discard the upload receipt before running the same recovery path.
            wait_until(lambda: self.owned_worker(name), "ambiguous upload")
            evidence("lost-response", resource=suffix, uploads=1, recovered=True)
        elif result["id"] != name:
            raise QualificationError("worker upload returned unexpected identity")
        settings = wait_until(lambda: self.owned_worker(name), "worker ownership")
        expected = {(row["name"], row["type"]) for row in bindings}
        actual = {(row["name"], row["type"]) for row in settings["bindings"]}
        if not expected <= actual:
            raise QualificationError("worker binding readback differed")
        evidence("worker-upload", role=suffix, parts=hashes)
        return name

    def namespace(self, script, cls):
        rows = self.api.request("GET", "workers/durable_objects/namespaces", params={"per_page": 1000})
        matches = [row["id"] for row in rows if row.get("script") == script
                   and row["class"] == cls and not row.get("preview")]
        if len(matches) != 1:
            raise QualificationError(f"expected one namespace for {cls}")
        return matches[0]

    def public_url(self, worker):
        self.check_deadline()
        if not self.owned_worker(worker):
            raise QualificationError("provider disappeared")
        host = self.api.request("GET", "workers/subdomain")["subdomain"]
        if not re.fullmatch(r"[a-z0-9-]+", host):
            raise QualificationError("invalid account workers.dev subdomain")
        self.api.request(
            "POST", f"workers/scripts/{worker}/subdomain",
            json={"enabled": True, "previews_enabled": False},
        )
        return f"https://{worker}.{host}.workers.dev/"

    def workers(self):
        runtime = self.upload("runtime", "phase0-runtime.mjs", (), [], lose_response=True)
        try:
            require_absent(self.settings(runtime))
        except QualificationError:
            evidence("occupied-resource", refused=True, uploads=0)
        else:
            raise QualificationError("occupied resource passed create preflight")
        launcher = self.upload(
            "launcher", "phase0-launcher.mjs", (),
            [{"type": "service", "name": "RUNTIME", "service": runtime}],
            ("CloudflareLauncher",),
        )
        self.namespace(launcher, "CloudflareLauncher")
        bearer = secrets.token_urlsafe(32)
        provider = self.upload(
            "provider", "phase0-provider.mjs", ("capacity.mjs", "capacity-core.mjs", "protocol.mjs"),
            [do_binding("LAUNCHER", "CloudflareLauncher", launcher),
             do_binding("REGISTRY", "ScopeRegistry"), secret_binding("TRIAL_BEARER", bearer)],
            ("ScopeRegistry",),
        )
        self.namespace(provider, "ScopeRegistry")
        url = self.public_url(provider)
        with httpx.Client(timeout=15, trust_env=False) as caller:
            def invoke():
                response = caller.get(url)
                if response.status_code == 404:
                    return False
                if response.status_code != 401:
                    raise QualificationError(f"anonymous probe returned HTTP {response.status_code}")
                authenticated = caller.get(url, headers={"Authorization": f"Bearer {bearer}"})
                if authenticated.status_code != 200 or authenticated.text != "phase0-runtime":
                    raise QualificationError(f"authenticated probe returned HTTP {authenticated.status_code}")
                return True
            wait_until(invoke, "provider invocation")
        evidence("workers-invocation", anonymous=401, authenticated=200, sqlite_round_trip=True)

    def cleanup_workers(self):
        failures = []
        for worker in reversed(self.worker_intents):
            try:
                if self.owned_worker(worker) is not None:
                    self.api.request("DELETE", f"workers/scripts/{worker}")
                wait_until(lambda worker=worker: self.settings(worker) is None, "worker deletion")
            except (QualificationError, httpx.TransportError) as error:
                failures.append(type(error).__name__)
        def namespaces_absent():
            rows = self.api.request(
                "GET", "workers/durable_objects/namespaces", params={"per_page": 1000},
            )
            return not any(row.get("script") in self.worker_intents for row in rows)
        wait_until(namespaces_absent, "namespace deletion")
        if failures:
            raise QualificationError(f"Worker teardown incomplete: {failures}")
        evidence("workers-teardown", scripts_absent=True, namespaces_absent=True)


class ContainerTrial:
    def __init__(self, trial, crane):
        self.trial = trial
        self.api = trial.api
        self.name = trial.prefix
        self.crane = crane
        self.image = pinned_registry_image(self.api.account, self.name, FIXTURE_DIGEST)
        self.tag = self.image.split("@")[0] + ":trial"
        self.copy_intent = False
        self.app_intent = None
        self.app_path = None
        self.caller = httpx.Client(timeout=20, trust_env=False)
        self.url = None

    def images(self):
        return self.trial.cf.json(
            "containers", "images", "list", "--filter", f"^(?:[a-f0-9]{{32}}/)?{self.name}$",
            quiet=False,
        )

    def applications(self):
        return self.api.request("GET", "containers/applications", params={"name": self.name})

    def owned_application(self):
        rows = [row for row in self.applications() if row["name"] == self.name]
        if not rows:
            return None
        if len(rows) != 1 or self.app_intent is None:
            raise QualificationError("Container application name conflict")
        row = rows[0]
        if any(row[key] != value for key, value in self.app_intent.items()):
            raise QualificationError("Container application ownership conflict")
        path = application_path(row["id"])
        if self.app_path and self.app_path != path:
            raise QualificationError("Container application ID changed")
        return path

    def prepare_image(self):
        self.trial.check_deadline()
        if self.images():
            raise QualificationError("registry repository already exists")
        self.crane.bind_credentials(self.api)
        self.copy_intent = True
        self.trial.record("image-intent", tag=self.tag, digest=FIXTURE_DIGEST)
        source = f"{ARTIFACTS['image']['repository']}@{FIXTURE_DIGEST}"
        self.crane.copy(source, self.tag, FIXTURE_DIGEST)
        evidence("oci-copy", platform=ARTIFACTS["image"]["platform"], digest=FIXTURE_DIGEST)
        if not self.images():
            raise QualificationError("cf did not list the copied trial image")
        evidence("registry-tags", path="cf-fallback", trial_image_visible=True)

        def prepare():
            response = self.api.request(
                "POST", "containers/image-preparations", json={"image": self.image},
            )
            if response["image"] != self.image or response["status"] == "error":
                raise QualificationError("image preparation failed")
            return response if response["status"] == "ready" else None
        wait_until(prepare, "image preparation", seconds=120)
        evidence("image-preparation", ready=True)

    def instances(self):
        return self.api.request("GET", self.app_path + "/instances-v2")

    def live_instances(self):
        rows = self.instances()
        return [row for row in rows if row["status"]["state"] not in {"inactive", "stopped", "failed"}]

    def invoke(self, method, attempt="first"):
        return self.caller.request(
            method, self.url + attempt, headers={"Authorization": f"Bearer {self.bearer}"},
        )

    def run(self):
        if any(row["name"] == self.name for row in self.applications()):
            raise QualificationError("Container application already exists")
        self.prepare_image()
        self.bearer = secrets.token_urlsafe(32)
        modules = ("capacity.mjs", "capacity-core.mjs", "protocol.mjs")
        bindings = [
            do_binding("REGISTRY", "ScopeRegistry"), do_binding("CONTAINER", "WorkerContainer"),
            secret_binding("TRIAL_BEARER", self.bearer),
        ]
        worker = self.trial.upload(
            "container", "phase0-container.mjs", modules, bindings, ("ScopeRegistry", "WorkerContainer"),
            container_images={"fixture": self.image},
        )
        namespace = self.trial.namespace(worker, "WorkerContainer")
        self.trial.check_deadline()
        if any(row["name"] == self.name for row in self.applications()):
            raise QualificationError("Container application already exists")
        self.app_intent = application_spec(self.name, namespace)
        self.trial.record("application-intent", **self.app_intent)
        try:
            created = self.api.request("POST", "containers/applications", json=self.app_intent)
            self.app_path = application_path(created["id"])
        except httpx.TransportError:
            pass  # Ownership is reconstructed by name and exact namespace below.
        self.app_path = wait_until(self.owned_application, "application creation")
        self.trial.record("application-observed", path=self.app_path)
        application = self.api.request("GET", self.app_path)
        if any(application[key] != value for key, value in self.app_intent.items()):
            raise QualificationError("Container application readback differed")
        evidence("container-application", scheduling_policy="durable_object", exact_namespace=True)
        self.url = self.trial.public_url(worker)
        wait_until(lambda: self.caller.get(self.url + "first").status_code == 401,
                   "Container provider auth")
        self.trial.check_deadline()
        self.trial.record("instance-start-intent", application=self.app_path)
        response = self.invoke("PUT")
        if response.status_code != 202:
            reason = response.text if response.text in ("container-not-bound", "image-not-bound") else ""
            raise QualificationError(f"instance start returned HTTP {response.status_code} {reason}".strip())
        evidence("instance-start-request", status=202)

        def running():
            rows = self.live_instances()
            if len(rows) > 1:
                raise QualificationError("physical instance cap exceeded")
            return rows and rows[0]["status"]["state"] == "running"
        wait_until(running, "running instance", seconds=90)
        evidence("instance-start", running=1)
        refused = self.invoke("PUT", "second")
        if refused.status_code != 409 or refused.json() != {"ok": False} or not running():
            raise QualificationError("second instance was not refused by the ScopeRegistry")
        evidence("instance-cap", cap=1, second_reservation=409, running=1)

    def cleanup(self):
        if self.app_intent is not None:
            existing = self.owned_application()
            if existing:
                self.app_path = existing
                if self.url:
                    try:
                        stopped = self.invoke("DELETE")
                        evidence("instance-stop-request", status=stopped.status_code)
                    except httpx.TransportError:
                        evidence("instance-stop-request", response_unknown=True)
                wait_until(lambda: not self.live_instances(), "physical stop", seconds=150)
                evidence("instance-stop", live=0)
                self.api.request("DELETE", self.app_path)
                wait_until(lambda: self.api.optional(self.app_path) is None
                           and self.owned_application() is None, "application deletion")
                evidence("application-teardown", absent=True)
        self.trial.cleanup_workers()
        if self.copy_intent:
            if self.images():
                if self.crane.run("digest", self.tag) != FIXTURE_DIGEST:
                    raise QualificationError("registry tag ownership conflict")
                self.trial.cf.json(
                    "containers", "images", "delete", self.name + ":trial", "--force", quiet=False,
                )
            wait_until(lambda: not self.images(), "registry deletion")
            evidence("registry-teardown", path="cf-fallback", tags_absent=True)


@contextlib.contextmanager
def trial_receipt(account, prefix, scope):
    fd, name = tempfile.mkstemp(prefix="bifrost-phase0-", suffix=".jsonl")
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        def record(operation, **fields):
            stream.write(json.dumps({"operation": operation, **fields}) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        record("trial", account=account, prefix=prefix, scope=scope)
        try:
            yield record
        except BaseException:
            evidence("retained-trial-receipt", path=name)
            raise
    Path(name).unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--scope", choices=("workers", "containers"), default="workers")
    parser.add_argument("--crane", type=Path, help="path to pinned crane for the Containers scope")
    args = parser.parse_args()
    if not ACCOUNT_ID.fullmatch(args.account):
        parser.error("expected explicit 32-hex account ID")
    containers = args.scope == "containers"
    if args.apply and containers and args.crane is None:
        parser.error("Containers apply requires --crane")
    prefix = "bifrost-p0-" + secrets.token_hex(6)
    evidence("envelope", minutes=30, max_launchers=2, max_containers=1, estimated_usd=0.10)
    with tempfile.TemporaryDirectory(prefix="bifrost-phase0-") as directory:
        cf = Cf(args.account, Path(directory))
        cf.preflight()
        if not args.apply:
            return
        with (trial_receipt(args.account, prefix, args.scope) as record,
              cf.scoped_token(args.account, prefix, containers=containers) as token):
            api = Cloudflare(args.account, token)
            with api.http:
                if api.request("GET")["id"] != args.account:
                    raise QualificationError("scoped token resolved a different account")
                entries = api.request("GET", "entitlements")
                if not active_entitlement(entries, "workers.enabled"):
                    raise QualificationError("Workers entitlement unavailable")
                container_enabled = active_entitlement(entries, "containers.enabled")
                evidence("rest-entitlements", workers=True, containers=container_enabled)
                if containers:
                    if not container_enabled:
                        raise QualificationError("Containers entitlement unavailable")
                    trial = Trial(Cf(args.account, Path(directory), token), api, prefix, record)
                    container_trial = ContainerTrial(trial, RegistryCopy(args.crane, Path(directory)))
                    with container_trial.caller:
                        try:
                            container_trial.run()
                        finally:
                            container_trial.cleanup()
                else:
                    trial = Trial(cf, api, prefix, record)
                    try:
                        trial.workers()
                    finally:
                        trial.cleanup_workers()


if __name__ == "__main__":
    try:
        main()
    except (QualificationError, httpx.TransportError, subprocess.TimeoutExpired) as error:
        # Exception types for transport failures, which can include resource URLs.
        raise SystemExit(str(error) if isinstance(error, QualificationError) else type(error).__name__)
