"""Cloudflare control-plane primitives for the disposable Phase 0 trial."""

import base64
import hashlib
import json
import os
import re
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path

import httpx

ACCOUNT_ID = re.compile(r"[0-9a-f]{32}")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
REGISTRY = "registry.cloudflare.com"
SUBPROCESS_ENV = {
    "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "TMPDIR",
    "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "XDG_CONFIG_HOME",
}


def subprocess_environment():
    return {key: value for key, value in os.environ.items() if key.upper() in SUBPROCESS_ENV}


class QualificationError(RuntimeError):
    pass


class ApiError(QualificationError):
    def __init__(self, method, status, codes, path=""):
        self.status = status
        operation = re.sub(r"bifrost-p0-[0-9a-f]{12}[a-z-]*|[0-9a-f-]{32,}", "<trial>", path)
        super().__init__(f"Cloudflare {method} /{operation} returned HTTP {status}, codes {codes}")


class Cloudflare:
    def __init__(self, account, token):
        if not ACCOUNT_ID.fullmatch(account):
            raise QualificationError("invalid account ID")
        self.account = account
        self.upload_attempts = 0
        self.http = httpx.Client(
            base_url=f"https://api.cloudflare.com/client/v4/accounts/{account}/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
            trust_env=False,
        )

    def envelope(self, method, path="", **kwargs):
        # Envelope only. Error bodies may echo bindings or credentials.
        response = self.http.request(method, path, **kwargs)
        if response.status_code == 204:
            return None
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not response.is_success or not payload.get("success"):
            codes = [item["code"] for item in payload.get("errors", []) if isinstance(item.get("code"), int)]
            raise ApiError(method, response.status_code, codes, path)
        return payload

    def request(self, method, path="", **kwargs):
        payload = self.envelope(method, path, **kwargs)
        return None if payload is None else payload["result"]

    def inventory(self, path, *, cursor=False, **filters):
        rows = []
        seen_ids = set()
        total = None
        params = {**filters, "per_page": 100}
        if not cursor:
            params["page"] = 1
        for _ in range(100):
            payload = self.envelope("GET", path, params=dict(params))
            info = payload.get("result_info") or {}
            batch = payload["result"]
            ids = {row["id"] for row in batch}
            if len(ids) != len(batch) or seen_ids & ids or info.get("count", len(batch)) != len(batch):
                raise QualificationError("inventory page count or identity mismatch")
            seen_ids.update(ids)
            rows.extend(batch)
            reported_total = info.get("total_count")
            if reported_total is not None:
                if total is not None and total != reported_total:
                    raise QualificationError("inventory total changed during pagination")
                total = reported_total
            if cursor:
                next_token = info.get("next_page_token")
                if next_token:
                    if not batch or next_token == params.get("page_token"):
                        raise QualificationError("inventory cursor did not advance")
                    params["page_token"] = next_token
                    continue
                if not info and len(batch) >= params["per_page"]:
                    raise QualificationError("full cursor page lacks pagination metadata")
            else:
                page = params["page"]
                if info.get("page", page) != page:
                    raise QualificationError("inventory page did not advance")
                pages = info.get("total_pages")
                if ((pages is not None and page < pages)
                        or (total is not None and len(rows) < total)
                        or (pages is None and total is None and len(batch) >= info.get("per_page", 100))):
                    if not batch:
                        raise QualificationError("inventory ended before its reported total")
                    params["page"] += 1
                    continue
            if total is not None and len(rows) != total:
                raise QualificationError("inventory total did not reconcile")
            return rows
        raise QualificationError("inventory exceeded the pagination bound")

    def optional(self, path):
        try:
            return self.request("GET", path)
        except ApiError as error:
            if error.status != 404:
                raise
            return None

    def upload(self, name, parts, **metadata):
        files = worker_parts(parts, metadata)
        self.upload_attempts += 1
        return self.request(
            "PUT", f"workers/scripts/{name}", files=files
        )


def worker_parts(parts, metadata):
    if metadata["main_module"] not in parts:
        raise QualificationError("entry module is absent from release parts")
    return {
        "metadata": ("metadata.json", json.dumps(metadata), "application/json"),
        **{name: (Path(name).name, body, "application/javascript+module")
           for name, body in parts.items()},
    }


def file_digest(body):
    return "sha256:" + hashlib.sha256(body).hexdigest()


def pinned_registry_image(account, name, digest):
    if not ACCOUNT_ID.fullmatch(account) or not DIGEST.fullmatch(digest):
        raise QualificationError("invalid registry account or content digest")
    if not re.fullmatch(r"bifrost-p0-[0-9a-f]{12}", name):
        raise QualificationError("invalid trial image name")
    return f"{REGISTRY}/{account}/{name}@{digest}"


def worker_owner(settings):
    return next(
        (binding.get("text") for binding in settings["bindings"]
         if binding["name"] == "PHASE0_OWNER" and binding["type"] == "plain_text"),
        None,
    )


def observe_owned(settings, owner):
    if settings is not None and worker_owner(settings) != owner:
        raise QualificationError("ownership conflict")
    return settings


class ResourceExists(QualificationError):
    pass


def require_absent(settings):
    if settings is not None:
        raise ResourceExists("resource already exists")


def application_spec(name, namespace):
    if not ACCOUNT_ID.fullmatch(namespace):
        raise QualificationError("invalid durable object namespace ID")
    return {
        "name": name,
        "scheduling_policy": "durable_object",
        "durable_objects": {"namespace_id": namespace},
    }


def application_path(application_id):
    if not re.fullmatch(r"[0-9a-f]{32}|[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", application_id):
        raise QualificationError("invalid Container application ID")
    # These identifiers are opaque. Cloudflare matches their original spelling.
    return f"containers/applications/{application_id}"


def matches_application(row, spec):
    return (
        row.get("name") == spec["name"]
        and row.get("scheduling_policy") == spec["scheduling_policy"]
        and (row.get("durable_objects") or {}).get("namespace_id") == spec["durable_objects"]["namespace_id"]
    )


def active_entitlement(entries, key):
    return any(
        row["id"] == key and not row["deleted_date"]
        and row["allocation"] == {"type": "bool", "value": True}
        for row in entries
    )


class RegistryCopy:
    """Copy a digest-pinned, single-platform fixture with pinned crane."""

    def __init__(self, executable, directory):
        self.executable = executable.resolve(strict=True)
        self.directory = directory
        self.env = {**subprocess_environment(), "DOCKER_CONFIG": str(directory / "anonymous-config")}
        if self.run("version") != "v0.20.6":
            raise QualificationError("crane v0.20.6 is required")

    def run(self, *args, timeout=20):
        result = subprocess.run(
            [str(self.executable), *args], env=self.env, capture_output=True,
            encoding="utf-8", errors="replace", check=False, timeout=timeout,
        )
        if result.returncode:
            raise QualificationError(f"crane {args[0]} failed")
        return result.stdout.strip()

    @contextmanager
    def credentials(self, api, *, push=False):
        credentials = api.request(
            "POST", f"containers/registries/{REGISTRY}/credentials",
            json={"expiration_minutes": 15, "permissions": ["pull", "push"] if push else ["pull"]},
        )
        if credentials["account_id"] != api.account or credentials["registry_host"] != REGISTRY:
            raise QualificationError("registry credentials resolved a different account or host")
        encoded = base64.b64encode(
            f"{credentials['username']}:{credentials['password']}".encode()
        ).decode()
        with tempfile.TemporaryDirectory(prefix="registry-", dir=self.directory) as config:
            token_file = Path(config) / "config.json"
            with os.fdopen(os.open(token_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as stream:
                json.dump({"auths": {REGISTRY: {"auth": encoded}}}, stream)
            previous = self.env["DOCKER_CONFIG"]
            self.env["DOCKER_CONFIG"] = config
            try:
                yield
            finally:
                self.env["DOCKER_CONFIG"] = previous

    def copy(self, source, tag, digest):
        if self.run("digest", source) != digest:
            raise QualificationError("source digest differs from pin")
        self.run("copy", source, tag, timeout=90)
        if self.run("digest", tag) != digest:
            raise QualificationError("destination digest differs from pin")
