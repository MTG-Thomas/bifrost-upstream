"""Cloudflare control-plane primitives for the disposable Phase 0 trial."""

import base64
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

import httpx

ACCOUNT_ID = re.compile(r"[0-9a-f]{32}")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
REGISTRY = "registry.cloudflare.com"


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
        self.http = httpx.Client(
            base_url=f"https://api.cloudflare.com/client/v4/accounts/{account}/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
            trust_env=False,
        )

    def request(self, method, path="", **kwargs):
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
        return payload["result"]

    def optional(self, path):
        try:
            return self.request("GET", path)
        except ApiError as error:
            if error.status != 404:
                raise
            return None

    def upload(self, name, parts, **metadata):
        return self.request(
            "PUT", f"workers/scripts/{name}", files=worker_parts(parts, metadata)
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


def require_absent(settings):
    if settings is not None:
        raise QualificationError("resource already exists")


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
        self.config = directory / "docker-config"
        self.env = {**os.environ, "DOCKER_CONFIG": str(self.config)}
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

    def bind_credentials(self, api):
        credentials = api.request(
            "POST", f"containers/registries/{REGISTRY}/credentials",
            json={"expiration_minutes": 15, "permissions": ["pull", "push"]},
        )
        if credentials["account_id"] != api.account or credentials["registry_host"] != REGISTRY:
            raise QualificationError("registry credentials resolved a different account or host")
        encoded = base64.b64encode(
            f"{credentials['username']}:{credentials['password']}".encode()
        ).decode()
        self.config.mkdir(mode=0o700)
        token_file = self.config / "config.json"
        with os.fdopen(os.open(token_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as stream:
            json.dump({"auths": {REGISTRY: {"auth": encoded}}}, stream)

    def copy(self, source, tag, digest):
        if self.run("digest", source) != digest:
            raise QualificationError("source digest differs from pin")
        self.run("copy", source, tag, timeout=90)
        if self.run("digest", tag) != digest:
            raise QualificationError("destination digest differs from pin")
