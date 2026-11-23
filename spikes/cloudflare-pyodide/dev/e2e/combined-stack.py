"""Prepare a disposable Compose project for the combined local rehearsal."""

import argparse
import ipaddress
import json
import re
import secrets
import socket
import subprocess
from hashlib import sha256
from pathlib import Path


def ports(count):
    bound = [socket.socket() for _ in range(count)]
    try:
        for listener in bound:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in bound]
    finally:
        for listener in bound:
            listener.close()


def container_fixture(image):
    linux, = json.loads(subprocess.check_output(["docker", "image", "inspect", image]))
    proxy_ref = (
        "cloudflare/proxy-everything@sha256:"
        "0ef6716c52430096900b150d84a3302057d6cd2319dae7987128c85d0733e3c8"
    )
    proxy, = json.loads(subprocess.check_output(["docker", "image", "inspect", proxy_ref]))
    runtime = linux["Config"]
    if (
        (linux["Os"], linux["Architecture"], runtime["User"]) != ("linux", "amd64", "0:0")
        or runtime["Entrypoint"] != ["/usr/local/bin/python", "-m", "src.worker.remote.container_entrypoint"]
        or "BIFROST_BAKED_WHEELS_ONLY=1" not in runtime["Env"]
        or re.fullmatch("[a-f0-9]{40}", (runtime["Labels"] or {}).get("org.opencontainers.image.revision", "")) is None
    ):
        raise ValueError("Use the matched local Dockerfile.containers release image")
    # Docker's local image ID fixes the runnable image. The synthetic
    # OCI document is the digest exposed by the local registry boundary.
    oci_config = json.dumps({
        "os": linux["Os"], "architecture": linux["Architecture"], "config": runtime,
    }, sort_keys=True, separators=(",", ":"))
    oci_manifest = json.dumps({
        "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"digest": "sha256:" + sha256(oci_config.encode()).hexdigest()},
        "layers": [{"digest": layer} for layer in linux["RootFS"]["Layers"]],
    }, sort_keys=True, separators=(",", ":"))
    return {
        "image": image, "imageId": linux["Id"], "proxyId": proxy["Id"],
        "digest": "sha256:" + sha256(oci_manifest.encode()).hexdigest(),
        "config": oci_config, "manifest": oci_manifest,
    }


def prepare(root, project, image, *, initial_tunnel_config, lag_worker_namespace, containers_image):
    containers = container_fixture(containers_image) if containers_image is not None else None
    fixture = root / ".rehearsal"
    fixture.mkdir(mode=0o755)
    site = root / "spikes/cloudflare-pyodide/dev/e2e/combined-site"
    site.chmod(0o755)
    (site / "sitecustomize.py").chmod(0o644)
    network = project + "-isolated"
    subprocess.run(["docker", "network", "create", "--internal", network], check=True)
    inspection, = json.loads(subprocess.check_output(["docker", "network", "inspect", network]))
    address = inspection["IPAM"]["Config"][0]["Gateway"]
    if not inspection["Internal"] or not ipaddress.IPv4Address(address).is_private:
        raise SystemExit("Expected a private Docker bridge address")
    edge, provider = ports(2)
    public_url = "http://localhost:8000"
    managed_host = f"bifrost-workers-{sha256(public_url.encode()).hexdigest()[:16]}.example.test"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-days", "1",
        "-subj", "/CN=Existing HTTPS rehearsal",
        "-addext", f"subjectAltName=DNS:existing.example.test,DNS:{managed_host}",
        "-keyout", str(fixture / "edge.key"), "-out", str(fixture / "edge.pem"),
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cfg = {
        "address": address, "subnet": inspection["IPAM"]["Config"][0]["Subnet"],
        "edgePort": edge, "providerPort": provider, "cert": str(fixture / "edge.pem"), "key": str(fixture / "edge.key"),
        "ready": str(fixture / "ready"), "project": project, "image": image, "network": network,
        "initialTunnelConfig": initial_tunnel_config,
        "workerNamespaceLagMs": 10_000 if lag_worker_namespace else 0,
    }
    if containers is not None:
        proxy_alias = project + "-containers-proxy:local"
        subprocess.run(["docker", "tag", containers["proxyId"], proxy_alias], check=True)
        cfg["containers"] = {**containers, "proxy": proxy_alias}
    (fixture / "config.json").write_text(json.dumps(cfg))
    (fixture / "connector.json").write_text(json.dumps({"base": f"http://{address}:{provider}/local/tunnel-"}))
    (fixture / "cloudflared").write_text("""#!/usr/local/bin/python
import json
import signal
import sys
from pathlib import Path
from urllib.request import Request, urlopen

path = Path(sys.argv[sys.argv.index("--token-file") + 1])
config = json.loads(Path("/rehearsal/connector.json").read_text())

def call(action, body):
    request = Request(config["base"] + action, data=json.dumps(body).encode(),
                      headers={"Authorization": "Bearer local-rehearsal-management-token",
                               "Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=5) as response:
        return json.load(response)["result"]

registration = call("connect", {"token": path.read_text()})

def stop(_signum, _frame):
    # Withdrawal disconnects the active session. Rotation deliberately keeps
    # the old session in provider inventory until Bifrost evicts it.
    if not path.exists() or not path.read_bytes():
        call("detach", {"id": registration["id"]})
    sys.exit(0)

signal.signal(signal.SIGTERM, stop)
signal.pause()
""")
    (fixture / "cloudflared").chmod(0o755)
    for filename in ("config.json", "connector.json", "edge.pem"):
        (fixture / filename).chmod(0o644)
    source = (root / "docker-compose.yml").read_text()
    source = re.sub(r"^    container_name:.*\n", "", source, flags=re.M)
    source = re.sub(r"^    ports:\n(?:      - .*\n)+", "", source, flags=re.M)
    source = source.replace("ghcr.io/gobifrost/bifrost-api:${BIFROST_VERSION:-latest}", image + ":release-${TRIAL_RELEASE:-1}")
    source = source.replace("BIFROST_API_URL: http://api:8000",
                            "BIFROST_API_URL: ${TRIAL_CONTROL_API_URL:-http://api:8000}")
    (root / "rehearsal-compose.yml").write_text(source)
    stub_env = f"""
      PYTHONPATH: /rehearsal-site:/app
      BIFROST_PUBLIC_URL: "{public_url}"
      REHEARSAL_MANAGED_HOST: "{managed_host}"
      REHEARSAL_ADDRESS: "{address}"
      REHEARSAL_PROVIDER_PORT: "{provider}"
      REHEARSAL_EDGE_PORT: "{edge}"
"""
    mounts = """
      - ./.rehearsal/edge.pem:/rehearsal/edge.pem:ro
      - ./spikes/cloudflare-pyodide/dev/e2e/combined-site:/rehearsal-site:ro
"""
    api_dependencies = probe_peer = ""
    if containers_image is not None:
        stub_env += '      REHEARSAL_CONTAINERS: "1"\n'
        mounts += "      - ./.rehearsal/config.json:/rehearsal/containers.json:ro\n"
        mounts += ("      - ./deploy/capacity/cloudflare:/rehearsal-container-bundle:ro\n")
        # A sibling on the internal API network is reachable from Bifrost.
        # Miniflare's Docker proxy can reach the host gateway directly, so a
        # host listener cannot stand in for an isolated alternate peer.
        api_dependencies = """    depends_on:
      container-probe-peer:
        condition: service_healthy
"""
        probe_peer = """  container-probe-peer:
    image: node:26.13.1-bookworm-slim
    pull_policy: never
    command: [node, -e, "require('node:net').createServer(socket => socket.end()).listen(8443, '0.0.0.0')"]
    healthcheck:
      test: [CMD, node, -e, "require('node:net').connect(8443, '127.0.0.1').on('connect', function () { this.end() })"]
      interval: 1s
"""
    overlay = f"""services:
  api:
    pull_policy: never
    environment:{stub_env}    volumes:{mounts}    stop_grace_period: 15s
{api_dependencies}  worker:
    pull_policy: never
    environment:{stub_env}    volumes:{mounts}    stop_grace_period: 15s
  cloudflare-connector:
    networks: [default, cloudflare-connector]
    volumes:
      - ./.rehearsal/connector.json:/rehearsal/connector.json:ro
      - ./.rehearsal/cloudflared:/usr/local/bin/cloudflared:ro
{probe_peer}networks:
  default:
    external: true
    name: {network}
  cloudflare-connector:
    internal: true
"""
    (root / "rehearsal-overlay.yml").write_text(overlay)
    env = {
        "POSTGRES_PASSWORD": secrets.token_hex(24), "SEAWEEDFS_SECRET_KEY": secrets.token_hex(24),
        "BIFROST_SECRET_KEY": secrets.token_hex(32), "BIFROST_DEFAULT_USER_EMAIL": "rehearsal@example.test",
        "BIFROST_DEFAULT_USER_PASSWORD": secrets.token_hex(32), "BIFROST_MFA_ENABLED": "false",
        "BIFROST_USAGE_REPORTING_ENABLED": "false", "WORKER_REPLICAS": "1", "API_REPLICAS": "1",
    }
    path = root / ".env"
    with path.open("x") as file:
        path.chmod(0o600)
        file.writelines(f"{key}={value}\n" for key, value in env.items())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project")
    parser.add_argument("image")
    parser.add_argument("--containers-image", help="Use a locally built, matched Dockerfile.containers image")
    parser.add_argument("--lag-worker-namespace", action="store_true")
    variants = parser.add_mutually_exclusive_group()
    variants.add_argument("--missing-tunnel-config", dest="initial_tunnel_config", action="store_const", const="http_404",
                          help="Return HTTP 404 until tunnel ingress is written")
    variants.add_argument("--default-tunnel-config", dest="initial_tunnel_config", action="store_const", const="default_404",
                          help="Return a single 404 catch-all before tunnel ingress is written")
    parser.set_defaults(initial_tunnel_config="version_zero")
    args = parser.parse_args()
    root = Path.cwd()
    if not root.name.startswith("combined-local-") or root.stat().st_mode & 0o077:
        parser.error("Use a fresh private combined-local-* archive")
    prepare(root, args.project, args.image, initial_tunnel_config=args.initial_tunnel_config,
            lag_worker_namespace=args.lag_worker_namespace, containers_image=args.containers_image)
