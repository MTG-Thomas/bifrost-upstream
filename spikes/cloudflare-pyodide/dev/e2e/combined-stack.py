"""Prepare a disposable Compose project for the combined local rehearsal."""

import argparse
import ipaddress
import json
import re
import secrets
import socket
import subprocess
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


def prepare(root, project, image, *, missing_tunnel_config):
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
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc", "-days", "1",
        "-subj", "/CN=Existing HTTPS rehearsal",
        "-addext", "subjectAltName=DNS:existing.example.test,DNS:managed.example.test",
        "-keyout", str(fixture / "edge.key"), "-out", str(fixture / "edge.pem"),
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cfg = {
        "address": address, "subnet": inspection["IPAM"]["Config"][0]["Subnet"],
        "edgePort": edge, "providerPort": provider, "cert": str(fixture / "edge.pem"), "key": str(fixture / "edge.key"),
        "ready": str(fixture / "ready"), "project": project, "image": image, "network": network,
        "missingTunnelConfig": missing_tunnel_config,
    }
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
    (root / "rehearsal-compose.yml").write_text(source)
    stub_env = f"""
      PYTHONPATH: /rehearsal-site:/app
      REHEARSAL_ADDRESS: "{address}"
      REHEARSAL_PROVIDER_PORT: "{provider}"
      REHEARSAL_EDGE_PORT: "{edge}"
"""
    mounts = """
      - ./.rehearsal/edge.pem:/rehearsal/edge.pem:ro
      - ./spikes/cloudflare-pyodide/dev/e2e/combined-site:/rehearsal-site:ro
"""
    overlay = f"""services:
  api:
    pull_policy: never
    environment:{stub_env}    volumes:{mounts}    stop_grace_period: 15s
  worker:
    pull_policy: never
    environment:{stub_env}    volumes:{mounts}    stop_grace_period: 15s
  cloudflare-connector:
    networks: [default, cloudflare-connector]
    volumes:
      - ./.rehearsal/connector.json:/rehearsal/connector.json:ro
      - ./.rehearsal/cloudflared:/usr/local/bin/cloudflared:ro
networks:
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
    parser.add_argument("--missing-tunnel-config", action="store_true", help="Return 404 until tunnel ingress is written")
    args = parser.parse_args()
    root = Path.cwd()
    if not root.name.startswith("combined-local-") or root.stat().st_mode & 0o077:
        parser.error("Use a fresh private combined-local-* archive")
    prepare(root, args.project, args.image, missing_tunnel_config=args.missing_tunnel_config)
