#!/bin/sh
# Run Enable's public ingress checks in the sole Compose worker replica.
set -eu

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 HTTPS_ORIGIN [docker compose options]" >&2
    exit 2
fi

origin=$1
shift
workers=$(docker compose "$@" ps --status running --quiet worker)
newline='
'
case "$workers" in
    ""|*"$newline"*)
        echo "The qualification project must have exactly one running Compose worker replica." >&2
        exit 2
        ;;
esac

exec docker compose "$@" exec -T -e "WORKER_INGRESS_ORIGIN=$origin" worker python - <<'PY'
import asyncio
import json
import os
import sys

from src.services.worker_ingress_probes import probe_public_worker_ingress

origin = os.environ["WORKER_INGRESS_ORIGIN"].rstrip("/")
try:
    result = asyncio.run(probe_public_worker_ingress(origin))
except ValueError:
    print("Supply a dedicated HTTPS origin without a port or path.", file=sys.stderr)
    raise SystemExit(2) from None

print(json.dumps({
    "preflight_execution_class": "remote_launcher",
    "reachable": result.reachable,
    "route_checks": len(result.routes),
    "http_connections": result.http_connections_as_result(),
    "failure": None if result.reachable else result.failure.as_result(),
}))
raise SystemExit(0 if result.reachable else 1)
PY
