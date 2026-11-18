# Workers for Platforms probe: a Python user Worker carrying the same runtime as
# the engine child, deployed as a *script* (normal or into a dispatch namespace)
# instead of loaded dynamically. It answers two questions empirically:
#
#   1. Does this script cold-start from a deploy-time memory snapshot?
#      Top-level code below runs exactly once per isolate. If Cloudflare took a
#      snapshot at deploy, it ran during snapshot creation, where the clock reads
#      the Unix epoch (measured: _MODULE_WALL_MS == 462). If not, it runs inside
#      the first request and reads real wall time.
#   2. How much CPU may one invocation use? `?action=burn&n=N` runs a pure
#      Python loop; the driver times it from outside (Workers clocks do not
#      advance during CPU work).
import hmac
import time
from urllib.parse import parse_qs, urlparse

from workers import Response, WorkerEntrypoint

import runtime_runner  # noqa: F401  the engine's full import closure

_MODULE_WALL_MS = int(time.time() * 1000)
_SERVED = 0


def _burn(n):
    x = 0
    for i in range(n):
        x += i
    return x


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        global _SERVED
        expires = int(getattr(self.env, "LAB_EXPIRES_AT", "0") or "0")
        secret = getattr(self.env, "LAB_AUTH", None) or ""
        presented = request.headers.get("x-lab-auth") or ""
        # compare_digest: constant time, so response timing does not leak the secret.
        if not secret or not hmac.compare_digest(presented.encode(), secret.encode()) or time.time() * 1000 > expires:
            return Response("forbidden", status=403)
        _SERVED += 1
        query = parse_qs(urlparse(request.url).query)
        action = query.get("action", ["ping"])[0]
        request_wall_ms = int(time.time() * 1000)
        body = {
            "module_wall_ms": _MODULE_WALL_MS,
            "request_wall_ms": request_wall_ms,
            "module_age_ms": request_wall_ms - _MODULE_WALL_MS,
            "served": _SERVED,
            "engine_imported": hasattr(runtime_runner, "run_authorized_attempt"),
        }
        if action == "burn":
            n = int(query.get("n", ["0"])[0])
            body["burn_n"] = n
            body["burn_checksum"] = _burn(n)
        return Response.json(body)
