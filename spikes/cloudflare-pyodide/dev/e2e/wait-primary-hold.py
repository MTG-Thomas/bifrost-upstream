"""Wait for the disposable Compose primary's workflow hold reservations."""

import argparse
import json
import subprocess
import time

WAIT_SECONDS = 10
MIB = 1024 * 1024
FREE_MIB = 24
SQL = """
SELECT coalesce(json_agg(memory_budget_bytes - memory_reserved_bytes
                         ORDER BY memory_budget_bytes - memory_reserved_bytes), '[]')
FROM execution_workers
WHERE form = 'static' AND state = 'ready' AND execution_lane = 'normal'
  AND supported_handlers @> '["workflow.execute"]'::jsonb
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="bifrost")
    parser.add_argument("workers", type=int, help="Expected number of ready primary workflow workers")
    parser.add_argument("compose_options", nargs=argparse.REMAINDER, help="Docker Compose project and file options")
    args = parser.parse_args()
    if args.workers < 1 or not args.compose_options:
        parser.error("supply a positive worker count and explicit Compose project options")

    command = [
        "docker", "compose", *args.compose_options, "exec", "-T", "postgres",
        "psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-U", "bifrost", "-d", args.database,
    ]
    deadline = time.monotonic() + WAIT_SECONDS
    last = "no completed reservation read"
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            result = subprocess.run(command, input=SQL, text=True, capture_output=True, check=True, timeout=remaining)
        except subprocess.TimeoutExpired:
            break
        except subprocess.CalledProcessError as error:
            raise SystemExit(f"Primary hold gate could not read worker reservations (psql exit {error.returncode})") from None
        last = [value // MIB if value is not None else None for value in json.loads(result.stdout)]
        if last == [FREE_MIB] * args.workers:
            print(f"Primary hold verified: {args.workers} ready worker(s) with {FREE_MIB} MiB free each")
            return
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))

    raise SystemExit(
        f"Primary hold was not observable within {WAIT_SECONDS}s: "
        f"expected {args.workers} ready worker(s) with {FREE_MIB} MiB free each; "
        f"last observed free MiB: {last}"
    )


if __name__ == "__main__":
    main()
