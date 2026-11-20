"""Run the pool controller's wake cadence against a local workerd provider."""

import asyncio
import json
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "api"))
from src.services.pool_cadence import (
    POOL_RECONCILE_SPACING_SECONDS,
    POOL_RECONCILE_SWEEP_SECONDS,
    wait_for_next_pass,
)


def assert_running(url: str, headers: dict[str, str]) -> None:
    with urlopen(Request(url, headers=headers), timeout=3) as response:
        assert json.load(response)["state"] == "running"


async def observe(url: str, count: int) -> None:
    if not url.startswith("http://127.0.0.1:"):
        raise ValueError("Cadence qualification requires a loopback provider")
    # Input is the workerd fixture's synthetic authorization, never a hosted
    # credential. Keep it out of command arguments and process output.
    headers = json.load(sys.stdin)
    stop, wake = asyncio.Event(), asyncio.Event()
    started = time.monotonic()
    for _ in range(count):
        since = time.monotonic()
        wake.set()
        await wait_for_next_pass(
            stop, wake, since=since, spacing=POOL_RECONCILE_SPACING_SECONDS, sweep=POOL_RECONCILE_SWEEP_SECONDS,
        )
        await asyncio.to_thread(assert_running, url, headers)
    print(json.dumps({
        "polls": count,
        "spacingSeconds": POOL_RECONCILE_SPACING_SECONDS,
        "elapsedSeconds": time.monotonic() - started,
    }))


if __name__ == "__main__":
    asyncio.run(observe(sys.argv[1], int(sys.argv[2])))
