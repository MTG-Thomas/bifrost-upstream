"""Timing shared by the pool controllers and cadence-priced usage estimates."""

import asyncio
import time

POOL_RECONCILE_SPACING_SECONDS = 2.0
POOL_RECONCILE_SWEEP_SECONDS = 5.0


async def wait_for_next_pass(
    stop: asyncio.Event, wake: asyncio.Event, *, since: float, spacing: float, sweep: float,
) -> None:
    """Wait for a wake within the configured spacing or run the next sweep."""
    stopped = asyncio.ensure_future(stop.wait())
    woken = asyncio.ensure_future(wake.wait())
    try:
        remaining = sweep - (time.monotonic() - since)
        await asyncio.wait({stopped, woken}, timeout=max(0.0, remaining), return_when=asyncio.FIRST_COMPLETED)
        if wake.is_set() and not stop.is_set():
            delay = spacing - (time.monotonic() - since)
            if delay > 0:
                await asyncio.wait({stopped}, timeout=delay)
    finally:
        stopped.cancel()
        woken.cancel()
        wake.clear()
