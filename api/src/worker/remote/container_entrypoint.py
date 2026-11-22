"""Entrypoint for the signed managed Container image."""

import asyncio
import os

from src.worker.remote.launcher import run_launcher


if __name__ == "__main__":
    if os.environ.get("BIFROST_BAKED_WHEELS_ONLY") != "1":
        raise SystemExit("Managed Container image requires baked wheels")
    raise SystemExit(asyncio.run(run_launcher()))
