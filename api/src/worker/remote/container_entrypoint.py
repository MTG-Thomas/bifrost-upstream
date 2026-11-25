"""Entrypoint for the signed managed Container image."""

import asyncio
import logging
import os

from src.worker.remote.launcher import run_launcher


def main() -> int:
    if os.environ.get("BIFROST_BAKED_WHEELS_ONLY") != "1":
        logging.error("Managed Container image requires baked wheels")
        return 1
    try:
        return asyncio.run(run_launcher())
    except Exception:
        # The launcher sends signed evidence while it still has a session.
        # Failures before hello or after transport loss cannot be reported there.
        logging.error("Managed Container launcher exited with a startup or shutdown error")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
