"""Archived spike settings are explicitly refused in product startup."""

import pytest
from pydantic import ValidationError
from src.config import Settings


def test_empty_retired_containers_settings_do_not_block_startup() -> None:
    Settings(cloudflare_containers_url="")
    with pytest.raises(ValidationError, match="require the Phase 6 spike"):
        Settings(cloudflare_containers_url="https://containers.example.test")
