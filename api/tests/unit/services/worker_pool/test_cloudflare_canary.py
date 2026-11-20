"""Canary credentials can be used only by one signed session and job lease."""

from uuid import UUID

import pytest
from bifrost.client import BifrostClient
from bifrost.commands.capacity import cloudflare_group
from bifrost.contracts.cloudflare_capacity import (
    CloudflareAccountTokenRotation as CliAccountTokenRotation,
)
from click.testing import CliRunner
from pydantic import ValidationError
from shared.models import CloudflareAccountTokenRotation
from src.config import Settings
from src.services.worker_pool.cloudflare_canary import _proof


def test_capacity_canary_is_bound_to_worker_lease_and_generation() -> None:
    worker = UUID(int=1)
    lease = UUID(int=2)
    key = "ab" * 32
    challenge = _proof(key, worker, lease, 1)
    assert len(challenge) == 64
    assert challenge != _proof(key, UUID(int=3), lease, 1)
    assert challenge != _proof(key, worker, UUID(int=3), 1)
    assert challenge != _proof(key, worker, lease, 2)


@pytest.mark.parametrize("model", [CliAccountTokenRotation, CloudflareAccountTokenRotation])
def test_rejected_rotation_input_does_not_appear_in_rendered_validation_error(model) -> None:
    candidate = "sensitive-token"
    with pytest.raises(ValidationError) as rejected:
        model.model_validate({
            "installation_id": UUID(int=1), "expected_revision": 1, "api_token": candidate,
        })
    assert candidate not in str(rejected.value)


def test_cli_rotation_keeps_rejected_prompt_out_of_output(monkeypatch) -> None:
    candidate = "sensitive-token"
    monkeypatch.setattr(BifrostClient, "get_instance", lambda **_: object())
    result = CliRunner().invoke(
        cloudflare_group, ["rotate", "--installation-id", str(UUID(int=1)), "--expected-revision", "1"],
        input=candidate + "\n",
    )
    assert result.exit_code == 1
    assert "Enter a valid Cloudflare API token" in result.output
    assert candidate not in result.output
    assert result.exc_info is not None and candidate not in str(result.exc_info[1])


def test_empty_retired_containers_settings_do_not_block_startup() -> None:
    Settings(cloudflare_containers_url="")
    with pytest.raises(ValidationError, match="require the Phase 6 spike"):
        Settings(cloudflare_containers_url="https://containers.example.test")
