"""Cloudflare credential input stays out of argv and validation output."""

from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from bifrost.client import BifrostClient
from bifrost.commands.capacity import cloudflare_group
from bifrost.contracts.cloudflare_capacity import CloudflareAccountTokenRotation as CliAccountTokenRotation
from click.testing import CliRunner
from pydantic import ValidationError
from shared.models import CloudflareAccountTokenRotation


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


@pytest.mark.parametrize("command", ["reconcile", "upgrade"])
@pytest.mark.parametrize("reset", [False, True])
def test_cli_workers_reconcile_submits_optional_reset(monkeypatch, command: str, reset: bool) -> None:
    installation_id = str(UUID(int=1))
    response = Mock()
    response.json.return_value = {"job_id": str(UUID(int=2))}
    post = AsyncMock(return_value=response)
    monkeypatch.setattr(BifrostClient, "get_instance", lambda **_: Mock(post=post))
    args = [command, "--installation-id", installation_id, "--expected-revision", "1"]
    payload = {"installation_id": installation_id, "expected_revision": 1}
    if reset:
        args.append("--reset-release-fence")
        payload["reset_release_fence"] = True

    result = CliRunner().invoke(cloudflare_group, args)

    assert result.exit_code == 0, result.exception
    post.assert_awaited_once_with(f"/api/capacity/cloudflare/{command}", json=payload)
