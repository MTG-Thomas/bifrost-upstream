"""Cloudflare credential input stays out of argv and validation output."""

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
