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


def test_activate_reviews_the_resolved_route_before_requesting_consent(monkeypatch) -> None:
    token = "synthetic-management-token"
    plan = Mock()
    plan.json.return_value = {
        "accounts": [{"id": "a" * 32, "name": "Fixture Account"}], "account_id": "a" * 32,
        "zone_id": "b" * 32, "zone_name": "example.test", "reachability_mode": "managed_tunnel",
        "worker_ingress_origin": "https://workers.example.test", "preview": {"fleet_envelope_usd": "32.13"},
        "preflight_receipt": "opaque-receipt", "consent_version": "v1", "consent_text": "Review this consent.",
    }
    queued = Mock()
    queued.json.return_value = {"job_id": str(UUID(int=2))}
    post = AsyncMock(side_effect=[plan, queued])
    monkeypatch.setattr(BifrostClient, "get_instance", lambda **_: Mock(post=post))
    result = CliRunner().invoke(cloudflare_group, ["activate", "--monthly-limit-usd", "75"], input=f"{token}\ny\n")
    assert result.exit_code == 0, result.exception
    summary, _, _ = result.output.partition("Accept the reviewed Cloudflare route and scale out")
    for value in ("Fixture Account", "example.test", "Managed Tunnel", "https://workers.example.test", "75.00 USD"):
        assert value in summary
    assert token not in result.output
    assert "opaque-receipt" not in result.output
    assert post.await_args_list[0].args == ("/api/capacity/cloudflare/preflight",)
    assert post.await_args_list[1].args == ("/api/capacity/cloudflare/activate",)
    assert post.await_args_list[1].kwargs["json"]["api_token"] == token


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


@pytest.mark.parametrize(("target_flag", "target_key", "target"), [
    ("--installation-id", "installation_id", str(UUID(int=1))),
    ("--account-id", "account_id", "a" * 32),
])
def test_cli_can_stage_rotation_by_installation_and_preserves_account_id_compatibility(
    monkeypatch, target_flag, target_key, target,
) -> None:
    token = "synthetic-rotation-token"
    response = Mock()
    response.json.return_value = {"credential_ref": str(UUID(int=2))}
    post = AsyncMock(return_value=response)
    monkeypatch.setattr(BifrostClient, "get_instance", lambda **_: Mock(post=post))
    result = CliRunner().invoke(
        cloudflare_group, ["stage-credential", "--purpose", "rotate", target_flag, target], input=token + "\n",
    )
    assert result.exit_code == 0, result.exception
    assert token not in result.output
    post.assert_awaited_once_with("/api/capacity/cloudflare/credential", json={
        target_key: target, "purpose": "rotate", "api_token": token,
    })
