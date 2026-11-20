"""Canary credentials can be used only by one signed session and job lease."""

from uuid import UUID

import pytest
from bifrost.contracts.cloudflare_capacity import (
    CloudflareAccountTokenRotation as CliAccountTokenRotation,
)
from pydantic import ValidationError
from shared.models import CloudflareAccountTokenRotation
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
