import asyncio
from dataclasses import replace
from hashlib import sha256
from uuid import uuid4

import pytest
from src.worker.remote.platform_stage_protocol import (
    MAX_STAGE_INPUT_BYTES,
    StageBinding,
    encode_bundle,
)
from src.worker.remote.platform_stages import PlatformStages, StageProgram


def binding_for(bundle):
    return StageBinding(
        str(uuid4()), str(uuid4()), str(uuid4()), "test.compute.v1",
        sha256(encode_bundle(bundle, MAX_STAGE_INPUT_BYTES)).hexdigest(),
    )


@pytest.mark.asyncio
async def test_stage_replay_is_bound_and_release_keeps_only_the_tombstone(monkeypatch):
    bundle = {"content": "immutable"}
    binding = binding_for(bundle)
    ledger = PlatformStages({binding.capability: StageProgram("unused.in.test", 1)}, asyncio.sleep)
    runs = []

    async def compute(stage, program, encoded):
        runs.append(encoded)
        stage.state = "succeeded"
        stage.result = {"value": 12}

    monkeypatch.setattr(ledger, "_run", compute)
    assert ledger.status(binding)["state"] == "unknown"
    ledger.start(binding, bundle)
    ledger.start(binding, bundle)
    await asyncio.sleep(0)
    assert runs == [b'{"content":"immutable"}']
    assert ledger.status(binding)["result"] == {"value": 12}
    assert ledger.inventory() == [binding.wire()]
    with pytest.raises(ValueError, match="binding changed"):
        ledger.status(replace(binding, lease_token=str(uuid4())))
    assert (await ledger.release(binding))["state"] == "released"
    assert ledger.inventory() == []
    assert ledger.start(binding, bundle)["state"] == "released"
    assert (await ledger.release(binding))["state"] == "released"


def test_stage_rejects_unbounded_or_mutated_input():
    binding = binding_for({"content": "original"})
    with pytest.raises(ValueError, match="digest mismatch"):
        binding.verify({"content": "changed"})
    with pytest.raises(ValueError, match="size limit"):
        binding.verify({"content": "x" * MAX_STAGE_INPUT_BYTES})
