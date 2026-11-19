import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace
from uuid import uuid4

import pytest
from src.jobs.schedulers.platform_jobs import ClaimedPlatformJob
from src.worker.remote import platform_stage_host
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
    ledger = PlatformStages({binding.capability: StageProgram("unused.in.test", 1)})
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


async def test_lost_start_ack_is_observed_before_another_command(monkeypatch):
    claim = ClaimedPlatformJob(uuid4(), uuid4(), 60, 0.9, 1024, "example.job")
    job = SimpleNamespace(attempt=1, remote_stage=None)
    worker_id = uuid4()
    bridge = SimpleNamespace(lease=SimpleNamespace(worker_id=worker_id))
    requests = []

    @asynccontextmanager
    async def database():
        async def commit():
            return None
        yield SimpleNamespace(commit=commit)

    async def lock_job(db, job_id, token):
        assert (job_id, token) == (claim.id, claim.lease_token)
        return job

    async def request(action, binding, *, release=False, **payload):
        assert job.remote_stage["stage_id"] == binding.stage_id
        assert job.remote_stage["worker_id"] == str(worker_id)
        requests.append(action)
        if action == "stage_start":
            raise TimeoutError("start reply lost")
        if action == "stage_release":
            return {"state": "released"}
        return (
            {"state": "succeeded", "result": {"version": "2.0"}}
            if "stage_start" in requests else {"state": "unknown"}
        )

    monkeypatch.setattr(platform_stage_host, "get_db_context", database)
    monkeypatch.setattr(platform_stage_host, "lock_stage_job", lock_job)
    backend = platform_stage_host.PlatformStageBackend(bridge, {claim.job_type: ("test.compute.v1", None)})
    monkeypatch.setattr(backend, "_request", request)
    assert await backend.compute(claim, {"input": "immutable"}) == {"version": "2.0"}
    assert requests == ["stage_status", "stage_start", "stage_status", "stage_status", "stage_release"]
    assert backend.active is None
