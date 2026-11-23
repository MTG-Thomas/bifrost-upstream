from hashlib import sha256
from uuid import uuid4

import pytest
from pydantic import ValidationError
from shared.solution_git_stage import (
    SOLUTION_GIT_CAPABILITY,
    DescriptorInput,
    GitInput,
    VersionResult,
)
from src.worker.remote.platform_stage_protocol import (
    MAX_STAGE_INPUT_BYTES,
    StageBinding,
    encode_bundle,
)
from src.worker.remote.platform_stages import PlatformStages, StageProgram


@pytest.mark.parametrize("repo", [
    "https://x-access-token:secret@example.com/owner/repo",
    "https://example.com/owner/repo?token=secret",
    "file:///srv/repo",
    "ext::some-command",
])
def test_git_input_rejects_credential_material_and_unsafe_transports(repo):
    with pytest.raises(ValidationError):
        GitInput(repo=repo, ref=None, subpath=None)


def test_git_input_confines_subpaths():
    repo = "https://example.com/owner/repo"
    with pytest.raises(ValidationError):
        GitInput(repo=repo, ref="test", subpath="../solution")
    assert GitInput(repo=repo, ref="test", subpath="solutions/./demo").subpath == "solutions/demo"


async def test_descriptor_compute_uses_the_real_stage_child():
    data = DescriptorInput(content='slug: demo\nname: Demo\nversion: "2.1.0"\n').model_dump(mode="json")
    binding = StageBinding(
        str(uuid4()), str(uuid4()), str(uuid4()), SOLUTION_GIT_CAPABILITY,
        sha256(encode_bundle(data, MAX_STAGE_INPUT_BYTES)).hexdigest(),
    )
    program = StageProgram("src.worker.remote.solution_git_stage", 10)
    ledger = PlatformStages({SOLUTION_GIT_CAPABILITY: program})
    expected = VersionResult(version="2.1.0", commit=None).model_dump()
    try:
        ledger.start(binding, data)
        assert ledger.active is not None and ledger.active.task is not None
        await ledger.active.task
        assert ledger.status(binding)["result"] == expected
    finally:
        await ledger.close()
