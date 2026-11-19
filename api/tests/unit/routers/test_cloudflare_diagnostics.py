"""Opt-in authoring diagnostics preserve the primary editor and validator outcomes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from src import config
from src.models.contracts.editor import FileContentRequest
from src.models.contracts.workflows import (
    WorkflowValidationRequest,
    WorkflowValidationResponse,
)
from src.routers import files, workflows
from src.services import file_storage, workflow_validation
from src.services.execution import cloudflare_analysis_runner
from src.services.file_storage.models import WriteResult


@pytest.mark.parametrize("analyzer_fails", [False, True])
async def test_editor_diagnostics_use_isolated_analysis_and_remain_advisory(monkeypatch, caplog, analyzer_fails):
    source = b"import subprocess\n"
    path = "workflows/example.py"
    analyzer = cloudflare_analysis_runner.diagnose_file

    async def analyze(normalized_path, content):
        assert (normalized_path, content) == (path, source)
        if analyzer_fails:
            raise RuntimeError("fixture analyzer failure")
        return await analyzer(normalized_path, content)

    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(cloudflare_diagnostics_enabled=True))
    monkeypatch.setattr(files, "authorize_operation", AsyncMock())
    monkeypatch.setattr(files, "_lock_file_mutation", AsyncMock())
    monkeypatch.setattr(cloudflare_analysis_runner, "diagnose_file", analyze)
    write = AsyncMock(return_value=WriteResult(file_record=None, final_content=source, content_modified=False))
    monkeypatch.setattr(file_storage, "FileStorageService", Mock(return_value=SimpleNamespace(write_file=write)))

    response = await files.put_file_content_editor(
        FileContentRequest(path=path, content=source.decode()),
        ctx=None, user=SimpleNamespace(email="tester@example.test"), db=AsyncMock(),
    )
    write.assert_awaited_once()
    assert response.content == source.decode()
    if analyzer_fails:
        assert response.diagnostics == []
        assert "Cloudflare editor diagnostics failed" in caplog.text
    else:
        [marker] = response.diagnostics
        assert (marker.source, marker.severity, marker.line) == ("cloudflare", "warning", 1)
        assert "CF_PROCESS" in marker.message


def _fixture_caller(monkeypatch, *, enabled):
    caller = SimpleNamespace(principal=SimpleNamespace(runtime_authority=None), ctx=None)
    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(cloudflare_diagnostics_enabled=enabled))
    monkeypatch.setattr(workflows, "load_caller", AsyncMock(return_value=caller))
    monkeypatch.setattr(workflows, "allows_permission", lambda *_: False)

    def require(_caller, permission, _org):
        if permission == "repository.read":
            raise HTTPException(status_code=403)

    monkeypatch.setattr(workflows, "require_permission", require)


@pytest.mark.parametrize("enabled", [False, True])
async def test_path_only_validation_retains_off_authorization(monkeypatch, enabled):
    _fixture_caller(monkeypatch, enabled=enabled)
    validate = AsyncMock(return_value=WorkflowValidationResponse(valid=True))
    monkeypatch.setattr(workflow_validation, "validate_workflow_file", validate)

    request = WorkflowValidationRequest(path="workflows/example.py")
    if enabled:
        with pytest.raises(HTTPException) as denied:
            await workflows.validate_workflow(request, user=None, db=AsyncMock())
        assert denied.value.status_code == 403
        validate.assert_not_awaited()
    else:
        assert (await workflows.validate_workflow(request, user=None, db=AsyncMock())).valid
        validate.assert_awaited_once_with(path=request.path, content=None)


async def test_enabled_validation_preserves_linux_import_error_and_reports_once(monkeypatch):
    _fixture_caller(monkeypatch, enabled=True)
    scan = AsyncMock(wraps=cloudflare_analysis_runner.diagnose)
    monkeypatch.setattr(cloudflare_analysis_runner, "diagnose", scan)
    request = WorkflowValidationRequest(
        path="workflows/example.py",
        content=(
            "import nonexistent_cloudflare_diagnostics_fixture\n"
            "import subprocess\n"
            "from bifrost import workflow, data_provider, service\n"
            "@workflow\ndef first(): pass\n@workflow\ndef second(): pass\n"
            "@data_provider\ndef provider(): pass\n@service\ndef worker(): pass\n"
        ),
    )
    response = await workflows.validate_workflow(request, user=None, db=AsyncMock())

    assert not response.valid
    assert any("Import error" in issue.message for issue in response.issues if issue.severity == "error")
    assert [issue.rule_id for issue in response.issues if issue.source == "cloudflare"] == ["CF_PROCESS"]
    scan.assert_awaited_once()
