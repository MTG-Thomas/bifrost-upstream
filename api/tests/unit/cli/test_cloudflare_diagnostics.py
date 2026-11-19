"""Exercise the local report through the CLI entrypoint and validate through both clients."""

import asyncio
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import httpx
import pytest
from click.testing import CliRunner

from bifrost import cli
from bifrost.commands.workflows import workflows_group
from src.services.mcp_server.tools import workflow


def test_workspace_report_is_offline_and_aggregate_only(tmp_path, capsys):
    manifest = tmp_path / ".bifrost"
    manifest.mkdir()
    (manifest / "workflows.yaml").write_text(
        f"workflows:\n  {uuid4()}:\n    path: private.py\n    name: private_name\n    function_name: run\n",
        encoding="utf-8",
    )
    (tmp_path / "private.py").write_text("import socket\ndef run(): pass\n", encoding="utf-8")
    with patch.object(cli, "_check_cli_version", side_effect=AssertionError("report must stay offline")):
        assert cli.main(["workflows", "--json", "cloudflare-report", str(tmp_path), "--aggregate-only"]) == 0
    output = capsys.readouterr()
    assert '"CF_TRANSPORT": 1' in output.out
    assert "private" not in output.out + output.err


def test_workspace_report_error_omits_private_names(tmp_path, capsys):
    manifest = tmp_path / ".bifrost"
    manifest.mkdir()
    (manifest / "workflows.yaml").write_text("private_invalid: 123", encoding="utf-8")
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["workflows", "cloudflare-report", str(tmp_path), "--aggregate-only"])
    assert exit_info.value.code == 1
    output = capsys.readouterr()
    assert "Could not read the workspace" in output.err
    assert "private_invalid" not in output.out + output.err


async def test_validate_cli_preserves_advisories_and_context(tmp_path):
    source = tmp_path / "draft.py"
    source.write_text("source", encoding="utf-8")
    body = {
        "path": "draft.py", "content": "source", "execution_lane": "fast_path",
        "inline": True, "artifact_workspace": True,
    }
    payload = {"valid": True, "issues": [{
        "rule_id": "CF_FAST_PATH", "severity": "warning", "source": "cloudflare",
        "message": "Won't burst to Cloudflare.", "hint": "Use the Normal lane.",
    }]}
    route = "/api/workflows/validate"
    rest = AsyncMock(return_value=httpx.Response(200, json=payload, request=httpx.Request(
        "POST", f"http://test.local{route}",
    )))
    with patch("bifrost.commands.workflows.BifrostClient.get_instance", return_value=Mock(post=rest)):
        result = await asyncio.to_thread(CliRunner().invoke, workflows_group, [
            "validate", "draft.py", "--from-file", str(source), "--execution-lane", "fast_path",
            "--inline", "--artifact-workspace",
        ])
    assert result.exit_code == 0, result.output
    assert "CF_FAST_PATH" in result.output and "Use the Normal lane." in result.output
    rest.assert_awaited_once_with(route, json=body)

async def test_validate_mcp_preserves_advisories_and_context():
    route = "/api/workflows/validate"
    body = {
        "path": "draft.py", "content": "source", "execution_lane": "fast_path",
        "inline": True, "artifact_workspace": True,
    }
    payload = {"valid": True, "issues": [{"rule_id": "CF_FAST_PATH", "source": "cloudflare"}]}
    bridge = AsyncMock(return_value=(200, payload))
    with patch.object(workflow, "call_rest", bridge):
        result = await workflow.bifrost_workflow_validate(
            None, "draft.py", "source", execution_lane="fast_path", inline=True, artifact_workspace=True,
        )
    assert result.structured_content == payload
    assert not result.is_error
    bridge.assert_awaited_once_with(None, "POST", route, json_body=body)
