"""Workers advice shares the approval analyzer and remains advisory."""

import ast
import tokenize
from types import SimpleNamespace

import pytest

from bifrost import cloudflare_analysis as analysis
from bifrost.cloudflare_analysis import diagnose, placement_findings
from bifrost.cloudflare_report import count_rules, inspect_bundle


async def diagnose_file(path: str, source: bytes):
    async def read(_path, _maximum):
        return source

    return await diagnose(read, path, single_file=True)


@pytest.mark.parametrize(("snippet", "rule", "line", "hint"), [
    ("import subprocess", "CF_PROCESS", 1, "Python code"),
    ("from os import system as launch\nlaunch('fixture')", "CF_PROCESS", 2, "Linux"),
    ("import socket", "CF_TRANSPORT", 1, "httpx"),
    ("import requests", "CF_TRANSPORT", 1, "HTTPS"),
    ("import httpx\nhttpx.get('http://example.test')", "CF_TRANSPORT", 2, "HTTPS"),
    ("from croniter import croniter", "CF_PACKAGE", 1, "Pyodide build"),
    ("import awscrt", "CF_NATIVE", 1, "WebAssembly"),
    ("exec('pass')", "CF_DYNAMIC", 1, "static dispatch"),
    ("eval('1')", "CF_DYNAMIC", 1, "static dispatch"),
    ("getattr(obj, name)()", "CF_DYNAMIC", 1, "static dispatch"),
    ("def broken(", "CF_SOURCE", 1, "entry function"),
    ("def broken(\r\n", "CF_SOURCE", 1, "entry function"),
    ("from importlib import import_module", "CF_DYNAMIC", 1, "explicit imports"),
    ("from bifrost import workflows\nworkflows.execute('example')", "CF_SDK_GATEWAY", 2, "Linux"),
    ("from bifrost import executions\nexecutions.get(execution_id)", "CF_SDK_GATEWAY", 2, "Linux"),
    ("from bifrost import api\napi.get(f'/api/executions/{execution_id}')", "CF_SDK_GATEWAY", 2, "Linux"),
    ("from bifrost.api import api\napi.get('/api/worker-control')", "CF_SDK_GATEWAY", 2, "Linux"),
    ("from bifrost import api\napi.get(f'/api/workflows{suffix}')", "CF_SDK_GATEWAY", 2, "Linux"),
    ("from bifrost import api\napi.patch('/api/users/bulk')", "CF_SDK_GATEWAY", 2, "Linux"),
    ("#" * (384 * 1024 + 1), "CF_PARSE_BUDGET", None, "smaller modules"),
], ids=lambda value: "oversized Python file" if isinstance(value, str) and len(value) > 1000 else None)
async def test_diagnostics_rules(snippet, rule, line, hint):
    async def read(path, _maximum):
        return snippet.encode() if path == "entry.py" else None

    findings = await diagnose(read, "entry.py")
    finding = next(item for item in findings if item.rule_id == rule)
    assert (finding.severity, finding.path, finding.line) == ("warning", "entry.py", line)
    if line is not None:
        assert finding.column is not None and finding.column > 0
    if snippet == "def broken(\r\n":
        assert finding.column == 1
    assert hint in finding.hint
    assert finding.message.startswith("Won't burst to Cloudflare")


async def test_tokenizer_crlf_preserves_column(monkeypatch):
    monkeypatch.setattr(analysis, "sys", SimpleNamespace(version_info=(3, 15)))

    def fail(_readline):
        raise tokenize.TokenError("invalid syntax", (1, 6))

    monkeypatch.setattr(analysis.tokenize, "generate_tokens", fail)
    [finding] = await diagnose_file("entry.py", b"bad =)\r\n")
    assert (finding.rule_id, finding.line, finding.column) == ("CF_SOURCE", 1, 6)


async def test_imported_unicode_failure_names_the_imported_module():
    async def read(path, _maximum):
        return {"entry.py": b"import helper", "helper.py": b"\xff"}.get(path)

    [issue] = await diagnose(read, "entry.py")
    assert (issue.rule_id, issue.path, issue.line) == ("CF_SOURCE", "helper.py", None)


async def test_forwarded_sdk_routes_have_no_gateway_warning():
    source = b"""from bifrost import api, artifacts, executions, files, roles, users
roles.list()
users.update(user_id, name="example")
api.get(f"/api/users/{user_id}")
api.get("/api/users/a^b@example.test")
api.get("/api/users/a%5Eb@example.test")
executions.get_current_logs()
files.move(source, destination)
files.move_many(moves)
artifacts.create_video(title)
api.get(f"/api/platform-jobs/{job_id}")
"""
    assert await diagnose_file("entry.py", source) == []


@pytest.mark.parametrize("expression", ['A + A', 'f"{A}{A}"', 'A'])
def test_url_resolution_uses_one_prefix_budget(monkeypatch, expression):
    monkeypatch.setattr(analysis, "_MAX_URL_PREFIX", 12)
    constant = "https://example.test/" if expression == "A" else "shorter"
    literal = analysis._url_literal(ast.parse(expression, mode="eval").body, {"A": constant})
    assert literal is not None and not literal.complete
    assert len(literal.prefix) <= 12
    assert literal.host() is None


@pytest.mark.parametrize("limit", ["_MAX_URL_PREFIX", "_MAX_URL_PARTS"])
def test_url_overflow_retains_a_complete_authority(monkeypatch, limit):
    monkeypatch.setattr(analysis, limit, 30 if limit == "_MAX_URL_PREFIX" else 2)
    node = ast.parse('f"https://example.test/{tail}"', mode="eval").body
    literal = analysis._url_literal(node, {"tail": "a" * 40})
    assert literal is not None and not literal.complete
    assert literal.host() == "example.test"


async def test_dense_source_is_checked_below_old_file_threshold(monkeypatch):
    monkeypatch.setattr(analysis, "_MAX_TOKEN_WEIGHT", 28)
    [finding] = await diagnose_file("entry.py", b"values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]\n")
    assert finding.rule_id == "CF_PARSE_BUDGET"


def test_legacy_fstring_token_charges_source_weight(monkeypatch):
    monkeypatch.setattr(analysis, "sys", SimpleNamespace(version_info=(3, 11)))
    monkeypatch.setattr(analysis, "_MAX_TOKEN_WEIGHT", 12)
    token = SimpleNamespace(type=tokenize.STRING, string='rf"prefix {value}"')
    monkeypatch.setattr(analysis.tokenize, "generate_tokens", lambda _: iter([token]))
    with pytest.raises(analysis.AnalysisRejected, match="token budget"):
        analysis._check_tokens(token.string)


@pytest.mark.parametrize(("failure", "rule"), [(MemoryError, "CF_PARSE_BUDGET"), (ValueError, "CF_SOURCE")])
async def test_parser_errors_have_specific_refusals(monkeypatch, failure, rule):
    def fail(*_args, **_kwargs):
        raise failure

    monkeypatch.setattr(analysis.ast, "parse", fail)
    [finding] = await diagnose_file("entry.py", b"def run(): pass\n")
    assert (finding.rule_id, finding.path) == (rule, "entry.py")


async def test_transitive_source_and_single_file_boundary():
    async def read(path, _maximum):
        return {"entry.py": b"import helper", "helper.py": b"import subprocess"}.get(path)

    [issue] = await diagnose(read, "entry.py")
    assert (issue.rule_id, issue.path, issue.line) == ("CF_PROCESS", "helper.py", 1)
    assert await diagnose_file("entry.py", b"import helper") == []


@pytest.mark.parametrize(("kwargs", "rule", "hint"), [
    ({"inline": True}, "CF_INLINE", "register"),
    ({"artifact_workspace": True}, "CF_ARTIFACT_WORKSPACE", "dependency"),
    ({"execution_lane": "fast_path"}, "CF_FAST_PATH", "Normal"),
    ({"outbound": True}, "CF_OUTBOUND", "vendor"),
    ({"executable_type": "tool"}, "CF_EXECUTABLE_TYPE", "Normal workflow"),
])
def test_placement_rules(kwargs, rule, hint):
    [issue] = placement_findings(**kwargs)
    assert (issue.rule_id, issue.severity) == (rule, "warning")
    assert hint in issue.hint


async def test_unresolved_url_is_info_and_subrequest_budget_is_bundle_only():
    source = b"import httpx\nhttpx.get(url)"
    async def read(_path, _maximum):
        return source

    [info] = await diagnose_file("entry.py", source)
    assert (info.rule_id, info.severity, info.path, info.line, info.column) == ("CF_URL_REVIEW", "info", "entry.py", 2, 1)
    assert "approve" in info.hint
    findings = await diagnose(read, "entry.py", subrequest_allowance=1)
    assert [item.rule_id for item in findings] == ["CF_URL_REVIEW", "CF_SUBREQUESTS"]
    assert "Batch" in findings[1].hint


@pytest.mark.parametrize(("snippet", "budget", "rule"), [
    ("x = 1\n" * 20, 1, "CF_CPU"),
    ("import httpx\n" + "".join(f"httpx.get('https://h{i}.example.test')\n" for i in range(33)), 1000, "CF_HOST_BUDGET"),
], ids=["cpu budget", "host budget"])
async def test_bundle_budget_rule(snippet, budget, rule):
    async def read(_path, _maximum):
        return snippet.encode()

    [finding] = await diagnose(read, "entry.py", cpu_budget_ms=budget)
    assert (finding.rule_id, finding.severity, finding.path) == (rule, "warning", "entry.py")
    assert finding.hint


async def test_report_counts_rules_once_per_artifact_and_notes_agents():
    async def read(path, _maximum):
        return {"entry.py": b"from bifrost import workflows\ndef run(): workflows.execute('a'); workflows.execute('b')"}.get(path)

    reports = await inspect_bundle(
        read,
        [{"path": "entry.py", "function_name": "run"}],
        [{"name": "assistant"}],
    )
    assert [report.status for report in reports] == ["linux", "info"]
    assert count_rules(reports) == {"CF_AGENT": 1, "CF_SDK_GATEWAY": 1}
    assert "summaries" in reports[1].findings[0].hint
