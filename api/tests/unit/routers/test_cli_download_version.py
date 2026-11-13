"""CLI build-time artifact generation."""

import ast
import asyncio
import tarfile
import tomllib
from pathlib import Path

from shared.cli_artifact import build_cli_artifact, cli_artifact_filename, to_pep440
import src.routers.cli as cli_router


def test_to_pep440_preserves_dev_release_versions() -> None:
    assert to_pep440("1.0.8-dev.11") == "1.0.8.dev11"
    assert to_pep440("v1.0.8-dev.11") == "1.0.8.dev11"


def test_to_pep440_preserves_dirty_dev_release_versions() -> None:
    assert to_pep440("1.0.8-dev.11-dirty") == "1.0.8.dev11+dirty"


def test_to_pep440_normalizes_supported_described_tags() -> None:
    assert to_pep440("v3.0.0-2-gabc1234") == "3.0.0.post2+gabc1234"
    assert to_pep440("v2.1.0-rc.7-2-gabc1234-dirty") == "2.1.0rc7.post2+gabc1234.dirty"
    assert to_pep440("v1.0.8-dev.11-4-gabc1234") == "1.0.8.dev11.post4+gabc1234"


def test_build_cli_artifact_stamps_and_filters_package(tmp_path: Path) -> None:
    source_dir = Path(__file__).resolve().parents[3] / "bifrost"

    artifact = build_cli_artifact(source_dir, tmp_path, "v1.2.3")

    assert artifact.name == cli_artifact_filename("v1.2.3")
    with tarfile.open(artifact, mode="r:gz") as archive:
        names = archive.getnames()
        pyproject = archive.extractfile("pyproject.toml")
        init = archive.extractfile("bifrost/__init__.py")
        assert pyproject is not None
        assert init is not None
        pyproject_text = pyproject.read().decode()
        assert 'version = "1.2.3"' in pyproject_text
        metadata = tomllib.loads(pyproject_text)
        assert any(
            requirement.startswith("packaging")
            for requirement in metadata["project"]["dependencies"]
        )
        assert "pydantic>=2.12.0" in metadata["project"]["dependencies"]
        assert "__version__ = '1.2.3'" in init.read().decode()

    assert "bifrost/lucide_icon_names.json" in names
    assert "bifrost/_logging.py" not in names


def test_build_cli_artifact_is_deterministic(tmp_path: Path) -> None:
    source_dir = Path(__file__).resolve().parents[3] / "bifrost"
    first = build_cli_artifact(source_dir, tmp_path / "first", "debug")
    second = build_cli_artifact(source_dir, tmp_path / "second", "debug")

    assert first.read_bytes() == second.read_bytes()


def test_build_cli_artifact_treats_version_as_data(tmp_path: Path) -> None:
    """Untrusted build-version text cannot escape the version assignment."""
    source_dir = Path(__file__).resolve().parents[3] / "bifrost"
    supplied = "1.2.3\" #\\';print(123);#-1-gabc"

    artifact = build_cli_artifact(source_dir, tmp_path, supplied)

    assert artifact.name == cli_artifact_filename(supplied)
    assert '"' not in artifact.name
    assert "\n" not in artifact.name
    assert "\r" not in artifact.name
    with tarfile.open(artifact, mode="r:gz") as archive:
        pyproject = archive.extractfile("pyproject.toml")
        init = archive.extractfile("bifrost/__init__.py")
        assert pyproject is not None
        assert init is not None
        metadata = tomllib.loads(pyproject.read().decode())
        module = ast.parse(init.read().decode())

    normalized = to_pep440(supplied)
    assert metadata["project"]["version"] == normalized
    assert '"' not in normalized
    assert "\n" not in normalized
    assert "\r" not in normalized

    assignments = [
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in node.targets
        )
    ]
    assert len(assignments) == 1
    assert isinstance(assignments[0].value, ast.Constant)
    assert assignments[0].value.value == normalized


def test_download_sdk_disposition_uses_normalized_filename(monkeypatch) -> None:
    """A raw version cannot inject headers into the SDK download filename."""
    supplied = '1.2.3.tgz\r\nX-Injected: yes"'
    monkeypatch.setattr("shared.version.get_version", lambda: supplied)
    monkeypatch.setattr(
        "src.services.sdk_package.build_sdk_tarball",
        lambda _version: b"tarball",
    )

    response = asyncio.run(cli_router.download_sdk())
    disposition = response.headers["content-disposition"]

    assert disposition == "attachment; filename=bifrost-sdk-1.2.3.tgz"
    assert "\r" not in disposition
    assert "\n" not in disposition
