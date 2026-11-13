"""Build the installable Bifrost CLI artifact bundled with the API image."""

from __future__ import annotations

import gzip
import io
import re
import tarfile
from pathlib import Path

_EXCLUDED_FILES = {
    "_internal.py",  # Platform-only permission checks
    "_logging.py",  # Platform-only logging
}
_RUNTIME_DATA_FILES = {"lucide_icon_names.json"}


def _normalize_release_tag(value: str) -> str | None:
    """Return a supported release tag as PEP 440, or reject it."""
    if re.fullmatch(r"\d+(\.\d+)*", value):
        return value

    prerelease = re.fullmatch(r"(\d+(?:\.\d+)*)-(dev|rc)\.(\d+)", value)
    if prerelease is None:
        return None
    base, kind, number = prerelease.groups()
    normalized = {"dev": f"{base}.dev{number}", "rc": f"{base}rc{number}"}
    return normalized[kind]


def to_pep440(version: str) -> str:
    """Coerce a Bifrost build version to a PEP 440 package version."""
    if not version or version == "unknown":
        return "0.0.0"

    value = version[1:] if version.startswith("v") else version
    dirty = value.endswith("-dirty")
    if dirty:
        value = value[: -len("-dirty")]

    normalized_release = _normalize_release_tag(value)
    if normalized_release:
        return f"{normalized_release}+dirty" if dirty else normalized_release

    described = re.fullmatch(r"(.+)-(\d+)-(g[0-9a-f]+)", value)
    if described:
        tag, count, sha = described.groups()
        normalized_tag = _normalize_release_tag(tag)
        if normalized_tag:
            local = f"{sha}.dirty" if dirty else sha
            return f"{normalized_tag}.post{count}+{local}"

    # ``git describe --always`` normally yields a hexadecimal SHA here. Keep
    # the fallback valid and filename-safe even when a development build
    # supplies a label such as ``debug`` instead.
    local = re.sub(r"[^a-zA-Z0-9]+", ".", value).strip(".").lower() or "unknown"
    if dirty:
        local = f"{local}.dirty"
    return f"0.0.0+g{local}"


def cli_artifact_filename(version: str) -> str:
    """Return the immutable, installer-compatible artifact filename."""
    return f"bifrost-cli-{to_pep440(version)}.tar.gz"


def _tar_info(name: str, data: bytes, mode: int = 0o644) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name=name)
    info.size = len(data)
    info.mode = mode
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    return info


def build_cli_artifact(source_dir: Path, output_dir: Path, version: str) -> Path:
    """Build a deterministic source archive from the standalone CLI package."""
    source_dir = source_dir.resolve()
    pyproject_path = source_dir / "pyproject.toml"
    if not pyproject_path.is_file():
        raise FileNotFoundError(f"CLI pyproject not found: {pyproject_path}")

    pep440_version = to_pep440(version)
    pyproject, replacements = re.subn(
        r'^version\s*=\s*"[^"]*"',
        f'version = "{pep440_version}"',
        pyproject_path.read_text(),
        count=1,
        flags=re.MULTILINE,
    )
    if replacements != 1:
        raise ValueError(f"CLI version field not found in {pyproject_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = output_dir / cli_artifact_filename(version)
    temporary_path = artifact_path.with_suffix(f"{artifact_path.suffix}.tmp")

    try:
        with temporary_path.open("wb") as raw_file:
            with gzip.GzipFile(fileobj=raw_file, mode="wb", mtime=0) as gzip_file:
                with tarfile.open(fileobj=gzip_file, mode="w") as archive:
                    pyproject_data = pyproject.encode()
                    archive.addfile(
                        _tar_info("pyproject.toml", pyproject_data),
                        io.BytesIO(pyproject_data),
                    )

                    for file_path in sorted(source_dir.rglob("*")):
                        if not file_path.is_file():
                            continue
                        if "__pycache__" in file_path.parts:
                            continue
                        if file_path.name in _EXCLUDED_FILES:
                            continue
                        if (
                            file_path.suffix not in {".py", ".toml"}
                            and file_path.name not in _RUNTIME_DATA_FILES
                        ):
                            continue
                        if file_path == pyproject_path:
                            continue

                        data = file_path.read_bytes()
                        if file_path == source_dir / "__init__.py":
                            # Stamp a Python literal. A callable replacement keeps
                            # backslashes in that literal from becoming re escapes.
                            version_assignment = (
                                f"__version__ = {pep440_version!r}".encode()
                            )
                            stamped = re.subn(
                                rb"^__version__\s*=\s*_compute_version\(\)",
                                lambda _match, replacement=version_assignment: (
                                    replacement
                                ),
                                data,
                                count=1,
                                flags=re.MULTILINE,
                            )
                            data, replacements = stamped
                            if replacements != 1:
                                raise ValueError(
                                    f"CLI __version__ assignment not found in {file_path}"
                                )

                        archive_name = f"bifrost/{file_path.relative_to(source_dir)}"
                        mode = file_path.stat().st_mode & 0o777
                        archive.addfile(
                            _tar_info(archive_name, data, mode),
                            io.BytesIO(data),
                        )

        temporary_path.replace(artifact_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

    return artifact_path
