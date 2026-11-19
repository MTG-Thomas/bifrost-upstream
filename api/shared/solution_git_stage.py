"""Credential-free Solution update-check input and result contracts."""

from __future__ import annotations

import posixpath
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

SOLUTION_GIT_CAPABILITY = "platform.solution_update_check.v1"
SOLUTION_UPDATE_CHECK_JOB = "solution.update_check"
MAX_DESCRIPTOR_BYTES = 32 * 1024


def credential_free_https(url: str) -> bool:
    if any(ch.isspace() or ord(ch) < 32 for ch in url) or "\\" in url:
        return False
    try:
        parsed = urlsplit(url)
        # Accessing port also checks malformed or out-of-range ports.
        return (
            parsed.scheme == "https" and bool(parsed.hostname) and parsed.port != 0
            and "@" not in parsed.netloc and "?" not in url and "#" not in url
            and parsed.path.startswith("/") and parsed.path != "/"
        )
    except ValueError:
        return False


class GitInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    kind: Literal["git"] = "git"
    repo: str = Field(max_length=2048)
    ref: str | None = Field(max_length=512)
    subpath: str | None = Field(max_length=1024)

    @field_validator("repo")
    @classmethod
    def check_repo(cls, url: str) -> str:
        if not credential_free_https(url):
            raise ValueError("Stage Git source must be credential-free HTTPS")
        return url

    @field_validator("subpath")
    @classmethod
    def check_subpath(cls, subpath: str | None) -> str | None:
        if not subpath:
            return None
        normalized = posixpath.normpath(subpath)
        if normalized in {".", ".."} or normalized.startswith(("/", "../")) or "\x00" in normalized:
            raise ValueError("Invalid stage Solution subpath")
        return normalized


class DescriptorInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    kind: Literal["descriptor"] = "descriptor"
    content: str = Field(max_length=MAX_DESCRIPTOR_BYTES)

    @field_validator("content")
    @classmethod
    def check_bytes(cls, content: str) -> str:
        if len(content.encode()) > MAX_DESCRIPTOR_BYTES:
            raise ValueError("Solution descriptor exceeds its byte limit")
        return content


UPDATE_INPUT = TypeAdapter(Annotated[GitInput | DescriptorInput, Field(discriminator="kind")])


class VersionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    version: str | None = Field(max_length=64)
    commit: str | None = Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
