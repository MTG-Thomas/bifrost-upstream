"""Cloudflare approval host policy. The Worker port uses the same PSL and vectors."""

import json
from collections.abc import Collection
from pathlib import Path

from src.services.execution.cloudflare_analysis import (
    AnalysisRejected,
    _exact_host,
    reviewed_https_hosts,
)

_PSL = json.loads(Path(__file__).with_name("cloudflare_psl.json").read_text(encoding="utf-8"))
_ICANN: frozenset[str] = frozenset(_PSL["icann"])
_PRIVATE_ROOTS: frozenset[str] = frozenset(_PSL["privateRoots"])


def _wildcard_base_allowed(base: str) -> bool:
    labels = base.split(".")
    has_registrable_domain = False
    for index in range(len(labels)):
        suffix = ".".join(labels[index:])
        if suffix in _PRIVATE_ROOTS:
            return False
        if not has_registrable_domain:
            if f"!{suffix}" in _ICANN:
                has_registrable_domain = True
            elif suffix in _ICANN or f"*.{'.'.join(labels[index + 1:])}" in _ICANN:
                if index == 0:
                    return False
                has_registrable_domain = True
    return has_registrable_domain


def normalize_egress_pattern(pattern: str) -> str | None:
    if not pattern.isascii() or len(pattern) > 253:
        return None
    pattern = pattern.lower()
    if pattern.startswith("*."):
        base = pattern[2:]
        return pattern if _exact_host(base) and _wildcard_base_allowed(base) else None
    return pattern if _exact_host(pattern) else None


def reviewed_https_allowlist(hosts: list[str]) -> list[str]:
    if any(not host.isascii() for host in hosts):
        raise AnalysisRejected("Operator HTTPS hostnames must use ASCII, including IDN A-labels")
    exact_hosts = reviewed_https_hosts([host for host in hosts if "*" not in host])
    wildcards: set[str] = set()
    for host in hosts:
        if "*" not in host:
            continue
        pattern = normalize_egress_pattern(host)
        if pattern is None:
            raise AnalysisRejected(
                "An HTTPS wildcard must be *. followed by a registrable domain outside the PSL private roots, without a port"
            )
        wildcards.add(pattern)
    return sorted(wildcards.union(exact_hosts))


def egress_host_allowed(hostname: str, patterns: Collection[str]) -> bool:
    """Check a concrete ASCII hostname against canonical approved entries."""
    if not hostname.isascii():
        return False
    host = hostname.lower()
    if not _exact_host(host):
        return False
    if host in patterns:
        return True
    wildcard = f"*.{host.partition('.')[2]}"
    return wildcard in patterns and normalize_egress_pattern(wildcard) is not None
