"""The shared approval host contract, also exercised in the Worker port."""

import json
from pathlib import Path

import pytest
from src.services.execution.cloudflare_hosts import (
    egress_host_allowed,
    normalize_egress_pattern,
)

VECTORS = json.loads(Path(__file__).with_name("cloudflare_host_vectors.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("pattern, canonical", VECTORS["patterns"])
def test_approval_pattern(pattern, canonical):
    assert normalize_egress_pattern(pattern) == canonical


@pytest.mark.parametrize("patterns, hostname, allowed", VECTORS["matches"])
def test_approved_host_matches(patterns, hostname, allowed):
    assert egress_host_allowed(hostname, set(patterns)) is allowed
