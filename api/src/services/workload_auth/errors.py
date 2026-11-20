"""Errors for workload token exchange and trust-rule administration."""

from __future__ import annotations


class WorkloadAuthError(Exception):
    """A token-request failure with an OAuth error code and an audit reason.

    ``description`` is safe to return to the client. ``reason`` is for the
    audit log and is not copied into the response.
    """

    def __init__(
        self,
        error: str,
        description: str,
        *,
        reason: str,
        status_code: int = 400,
    ) -> None:
        super().__init__(description)
        self.error = error
        self.description = description
        self.reason = reason
        self.status_code = status_code


class RuleValidationError(ValueError):
    """An administrator's trust rule is not acceptable. The message is safe to show."""


class JwksFetchError(Exception):
    """JWKS discovery failed closed. The message must not include token material."""
