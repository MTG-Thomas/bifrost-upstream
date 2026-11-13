"""
SecretString: a str subclass that masks itself in display/logging contexts.

Used for config values of type SECRET. Works transparently as a string
for HTTP headers, f-strings, concatenation, etc. Only masks in:
- repr() / str() / print() — display and logging
- % formatting — logging.info("key=%s", secret)

Note: json.dumps bypasses __str__ for str subclasses (uses C-level buffer).
Secret protection in JSON serialization is handled by:
- redact_secrets(), implemented in bifrost._secret_redaction and re-exported here
- remove_circular_refs() in engine.py, which converts SecretString to [REDACTED]
"""

from __future__ import annotations

from bifrost._secret_redaction import REDACTED, redact_secrets

__all__ = ["REDACTED", "SecretString", "redact_secrets"]


class SecretString(str):
    """A string that masks itself in repr/logging but works normally as a value."""

    def __repr__(self) -> str:
        return f"'{REDACTED}'"

    def __str__(self) -> str:
        return REDACTED

    def __format__(self, format_spec: str) -> str:
        return super().__str__().__format__(format_spec)

    def get_secret_value(self) -> str:
        """Get the actual secret value."""
        return super().__str__()
