"""
Core Exceptions

Custom exceptions for the Bifrost platform.
"""

from typing import Any


class StructuredRequestRefusal(ValueError):
    """A request rejection with operator-safe field and recovery details."""

    def __init__(self, code: str, message: str, details: dict[str, Any], *, status_code: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.details = details
        self.status_code = status_code


class AccessDeniedError(Exception):
    """
    Raised when a user does not have access to an entity.

    This exception is used by OrgScopedRepository.can_access() when:
    - The entity does not exist in the user's scope
    - The entity exists but the user lacks role-based access
    - The entity's organization does not match the user's scope

    Usage:
        entity = await repo.can_access(id=entity_id)
        # Raises AccessDeniedError if not accessible
    """

    def __init__(self, message: str = "Access denied"):
        self.message = message
        super().__init__(self.message)
