"""Bounded, sanitized observation of a readiness connection reused by the pool."""

import logging
import re
from collections.abc import Mapping, MutableMapping

logger = logging.getLogger(__name__)
IDENTITY_QUERY = (
    "SELECT session_user AS session_user, current_user AS current_user, "
    "pg_catalog.current_database() AS current_database"
)
_OBSERVED = "bifrost.readiness_identity_observed"
_FIELDS = ("session_user", "current_user", "current_database")


class RuntimeDatabaseIdentityObserver:
    """Log at most one reused-client-connection sample per observer/process.

    SQLAlchemy's Connection.info belongs to its pooled DBAPI connection and is
    cleared when that physical client connection is replaced. This marker proves
    an earlier readiness observation, not a complete pool inventory or a stable
    server backend behind an intermediate pooler.
    """

    def __init__(self) -> None:
        self._reported = False

    def observe(self, row: Mapping[str, object], connection_info: MutableMapping[str, object]) -> None:
        if self._reported:
            return
        previously_observed = connection_info.get(_OBSERVED) is True
        connection_info[_OBSERVED] = True
        if not previously_observed:
            return
        self._reported = True
        valid = set(row.keys()) == set(_FIELDS)
        if valid:
            for key in _FIELDS:
                value = row[key]
                if not isinstance(value, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,62}", value) is None:
                    valid = False
                    break
        if not valid:
            logger.warning("Runtime database identity sample unavailable: unsafe_identity_result")
            return
        logger.info(
            "Runtime database session identity: session_user=%s current_user=%s current_database=%s "
            "client_connection_previously_observed=true",
            row["session_user"], row["current_user"], row["current_database"],
        )
