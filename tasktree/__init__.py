"""tasktree — structured task-tree cancellation and exception aggregation.

Offline, event-driven, standard library only.  See ``tasktree.core`` for
the full semantics contract and README.md for usage.
"""

from .core import (
    ACTIVE,
    COMPLETING,
    DONE,
    Event,
    InvalidInputError,
    ResultConflictError,
    ScopeClosedError,
    ShieldError,
    TaskTree,
    TaskTreeError,
    UnknownTaskError,
)

__version__ = "1.0.0"

__all__ = [
    "ACTIVE",
    "COMPLETING",
    "DONE",
    "Event",
    "InvalidInputError",
    "ResultConflictError",
    "ScopeClosedError",
    "ShieldError",
    "TaskTree",
    "TaskTreeError",
    "UnknownTaskError",
    "__version__",
]
