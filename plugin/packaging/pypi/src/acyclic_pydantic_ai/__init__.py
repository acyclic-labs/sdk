"""acyclic for Pydantic AI: recursive decomposition and speculative execution.

    from acyclic_pydantic_ai import Session, fork, speculate, workspace_tools

    async with Session.open(".") as session:
        await planner.run(task, deps=session.root)

Inside a planner tool, ``fork(ctx)`` gives delegated work its own forked
workspace, merged back on success and discarded on failure; children fork
their own children. ``speculate(agents, prompt, parent=ctx, check="pytest -q")``
runs several attempts in parallel forks, merges the one that passes, and
discards the rest. The engine is the `acyclic` binary (0.1+).
"""

from ._engine import AcyclicError, Engine, MergeConflict, Session, Workspace
from ._forkjoin import Attempt, Speculation, fewest_changes, fork, speculate, workspace_of
from .tools import workspace_tools

__all__ = [
    "AcyclicError",
    "Attempt",
    "Engine",
    "MergeConflict",
    "Session",
    "Speculation",
    "Workspace",
    "fewest_changes",
    "fork",
    "speculate",
    "workspace_of",
    "workspace_tools",
    "__version__",
]
__version__ = "0.1.0"
