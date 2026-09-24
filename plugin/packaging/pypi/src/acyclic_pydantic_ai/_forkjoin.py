"""Recursive decomposition (`fork`) and speculative execution (`speculate`)."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Union

from ._engine import AcyclicError, Workspace

Check = Union[str, Callable[[Workspace], Union[bool, Awaitable[bool]]]]


def workspace_of(source: Any) -> Workspace:
    """The workspace behind a Workspace, a RunContext, or deps carrying one.

    A tool receives ``ctx: RunContext[Workspace]``; ``fork(ctx)`` forks the
    workspace the calling agent runs in. Deps that wrap a workspace can expose
    it as a ``workspace`` attribute.
    """
    if isinstance(source, Workspace):
        return source
    deps = getattr(source, "deps", source)
    if isinstance(deps, Workspace):
        return deps
    inner = getattr(deps, "workspace", None)
    if isinstance(inner, Workspace):
        return inner
    raise TypeError(
        "fork()/speculate() need a Workspace, a RunContext whose deps is a Workspace, "
        f"or deps with a `workspace` attribute; got {type(source).__name__}"
    )


@asynccontextmanager
async def fork(parent: Any, *, name: str | None = None, merge: bool = True) -> AsyncIterator[Workspace]:
    """Fork the parent's workspace for one piece of delegated work.

    On a normal exit the child is merged into its parent (unless
    ``merge=False``, which leaves it stopped for the caller to decide). If the
    block raises, the child and everything it forked are discarded and the
    parent is untouched::

        @planner.tool
        async def delegate(ctx: RunContext[Workspace], part: str) -> str:
            async with fork(ctx, name="part") as ws:
                result = await worker.run(part, deps=ws)
            return result.output

    Children fork their own children the same way, to any depth.
    """
    child = await workspace_of(parent).spawn(name)
    try:
        yield child
    except BaseException:
        try:
            await child.discard()
        except AcyclicError:
            pass
        raise
    if merge:
        await child.merge()
    else:
        await child.stop()


@dataclass
class Attempt:
    """One speculative branch."""

    index: int
    workspace: Workspace
    output: Any = None
    error: BaseException | None = None
    passed: bool | None = None
    check_output: str = ""
    changes: list[str] = field(default_factory=list)
    usage: Any = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.passed is not False


@dataclass
class Speculation:
    """What `speculate` did: every attempt, and the one that was merged."""

    attempts: list[Attempt]
    winner: Attempt | None

    @property
    def output(self) -> Any:
        return self.winner.output if self.winner else None


def fewest_changes(attempts: Sequence[Attempt]) -> Attempt | None:
    """Default pick: among attempts that finished and passed the check, the one
    that changed the fewest paths; ties go to the earliest agent."""
    good = [a for a in attempts if a.ok]
    return min(good, key=lambda a: (len(a.changes), a.index)) if good else None


async def speculate(
    agents: Sequence[Any],
    prompt: str,
    *,
    parent: Any,
    check: Check | None = None,
    choose: Callable[[Sequence[Attempt]], Attempt | None] = fewest_changes,
    deps: Callable[[Workspace], Any] | None = None,
    name: str = "try",
) -> Speculation:
    """Run the same task several ways at once and keep one.

    Every agent gets its own fork of ``parent``'s workspace and runs
    concurrently. ``check`` (a shell command run inside each fork, or a
    callable taking the fork) decides which attempts passed; ``choose`` picks
    the winner among them. The winner is merged into ``parent``, every other
    attempt is discarded, and nothing reaches ``parent`` if none qualifies.

    ``deps`` builds each agent's deps from its fork (default: the fork itself).
    """
    base = workspace_of(parent)
    forks: list[Workspace] = []
    try:
        for i in range(len(agents)):
            forks.append(await base.spawn(f"{name}-{i}"))
    except BaseException:
        await _discard_all(forks)
        raise
    attempts = [Attempt(i, ws) for i, ws in enumerate(forks)]

    async def run(agent: Any, attempt: Attempt) -> None:
        try:
            result = await agent.run(prompt, deps=deps(attempt.workspace) if deps else attempt.workspace)
        except Exception as error:  # an attempt failing is an outcome, not a crash
            attempt.error = error
            return
        attempt.output = result.output
        usage = getattr(result, "usage", None)  # a method before pydantic-ai 2.48, a property after
        attempt.usage = usage() if callable(usage) else usage
        if check is not None:
            attempt.passed, attempt.check_output = await _run_check(check, attempt.workspace)

    try:
        await asyncio.gather(*(run(agent, attempt) for agent, attempt in zip(agents, attempts)))
        for attempt in attempts:
            await attempt.workspace.stop()
            attempt.changes = await attempt.workspace.changes()
        winner = choose(attempts)
        losers = [a.workspace for a in attempts if a is not winner]
        await _discard_all(losers)
        if winner is not None:
            await winner.workspace.merge()
    except BaseException:
        await _discard_all([a.workspace for a in attempts if a.workspace.state != "merged"])
        raise
    return Speculation(attempts, winner)


async def _run_check(check: Check, ws: Workspace) -> tuple[bool, str]:
    """Run the check in a throwaway fork of the attempt and discard it after,
    so nothing the check writes (`__pycache__`, `.pytest_cache`, build output)
    can be merged with the attempt."""
    probe = await ws.spawn("check")
    try:
        return await _run_check_in(check, probe)
    finally:
        try:
            await probe.discard()
        except AcyclicError:
            pass


async def _run_check_in(check: Check, ws: Workspace) -> tuple[bool, str]:
    if isinstance(check, str):
        proc = await asyncio.create_subprocess_shell(
            check,
            cwd=str(ws.path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        return proc.returncode == 0, out.decode(errors="replace")[-4000:]
    verdict = check(ws)
    if inspect.isawaitable(verdict):
        verdict = await verdict
    return bool(verdict), ""


async def _discard_all(workspaces: Sequence[Workspace]) -> None:
    for ws in workspaces:
        try:
            await ws.discard()
        except AcyclicError:
            pass
