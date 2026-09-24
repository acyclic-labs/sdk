"""The acyclic workspace engine, driven over its public hook protocol.

Coding hosts (Claude Code, Codex) fire SessionStart, PreToolUse, SubagentStart
and SubagentStop themselves. Pydantic AI has no subagent lifecycle, so this
module fires them explicitly as the `sdk` host:

    SessionStart                    attach the repo root as a session
    PreToolUse (tool_name=Agent)    announce a spawn by the calling workspace
    SubagentStart (agent_id=...)    fork the caller's workspace into a mount
    SubagentStop                    freeze the child so its parent can merge it
    SessionEnd                      release the session

Merging and discarding are ordinary CLI commands resolved by working
directory: `acyclic git merge agents/<id>` and `acyclic discard agents/<id>`,
run from the parent's path. The engine allows only the direct parent to do
either.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

#: The host name the binary records for sessions this package opens.
HOST = "sdk"

_HOOK_TIMEOUT = 120.0


class AcyclicError(RuntimeError):
    """The acyclic binary refused or failed an operation."""


class MergeConflict(AcyclicError):
    """A child could not be merged into its parent without conflicts.

    ``conflicts`` is the engine's typed conflict list (path, kind, base, ours,
    theirs). ``aborted`` says whether the pending merge was rolled back; when
    it is False the parent still has an unresolved merge and needs
    ``acyclic git merge --continue`` or ``--abort`` run from its path.
    """

    def __init__(self, ref: str, conflicts: list[dict[str, Any]], aborted: bool, detail: str = "") -> None:
        self.ref = ref
        self.conflicts = conflicts
        self.aborted = aborted
        paths = ", ".join(str(c.get("path")) for c in conflicts) or "unknown paths"
        state = "merge aborted" if aborted else f"merge left pending ({detail})" if detail else "merge left pending"
        super().__init__(f"merging {ref} conflicted on {paths}; {state}")


def find_binary(explicit: str | os.PathLike[str] | None = None) -> str:
    candidate = os.fspath(explicit) if explicit else os.environ.get("ACYCLIC_BIN") or "acyclic"
    found = shutil.which(candidate)
    if found is None:
        raise AcyclicError(
            f"no acyclic binary at {candidate!r}; set ACYCLIC_BIN or put `acyclic` on PATH"
        )
    return found


class Engine:
    """Runs the `acyclic` binary. One instance can serve many sessions."""

    def __init__(self, binary: str | os.PathLike[str] | None = None, *, timeout: float = _HOOK_TIMEOUT) -> None:
        self.binary = find_binary(binary)
        self.timeout = timeout

    async def _run(self, argv: list[str], cwd: Path, stdin: bytes | None = None) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            self.binary,
            *argv,
            cwd=str(cwd),
            stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(stdin), self.timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise AcyclicError(f"acyclic {' '.join(argv)} timed out after {self.timeout:.0f}s") from None
        return proc.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")

    async def hook(self, event: str, payload: dict[str, Any], cwd: Path) -> dict[str, Any]:
        code, out, err = await self._run(["__hook", HOST, event], cwd, json.dumps(payload).encode())
        if code != 0:
            detail = (err or out).strip()
            if "unsupported native hook host" in detail or "unrecognized" in detail.lower():
                detail += " (this acyclic binary predates the `sdk` host; install acyclic 0.1 or newer)"
            raise AcyclicError(f"acyclic hook {event} failed: {detail}")
        return json.loads(out) if out.strip() else {}

    async def cli(self, *args: str, cwd: Path) -> str:
        code, out, err = await self._run(list(args), cwd)
        if code != 0:
            raise AcyclicError(f"acyclic {' '.join(args)} failed: {(err or out).strip()}")
        return out

    async def cli_json(self, *args: str, cwd: Path) -> dict[str, Any]:
        out = await self.cli(*args, cwd=cwd)
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            raise AcyclicError(f"acyclic {' '.join(args)} did not return JSON: {out.strip()[:300]}") from None


class Session:
    """One acyclic session rooted at a repository.

    Use it as an async context manager; ``session.root`` is the workspace for
    the repository itself, and every fork descends from it::

        async with Session.open(".") as session:
            await planner.run(task, deps=session.root)
    """

    def __init__(self, repo: Path, engine: Engine, session_id: str) -> None:
        self.repo = repo
        self.engine = engine
        self.id = session_id
        self.root = Workspace(self, repo, None, None)
        # The engine keeps one pending spawn per session: a PreToolUse(Agent)
        # must be followed by its SubagentStart before the next spawn.
        self._spawn_lock = asyncio.Lock()
        self._open = False

    @classmethod
    def open(
        cls,
        repo: str | os.PathLike[str] = ".",
        *,
        binary: str | os.PathLike[str] | None = None,
        engine: Engine | None = None,
        session_id: str | None = None,
    ) -> "Session":
        return cls(Path(repo).resolve(), engine or Engine(binary), session_id or f"sdk-{uuid.uuid4().hex}")

    async def start(self) -> "Session":
        if not self._open:
            await self.engine.hook("SessionStart", {"session_id": self.id, "cwd": str(self.repo)}, self.repo)
            await self.engine.hook(
                "UserPromptSubmit",
                {"session_id": self.id, "cwd": str(self.repo), "turn_id": f"{self.id}:root"},
                self.repo,
            )
            self._open = True
        return self

    async def close(self) -> None:
        if self._open:
            self._open = False
            await self.engine.hook("SessionEnd", {"session_id": self.id, "cwd": str(self.repo)}, self.repo)

    async def __aenter__(self) -> "Session":
        return await self.start()

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def agents(self) -> list[dict[str, Any]]:
        status = await self.engine.cli_json("agents", "--json", cwd=self.repo)
        return list(status.get("agents", []))


@dataclass
class Workspace:
    """A working tree the engine manages: the repo root, or a forked child.

    Hand ``path`` to whatever does the work (tools, shell commands). A child
    sees its parent's tree as of the fork and nothing its siblings write.
    """

    session: Session
    path: Path
    agent_id: str | None
    parent: "Workspace | None"
    state: str = field(default="running")

    @property
    def is_root(self) -> bool:
        return self.agent_id is None

    @property
    def ref(self) -> str:
        return "root" if self.agent_id is None else f"agents/{self.agent_id}"

    def _base(self, **extra: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {"session_id": self.session.id, "cwd": str(self.path), **extra}
        return payload

    async def spawn(self, name: str | None = None) -> "Workspace":
        """Fork this workspace into a new child and return the child."""
        if self.state != "running":
            raise AcyclicError(f"{self.ref} is {self.state}; only a running workspace can fork")
        await self.session.start()
        agent_id = f"{_slug(name or 'worker')}-{uuid.uuid4().hex[:8]}"
        caller = {} if self.agent_id is None else {"agent_id": self.agent_id}
        engine = self.session.engine
        async with self.session._spawn_lock:
            await engine.hook(
                "PreToolUse",
                self._base(tool_name="Agent", tool_use_id=f"spawn-{agent_id}", tool_input={}, **caller),
                self.path,
            )
            await engine.hook("SubagentStart", self._base(agent_id=agent_id, agent_type="sdk"), self.path)
        mount = await self._mount_of(agent_id)
        return Workspace(self.session, mount, agent_id, self)

    async def _mount_of(self, agent_id: str) -> Path:
        for agent in await self.session.agents():
            if agent.get("ref") == f"agents/{agent_id}" and agent.get("mount"):
                return Path(agent["mount"])
        raise AcyclicError(f"acyclic started agents/{agent_id} but reported no mount for it")

    async def stop(self) -> None:
        """Freeze this child. Its parent can then merge or discard it."""
        if self.agent_id is None or self.state != "running":
            return
        # Never from inside the child's own mount: stopping unmounts it, and a
        # hook process whose cwd is in the mount would hold it busy.
        where = self.parent.path if self.parent is not None else self.session.repo
        await self.session.engine.hook("SubagentStop", self._base(agent_id=self.agent_id), where)
        self.state = "stopped"

    async def changes(self) -> list[str]:
        """Paths this child changed relative to its fork point, repo-relative."""
        return sorted(set(await self._changed_paths()))

    async def _changed_paths(self) -> list[str]:
        if self.agent_id is None:
            return []
        for agent in await self.session.agents():
            if agent.get("ref") == self.ref:
                return [p.lstrip("/") for p in agent.get("changedPaths") or []]
        return []

    async def merge(self) -> dict[str, Any]:
        """Stop this child and merge it into its direct parent."""
        parent = self._require_parent()
        await self.stop()
        engine = self.session.engine
        result = await engine.cli_json("git", "merge", self.ref, cwd=parent.path)
        if result.get("status") == "conflicted":
            conflicts = list(result.get("conflicts") or [])
            try:
                await engine.cli_json("git", "merge", "--abort", cwd=parent.path)
            except AcyclicError as error:
                raise MergeConflict(self.ref, conflicts, aborted=False, detail=str(error)) from None
            raise MergeConflict(self.ref, conflicts, aborted=True)
        if result.get("status") not in ("applied", "no-changes"):
            raise AcyclicError(f"merging {self.ref} returned {result!r}")
        self.state = "merged"
        return result

    async def discard(self) -> None:
        """Stop this child and throw it away, with everything it forked."""
        parent = self._require_parent()
        if self.state in ("discarded", "merged"):
            return
        await self.stop()
        await self.session.engine.cli("discard", self.ref, cwd=parent.path)
        self.state = "discarded"

    def _require_parent(self) -> "Workspace":
        if self.parent is None:
            raise AcyclicError("the root workspace has no parent to merge into")
        return self.parent


def _slug(name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in name.lower()).strip("-")
    return cleaned[:32] or "worker"
