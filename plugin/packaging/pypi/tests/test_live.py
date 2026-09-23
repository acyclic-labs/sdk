"""The pitch, run for real: Pydantic AI agents on the actual acyclic engine.

Scripted models (no credentials, no network) drive a planner that splits a
migration into parts, a part that splits itself again, a part that fails, and
a risky step speculated three ways. Skipped unless a real `acyclic` binary is
available (ACYCLIC_LIVE_BIN, or this checkout's target/debug build). All engine
state lives in a temporary XDG_STATE_HOME.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic_ai import Agent, RunContext

from acyclic_pydantic_ai import Engine, Session, Workspace, fork, speculate, workspace_tools

from .conftest import live_binary
from .scripted import scripted, writes

BINARY = live_binary()
pytestmark = pytest.mark.skipif(BINARY is None, reason="no live acyclic binary (set ACYCLIC_LIVE_BIN)")

CHECK = f"{sys.executable} -c 'import retries, sys; sys.exit(0 if retries.ok() else 1)'"


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    root = tmp_path / "billing"
    root.mkdir()
    (root / "README.md").write_text("billing service\n")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    return root


def tree(root: Path) -> list[str]:
    return sorted(
        str(p.relative_to(root))
        for p in root.rglob("*")
        if p.is_file() and ".git" not in p.relative_to(root).parts
    )


def build_planner() -> Agent[Workspace, str]:
    tools = [workspace_tools(shell=False)]
    models_worker = Agent(writes(("models.py", "class Charge: ...\n")), deps_type=Workspace, toolsets=tools)
    leaf = {
        "refunds": Agent(writes(("refunds.py", "def on_refund(e): ...\n")), deps_type=Workspace, toolsets=tools),
        "disputes": Agent(writes(("disputes.py", "def on_dispute(e): ...\n")), deps_type=Workspace, toolsets=tools),
    }
    handlers_worker: Agent[Workspace, str] = Agent(
        scripted(lambda _p: [("split", {"event": "refunds"}), ("split", {"event": "disputes"})]),
        deps_type=Workspace,
    )

    @handlers_worker.tool
    async def split(ctx: RunContext[Workspace], event: str) -> str:
        async with fork(ctx, name=event) as ws:  # a grandchild of the root
            return (await leaf[event].run(f"handle {event}", deps=ws)).output

    attempts = [
        Agent(writes(("retries.py", "def ok():\n    return False\n")), deps_type=Workspace, toolsets=tools),
        Agent(writes(("retries.py", "def ok():\n    return True\n")), deps_type=Workspace, toolsets=tools),
        Agent(
            writes(("retries.py", "def ok():\n    return True\n"), ("outbox.py", "TABLE = 'outbox'\n")),
            deps_type=Workspace,
            toolsets=tools,
        ),
    ]

    planner: Agent[Workspace, str] = Agent(
        scripted(
            lambda _p: [
                ("delegate", {"part": "models"}),
                ("delegate", {"part": "handlers"}),
                ("delegate", {"part": "broken"}),
                ("risky", {"step": "idempotent retries"}),
            ]
        ),
        deps_type=Workspace,
    )

    @planner.tool
    async def delegate(ctx: RunContext[Workspace], part: str) -> str:
        try:
            async with fork(ctx, name=part) as ws:
                if part == "broken":
                    (ws.path / "half-done.py").write_text("# abandoned\n")
                    raise RuntimeError("this part failed")
                worker = models_worker if part == "models" else handlers_worker
                return (await worker.run(f"do {part}", deps=ws)).output
        except RuntimeError as error:
            return f"failed: {error}"

    @planner.tool
    async def risky(ctx: RunContext[Workspace], step: str) -> str:
        outcome = await speculate(attempts, step, parent=ctx, check=CHECK)
        planner.state = outcome  # type: ignore[attr-defined]
        return "no attempt passed" if outcome.winner is None else f"kept attempt {outcome.winner.index}"

    return planner


async def test_decompose_and_speculate_against_the_real_engine(repo: Path):
    planner = build_planner()
    engine = Engine(BINARY)
    async with Session.open(repo, engine=engine) as session:
        result = await planner.run("Move billing to the v2 payments API", deps=session.root)
        assert result.output == "done"
        agents = {a["ref"].split("/")[1].rsplit("-", 1)[0]: a for a in await session.agents()}

    # Every part landed in the real tree; the failed part and the losing attempts did not.
    assert tree(repo) == [
        "README.md",
        "disputes.py",
        "models.py",
        "refunds.py",
        "retries.py",
    ]
    assert (repo / "retries.py").read_text() == "def ok():\n    return True\n"

    outcome = planner.state  # type: ignore[attr-defined]
    assert [a.passed for a in outcome.attempts] == [False, True, True]
    assert outcome.winner.index == 1  # both 1 and 2 passed; 1 changed fewer paths
    assert outcome.attempts[2].changes == ["outbox.py", "retries.py"]

    # The engine saw a real tree: grandchildren under `handlers`, one level at a time.
    assert agents["refunds"]["parent"] == agents["handlers"]["ref"]
    assert agents["refunds"]["depth"] == 2
    assert "broken" not in agents  # discarded with its subtree


async def test_only_the_direct_parent_can_merge(repo: Path):
    engine = Engine(BINARY)
    async with Session.open(repo, engine=engine) as session:
        outer = await session.root.spawn("outer")
        inner = await outer.spawn("inner")
        (inner.path / "deep.txt").write_text("deep\n")
        await inner.stop()
        refused = await asyncio.create_subprocess_exec(
            BINARY, "git", "merge", inner.ref, cwd=str(repo),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await refused.communicate()
        assert refused.returncode != 0 and b"only the direct parent" in out
        await inner.merge()
        assert (outer.path / "deep.txt").exists() and not (repo / "deep.txt").exists()
        await outer.merge()
    assert (repo / "deep.txt").read_text() == "deep\n"
    assert json.loads(json.dumps(tree(repo))) == ["README.md", "deep.txt"]


async def test_a_fork_can_create_directories(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        async with fork(session.root, name="dirs") as ws:
            (ws.path / "handlers").mkdir()
            (ws.path / "handlers" / "refunds.py").write_text("x\n")
            assert "refunds.py" in os.listdir(ws.path / "handlers")
    assert (repo / "handlers" / "refunds.py").read_text() == "x\n"
