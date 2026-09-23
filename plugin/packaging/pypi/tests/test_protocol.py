"""What the package sends the engine, checked against a fake binary."""

from __future__ import annotations

import pytest
from pydantic_ai import Agent, RunContext

from acyclic_pydantic_ai import MergeConflict, Session, Workspace, fork, speculate, workspace_tools

from .scripted import scripted, writes


def worker(*files: tuple[str, str]) -> Agent[Workspace, str]:
    return Agent(writes(*files), deps_type=Workspace, toolsets=[workspace_tools(shell=False)])


async def test_session_and_fork_fire_the_sdk_handshake_in_order(fake_engine):
    async with Session.open(fake_engine.root) as session:
        async with fork(session.root, name="part") as ws:
            (ws.path / "new.txt").write_text("child\n")
            assert not (fake_engine.root / "new.txt").exists()
    assert (fake_engine.root / "new.txt").read_text() == "child\n"

    events = [event for event, _ in fake_engine.hooks()]
    assert events == [
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "SubagentStart",
        "SubagentStop",
        "SessionEnd",
    ]
    assert all(c["argv"][1] == "sdk" for c in fake_engine.calls() if c["argv"][0] == "__hook")
    _, spawn = fake_engine.hooks()[2]
    assert spawn["tool_name"] == "Agent" and "agent_id" not in spawn  # the root is the caller
    merge = [c for c in fake_engine.calls() if c["argv"][:2] == ["git", "merge"]]
    assert merge[0]["cwd"] == str(fake_engine.root)  # merged from the parent's path


async def test_a_nested_fork_names_its_parent_as_the_caller(fake_engine):
    async with Session.open(fake_engine.root) as session:
        async with fork(session.root, name="outer") as outer:
            async with fork(outer, name="inner") as inner:
                (inner.path / "deep.txt").write_text("deep\n")
            assert (outer.path / "deep.txt").exists()
            assert not (fake_engine.root / "deep.txt").exists()  # one level at a time
    assert (fake_engine.root / "deep.txt").exists()

    spawns = [payload for event, payload in fake_engine.hooks() if event == "PreToolUse"]
    assert "agent_id" not in spawns[0]
    assert spawns[1]["agent_id"] == outer.agent_id
    assert spawns[1]["cwd"] == str(outer.path)
    merges = [c for c in fake_engine.calls() if c["argv"][:2] == ["git", "merge"]]
    assert [m["cwd"] for m in merges] == [str(outer.path), str(fake_engine.root)]


async def test_an_exception_discards_the_child_and_leaves_the_parent_alone(fake_engine):
    async with Session.open(fake_engine.root) as session:
        with pytest.raises(RuntimeError, match="worker blew up"):
            async with fork(session.root) as ws:
                (ws.path / "half.txt").write_text("half-finished\n")
                raise RuntimeError("worker blew up")
        assert not (fake_engine.root / "half.txt").exists()
        assert ws.state == "discarded"
        assert await session.agents() == []


async def test_a_conflicting_merge_is_aborted_and_raised(fake_engine, monkeypatch):
    monkeypatch.setenv("FAKE_CONFLICT", "base.txt")
    async with Session.open(fake_engine.root) as session:
        with pytest.raises(MergeConflict) as caught:
            async with fork(session.root) as ws:
                (ws.path / "base.txt").write_text("changed\n")
    assert caught.value.aborted and caught.value.conflicts[0]["path"] == "/base.txt"
    assert (fake_engine.root / "base.txt").read_text() == "base\n"


async def test_a_failed_abort_is_reported_not_hidden(fake_engine, monkeypatch):
    monkeypatch.setenv("FAKE_CONFLICT", "base.txt")
    monkeypatch.setenv("FAKE_ABORT_FAILS", "1")
    async with Session.open(fake_engine.root) as session:
        with pytest.raises(MergeConflict) as caught:
            async with fork(session.root) as ws:
                (ws.path / "base.txt").write_text("changed\n")
    assert not caught.value.aborted
    assert "path lookup batch is empty" in str(caught.value)


async def test_fork_from_a_run_context_uses_the_agents_workspace(fake_engine):
    part_worker = worker(("part.txt", "from the worker\n"))
    planner: Agent[Workspace, str] = Agent(
        scripted(lambda _p: [("delegate", {"part": "write part.txt"})]), deps_type=Workspace
    )

    @planner.tool
    async def delegate(ctx: RunContext[Workspace], part: str) -> str:
        async with fork(ctx, name="part") as ws:
            result = await part_worker.run(part, deps=ws)
        return result.output

    async with Session.open(fake_engine.root) as session:
        await planner.run("split the work", deps=session.root)
    assert (fake_engine.root / "part.txt").read_text() == "from the worker\n"


async def test_speculate_merges_the_passing_attempt_with_fewest_changes(fake_engine):
    attempts = [
        worker(("answer.txt", "wrong\n")),
        worker(("answer.txt", "right\n")),
        worker(("answer.txt", "right\n"), ("extra.txt", "noise\n")),
    ]
    async with Session.open(fake_engine.root) as session:
        result = await speculate(
            attempts, "fix it", parent=session.root, check="grep -q right answer.txt"
        )
        assert [a.passed for a in result.attempts] == [False, True, True]
        assert result.winner is result.attempts[1]
        assert [a.workspace.state for a in result.attempts] == ["discarded", "merged", "discarded"]
        assert result.attempts[2].changes == ["answer.txt", "extra.txt"]
        assert await session.agents() == [
            a for a in await session.agents() if a["state"] == "merged"
        ]
    assert (fake_engine.root / "answer.txt").read_text() == "right\n"
    assert not (fake_engine.root / "extra.txt").exists()


async def test_what_the_check_writes_is_never_merged(fake_engine):
    check = "mkdir -p .pytest_cache && touch .pytest_cache/v probe.log && grep -q right answer.txt"
    async with Session.open(fake_engine.root) as session:
        result = await speculate(
            [worker(("answer.txt", "right\n"))], "fix it", parent=session.root, check=check
        )
    assert result.winner is not None and result.winner.changes == ["answer.txt"]
    assert not (fake_engine.root / ".pytest_cache").exists()
    assert not (fake_engine.root / "probe.log").exists()


async def test_speculate_with_no_passing_attempt_leaves_the_parent_untouched(fake_engine):
    async with Session.open(fake_engine.root) as session:
        result = await speculate(
            [worker(("answer.txt", "wrong\n"))], "fix it", parent=session.root, check="false"
        )
    assert result.winner is None and result.output is None
    assert not (fake_engine.root / "answer.txt").exists()


async def test_spawns_are_serialised_so_concurrent_forks_never_collide(fake_engine):
    import asyncio

    async with Session.open(fake_engine.root) as session:
        children = await asyncio.gather(*(session.root.spawn(f"c{i}") for i in range(4)))
        assert len({c.path for c in children}) == 4
        for child in children:
            await child.discard()
