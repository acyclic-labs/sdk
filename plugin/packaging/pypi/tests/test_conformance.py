"""Black-box fork/join conformance on the real engine.

Every assertion goes through the public surface only: the `__hook sdk` events,
`acyclic git merge/discard`, `acyclic agents --json`, and POSIX operations on
the mounts the engine hands out. Nothing here knows how the filesystem layer
is built, so these must hold whatever replaces it. Skipped without a live
binary (ACYCLIC_LIVE_BIN, or this checkout's target/debug build).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from acyclic_pydantic_ai import Engine, MergeConflict, Session

from .conftest import live_binary

BINARY = live_binary()
pytestmark = pytest.mark.skipif(BINARY is None, reason="no live acyclic binary (set ACYCLIC_LIVE_BIN)")


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    root = tmp_path / "repo"
    (root / "pkg").mkdir(parents=True)
    (root / "README.md").write_text("base\n")
    (root / "pkg" / "shared.py").write_text("VALUE = 1\n")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    return root


def files(root: Path) -> list[str]:
    return sorted(
        str(p.relative_to(root))
        for p in root.rglob("*")
        if p.is_file() and ".git" not in p.relative_to(root).parts
    )


@pytest.mark.xfail(
    strict=True,
    reason="known engine gap: deleting a file the fork only read lazily from the physical "
    "root is not propagated by the merge (the fork's base never recorded it); see "
    "BUG_REPORT-fork-join.md, open item A",
)
async def test_posix_operations_inside_a_fork(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        ws = await session.root.spawn("posix")
        (ws.path / "new" / "deeper").mkdir(parents=True)
        (ws.path / "new" / "deeper" / "a.py").write_text("A = 1\n")
        assert os.listdir(ws.path / "new" / "deeper") == ["a.py"]
        (ws.path / "new" / "deeper" / "a.py").rename(ws.path / "new" / "b.py")
        assert sorted(os.listdir(ws.path / "new")) == ["b.py", "deeper"]
        (ws.path / "new" / "deeper").rmdir()
        (ws.path / "README.md").unlink()
        (ws.path / "pkg" / "shared.py").write_text("VALUE = 2\n")
        assert (ws.path / "pkg" / "shared.py").read_text() == "VALUE = 2\n"
        assert not (repo / "new").exists(), "a fork's writes must not reach its parent before merge"
        await ws.merge()
    assert files(repo) == ["new/b.py", "pkg/shared.py"]
    assert (repo / "pkg" / "shared.py").read_text() == "VALUE = 2\n"


async def test_posix_operations_inside_a_fork_without_source_deletion(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        ws = await session.root.spawn("posix")
        (ws.path / "new" / "deeper").mkdir(parents=True)
        (ws.path / "new" / "deeper" / "a.py").write_text("A = 1\n")
        assert os.listdir(ws.path / "new" / "deeper") == ["a.py"]
        (ws.path / "new" / "deeper" / "a.py").rename(ws.path / "new" / "b.py")
        assert sorted(os.listdir(ws.path / "new")) == ["b.py", "deeper"]
        (ws.path / "new" / "deeper").rmdir()
        (ws.path / "pkg" / "shared.py").write_text("VALUE = 2\n")
        assert not (repo / "new").exists(), "a fork's writes must not reach its parent before merge"
        await ws.merge()
    assert files(repo) == ["README.md", "new/b.py", "pkg/shared.py"]
    assert (repo / "pkg" / "shared.py").read_text() == "VALUE = 2\n"


async def test_parallel_siblings_adding_to_one_directory_both_land(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        a = await session.root.spawn("a")
        b = await session.root.spawn("b")
        (a.path / "pkg" / "a.py").write_text("a\n")
        (b.path / "pkg" / "b.py").write_text("b\n")
        await a.merge()
        await b.merge()
    assert files(repo) == ["README.md", "pkg/a.py", "pkg/b.py", "pkg/shared.py"]


async def test_a_sibling_edit_is_not_reverted_by_a_fork_that_never_touched_the_file(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        editor = await session.root.spawn("editor")
        bystander = await session.root.spawn("bystander")
        (editor.path / "pkg" / "shared.py").write_text("VALUE = 2\n")
        (bystander.path / "other.py").write_text("x\n")
        await editor.merge()
        await bystander.merge()
    assert (repo / "pkg" / "shared.py").read_text() == "VALUE = 2\n"
    assert files(repo) == ["README.md", "other.py", "pkg/shared.py"]


async def test_grandchildren_merge_into_their_parent_only(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        child = await session.root.spawn("child")
        grandchild = await child.spawn("grandchild")
        (grandchild.path / "deep.py").write_text("deep\n")
        await grandchild.merge()
        assert (child.path / "deep.py").read_text() == "deep\n"
        assert not (repo / "deep.py").exists()
        await child.merge()
    assert (repo / "deep.py").read_text() == "deep\n"


async def test_discard_drops_a_whole_subtree(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        child = await session.root.spawn("doomed")
        grandchild = await child.spawn("doomed-child")
        (child.path / "c.py").write_text("c\n")
        (grandchild.path / "g.py").write_text("g\n")
        await child.discard()
        refs = [a["ref"] for a in await session.agents()]
        assert child.ref not in refs and grandchild.ref not in refs
    assert files(repo) == ["README.md", "pkg/shared.py"]


async def test_a_conflict_is_reported_aborted_and_does_not_block_later_merges(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        first = await session.root.spawn("first")
        second = await session.root.spawn("second")
        third = await session.root.spawn("third")
        (first.path / "README.md").write_text("from first\n")
        (second.path / "README.md").write_text("from second\n")
        (third.path / "third.py").write_text("t\n")
        await first.merge()
        with pytest.raises(MergeConflict) as conflict:
            await second.merge()
        assert conflict.value.aborted
        await second.discard()
        await third.merge()
    assert (repo / "README.md").read_text() == "from first\n"
    assert files(repo) == ["README.md", "pkg/shared.py", "third.py"]


async def test_a_fork_keeps_creating_files_after_the_root_changes(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        waiting = await session.root.spawn("waiting")
        for name in ("one", "two"):
            ws = await session.root.spawn(name)
            (ws.path / f"{name}.py").write_text(f"{name}\n")
            await ws.merge()
        (waiting.path / "late.py").write_text("late\n")
        (waiting.path / "late-dir").mkdir()
        (waiting.path / "late-dir" / "x.py").write_text("x\n")
        await waiting.merge()
    assert files(repo) == ["README.md", "late-dir/x.py", "late.py", "one.py", "pkg/shared.py", "two.py"]


async def test_no_host_metadata_files_are_merged(repo: Path):
    async with Session.open(repo, engine=Engine(BINARY)) as session:
        ws = await session.root.spawn("meta")
        target = ws.path / "tagged.py"
        target.write_text("x\n")
        if hasattr(os, "setxattr"):  # Linux; the mount may not support user xattrs
            try:
                os.setxattr(target, "user.acyclic.test", b"1")
            except OSError:
                pass
        elif shutil.which("xattr"):  # macOS: stored as `._tagged.py` over NFS
            subprocess.run(["xattr", "-w", "com.acyclic.test", "1", str(target)], check=False)
        await ws.merge()
    assert not [p for p in files(repo) if Path(p).name.startswith("._")]
