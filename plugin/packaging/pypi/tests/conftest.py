"""A fake `acyclic` binary that models the engine's fork-join contract closely
enough to check what this package sends it: the hook sequence, who the caller
is, direct-parent-only merge/discard, and one pending spawn at a time.

Forks are plain directory copies; merges copy the child's changed files into
the parent. Every invocation is logged as one JSON line.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

import pytest

FAKE = r'''#!PYTHON
import fcntl, json, os, shutil, sys
from pathlib import Path

state_dir = Path(os.environ["FAKE_ACYCLIC_STATE"])
state_file = state_dir / "state.json"
# Like the real service, one invocation at a time: concurrent tool calls must
# not read a half-written state or lose each other's updates.
_serial = open(state_dir / "lock", "w")
fcntl.flock(_serial, fcntl.LOCK_EX)
state = json.loads(state_file.read_text()) if state_file.exists() else {"routes": {}, "pending": []}
argv = sys.argv[1:]
stdin = sys.stdin.read() if argv[:1] == ["__hook"] else ""
with open(state_dir / "calls.jsonl", "a") as log:
    log.write(json.dumps({"argv": argv, "cwd": os.getcwd(), "stdin": json.loads(stdin) if stdin else None}) + "\n")

def save():
    state_file.write_text(json.dumps(state))

def fail(msg):
    save(); sys.stderr.write(msg); sys.exit(1)

def path_of(agent):
    return state["root"] if agent == "root" else state["routes"][agent]["mount"]

def caller_of(cwd):
    cwd = os.path.realpath(cwd)
    for aid, r in state["routes"].items():
        if os.path.realpath(r["mount"]) == cwd:
            return aid
    return "root"

def files(root):
    out = {}
    for d, dirs, fs in os.walk(root):
        dirs[:] = [x for x in dirs if x != ".git"]
        for f in fs:
            p = os.path.join(d, f)
            out[os.path.relpath(p, root)] = Path(p).read_bytes()
    return out

def changed(aid):
    r = state["routes"][aid]
    base = {k: v.encode("latin1") for k, v in r["base"].items()}
    now = files(r["mount"])
    return sorted({k for k in set(base) | set(now) if base.get(k) != now.get(k)})

if argv[:2] == ["__hook", "sdk"]:
    event, payload = argv[2], json.loads(stdin)
    if event == "SessionStart":
        state["root"] = payload["cwd"]; save()
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": "SDK lifecycle routing is explicit"}}))
    elif event == "PreToolUse" and payload["tool_name"] == "Agent":
        if state["pending"]:
            fail("a subagent spawn handshake is already pending; retry after SubagentStart")
        state["pending"].append(payload.get("agent_id", "root")); save(); print("{}")
    elif event == "SubagentStart":
        if not state["pending"]:
            fail("subagent start has no serialized spawn or resume identity")
        parent = state["pending"].pop(0)
        aid = payload["agent_id"]
        mount = str(state_dir / "mounts" / aid)
        shutil.copytree(path_of(parent), mount, ignore=shutil.ignore_patterns(".git"))
        state["routes"][aid] = {"parent": parent, "mount": mount, "state": "running",
                                "base": {k: v.decode("latin1") for k, v in files(mount).items()}}
        save()
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "SubagentStart", "additionalContext": f"Your workspace mount is {mount}."}}))
    elif event == "SubagentStop":
        state["routes"][payload["agent_id"]]["state"] = "frozen"; save(); print("{}")
    else:
        save(); print("{}")
elif argv == ["agents", "--json"]:
    print(json.dumps({"agents": [
        {"ref": f"agents/{aid}", "mount": r["mount"], "state": r["state"],
         "parent": "root" if r["parent"] == "root" else f"agents/{r['parent']}",
         "changedPaths": ["/" + p for p in changed(aid)]}
        for aid, r in state["routes"].items() if r["state"] != "discarded"]}))
elif argv[:2] == ["git", "merge"] and argv[2] == "--abort":
    if os.environ.get("FAKE_ABORT_FAILS"):
        fail("multi-root publisher failed: workspace engine failure: path lookup batch is empty")
    print(json.dumps({"status": "aborted"}))
elif argv[:2] == ["git", "merge"]:
    aid = argv[2].removeprefix("agents/")
    r = state["routes"][aid]
    if r["parent"] != caller_of(os.getcwd()):
        fail("only the direct parent may perform this workspace-context operation")
    paths = changed(aid)
    conflict = [p for p in paths if (Path(path_of(r["parent"])) / p).exists()
                and p in os.environ.get("FAKE_CONFLICT", "").split(",")]
    if conflict:
        save(); print(json.dumps({"status": "conflicted", "conflicts": [{"path": "/" + p, "kind": "Content"} for p in conflict]}))
        sys.exit(0)
    for p in paths:
        src, dst = Path(r["mount"]) / p, Path(path_of(r["parent"])) / p
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True); shutil.copy2(src, dst)
        elif dst.exists():
            dst.unlink()
    r["state"] = "merged"; save(); print(json.dumps({"status": "applied"}))
elif argv[:1] == ["discard"]:
    aid = argv[1].removeprefix("agents/")
    r = state["routes"][aid]
    if r["parent"] != caller_of(os.getcwd()):
        fail("only the direct parent may discard this workspace")
    stack = [aid]
    while stack:
        cur = stack.pop()
        stack += [k for k, v in state["routes"].items() if v["parent"] == cur]
        state["routes"][cur]["state"] = "discarded"
        shutil.rmtree(state["routes"][cur]["mount"], ignore_errors=True)
    save(); print(json.dumps({"agent": aid, "status": "discarded"}))
else:
    fail("unsupported " + " ".join(argv))
'''


class FakeEngine:
    def __init__(self, root: Path, state: Path, binary: Path) -> None:
        self.root = root
        self.state = state
        self.binary = binary

    def calls(self) -> list[dict]:
        log = self.state / "calls.jsonl"
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def hooks(self) -> list[tuple[str, dict]]:
        return [(c["argv"][2], c["stdin"]) for c in self.calls() if c["argv"][:1] == ["__hook"]]


@pytest.fixture
def fake_engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeEngine:
    state = tmp_path / "state"
    state.mkdir()
    root = tmp_path / "repo"
    root.mkdir()
    (root / "base.txt").write_text("base\n")
    binary = tmp_path / "acyclic"
    binary.write_text(FAKE.replace("#!PYTHON", "#!" + sys.executable, 1))
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("FAKE_ACYCLIC_STATE", str(state))
    monkeypatch.setenv("ACYCLIC_BIN", str(binary))
    monkeypatch.delenv("FAKE_CONFLICT", raising=False)
    monkeypatch.delenv("FAKE_ABORT_FAILS", raising=False)
    return FakeEngine(root, state, binary)


def live_binary() -> str | None:
    """The real engine for live tests: ACYCLIC_LIVE_BIN, else this checkout's build."""
    explicit = os.environ.get("ACYCLIC_LIVE_BIN")
    if explicit:
        return explicit
    for profile in ("debug", "release"):
        candidate = Path(__file__).resolve().parents[4] / "target" / profile / "acyclic"
        if candidate.exists():
            return str(candidate)
    return None
