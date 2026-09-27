"""jev over acyclic forks: fork N ways, let anything work in the forks, then
let System One judge which fork lands.

All fork diffs go into ONE shared state, so the swarm encodes the arena once
and every judgement (which fork wins, is fork X safe, how complete is fork X)
is scored from that cache in a single batch. Every fork id, base generation,
decision, cost, promotion and drop is written to the log.

Talks to the `acyclic` binary over subprocess, like the pydantic-ai adapter.
"""
from __future__ import annotations

import difflib
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from .types import Question, Decision
from .backends.base import Backend
from .log import DecisionLog
from .swarm import Swarm, FunctionAgent, StepResult

LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
Runner = Callable[[list[str]], str]  # argv (without the binary) -> stdout


@dataclass
class Fork:
    id: str
    path: str
    base: str = ""
    label: str = ""

    @property
    def name(self) -> str:
        return f"fork {self.label}" if self.label else self.id


@dataclass
class ForkChange:
    status: str  # A M D R
    path: str


class AcyclicCLI:
    """Thin, parsing wrapper over the `acyclic` CLI. `runner` can be swapped for tests."""

    _FORK = re.compile(r"^fork\s+([0-9a-f]+)\s+\((\w+)\)\s+(\S+)")
    _LIST = re.compile(r"^([0-9a-f]+)\s+(\w+)\s+(.+?)\s+(\S+)\s+base\s+([0-9a-f]+)")
    _CHANGE = re.compile(r"^([AMDR])\s+(.+)$")

    def __init__(self, repo: str = ".", binary: Optional[str] = None, runner: Optional[Runner] = None,
                 timeout: float = 120.0):
        self.repo = os.path.abspath(repo)
        self.binary = binary or os.environ.get("ACYCLIC_BIN") or shutil.which("acyclic") or "acyclic"
        self.timeout = timeout
        self._runner = runner
        self.last_output = ""

    def run(self, *args: str) -> str:
        argv = [str(a) for a in args]
        if self._runner:
            out = self._runner(argv)
        else:
            p = subprocess.run([self.binary, "--repo", self.repo, *argv], capture_output=True, text=True,
                               timeout=self.timeout)
            out = p.stdout
            if p.returncode != 0:
                raise RuntimeError(f"acyclic {' '.join(argv)} failed ({p.returncode}): {(p.stderr or out).strip()}")
        self.last_output = out
        return out

    def fork(self, n: int = 2) -> list[Fork]:
        out = self.run("fork", "-n", str(n))
        forks = [Fork(m.group(1), m.group(3)) for m in map(self._FORK.match, out.splitlines()) if m]
        if len(forks) != n:
            raise RuntimeError(f"asked for {n} forks, parsed {len(forks)} from:\n{out}")
        bases = {f.id: f.base for f in self.forks()}
        for i, f in enumerate(forks):
            f.base = bases.get(f.id, "")
            f.label = LABELS[i] if i < len(LABELS) else str(i)
        return forks

    def forks(self) -> list[Fork]:
        try:
            out = self.run("forks")
        except RuntimeError:
            out = self.run("fork-list")  # newer builds
        return [Fork(m.group(1), m.group(4), m.group(5)) for m in map(self._LIST.match, out.splitlines()) if m]

    def fork_diff(self, fork_id: str) -> list[ForkChange]:
        out = self.run("fork-diff", fork_id)
        return [ForkChange(m.group(1), m.group(2)) for m in map(self._CHANGE.match, out.splitlines()) if m]

    def drop(self, fork_id: str) -> str:
        return self.run("fork-drop", fork_id).strip()

    def promote(self, *fork_ids: str) -> str:
        return self.run("promote", *fork_ids).strip()

    def checkpoint(self, message: str = "") -> str:
        return self.run("checkpoint", *(["-m", message] if message else [])).strip()

    def status(self) -> str:
        return self.run("status")


# ----------------------------------------------------------------- rendering

def unified_diff(base_path: str, fork_path: str, rel: str, max_chars: int) -> str:
    def read(p):
        try:
            with open(p, encoding="utf-8", errors="replace") as f:
                return f.read().splitlines(keepends=True)
        except (FileNotFoundError, IsADirectoryError):
            return []
    a, b = read(os.path.join(base_path, rel)), read(os.path.join(fork_path, rel))
    text = "".join(difflib.unified_diff(a, b, "a/" + rel, "b/" + rel, n=2))
    if len(text) > max_chars:
        text = text[:max_chars] + f"\n... (+{len(text) - max_chars} chars)\n"
    return text


def render_fork(repo: str, fork: Fork, changes: Sequence[ForkChange], probe: Optional[str],
                max_files: int, max_chars_per_file: int) -> str:
    lines = [f"=== {fork.name} ({fork.id})", f"Changed paths: {len(changes)}"]
    for c in changes[:max_files]:
        lines.append(f"  {c.status} {c.path}")
    if len(changes) > max_files:
        lines.append(f"  ... {len(changes) - max_files} more")
    for c in changes[:max_files]:
        if c.status in ("A", "M"):
            lines.append(unified_diff(repo, fork.path, c.path, max_chars_per_file).rstrip())
    if probe:
        lines += ["Probe output:", probe.strip()]
    return "\n".join(lines)


def render_arena(task: str, blocks: Sequence[str]) -> str:
    return "Task:\n" + task.strip() + "\n\n" + "\n\n".join(blocks)


# --------------------------------------------------------------------- arena

@dataclass
class Verdict:
    winner: Optional[Fork]
    winner_conf: float
    ranking: list[tuple[Fork, float]]           # by P(winner)
    per_fork: dict[str, dict[str, Decision]]    # label -> {"safe": ..., "complete": ...}
    step: StepResult
    state: str
    promoted: Optional[str] = None
    dropped: list[str] = field(default_factory=list)
    note: str = ""


class ForkArena:
    """Fork, work, judge, land.

        arena = ForkArena(AcyclicCLI(repo), SpaceBackend(), DecisionLog("run.jsonl"), task="...")
        forks = arena.open(3)
        ... do work in each fork.path (any agent, any tool) ...
        v = arena.judge(probe=lambda f: run_tests_in(f.path))
        arena.resolve(v)       # promote the winner, drop the rest, all logged
    """

    def __init__(self, cli: AcyclicCLI, backend: Backend, log: Optional[DecisionLog] = None, *,
                 task: str = "", max_files: int = 12, max_chars_per_file: int = 1_500,
                 extra_questions: Optional[Callable[[list[Fork]], Sequence[Question]]] = None):
        self.cli = cli
        self.backend = backend
        self.log = log if log is not None else DecisionLog()
        self.task = task
        self.max_files = max_files
        self.max_chars_per_file = max_chars_per_file
        self.extra_questions = extra_questions
        self.forks: list[Fork] = []
        self.swarm = Swarm([], backend, self.log)

    # -- lifecycle -------------------------------------------------------------
    def open(self, n: int = 2) -> list[Fork]:
        self.forks = self.cli.fork(n)
        self.log.note("fork_open", repo=self.cli.repo, task=self.task,
                      forks=[f.__dict__ for f in self.forks])
        return self.forks

    def adopt(self, forks: Optional[Sequence[Fork]] = None) -> list[Fork]:
        """Judge forks that already exist (opened by someone else)."""
        self.forks = list(forks) if forks is not None else self.cli.forks()
        for i, f in enumerate(self.forks):
            f.label = f.label or (LABELS[i] if i < len(LABELS) else str(i))
        self.log.note("fork_adopt", repo=self.cli.repo, forks=[f.__dict__ for f in self.forks])
        return self.forks

    # -- judging ---------------------------------------------------------------
    def state(self, probe: Optional[Callable[[Fork], str]] = None) -> tuple[str, dict[str, list[ForkChange]]]:
        blocks, changes = [], {}
        for f in self.forks:
            ch = self.cli.fork_diff(f.id)
            changes[f.id] = ch
            p = probe(f) if probe else None
            blocks.append(render_fork(self.cli.repo, f, ch, p, self.max_files, self.max_chars_per_file))
        return render_arena(self.task, blocks), changes

    def questions(self) -> tuple[Question, list[tuple[Fork, Question, Question]]]:
        labels = tuple(f.name for f in self.forks)
        pick = Question("Which fork best completes the task and should be promoted?", labels, id="winner")
        per = []
        for f in self.forks:
            safe = Question.yes_no(f"Is {f.name} safe to land as-is, without breaking existing behaviour?",
                                   id=f"safe:{f.label}")
            complete = Question.score(f"How completely does {f.name} accomplish the task, 1 (not at all) to 5 (fully)?",
                                      1, 5, id=f"complete:{f.label}")
            per.append((f, safe, complete))
        return pick, per

    def judge(self, probe: Optional[Callable[[Fork], str]] = None, meta: Optional[dict] = None) -> Verdict:
        if len(self.forks) < 2:
            raise ValueError("need at least two forks to judge; open() or adopt() first")
        state, changes = self.state(probe)
        pick, per = self.questions()
        agents = [FunctionAgent("picker", [pick]),
                  FunctionAgent("safety", [s for _, s, _ in per]),
                  FunctionAgent("completeness", [c for _, _, c in per])]
        if self.extra_questions:
            agents.append(FunctionAgent("extra", list(self.extra_questions(self.forks))))
        self.swarm.agents = agents
        m = {"arena": True, "forks": {f.label: f.id for f in self.forks}, **(meta or {})}
        step = self.swarm.step(state, meta=m)
        d_pick = step["picker"][0]
        ranking = sorted(((f, d_pick.prob(f.name)) for f in self.forks), key=lambda x: -x[1])
        per_fork = {f.label: {"safe": step["safety"][f"safe:{f.label}"],
                              "complete": step["completeness"][f"complete:{f.label}"]} for f in self.forks}
        winner, conf = ranking[0]
        return Verdict(winner, conf, ranking, per_fork, step, state)

    # -- landing ---------------------------------------------------------------
    def resolve(self, v: Verdict, *, promote: bool = True, drop_losers: bool = True, min_conf: float = 0.0,
                require_safe: bool = False) -> Verdict:
        w = v.winner
        ok = w is not None and v.winner_conf >= min_conf and (not require_safe or v.per_fork[w.label]["safe"].yes)
        if promote and ok:
            out = self.cli.promote(w.id)
            v.promoted = w.id
            v.note = out
            self.log.note("promote", fork=w.id, label=w.label, conf=v.winner_conf, output=out)
        elif promote:
            reason = "below min_conf" if w is not None and v.winner_conf < min_conf else "winner judged unsafe"
            v.note = f"not promoted: {reason}"
            self.log.note("promote_skipped", fork=w.id if w else None, conf=v.winner_conf, reason=reason)
        if drop_losers:
            for f in self.forks:
                if f.id != v.promoted:
                    self.cli.drop(f.id)
                    v.dropped.append(f.id)
                    self.log.note("fork_drop", fork=f.id, label=f.label)
        return v

    def drop_all(self) -> None:
        for f in self.forks:
            self.cli.drop(f.id)
            self.log.note("fork_drop", fork=f.id, label=f.label)
        self.forks = []
