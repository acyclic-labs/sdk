"""Many agents, one state, one encode.

An `Agent` looks at the state and asks typed questions. The `Swarm` collects
every agent's questions for a step, sends them to the backend in one batch
(so the state is prefilled once and every branch is scored from that
cache), hands each agent its decisions, appends everything to the log, and
lets each agent act. Agents never see the model; they see distributions.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional, Protocol, Sequence, runtime_checkable

from .types import Question, Decision, DecideResult, Timing, Plan
from .backends.base import Backend
from .log import DecisionLog


@runtime_checkable
class Agent(Protocol):
    name: str

    def ask(self, state: str, memory: dict) -> Sequence[Question]:
        """Questions to decide against this state. Return [] to sit this step out."""
        ...

    def act(self, decisions: Sequence[Decision], memory: dict) -> Any:
        """Called with this agent's decisions, in the order asked. Return value is kept in StepResult."""
        ...


class FunctionAgent:
    """An agent made from a fixed question list or an `ask(state, memory)` function."""

    def __init__(self, name: str, questions: Sequence[Question] | Callable[[str, dict], Sequence[Question]],
                 act: Optional[Callable[[Sequence[Decision], dict], Any]] = None):
        self.name = name
        self._questions = questions
        self._act = act

    def ask(self, state: str, memory: dict) -> Sequence[Question]:
        if callable(self._questions):
            return list(self._questions(state, memory))
        return list(self._questions)

    def act(self, decisions: Sequence[Decision], memory: dict) -> Any:
        if self._act is None:
            return None
        return self._act(decisions, memory)


@dataclass
class AgentResult:
    agent: str
    questions: list[Question]
    decisions: list[Decision]
    action: Any = None

    def __iter__(self):
        return iter(self.decisions)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.decisions[key]
        for q, d in zip(self.questions, self.decisions):  # by question id or text
            if key in (q.id, q.question):
                return d
        raise KeyError(key)

    def chosen(self) -> dict[str, str]:
        return {(q.id or q.question): d.chosen for q, d in zip(self.questions, self.decisions)}


@dataclass
class StepResult:
    step: int
    state_sha: str
    results: dict[str, AgentResult]
    timing: Timing
    plan: Plan
    wall_ms: float
    n_questions: int
    log_range: tuple[int, int]  # [start, end) seq numbers appended this step

    def __getitem__(self, agent: str) -> AgentResult:
        return self.results[agent]

    def decisions(self) -> list[Decision]:
        return [d for r in self.results.values() for d in r.decisions]

    def chosen(self) -> dict[str, dict[str, str]]:
        return {a: r.chosen() for a, r in self.results.items()}


class Swarm:
    """A set of agents deciding against a shared state through one backend."""

    def __init__(self, agents: Iterable[Agent], backend: Backend, log: Optional[DecisionLog] = None,
                 memory: Optional[dict] = None):
        self.agents: list[Agent] = list(agents)
        names = [a.name for a in self.agents]
        if len(set(names)) != len(names):
            raise ValueError(f"agent names must be unique: {names}")
        self.backend = backend
        self.log = log if log is not None else DecisionLog()
        self.memory: dict = memory if memory is not None else {}
        self.steps = 0
        self.history: list[StepResult] = []

    def add(self, agent: Agent) -> None:
        if any(a.name == agent.name for a in self.agents):
            raise ValueError(f"an agent named {agent.name!r} already exists")
        self.agents.append(agent)

    def step(self, state: str, *, meta: Optional[dict] = None) -> StepResult:
        """One round: every agent asks, one batched decide, every agent acts."""
        from .log import sha256
        t0 = time.perf_counter()
        state = str(state or "")
        asked: list[tuple[Agent, list[Question]]] = []
        flat: list[Question] = []
        for a in self.agents:
            mem = self.memory.setdefault(a.name, {})
            qs = list(a.ask(state, mem))
            asked.append((a, qs))
            flat.extend(qs)

        if flat:
            result = self.backend.decide(state, flat)
        else:
            result = DecideResult([], Timing(), Plan(), self.backend.name, self.backend.temperature)

        start = len(self.log)
        results: dict[str, AgentResult] = {}
        i = 0
        for a, qs in asked:
            ds = result.decisions[i:i + len(qs)]
            i += len(qs)
            if qs:
                self.log.append_result(state, a.name,
                                       DecideResult(ds, backend=result.backend, temperature=result.temperature),
                                       step=self.steps, meta=meta)
            results[a.name] = AgentResult(a.name, qs, ds)
        for a, qs in asked:
            r = results[a.name]
            r.action = a.act(r.decisions, self.memory[a.name]) if qs else None

        wall_ms = (time.perf_counter() - t0) * 1000.0
        if flat:
            self.log.step_cost(self.steps, state, result, wall_ms,
                               agents={a.name: len(qs) for a, qs in asked if qs}, meta=meta)
        out = StepResult(self.steps, sha256(state), results, result.timing, result.plan,
                         wall_ms, len(flat), (start, len(self.log)))
        self.steps += 1
        self.history.append(out)
        return out

    def run(self, states: Iterable[str], *, until: Optional[Callable[[StepResult], bool]] = None) -> list[StepResult]:
        """Step over a stream of states; stop early when `until(step)` is true."""
        out = []
        for s in states:
            r = self.step(s)
            out.append(r)
            if until and until(r):
                break
        return out

    def decide(self, state: str, questions: Sequence[Question], agent: str = "swarm") -> list[Decision]:
        """A one-off decision outside the agent loop, still logged."""
        res = self.backend.decide(state, questions)
        self.log.append_result(state, agent, res, step=self.steps)
        return res.decisions
