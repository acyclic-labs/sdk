"""An append-only, time-ordered, hash-chained log of everything a run does.

One JSON object per line. Every record has `seq`, `ts`, `kind`, `prev`, and
`hash`; the rest depends on `kind`:

  decision   one calibrated distribution: state sha, agent, question, options, probs
  step       one swarm step's cost: questions, branches, tokens cached vs naive, forwards, ms
  eval       accuracy / ECE / Brier of a set of decisions against labels
  note       anything else the caller wants on the record (fork ids, promotions, ...)

Nothing is ever rewritten. A replay reproduces every decision without the
model, and `verify()` detects truncation, reordering and accidental edits. The chain is
unkeyed (SHA-256 over the file's own contents from a public root), so it is not proof against
someone who can rewrite the whole file; anchor the tip hash elsewhere if you need that.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

from .types import Question, Decision, DecideResult, Timing
from .backends.base import Backend

GENESIS = "0" * 64


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


@dataclass
class Record:
    """One log line. `data` holds the kind-specific fields; convenience properties read the common ones."""
    seq: int
    ts: str
    kind: str
    data: dict[str, Any]
    prev: str = GENESIS
    hash: str = ""

    def body(self) -> dict:
        return {"seq": self.seq, "ts": self.ts, "kind": self.kind, "data": self.data, "prev": self.prev}

    def compute_hash(self) -> str:
        return sha256(_canon(self.body()))

    def to_json(self) -> str:
        return _canon({**self.body(), "hash": self.hash})

    @classmethod
    def from_json(cls, line: str) -> "Record":
        d = json.loads(line)
        return cls(d["seq"], d["ts"], d["kind"], d["data"], d["prev"], d["hash"])

    def __getattr__(self, name):  # r.agent, r.question, r.probs, ... read through to data
        data = self.__dict__.get("data")
        if data is not None and name in data:
            return data[name]
        raise AttributeError(name)

    # -- decision-record helpers ------------------------------------------------
    @property
    def chosen(self) -> str:
        return self.data["options"][self.data["chosen_index"]]

    @property
    def conf(self) -> float:
        return self.data["probs"][self.data["chosen_index"]]

    def to_question(self) -> Question:
        return Question(self.data["question"], tuple(self.data["options"]), self.data["type"])  # type: ignore[arg-type]

    def to_decision(self) -> Decision:
        return Decision.from_probs(self.to_question(), self.data["probs"])

    def key(self) -> str:
        return decision_key(self.data["state_sha"], self.to_question())


LogRecord = Record  # older name


def decision_key(state_sha: str, q: Question) -> str:
    return state_sha + "\x1d" + q.key()


class DecisionLog:
    """Append-only JSONL. Safe for many threads in one process; one writer per file across processes."""

    def __init__(self, path: Optional[str] = None):
        self.path = path
        self._lock = threading.Lock()
        self._records: list[Record] = []
        self._tip = GENESIS
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        r = Record.from_json(line)
                        self._records.append(r)
                        self._tip = r.hash
        self._fh = open(path, "a", encoding="utf-8") if path else None

    # -- writing ---------------------------------------------------------------
    def append(self, kind: str, data: dict[str, Any]) -> Record:
        with self._lock:
            r = Record(len(self._records), _now(), kind, data, self._tip)
            r.hash = r.compute_hash()
            self._records.append(r)
            self._tip = r.hash
            if self._fh:
                self._fh.write(r.to_json() + "\n")
                self._fh.flush()
            return r

    def note(self, event: str, **data) -> Record:
        return self.append("note", {"event": event, **data})

    def decision(self, state: str, agent: str, decision: Decision, *, backend: str = "", temperature: float = 0.0,
                 step: Optional[int] = None, meta: Optional[dict] = None, state_sha: Optional[str] = None) -> Record:
        return self.append("decision", {
            "state_sha": state_sha or sha256(state), "agent": agent, "question": decision.question.question,
            "type": decision.question.type, "options": list(decision.question.options),
            "probs": [float(p) for p in decision.probs], "chosen_index": decision.chosen_index,
            "backend": backend, "temperature": temperature, "step": step, "meta": dict(meta or {})})

    def append_result(self, state: str, agent: str, result: DecideResult, *, step: Optional[int] = None,
                      meta: Optional[dict] = None) -> list[Record]:
        sha = sha256(state)
        return [self.decision(state, agent, d, backend=result.backend, temperature=result.temperature,
                              step=step, meta=meta, state_sha=sha) for d in result.decisions]

    def step_cost(self, step: int, state: str, result: DecideResult, wall_ms: float, *, agents: Optional[dict] = None,
                  meta: Optional[dict] = None) -> Record:
        from .costs import cost_of
        c = cost_of(result, wall_ms)
        c.update({"step": step, "state_sha": sha256(state), "agents": agents or {}, "meta": dict(meta or {})})
        return self.append("step", c)

    # -- reading ---------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._records)

    def __iter__(self) -> Iterator[Record]:
        return iter(list(self._records))

    def __getitem__(self, i) -> Record:
        return self._records[i]

    @property
    def tip(self) -> str:
        return self._tip

    def kind(self, kind: str) -> list[Record]:
        return [r for r in self._records if r.kind == kind]

    def decisions(self) -> list[Record]:
        return self.kind("decision")

    def since(self, seq: int) -> list[Record]:
        return list(self._records[seq:])

    def by_agent(self, agent: str) -> list[Record]:
        return [r for r in self.decisions() if r.data["agent"] == agent]

    def by_state(self, state: str) -> list[Record]:
        sha = sha256(state)
        return [r for r in self.decisions() if r.data["state_sha"] == sha]

    def latest(self, state: str, q: Question) -> Optional[Record]:
        key = decision_key(sha256(state), q)
        for r in reversed(self._records):
            if r.kind == "decision" and r.key() == key:
                return r
        return None

    def costs(self) -> dict:
        """Totals over every `step` record: what this log's runs cost so far."""
        from .costs import summarize
        return summarize([r.data for r in self.kind("step")])

    def verify(self) -> None:
        """Raise ValueError at the first record whose hash or chain link does not match."""
        prev = GENESIS
        for i, r in enumerate(self._records):
            if r.seq != i:
                raise ValueError(f"record {i}: seq is {r.seq}")
            if r.prev != prev:
                raise ValueError(f"record {i}: prev {r.prev[:12]} does not chain to {prev[:12]}")
            if r.compute_hash() != r.hash:
                raise ValueError(f"record {i}: hash mismatch (edited?)")
            prev = r.hash

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class ReplayBackend(Backend):
    """Answer decisions from a log instead of a model.

    Every (state, question, options) asked must have been decided before;
    the newest matching record wins. Missing decisions raise `KeyError`
    unless a `fallback` backend is given, in which case they are decided
    there and appended to the log, so the replay extends the log
    deterministically.
    """
    name = "replay"
    max_questions = 10_000

    def __init__(self, log: DecisionLog, fallback: Optional[Backend] = None, agent: str = "replay"):
        self.log = log
        self.fallback = fallback
        self.agent = agent
        self.hits = 0
        self.misses = 0

    def _decide(self, state: str, questions: list[Question]) -> DecideResult:
        sha = sha256(state)
        index = {}
        for r in self.log.decisions():
            if r.data["state_sha"] == sha:
                index[r.key()] = r  # newest wins
        decisions: list[Optional[Decision]] = []
        missing: list[tuple[int, Question]] = []
        temperature = self.temperature
        for i, q in enumerate(questions):
            r = index.get(decision_key(sha, q))
            if r is None:
                decisions.append(None)
                missing.append((i, q))
            else:
                decisions.append(r.to_decision())
                temperature = r.data.get("temperature") or temperature
                self.hits += 1
        if missing:
            if self.fallback is None:
                raise KeyError(f"{len(missing)} decision(s) not in the log, e.g. {missing[0][1].question!r}")
            self.misses += len(missing)
            res = self.fallback.decide(state, [q for _, q in missing])
            for (i, _), d in zip(missing, res.decisions):
                decisions[i] = d
                self.log.decision(state, self.agent, d, backend=res.backend, temperature=res.temperature,
                                  state_sha=sha, meta={"replay_miss": True})
            temperature = res.temperature
        out = DecideResult([d for d in decisions if d is not None], Timing())
        out.temperature = temperature
        return out
