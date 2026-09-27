"""Evaluate decisions against labels: accuracy, expected calibration error, Brier.

A dataset is a list of cases: {"state": str, "questions": [{"question", "options", "type"?, "label"}]}.
`run_eval` decides every case through a backend (one batch per case), logs
each decision and each case's cost, then logs one `eval` record with the
metrics. Metrics are the ones on the model card, computed the same way:
ECE over 15 equal-width confidence bins, multi-class Brier over the
distribution.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence

from .types import Question, Decision
from .backends.base import Backend
from .log import DecisionLog


@dataclass
class Labeled:
    question: Question
    label: str

    def __post_init__(self):
        if self.label not in self.question.options:
            raise ValueError(f"label {self.label!r} is not one of {self.question.options}")


@dataclass
class Case:
    state: str
    items: list[Labeled]
    id: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "Case":
        items = []
        for q in d["questions"]:
            qq = Question(q["question"], tuple(q.get("options", ())), q.get("type", "choice"), q.get("id"))
            items.append(Labeled(qq, str(q["label"])))
        return cls(str(d["state"]), items, d.get("id"))


def load_cases(path: str) -> list[Case]:
    with open(path, encoding="utf-8") as f:
        text = f.read().strip()
    data = json.loads(text) if text.startswith("[") else [json.loads(l) for l in text.splitlines() if l.strip()]
    return [Case.from_dict(d) for d in data]


def metrics(decisions: Sequence[Decision], labels: Sequence[str], bins: int = 15) -> dict:
    n = len(decisions)
    if n == 0:
        return {"n": 0}
    correct = [d.chosen == y for d, y in zip(decisions, labels)]
    conf = [d.conf for d in decisions]
    # ECE: |acc - conf| weighted by bin mass, 15 equal-width bins on top-1 confidence
    ece = 0.0
    for b in range(bins):
        lo, hi = b / bins, (b + 1) / bins
        idx = [i for i, c in enumerate(conf) if (lo < c <= hi) or (b == 0 and c == 0.0)]
        if idx:
            acc_b = sum(correct[i] for i in idx) / len(idx)
            conf_b = sum(conf[i] for i in idx) / len(idx)
            ece += len(idx) / n * abs(acc_b - conf_b)
    # multi-class Brier: sum over options of (p - onehot)^2, averaged over questions
    brier = 0.0
    for d, y in zip(decisions, labels):
        brier += sum((p - (1.0 if o == y else 0.0)) ** 2 for o, p in zip(d.question.options, d.probs))
    brier /= n
    # majority-class baseline per distinct option set, for context
    nll = -sum(__import__("math").log(max(d.prob(y), 1e-12)) for d, y in zip(decisions, labels)) / n
    return {"n": n, "accuracy": round(sum(correct) / n, 4), "ece": round(ece, 4), "brier": round(brier, 4),
            "nll": round(nll, 4), "mean_conf": round(sum(conf) / n, 4)}


@dataclass
class EvalResult:
    name: str
    backend: str
    metrics: dict
    per_case: list[dict] = field(default_factory=list)
    wall_ms: float = 0.0
    cost: dict = field(default_factory=dict)


def run_eval(cases: Iterable[Case], backend: Backend, log: Optional[DecisionLog] = None, *, name: str = "eval",
             agent: str = "eval", meta: Optional[dict] = None) -> EvalResult:
    from .costs import cost_of, summarize
    log = log if log is not None else DecisionLog()
    all_d: list[Decision] = []
    all_y: list[str] = []
    per_case, costs = [], []
    t0 = time.perf_counter()
    start = len(log)
    log.note("eval_start", name=name, backend=backend.name, meta=dict(meta or {}))
    for k, case in enumerate(cases):
        qs = [it.question for it in case.items]
        ys = [it.label for it in case.items]
        t1 = time.perf_counter()
        try:
            res = backend.decide(case.state, qs)
        except Exception as e:  # leave the abort on the record, then re-raise
            log.append("eval", {"name": name, "backend": backend.name, "aborted": True, "case": case.id or k,
                                "error": repr(e), "cases_done": len(per_case), "log_range": [start, len(log)]})
            raise
        wall = (time.perf_counter() - t1) * 1000.0
        recs = log.append_result(case.state, agent, res, meta={"eval": name, "case": case.id or k,
                                                                "labels": ys})
        c = log.step_cost(k, case.state, res, wall, meta={"eval": name, "case": case.id or k})
        costs.append(c.data)
        m = metrics(res.decisions, ys)
        per_case.append({"case": case.id or k, **m, "seq": [r.seq for r in recs]})
        all_d += res.decisions
        all_y += ys
    m = metrics(all_d, all_y)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    cost = summarize(costs)
    log.append("eval", {"name": name, "backend": backend.name, "temperature": backend.temperature,
                        "metrics": m, "cases": len(per_case), "wall_ms": round(wall_ms, 1), "cost": cost,
                        "log_range": [start, len(log)], "meta": dict(meta or {})})
    return EvalResult(name, backend.name, m, per_case, wall_ms, cost)
