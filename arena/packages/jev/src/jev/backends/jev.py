"""TypeSafe's Jev, the original System One model, through OpenRouter's Decisions
endpoint (default) or TypeSafe's own API.

Wire shape (identical on both hosts):
  POST {base_url}   {"model", "state", "questions": {id: {type, instructions, criteria?}}}
  -> {"answers": {id: {...}}, "usage": {"input_tokens", "output_tokens", "cost"}}

  choice  criteria = {option: description}   -> {"choice", "probabilities": {option: p}, "confidence"}
  score   criteria = [level, ...]            -> {"score": float, "probabilities": {"0": p, ...}, "legend"}
  noul    (no criteria)                      -> {"noul": p}

Every question rides in one request, so the state is encoded once. Dollars
come from the usage field. Needs OPENROUTER_API_KEY (or TYPESAFE_API_KEY
with base_url="https://api.typesafe.ai/v1/systemone" and model="jev-latest").
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Callable, Optional

from ..types import Question, Decision, DecideResult, Timing
from .base import Backend

OPENROUTER_DECISIONS = "https://openrouter.ai/api/alpha/decisions"
TYPESAFE_SYSTEMONE = "https://api.typesafe.ai/v1/systemone"
DEFAULT_MODEL = "typesafe/jev-1.13"
Post = Callable[[str, dict, dict], dict]


def _default_post(url: str, headers: dict, body: dict) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{url} -> {e.code}: {e.read().decode(errors='replace')[:600]}") from None


def to_wire(q: Question) -> dict:
    if q.type == "noul":
        return {"type": "noul", "instructions": q.question}
    if q.type == "score":
        return {"type": "score", "instructions": q.question, "criteria": list(q.options)}
    return {"type": "choice", "instructions": q.question, "criteria": {o: o for o in q.options}}


def from_wire(q: Question, a: dict) -> list[float]:
    """Probabilities in the order of q.options; missing keys are 0."""
    if q.type == "noul":
        p = float(a["noul"])
        return [p, 1.0 - p] if q.options == ("yes", "no") else [p, 1.0 - p]
    probs = a.get("probabilities") or {}
    if q.type == "score":
        out = [float(probs.get(str(i), 0.0)) for i in range(len(q.options))]
    else:
        out = [float(probs.get(o, 0.0)) for o in q.options]
    s = sum(out)
    return [x / s for x in out] if s > 0 else [1.0 / len(out)] * len(out)


class JevBackend(Backend):
    name = "jev"
    max_questions = 10_000
    temperature = 1.0  # the model is calibrated as served; nothing is rescaled here

    def __init__(self, model: str = DEFAULT_MODEL, api_key: Optional[str] = None,
                 base_url: str = OPENROUTER_DECISIONS, retries: int = 3, post: Optional[Post] = None):
        from .openrouter import _key_from_files
        self.model = model
        self.base_url = base_url
        self.retries = retries
        self.post = post or _default_post
        if "typesafe.ai" in base_url:
            self.api_key = api_key or os.environ.get("TYPESAFE_API_KEY")
        else:
            self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY") or _key_from_files()
        if not self.api_key and post is None:
            raise RuntimeError("no API key: set OPENROUTER_API_KEY (or TYPESAFE_API_KEY for the TypeSafe host)")
        self.name = "jev:" + model
        self.last_response: Optional[dict] = None

    def _decide(self, state: str, questions: list[Question]) -> DecideResult:
        ids = [f"q{i}" for i in range(len(questions))]
        body = {"model": self.model, "state": state,
                "questions": {i: to_wire(q) for i, q in zip(ids, questions)}}
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}",
                   "HTTP-Referer": "https://acyclic.dev", "X-Title": "jev"}
        delay = 1.0
        t0 = time.perf_counter()
        for attempt in range(self.retries + 1):
            try:
                out = self.post(self.base_url, headers, body)
                break
            except RuntimeError as e:
                if attempt < self.retries and any(c in str(e) for c in ("-> 429", "-> 500", "-> 502", "-> 503")):
                    time.sleep(delay); delay *= 2; continue
                raise
        ms = (time.perf_counter() - t0) * 1000.0
        if "error" in out:
            raise RuntimeError(f"jev: {out['error']}")
        self.last_response = out
        answers = out.get("answers") or {}
        decisions = []
        for i, q in zip(ids, questions):
            if i not in answers:
                raise RuntimeError(f"jev: no answer for {q.question!r}")
            decisions.append(Decision.from_probs(q, from_wire(q, answers[i])))
        u = out.get("usage") or {}
        tokens = int(u.get("input_tokens", 0)) + int(u.get("output_tokens", 0))
        usd = float(u.get("cost", 0.0) or 0.0)
        return DecideResult(decisions, Timing(prefill_ms=0.0, branch_ms=ms, forwards=1, tokens=tokens,
                                              usd=usd if usd else None))
