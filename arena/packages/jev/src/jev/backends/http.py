"""Client for `jev serve`. Stdlib only."""
from __future__ import annotations

import json
import urllib.request

from ..types import Question, Decision, DecideResult, Timing
from .base import Backend


class HttpBackend(Backend):
    name = "http"
    max_questions = 10_000

    def __init__(self, url: str = "http://127.0.0.1:8788", timeout: float = 120.0):
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _post(self, path: str, obj: dict) -> dict:
        req = urllib.request.Request(self.url + path, data=json.dumps(obj).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read())

    def health(self) -> dict:
        with urllib.request.urlopen(self.url + "/health", timeout=self.timeout) as r:
            return json.loads(r.read())

    def _decide(self, state: str, questions: list[Question]) -> DecideResult:
        out = self._post("/decide", {"state": state, "questions": [q.to_wire() for q in questions]})
        if "error" in out:
            raise RuntimeError(f"jev server: {out['error']}")
        ds = [Decision.from_probs(q, d["probs"]) for q, d in zip(questions, out["decisions"])]
        t = out.get("timing", {})
        res = DecideResult(ds, Timing(t.get("prefill_ms", 0.0), t.get("branch_ms", 0.0), t.get("forwards", 0),
                                      t.get("tokens", 0), t.get("usd")))
        self.temperature = float(out.get("temperature", self.temperature))
        self.name = "http:" + out.get("backend", "?")
        return res
