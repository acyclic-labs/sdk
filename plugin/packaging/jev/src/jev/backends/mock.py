"""A deterministic fake scorer: no model, no network, stable across runs.

Scores are a hash of (state, question, option), so the same inputs always
give the same distribution and a different state moves it. Good for tests,
for exercising swarm/log plumbing, and for the frontend of a pipeline
before the real model is wired in.
"""
from __future__ import annotations

import hashlib
import time

from ..types import Question, Decision, DecideResult, Timing
from .. import format as fmt
from .base import Backend


class MockBackend(Backend):
    name = "mock"

    def __init__(self, temperature: float = fmt.TEMPERATURE, seed: str = "", latency_ms: float = 0.0):
        self.temperature = temperature
        self.seed = seed
        self.latency_ms = latency_ms
        self.calls: list[tuple[str, list[Question]]] = []

    def _logit(self, state: str, q: Question, option: str) -> float:
        h = hashlib.blake2b((self.seed + fmt.head_text(state) + fmt.tail_text(q.question, option)).encode(),
                            digest_size=8).digest()
        return (int.from_bytes(h, "big") / 2**64 - 0.5) * 8.0  # roughly [-4, 4]

    def _decide(self, state: str, questions: list[Question]) -> DecideResult:
        self.calls.append((state, list(questions)))
        if self.latency_ms:
            time.sleep(self.latency_ms / 1000.0)
        decisions = []
        for q in questions:
            logits = [self._logit(state, q, o) for o in q.options]
            decisions.append(Decision.from_probs(q, fmt.distribution(logits, self.temperature)))
        P = fmt.approx_token_len(fmt.head_text(state))
        B = sum(len(q.options) for q in questions)
        forwards = 1 + -(-B // fmt.branch_chunk(P))
        return DecideResult(decisions, Timing(prefill_ms=0.0, branch_ms=self.latency_ms, forwards=forwards,
                                              tokens=P + sum(fmt.approx_token_len(fmt.tail_text(q.question, o))
                                                             for q in questions for o in q.options)))
