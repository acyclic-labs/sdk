from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from ..types import Question, DecideResult
from .. import format as fmt


class Backend(ABC):
    """Something that turns (state, questions) into calibrated distributions.

    Subclasses implement `_decide` for one batch of at most `max_questions`.
    `decide` validates, chunks, and stitches results, so every backend sees
    the same contract.
    """
    name: str = "backend"
    temperature: float = fmt.TEMPERATURE
    max_questions: int = fmt.MAX_QUESTIONS

    def token_len(self, text: str) -> int:
        return fmt.approx_token_len(text)

    def decide(self, state: str, questions: Sequence[Question]) -> DecideResult:
        questions = list(questions)
        fmt.validate(questions, self.token_len)
        state = str(state or "")
        parts = [self._decide(state, chunk) for chunk in fmt.chunk_questions(questions, self.max_questions)]
        out = parts[0]
        for p in parts[1:]:
            out.decisions.extend(p.decisions)
            out.timing.prefill_ms += p.timing.prefill_ms
            out.timing.branch_ms += p.timing.branch_ms
            out.timing.forwards += p.timing.forwards
            out.timing.tokens += p.timing.tokens
            if p.timing.usd is not None:
                out.timing.usd = (out.timing.usd or 0.0) + p.timing.usd
        out.plan = fmt.plan(state, questions, self.token_len)
        out.backend = self.name
        out.temperature = self.temperature
        return out

    @abstractmethod
    def _decide(self, state: str, questions: list[Question]) -> DecideResult: ...

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
