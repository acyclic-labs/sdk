"""The System One prompt format and its limits. Pure Python; no model code.

The scorer was trained on `State:\\n{state}` (the head) concatenated with
`\\n\\nQuestion:\\n{q}\\n\\nOption:\\n{o}` (the tail). The head is the shared
prefix every branch reuses; each tail is one branch. Everything in this
module mirrors the Space's app.py so a local run and a Space run agree.
"""
from __future__ import annotations

import math
from typing import Callable, Sequence

from .types import Question, Plan

TEMPERATURE = 1.75          # fitted on the validation split; ECE 0.044 held-out
TRAIN_MAX_LEN = 384         # sequences were truncated here in training
TRAIN_MAX_OPTIONS = 16
MAX_STATE_TOKENS = 16_384
MAX_QUESTIONS = 24          # per Space call; the swarm chunks above this
MAX_OPTIONS = 32
MAX_QUESTION_TOKENS = 96
MAX_OPTION_TOKENS = 64
BRANCH_CHUNK = 96
BRANCH_TOKEN_BUDGET = 200_000
NAIVE_FORWARD_TOKENS = 48_000

TokenLen = Callable[[str], int]


def head_text(state: str) -> str:
    return "State:\n" + state


def tail_text(question: str, option: str) -> str:
    return "\n\nQuestion:\n" + question + "\n\nOption:\n" + option


def softmax(xs: Sequence[float]) -> list[float]:
    m = max(xs)
    es = [math.exp(x - m) for x in xs]
    s = sum(es)
    return [e / s for e in es]


def distribution(logits: Sequence[float], temperature: float = TEMPERATURE) -> list[float]:
    return softmax([x / temperature for x in logits])


def branch_chunk(state_tokens: int) -> int:
    return max(1, min(BRANCH_CHUNK, BRANCH_TOKEN_BUDGET // max(state_tokens, 1)))


def approx_token_len(text: str) -> int:
    """A tokenizer-free estimate (~4 chars per token) for planning when no tokenizer is loaded."""
    return max(1, len(text) // 4)


def validate(questions: Sequence[Question], token_len: TokenLen = approx_token_len) -> None:
    """Raise ValueError for anything the scorer refuses rather than truncates."""
    if not questions:
        raise ValueError("at least one question is required")
    for q in questions:
        if len(q.options) > MAX_OPTIONS:
            raise ValueError(f"{q.question!r}: {len(q.options)} options; the limit is {MAX_OPTIONS}")
        n = token_len(q.question)
        if n > MAX_QUESTION_TOKENS:
            raise ValueError(f"{q.question!r}: the question is ~{n} tokens; the limit is {MAX_QUESTION_TOKENS}")
        for o in q.options:
            n = token_len(o)
            if n > MAX_OPTION_TOKENS:
                raise ValueError(f"{q.question!r}: option {o!r} is ~{n} tokens; the limit is {MAX_OPTION_TOKENS}")


def plan(state: str, questions: Sequence[Question], token_len: TokenLen = approx_token_len) -> Plan:
    """Token accounting for one batch: cached is P + sum(tails), naive is P*B + sum(tails)."""
    P_full = token_len(head_text(state))
    P = min(P_full, MAX_STATE_TOKENS)
    tails = [token_len(tail_text(q.question, o)) for q in questions for o in q.options]
    suffix = sum(tails)
    B = len(tails)
    return Plan(
        state_tokens=P,
        n_questions=len(questions),
        n_branches=B,
        suffix_tokens=suffix,
        cached_tokens=P + suffix,
        naive_tokens=P * B + suffix,
        beyond_train_len=P + (max(tails) if tails else 0) > TRAIN_MAX_LEN,
        truncated=P_full > P,
        over_option_cap=[q.id or q.question for q in questions if len(q.options) > TRAIN_MAX_OPTIONS],
    )


def chunk_questions(questions: Sequence[Question], size: int = MAX_QUESTIONS) -> list[list[Question]]:
    return [list(questions[i:i + size]) for i in range(0, len(questions), size)]
