"""Plain data types. No model code here."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Literal, Optional

QuestionType = Literal["choice", "noul", "score"]


@dataclass(frozen=True)
class Question:
    """One typed question. The answer is always one of `options`.

    type:
      choice  free option set (2..32 options)
      noul    yes/no; options are forced to ["yes", "no"]
      score   ordered options (e.g. "1".."5"); the decision also carries an expected value
    """
    question: str
    options: tuple[str, ...] = ()
    type: QuestionType = "choice"
    id: Optional[str] = None

    def __post_init__(self):
        opts = tuple(dict.fromkeys(str(o).strip() for o in self.options if str(o).strip()))
        if self.type == "noul":
            opts = ("yes", "no")
        object.__setattr__(self, "options", opts)
        object.__setattr__(self, "question", str(self.question).strip())
        if not self.question:
            raise ValueError("question text is empty")
        if len(opts) < 2:
            raise ValueError(f"{self.question!r}: a question needs at least two distinct options")

    @classmethod
    def yes_no(cls, question: str, id: Optional[str] = None) -> "Question":
        return cls(question, ("yes", "no"), "noul", id)

    @classmethod
    def score(cls, question: str, lo: int = 1, hi: int = 5, id: Optional[str] = None) -> "Question":
        return cls(question, tuple(str(i) for i in range(lo, hi + 1)), "score", id)

    def key(self) -> str:
        """Identity used by the log: the question text and its option set, not the id."""
        return self.question + "\x1f" + "\x1e".join(self.options)

    def to_wire(self, id: Optional[str] = None) -> dict:
        return {"id": id or self.id, "type": self.type, "question": self.question, "options": list(self.options)}


@dataclass(frozen=True)
class Decision:
    """A calibrated distribution over one question's options."""
    question: Question
    probs: tuple[float, ...]
    chosen_index: int
    expected: Optional[float] = None  # only for type == "score": sum (k+1) p_k

    @property
    def chosen(self) -> str:
        return self.question.options[self.chosen_index]

    @property
    def conf(self) -> float:
        return self.probs[self.chosen_index]

    def prob(self, option: str) -> float:
        return self.probs[self.question.options.index(option)]

    @property
    def yes(self) -> Optional[bool]:
        """For yes/no questions: True if "yes" is the argmax, else False. None otherwise."""
        if self.question.type != "noul":
            return None
        return self.chosen == "yes"

    def to_dict(self) -> dict:
        d = {"question": self.question.question, "type": self.question.type,
             "options": list(self.question.options), "probs": [round(p, 6) for p in self.probs],
             "chosen": self.chosen, "chosen_index": self.chosen_index, "conf": round(self.conf, 6)}
        if self.expected is not None:
            d["expected"] = round(self.expected, 4)
        return d

    @classmethod
    def from_probs(cls, question: Question, probs: list[float]) -> "Decision":
        if len(probs) != len(question.options):
            raise ValueError("probs and options differ in length")
        i = max(range(len(probs)), key=probs.__getitem__)
        expected = None
        if question.type == "score":
            expected = sum((k + 1) * p for k, p in enumerate(probs))
        return cls(question, tuple(float(p) for p in probs), i, expected)


@dataclass
class Timing:
    prefill_ms: float = 0.0
    branch_ms: float = 0.0
    forwards: int = 0
    tokens: int = 0
    usd: Optional[float] = None  # metered backends (OpenRouter) report dollars; GPU backends leave it None

    @property
    def total_ms(self) -> float:
        return self.prefill_ms + self.branch_ms


@dataclass
class Plan:
    """Token accounting for a batch, in the terms of the System One report (section 4.1)."""
    state_tokens: int = 0
    n_questions: int = 0
    n_branches: int = 0
    suffix_tokens: int = 0
    cached_tokens: int = 0
    naive_tokens: int = 0
    beyond_train_len: bool = False
    truncated: bool = False
    over_option_cap: list[str] = field(default_factory=list)

    @property
    def ratio(self) -> float:
        return self.naive_tokens / self.cached_tokens if self.cached_tokens else 1.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DecideResult:
    decisions: list[Decision]
    timing: Timing = field(default_factory=Timing)
    plan: Plan = field(default_factory=Plan)
    backend: str = ""
    temperature: float = 1.75

    def __iter__(self):
        return iter(self.decisions)

    def __len__(self):
        return len(self.decisions)

    def __getitem__(self, i):
        return self.decisions[i]
