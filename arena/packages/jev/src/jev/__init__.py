"""jev: multi-agent swarms over System One typed decisions.

A *decision* is (state, question, options) -> a calibrated distribution over
exactly those options. A *swarm* is a set of agents that all decide against
one shared state: the state is encoded once and every agent's questions are
scored in one batched forward. Every decision is appended to a time-ordered,
hash-chained log that can be replayed without the model.
"""
from .types import Question, Decision, DecideResult, Timing, Plan
from .backends.base import Backend
from .backends.mock import MockBackend
from .log import DecisionLog, ReplayBackend, Record, LogRecord
from .evals import Case, Labeled, run_eval, metrics, load_cases
from .costs import cost_of, summarize
from .swarm import Agent, FunctionAgent, Swarm, StepResult

__all__ = [
    "Question", "Decision", "DecideResult", "Timing", "Plan",
    "Backend", "MockBackend",
    "DecisionLog", "ReplayBackend", "Record", "LogRecord",
    "Case", "Labeled", "run_eval", "metrics", "load_cases", "cost_of", "summarize",
    "Agent", "FunctionAgent", "Swarm", "StepResult",
]
__version__ = "0.1.0"
