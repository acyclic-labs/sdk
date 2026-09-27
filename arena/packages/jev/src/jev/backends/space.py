"""Decide through the hosted open-jev Space (pngwn/open-jev) over its Gradio API.

Requires the `space` extra (gradio_client). The Space runs on a shared
ZeroGPU, accepts up to 24 questions per call, and can be rate limited; the
base class chunks larger batches into several calls. The comparison lanes
(instruct baseline, naive equivalence check) are off by default.
"""
from __future__ import annotations

from typing import Optional

from ..types import Question, Decision, DecideResult, Timing
from .. import format as fmt
from .base import Backend

DEFAULT_SPACE = "pngwn/open-jev"


class SpaceBackend(Backend):
    name = "space"

    def __init__(self, space: str = DEFAULT_SPACE, hf_token: Optional[str] = None,
                 verify: bool = False, api_name: str = "/run", client=None):
        if client is None:
            import os
            from gradio_client import Client
            # Anonymous use works but has a small daily ZeroGPU quota; a free HF token raises it.
            hf_token = hf_token or os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
            client = Client(space, token=hf_token, verbose=False)
        self.client = client
        self.verify = verify
        self.api_name = api_name
        self.last_snapshot: Optional[dict] = None

    def _call(self, state: str, wire: list[dict]) -> dict:
        # `run` is a generator endpoint; predict() returns its final value.
        return self.client.predict(state, wire, False, self.verify, api_name=self.api_name)

    def _decide(self, state: str, questions: list[Question]) -> DecideResult:
        wire = [q.to_wire(id=f"q{i + 1}") for i, q in enumerate(questions)]
        snap = self._call(state, wire)
        self.last_snapshot = snap
        if not isinstance(snap, dict):
            raise RuntimeError(f"unexpected Space response: {snap!r}")
        if snap.get("error"):
            raise RuntimeError(f"Space error: {snap['error']}")
        sc = snap.get("scorer") or {}
        blocks = sc.get("questions") or []
        if len(blocks) != len(questions):
            raise RuntimeError(f"Space returned {len(blocks)} questions for {len(questions)} sent")
        self.temperature = float((snap.get("models") or {}).get("temperature", self.temperature))
        decisions = []
        for q, b in zip(questions, blocks):
            if list(b["options"]) != list(q.options):
                raise RuntimeError(f"Space reordered options for {q.question!r}")
            decisions.append(Decision.from_probs(q, b["probs"]))
        timing = Timing(prefill_ms=float(sc.get("prefill_ms", 0)), branch_ms=float(sc.get("branch_ms", 0)),
                        forwards=int(sc.get("forwards", 0)), tokens=int(sc.get("tokens", 0)))
        return DecideResult(decisions, timing)
