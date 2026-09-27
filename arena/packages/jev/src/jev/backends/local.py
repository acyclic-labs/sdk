"""Run the System One scorer in-process: prefill the state once, score every
(question, option) branch from that cache in batched forwards.

This is a port of the cached lane in the open-jev Space (pngwn/open-jev,
app.py). Requires the `local` extra: torch, transformers==5.17.0, peft.
The adapter is non-commercial (CC-BY-NC-4.0 training data).
"""
from __future__ import annotations

import copy
import time
from typing import Optional

from ..types import Question, Decision, DecideResult, Timing
from .. import format as fmt
from .base import Backend

BASE_MODEL = "Qwen/Qwen3.5-4B-Base"
DEFAULT_ADAPTER = "pngwn/system-one-qwen3.5-4b-scorer"


class LocalBackend(Backend):
    name = "local"
    max_questions = 10_000  # no per-call cap; branches are chunked by token budget

    def __init__(self, adapter: Optional[str] = None, base_model: str = BASE_MODEL,
                 temperature: Optional[float] = None, device: Optional[str] = None,
                 dtype=None, attn_implementation: str = "sdpa"):
        import os
        import torch
        from transformers import AutoTokenizer

        # Same env overrides as the Space, so a retrained adapter (see train/) drops in.
        adapter = adapter or os.environ.get("SYSTEM_ONE_ADAPTER", DEFAULT_ADAPTER)
        if temperature is None:
            temperature = float(os.environ.get("SYSTEM_ONE_TEMPERATURE", fmt.TEMPERATURE))
        self.torch = torch
        self.adapter = adapter
        self.temperature = temperature
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.dtype = dtype or (torch.bfloat16 if self.device == "cuda" else torch.float32)
        self.tok = AutoTokenizer.from_pretrained(base_model)
        if self.tok.pad_token is None:
            self.tok.pad_token = self.tok.eos_token
        self.model = self._load(base_model, adapter, attn_implementation)
        self._warm = False

    def _load(self, base_model, adapter, attn_implementation):
        from peft import PeftModel
        from transformers import Qwen3_5TextForSequenceClassification

        model = Qwen3_5TextForSequenceClassification.from_pretrained(
            base_model, num_labels=1, dtype=self.dtype, low_cpu_mem_usage=True,
            attn_implementation=attn_implementation)
        model.config.pad_token_id = self.tok.pad_token_id
        model.config.eos_token_id = self.tok.eos_token_id
        tc = model.config.get_text_config()
        tc.pad_token_id = self.tok.pad_token_id
        tc.eos_token_id = self.tok.eos_token_id
        model = PeftModel.from_pretrained(model, adapter, torch_device="cpu")
        model = model.merge_and_unload()  # fold LoRA in; `score` head restored from modules_to_save
        return model.to(self.device).eval()

    # -- tokens ---------------------------------------------------------------
    def _ids(self, text: str) -> list[int]:
        return self.tok(text, add_special_tokens=False)["input_ids"]

    def token_len(self, text: str) -> int:
        return len(self._ids(text))

    def _sync(self):
        if self.device == "cuda":
            self.torch.cuda.synchronize()

    def _pad_right(self, seqs):
        torch = self.torch
        L = max(len(s) for s in seqs)
        ids = torch.full((len(seqs), L), self.tok.pad_token_id, dtype=torch.long)
        attn = torch.zeros((len(seqs), L), dtype=torch.long)
        for i, s in enumerate(seqs):
            ids[i, : len(s)] = torch.tensor(s, dtype=torch.long)
            attn[i, : len(s)] = 1
        return ids, attn

    # -- scoring --------------------------------------------------------------
    def _score_cached(self, head: list[int], branches: list[list[int]]):
        torch = self.torch
        P = len(head)
        with torch.no_grad():
            x = torch.tensor([head], dtype=torch.long, device=self.device)
            self._sync()
            t0 = time.perf_counter()
            prefix = self.model.model(input_ids=x, use_cache=True).past_key_values
            self._sync()
            prefill_ms = (time.perf_counter() - t0) * 1000.0

            scores = []
            step = fmt.branch_chunk(P)
            chunks = [branches[i: i + step] for i in range(0, len(branches), step)]
            t1 = time.perf_counter()
            for chunk in chunks:
                n = len(chunk)
                # Batch 1 -> n. reorder_cache index-selects the KV of the full-attention layers
                # and the conv/recurrent state of the gated-delta-net layers alike.
                cache = prefix if len(chunks) == 1 else copy.deepcopy(prefix)
                cache.reorder_cache(torch.zeros(n, dtype=torch.long, device=self.device))
                ids, sattn = self._pad_right(chunk)
                attn = torch.cat([torch.ones((n, P), dtype=torch.long), sattn], dim=1)
                h = self.model.model(input_ids=ids.to(self.device), attention_mask=attn.to(self.device),
                                     past_key_values=cache, use_cache=True).last_hidden_state
                last = (sattn.sum(1) - 1).to(self.device)
                pooled = h[torch.arange(n, device=self.device), last]
                scores += self.model.score(pooled).squeeze(-1).float().cpu().tolist()
                del cache, h
            self._sync()
            branch_ms = (time.perf_counter() - t1) * 1000.0
        return scores, prefill_ms, branch_ms, 1 + len(chunks)

    def _decide(self, state: str, questions: list[Question]) -> DecideResult:
        head = self._ids(fmt.head_text(state))[: fmt.MAX_STATE_TOKENS]
        branches, owner = [], []
        for qi, q in enumerate(questions):
            for o in q.options:
                branches.append(self._ids(fmt.tail_text(q.question, o)))
                owner.append(qi)
        if not self._warm:  # pay lazy CUDA init outside the timed region
            self._score_cached([1, 2, 3, 4], [[5, 6], [5, 7]])
            self._warm = True
        scores, prefill_ms, branch_ms, forwards = self._score_cached(head, branches)
        decisions = []
        for qi, q in enumerate(questions):
            logits = [s for o, s in zip(owner, scores) if o == qi]
            decisions.append(Decision.from_probs(q, fmt.distribution(logits, self.temperature)))
        tokens = len(head) + sum(len(b) for b in branches)
        return DecideResult(decisions, Timing(prefill_ms, branch_ms, forwards, tokens))
