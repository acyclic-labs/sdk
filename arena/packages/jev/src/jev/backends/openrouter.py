"""Typed decisions through OpenRouter (or any OpenAI-compatible chat endpoint).

OpenRouter does not host the System One scorer, so this backend uses the
"letter-logit" method from the System One report: list the options as
letters, ask the model for exactly one token with logprobs, and turn the
logprobs of the option letters into a distribution. It is a generative
model read through its next-token distribution, so:

  * one request per question (the state is re-sent each time; provider-side
    prompt caching may soften that), run concurrently;
  * calibration is whatever the model gives; `temperature` rescales it;
  * a letter the model never put in its top-k gets a floor logprob, so the
    distribution still covers every option;
  * dollars come straight from OpenRouter's usage accounting into the log.

Needs OPENROUTER_API_KEY (or `api_key=`). Model must support `logprobs` and
`top_logprobs`; 150+ on OpenRouter do.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from ..types import Question, Decision, DecideResult, Timing
from .. import format as fmt
from .base import Backend

DEFAULT_MODEL = "meta-llama/llama-3.1-8b-instruct"  # many providers, logprobs, cheap
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"
LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
SYSTEM = ("You are a decision function. You will be given a state, a question, and lettered options. "
          "Reply with the single letter of the best option and nothing else.")
Post = Callable[[str, dict, dict], dict]  # (url, headers, body) -> parsed json


def render(state: str, q: Question) -> str:
    opts = "\n".join(f"{LETTERS[i]}. {o}" for i, o in enumerate(q.options))
    return f"State:\n{state}\n\nQuestion:\n{q.question}\n\nOptions:\n{opts}\n\nAnswer with the letter only."


def letter_logprobs(top: list[dict], n: int) -> list[Optional[float]]:
    """Best logprob seen for each of the first n letters, from a top_logprobs list."""
    best: list[Optional[float]] = [None] * n
    for t in top:
        tok = str(t.get("token", "")).strip().strip(".):").upper()
        if len(tok) == 1 and tok in LETTERS[:n]:
            i = LETTERS.index(tok)
            lp = float(t["logprob"])
            if best[i] is None or lp > best[i]:
                best[i] = lp
    return best


def to_logits(best: list[Optional[float]], floor_gap: float = 3.0) -> list[float]:
    seen = [b for b in best if b is not None]
    if not seen:
        return [0.0] * len(best)
    floor = min(seen) - floor_gap
    return [floor if b is None else b for b in best]


def _key_from_files() -> Optional[str]:
    """OPENROUTER_API_KEY=... in ./.env, the package's .env, or ~/.config/jev/env."""
    here = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # .../jev/src
    for path in (os.path.join(os.getcwd(), ".env"), os.path.join(os.path.dirname(here), ".env"),
                 os.path.expanduser("~/.config/jev/env")):
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("OPENROUTER_API_KEY="):
                        return line.split("=", 1)[1].strip().strip('"').strip("'") or None
        except OSError:
            continue
    return None


def _default_post(url: str, headers: dict, body: dict) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{url} -> {e.code}: {e.read().decode(errors='replace')[:400]}") from None


class OpenRouterBackend(Backend):
    name = "openrouter"
    max_questions = 10_000

    def __init__(self, model: str = DEFAULT_MODEL, api_key: Optional[str] = None, base_url: str = DEFAULT_BASE_URL,
                 temperature: float = 1.0, top_logprobs: int = 20, concurrency: int = 8,
                 retries: int = 3, post: Optional[Post] = None, extra: Optional[dict] = None):
        self.retries = retries
        self.model = model
        self.api_key = (api_key or os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OPENAI_API_KEY")
                        or _key_from_files())
        if not self.api_key and post is None:
            raise RuntimeError("OPENROUTER_API_KEY is not set (env, ./.env, jev/.env, or ~/.config/jev/env)")
        self.base_url = base_url.rstrip("/")
        self.temperature = temperature
        self.top_logprobs = top_logprobs
        self.concurrency = concurrency
        self.post = post or _default_post
        self.extra = extra or {}
        self.name = "openrouter:" + model
        self.last_responses: list[dict] = []

    def _headers(self) -> dict:
        return {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}",
                "HTTP-Referer": "https://acyclic.dev", "X-Title": "jev"}

    def _post_with_retries(self, body: dict) -> dict:
        """Retry 429/5xx with backoff; if a provider caps top_logprobs, drop to its cap once."""
        delay = 1.0
        for attempt in range(self.retries + 1):
            try:
                out = self.post(self.base_url + "/chat/completions", self._headers(), body)
            except RuntimeError as e:
                msg = str(e)
                if "top_logprobs" in msg and body["top_logprobs"] > 5:
                    body["top_logprobs"] = 5  # e.g. Alibaba: "Range of top_logprobs should be [0, 5]"
                    continue
                if attempt < self.retries and any(code in msg for code in ("-> 429", "-> 500", "-> 502", "-> 503")):
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise
            if "error" in out:
                raise RuntimeError(f"openrouter: {out['error']}")
            return out
        raise RuntimeError("openrouter: retries exhausted")

    def _one(self, state: str, q: Question) -> tuple[list[float], dict]:
        body = {"model": self.model, "max_tokens": 1, "temperature": 0, "logprobs": True,
                "top_logprobs": self.top_logprobs, "usage": {"include": True},
                # only route to providers that honour logprobs; others silently drop them.
                # No `reasoning` key: with require_parameters it filters out every non-reasoning
                # provider. For reasoning models pass extra={"reasoning": {"enabled": False}}.
                "provider": {"require_parameters": True},
                "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": render(state, q)}],
                **self.extra}
        out = self._post_with_retries(body)
        ch = out["choices"][0]
        content = ((ch.get("logprobs") or {}).get("content") or [])
        top = content[0].get("top_logprobs", []) if content else []
        if not top and content:  # some providers only return the chosen token
            top = [{"token": content[0].get("token", ""), "logprob": content[0].get("logprob", 0.0)}]
        if not top:
            raise RuntimeError(f"{self.model} returned no logprobs; pick a model with logprobs+top_logprobs")
        return to_logits(letter_logprobs(top, len(q.options))), out.get("usage") or {}

    def _decide(self, state: str, questions: list[Question]) -> DecideResult:
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.concurrency) as ex:
            results = list(ex.map(lambda q: self._one(state, q), questions))
        ms = (time.perf_counter() - t0) * 1000.0
        decisions, tokens, usd = [], 0, 0.0
        self.last_responses = []
        for q, (logits, usage) in zip(questions, results):
            decisions.append(Decision.from_probs(q, fmt.distribution(logits, self.temperature)))
            tokens += int(usage.get("prompt_tokens", 0)) + int(usage.get("completion_tokens", 0))
            usd += float(usage.get("cost", 0.0) or 0.0)
            self.last_responses.append(usage)
        return DecideResult(decisions, Timing(prefill_ms=0.0, branch_ms=ms, forwards=len(questions), tokens=tokens,
                                              usd=usd if usd else None))
