"""What a run costs, in the hardware-independent units of the System One report.

Per batch: state tokens P, branches B, the tokens the cached path actually
processed (P + sum of tails), the tokens the naive path would have
(P*B + sum of tails), forward passes, and milliseconds. Dollars are
optional: pass a GPU-second rate and the wall time is priced.
"""
from __future__ import annotations

from typing import Iterable, Optional

from .types import DecideResult

# ZeroGPU A10G class hardware, on-demand, order of magnitude only. Override per backend.
DEFAULT_USD_PER_GPU_SECOND: Optional[float] = None


def cost_of(result: DecideResult, wall_ms: float, usd_per_gpu_second: Optional[float] = DEFAULT_USD_PER_GPU_SECOND) -> dict:
    p, t = result.plan, result.timing
    c = {
        "backend": result.backend,
        "n_questions": p.n_questions,
        "n_branches": p.n_branches,
        "state_tokens": p.state_tokens,
        "cached_tokens": p.cached_tokens,
        "naive_tokens": p.naive_tokens,
        "saved_tokens": p.naive_tokens - p.cached_tokens,
        "ratio": round(p.ratio, 3),
        "forwards": t.forwards,
        "model_tokens": t.tokens,        # what the backend reports it pushed through the model
        "prefill_ms": round(t.prefill_ms, 2),
        "branch_ms": round(t.branch_ms, 2),
        "model_ms": round(t.total_ms, 2),
        "wall_ms": round(wall_ms, 2),
        "beyond_train_len": p.beyond_train_len,
        "over_option_cap": list(p.over_option_cap),
    }
    if t.usd is not None:
        c["usd"] = round(t.usd, 8)
    elif usd_per_gpu_second is not None:
        c["usd"] = round(usd_per_gpu_second * t.total_ms / 1000.0, 6)
    return c


def summarize(costs: Iterable[dict]) -> dict:
    costs = list(costs)
    keys = ["n_questions", "n_branches", "cached_tokens", "naive_tokens", "saved_tokens", "forwards",
            "model_tokens", "model_ms", "wall_ms", "usd"]
    tot = {k: sum(c.get(k, 0) or 0 for c in costs) for k in keys}
    tot["steps"] = len(costs)
    tot["ratio"] = round(tot["naive_tokens"] / tot["cached_tokens"], 3) if tot["cached_tokens"] else 1.0
    tot["ms_per_question"] = round(tot["model_ms"] / tot["n_questions"], 2) if tot["n_questions"] else 0.0
    tot["by_backend"] = sorted({c.get("backend", "") for c in costs})
    if not any("usd" in c for c in costs):
        tot.pop("usd")
    return tot
