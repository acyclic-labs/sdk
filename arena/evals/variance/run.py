"""Run-to-run variance, entropy and accuracy of decision backends on the same inputs, bucketed by confidence.
Inputs: the 24 judge pairs (2 options, label = better fork or tie) and the 48 gate commands (3 options, labelled).
Backends: Jev, letter-logit readings of small chat models, and open-jev's Space when its quota allows."""
import json, math, os, sys, time, statistics
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "packages", "jev", "src"))  # the checked-out jev package
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "judge"))
from jev import Question
from jev.backends.jev import JevBackend
from jev.backends.openrouter import OpenRouterBackend
from pairs import CASES
from run_judge import state as pair_state

N = int(os.environ.get("REPEATS", "5"))
SLEEP = float(os.environ.get("SLEEP", "0"))  # seconds between calls, for rate-limited providers
inputs = []
for i, c in enumerate(CASES):
    for swap in (False, True):
        better = c["better"] if not swap else {"A": "B", "B": "A"}.get(c["better"], "tie")
        inputs.append({"set": "judge", "id": f"j{i}{'s' if swap else ''}", "state": pair_state(c, swap),
                       "q": Question("Which fork best completes the task and should be promoted?", ("fork A", "fork B")),
                       "label": None if better == "tie" else f"fork {better}", "tie": better == "tie"})
for line in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gate", "cases.jsonl")):
    c = json.loads(line); q = c["questions"][0]
    inputs.append({"set": "gate", "id": c["id"], "state": c["state"], "q": Question(q["question"], tuple(q["options"])), "label": q["label"], "tie": False})

ONLY = os.environ.get("ONLY")  # comma-separated backend names to run; default: the OpenRouter ones
OUT = os.environ.get("OUT", "variance")
def backends():
    all_ = [("jev", lambda: JevBackend()),
            ("llama-3.1-8b letter-logits", lambda: OpenRouterBackend(model="meta-llama/llama-3.1-8b-instruct")),
            ("deepseek-v4-flash letter-logits", lambda: OpenRouterBackend(model="deepseek/deepseek-v4-flash")),
            ("open-jev (Space)", lambda: __import__("jev.backends.space", fromlist=["SpaceBackend"]).SpaceBackend())]
    want = ONLY.split(",") if ONLY else ["jev", "llama-3.1-8b letter-logits", "deepseek-v4-flash letter-logits"]
    for name, mk in all_:
        if name in want: yield name, mk

def entropy(p): return -sum(x * math.log2(x) for x in p if x > 0)

out = {}
for name, mk in backends():
    try: b = mk()
    except Exception as e: print(name, "unavailable:", e); continue
    rows = []; usd = 0.0; ms = []; failed = 0
    print(f"== {name}", flush=True)
    for inp in inputs:
        runs = []
        for _ in range(N):
            try:
                t0 = time.perf_counter(); r = b.decide(inp["state"], [inp["q"]]); ms.append((time.perf_counter() - t0) * 1000)
                runs.append(list(r[0].probs)); usd += r.timing.usd or 0
                if SLEEP: time.sleep(SLEEP)
            except Exception as e:
                failed += 1; print("   fail", inp["id"], str(e)[:120]); quota = "ZeroGPU" in str(e) or "quota" in str(e).lower(); break
        else:
            quota = False
        if quota: print("   quota exhausted; stopping this backend"); break
        if not runs: continue
        opts = inp["q"].options; k = len(opts)
        tops = [max(range(k), key=p.__getitem__) for p in runs]
        mean_p = [statistics.fmean(p[j] for p in runs) for j in range(k)]
        top = max(range(k), key=mean_p.__getitem__)
        sd_top = statistics.pstdev(p[top] for p in runs) if len(runs) > 1 else 0.0
        sd_all = statistics.fmean(statistics.pstdev(p[j] for p in runs) if len(runs) > 1 else 0.0 for j in range(k))
        flips = len(set(tops)) > 1
        ent = statistics.fmean(entropy(p) for p in runs)
        correct = None if inp["label"] is None else (opts[top] == inp["label"])
        rows.append({"set": inp["set"], "id": inp["id"], "k": k, "mean_top": mean_p[top], "sd_top": sd_top, "sd_all": sd_all,
                     "flip": flips, "entropy_bits": ent, "max_entropy_bits": math.log2(k), "correct": correct, "tie": inp["tie"], "runs": runs})
    out[name] = {"rows": rows, "usd": usd, "median_ms": statistics.median(ms) if ms else None, "failed": failed, "n_inputs": len(rows), "repeats": N}
    json.dump(out, open(f"{OUT}.json", "w"), indent=1)

# ---- report
buckets = [(0.0, 0.5, "≤0.50 (coin flip)"), (0.5, 0.7, "0.50–0.70 (boundary)"), (0.7, 0.9, "0.70–0.90"), (0.9, 1.01, "≥0.90 (confident)")]
md = f"# Variance and entropy at the decision boundary\n\n{len(inputs)} inputs ({sum(1 for i in inputs if i['set']=='judge')} judge pairs with 2 options, {sum(1 for i in inputs if i['set']=='gate')} gate commands with 3 options), {N} repeats each, same text every time.\n\n"
md += "| backend | inputs | median ms | cost | mean SD of top prob | max SD | inputs whose verdict flipped | mean entropy (bits) | accuracy on labelled | identical-pair controls called tie |\n|---|---|---|---|---|---|---|---|---|---|\n"
for name, d in out.items():
    R = d["rows"]; lab = [r for r in R if r["correct"] is not None]; ties = [r for r in R if r["tie"]]
    tie_ok = sum(1 for r in ties if r["mean_top"] <= 0.6)
    md += f"| {name} | {len(R)} | {d['median_ms']:.0f} | ${d['usd']:.4f} | {statistics.fmean(r['sd_top'] for r in R):.4f} | {max(r['sd_top'] for r in R):.3f} | {sum(r['flip'] for r in R)} | {statistics.fmean(r['entropy_bits'] for r in R):.3f} | {sum(r['correct'] for r in lab)}/{len(lab)} | {tie_ok}/{len(ties)} |\n"
md += "\n## By confidence bucket (mean top probability across repeats)\n\n"
for name, d in out.items():
    md += f"\n### {name}\n\n| bucket | inputs | mean SD | verdict flips | mean entropy | accuracy |\n|---|---|---|---|---|---|\n"
    for lo, hi, lab_ in buckets:
        R = [r for r in d["rows"] if lo <= r["mean_top"] < hi]
        if not R: md += f"| {lab_} | 0 | | | | |\n"; continue
        L = [r for r in R if r["correct"] is not None]
        md += f"| {lab_} | {len(R)} | {statistics.fmean(r['sd_top'] for r in R):.4f} | {sum(r['flip'] for r in R)} | {statistics.fmean(r['entropy_bits'] for r in R):.3f} | {(sum(r['correct'] for r in L)/len(L)) if L else float('nan'):.2f} ({len(L)}) |\n"
md += "\nSD is the population standard deviation of the top option's probability across repeats; entropy is of the full distribution (max 1 bit for 2 options, 1.585 for 3). Accuracy uses the mean distribution's argmax against the label; identical-pair controls count as correct when the top probability is at most 0.6.\n"
open(f"{OUT}.md", "w").write(md); print(md)
