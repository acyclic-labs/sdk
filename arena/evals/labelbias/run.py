"""Replicate the label-A preference and test what drives it.
Identical diffs under different option names, 3 repeats each. Only the option strings change."""
import json, os, sys, statistics
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "packages", "jev", "src"))  # the checked-out jev package
from jev import Question
from jev.backends.jev import JevBackend
b = JevBackend()
DIFF = "-        if best is None or it.price_cents > best.price_cents:\n+        if best is None or it.price_cents < best.price_cents:\n"
def state(n1, n2):
    return f"Task:\ncheapest() in shop.py returns the most expensive item. Fix it so it returns the cheapest, and returns None for an empty iterable.\n\n=== {n1} (deepseek/deepseek-v4-flash)\nChanged paths: 1\n  M shop.py\n{DIFF}\n=== {n2} (deepseek/deepseek-v4-flash)\nChanged paths: 1\n  M shop.py\n{DIFF}"
q = "Which fork best completes the task and should be promoted?"
variants = [
 ("fork A / fork B  (original)",        "fork A", "fork B"),
 ("fork B / fork A  (names swapped)",   "fork B", "fork A"),
 ("fork X / fork Y",                    "fork X", "fork Y"),
 ("fork Y / fork X",                    "fork Y", "fork X"),
 ("fork 1 / fork 2",                    "fork 1", "fork 2"),
 ("fork 2 / fork 1",                    "fork 2", "fork 1"),
 ("left / right",                       "left", "right"),
 ("right / left",                       "right", "left"),
 ("alpha / bravo",                      "alpha", "bravo"),
 ("bravo / alpha",                      "bravo", "alpha"),
 ("fork B / fork C  (no A present)",    "fork B", "fork C"),
 ("fork C / fork B",                    "fork C", "fork B"),
]
rows = []; usd = 0
print(f"{'option names (first listed / second listed)':44s} P(first listed)  P(second)   runs")
for label, n1, n2 in variants:
    ps = []
    for _ in range(3):
        r = b.decide(state(n1, n2), [Question(q, (n1, n2))]); ps.append(r[0].probs[0]); usd += r.timing.usd or 0
    rows.append({"variant": label, "first": n1, "second": n2, "p_first": ps})
    print(f"{label:44s} {statistics.fmean(ps):.2f}             {1-statistics.fmean(ps):.2f}        {[round(p,2) for p in ps]}")
# a third option that admits a tie
print("\nwith a third option 'they are the same':")
for n1, n2 in (("fork A", "fork B"), ("fork X", "fork Y")):
    ps = []
    for _ in range(3):
        r = b.decide(state(n1, n2), [Question(q, (n1, n2, "they are the same"))]); ps.append([round(p, 2) for p in r[0].probs]); usd += r.timing.usd or 0
    print(f"  {n1} / {n2} / same: {ps}")
json.dump(rows, open(os.path.join(os.path.dirname(__file__), "results.json"), "w"), indent=1)
print(f"\ncost ${usd:.4f}")
