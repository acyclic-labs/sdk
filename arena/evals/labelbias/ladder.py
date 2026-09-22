"""A ladder of pairs with a controlled quality gap, judged in four label/position configurations.
Question: when one fork is genuinely a little better, can the label or the slot flip the verdict?"""
import sys, os, statistics, json
sys.path.insert(0, "/Users/avinjamuri/Projects/graphcoder-all-stuff/graphcoder-plugin/jev/src")
from jev import Question
from jev.backends.jev import JevBackend
b = JevBackend(); usd = 0
task = "Task:\ncheapest() in shop.py returns the most expensive item. Fix it so it returns the cheapest, and returns None for an empty iterable."
BASE = "-        if best is None or it.price_cents > best.price_cents:\n+        if best is None or it.price_cents < best.price_cents:\n"
RUNGS = [
 ("identical",           BASE, BASE),
 ("cosmetic: comment",   BASE, BASE + "+    # cheapest by price_cents\n"),
 ("rename loop var",     BASE, "-    for it in items:\n+    for item in items:\n-        if best is None or it.price_cents > best.price_cents:\n+        if best is None or item.price_cents < best.price_cents:\n-            best = it\n+            best = item\n"),
 ("adds docstring",      BASE, '+    """Return the cheapest item, or None if items is empty."""\n' + BASE),
 ("adds type hints",     BASE, "-def cheapest(items):\n+def cheapest(items: Iterable[Item]) -> Item | None:\n" + BASE),
 ("idiomatic min()",     BASE, "-    best = None\n-    for it in items:\n-        if best is None or it.price_cents > best.price_cents:\n-            best = it\n-    return best\n+    return min(items, key=lambda it: it.price_cents, default=None)\n"),
 ("also adds a test",    BASE, BASE + "+\n+def test_cheapest_empty():\n+    assert cheapest([]) is None\n"),
 ("other fork is wrong", BASE, "-        if best is None or it.price_cents > best.price_cents:\n+        if best is None or it.price_cents >= best.price_cents:\n"),
]
q = "Which fork best completes the task and should be promoted?"
def state(n1, d1, n2, d2): return f"{task}\n\n=== {n1}\nChanged paths: 1\n  M shop.py\n{d1}\n=== {n2}\nChanged paths: 1\n  M shop.py\n{d2}"
def ask(n1, d1, n2, d2, reps=2):
    global usd; ps = []
    for _ in range(reps):
        r = b.decide(state(n1, d1, n2, d2), [Question(q, (n1, n2))]); usd += r.timing.usd or 0; ps.append(r[0].probs[0])
    return statistics.fmean(ps)
print(f"{'rung':22s} {'A/B, better=A first':20s} {'A/B, better=B second':21s} {'names, better first':20s} {'names, better second':21s} evidence  label+pos swing  pos-only swing")
rows = []
for name, worse, better in RUNGS:
    # "better" is the second diff in RUNGS except identical (tie) and the last rung (first is right, second wrong -> better = first)
    if name == "other fork is wrong": better, worse = worse, better
    ab1 = ask("fork A", better, "fork B", worse)            # better listed first, called A
    ab2 = 1 - ask("fork A", worse, "fork B", better)        # better listed second, called B -> P(better)
    nm1 = ask("kevin", better, "greg", worse)
    nm2 = 1 - ask("kevin", worse, "greg", better)
    evidence = statistics.fmean([nm1, nm2])
    rows.append({"rung": name, "ab_first": ab1, "ab_second": ab2, "nm_first": nm1, "nm_second": nm2})
    print(f"{name:22s} {ab1:20.2f} {ab2:21.2f} {nm1:20.2f} {nm2:21.2f} {evidence:8.2f}  {ab1-ab2:+.2f}            {nm1-nm2:+.2f}")
json.dump({"rows": rows, "usd": usd}, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "ladder.json"), "w"), indent=1)
print(f"\nP(better fork) in each configuration; evidence = mean over the two name orders. cost ${usd:.4f}")
