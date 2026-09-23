"""Label prior with people's names as option labels, and three-way / six-way orderings, on identical diffs.
Separates the position prior from the sequence-name prior: people's names carry no sequence."""
import sys, os, itertools, random, statistics, json
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "packages", "jev", "src"))  # the checked-out jev package
from jev import Question
from jev.backends.jev import JevBackend
b = JevBackend(); usd = 0
DIFF = "-        if best is None or it.price_cents > best.price_cents:\n+        if best is None or it.price_cents < best.price_cents:\n"
task = "Task:\ncheapest() in shop.py returns the most expensive item. Fix it so it returns the cheapest, and returns None for an empty iterable."
q = "Which fork best completes the task and should be promoted?"
def state(names): return task + "".join(f"\n\n=== {n}\nChanged paths: 1\n  M shop.py\n{DIFF}" for n in names)
def ask(names, reps=2):
    global usd; ps = []
    for _ in range(reps):
        r = b.decide(state(names), [Question(q, tuple(names))]); usd += r.timing.usd or 0; ps.append(list(r[0].probs))
    return [statistics.fmean(c) for c in zip(*ps)]
out = {"pairs": [], "letters3": [], "names6": [], "same": {}}
for a, c in [("kevin","greg"),("greg","kevin"),("bob","sam"),("sam","bob"),("ram","hao"),("hao","ram"),("sam","ram"),("ram","sam"),("kevin","hao"),("hao","kevin"),("zed","adam"),("adam","zed")]:
    p = ask([a, c]); out["pairs"].append({"first": a, "second": c, "p": p}); print(f"{a:6s}/{c:6s} {p[0]:.2f} {p[1]:.2f}")
for perm in itertools.permutations(["fork A", "fork B", "fork C"]):
    p = ask(list(perm), 1); out["letters3"].append({"order": perm, "p": p}); print(" / ".join(perm), [round(x, 2) for x in p])
names = ["kevin", "greg", "bob", "sam", "ram", "hao"]; random.seed(7)
for _ in range(3):
    perm = names[:]; random.shuffle(perm); p = ask(perm, 1); out["names6"].append({"order": perm, "p": p}); print(" ".join(f"{n}={v:.2f}" for n, v in zip(perm, p)))
out["same"]["names6"] = ask(names + ["they are the same"], 1); out["same"]["letters3"] = ask(["fork A", "fork B", "fork C", "they are the same"], 1)
print("same:", out["same"]); out["usd"] = usd
json.dump(out, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "names.json"), "w"), indent=1)
print(f"cost ${usd:.4f}")
