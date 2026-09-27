import json, sys, time, urllib.request, os
sys.path.insert(0, os.path.dirname(__file__))
from pairs import CASES
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "packages", "jev", "src"))  # the checked-out jev package
from jev import Question, DecisionLog
from jev.backends.jev import JevBackend
KEY = os.environ["OPENROUTER_API_KEY"]

def state(c, swap=False):
    A, B = (c["B"], c["A"]) if swap else (c["A"], c["B"])
    return f"Task:\n{c['task']}\n\n=== fork A\n{A or '(no changes)'}\n\n=== fork B\n{B or '(no changes)'}"

def jev_judge(c, swap):
    b = JevBackend()
    qs = [Question("Which fork best completes the task and should be promoted?", ("fork A", "fork B")),
          Question.yes_no("Fork A is safe to land as-is, without breaking existing behaviour."),
          Question.yes_no("Fork B is safe to land as-is, without breaking existing behaviour.")]
    r = b.decide(state(c, swap), qs)
    pa = r[0].prob("fork A"); return pa, r[1].prob("yes"), r[2].prob("yes"), r.timing.usd or 0, r.timing.branch_ms

def llm_judge(c, swap, model="anthropic/claude-sonnet-5"):
    body = {"model": model, "temperature": 0, "max_tokens": 200,
            "messages": [{"role": "system", "content": "You are a strict code reviewer. Answer with JSON only: {\"winner\": \"A\"|\"B\"|\"tie\", \"p_a\": <probability 0..1 that A is the better, safer fork>, \"safe_a\": true|false, \"safe_b\": true|false}"},
                         {"role": "user", "content": state(c, swap)}],
            "usage": {"include": True}}
    req = urllib.request.Request("https://openrouter.ai/api/v1/chat/completions", data=json.dumps(body).encode(), headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"})
    t0 = time.time(); out = json.loads(urllib.request.urlopen(req, timeout=120).read()); ms = (time.time() - t0) * 1000
    txt = out["choices"][0]["message"].get("content") or ""; import re
    m = re.search(r"\{.*\}", txt, re.S)
    try: j = json.loads(m.group(0)) if m else {}
    except json.JSONDecodeError: j = {}
    return float(j.get("p_a", 0.5)), bool(j.get("safe_a", True)), bool(j.get("safe_b", True)), out.get("usage", {}).get("cost", 0), ms

if __name__ == "__main__":
    rows = []
    for i, c in enumerate(CASES):
        for swap in (False, True):
            better = c["better"] if not swap else ({"A": "B", "B": "A"}.get(c["better"], "tie"))
            pa_j, sa_j, sb_j, usd_j, ms_j = jev_judge(c, swap)
            pa_l, sa_l, sb_l, usd_l, ms_l = llm_judge(c, swap)
            def verdict(pa): return "A" if pa > 0.6 else "B" if pa < 0.4 else "tie"
            rows.append({"i": i, "tag": c["tag"], "swap": swap, "better": better,
                         "jev": {"p_a": round(pa_j, 3), "verdict": verdict(pa_j), "safe_a": round(sa_j, 2), "safe_b": round(sb_j, 2), "usd": usd_j, "ms": round(ms_j)},
                         "llm": {"p_a": round(pa_l, 3), "verdict": verdict(pa_l), "safe_a": sa_l, "safe_b": sb_l, "usd": usd_l, "ms": round(ms_l)}})
            print(f'{c["tag"]:38s} swap={int(swap)} better={better:3s}  jev {verdict(pa_j):3s} p_a={pa_j:.2f} safeA={sa_j:.2f} safeB={sb_j:.2f}  |  sonnet {verdict(pa_l):3s} p_a={pa_l:.2f}', flush=True)
    json.dump(rows, open(os.path.join(os.path.dirname(__file__), "results.json"), "w"), indent=1)
    def score(k):
        ok = sum(r[k]["verdict"] == r["better"] for r in rows); usd = sum(r[k]["usd"] for r in rows); ms = sum(r[k]["ms"] for r in rows) / len(rows)
        # position consistency: same case, swapped, should flip p_a
        flips = 0; pairs = 0
        for a in rows:
            if not a["swap"]:
                b = next(x for x in rows if x["i"] == a["i"] and x["swap"]); pairs += 1
                flips += abs((a[k]["p_a"] + b[k]["p_a"]) - 1.0) < 0.25
        print(f"{k}: correct {ok}/{len(rows)}  position-consistent {flips}/{pairs}  cost ${usd:.4f}  mean {ms:.0f} ms")
    score("jev"); score("llm")
    # swap-averaged Jev: p_a(order AB) and 1 - p_a(order BA), averaged, removes position bias
    ok = 0; ties_ok = 0; ties = 0
    for a in rows:
        if a["swap"]: continue
        b = next(x for x in rows if x["i"] == a["i"] and x["swap"])
        p = (a["jev"]["p_a"] + (1 - b["jev"]["p_a"])) / 2
        v = "A" if p > 0.6 else "B" if p < 0.4 else "tie"
        ok += v == a["better"]
        if a["better"] == "tie": ties += 1; ties_ok += v == "tie"
    print(f"jev swap-averaged: correct {ok}/{len(rows)//2}  identical controls called tie {ties_ok}/{ties}")
