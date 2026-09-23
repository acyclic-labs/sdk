"""Merge the per-backend variance files into one report and one chart-data file."""
import glob, json, math, os, statistics

here = os.path.dirname(os.path.abspath(__file__))
out = {}
for f in sorted(glob.glob(os.path.join(here, "variance*.json"))):
    if f.endswith("chart.json"): continue
    for k, v in json.load(open(f)).items():
        if v["rows"]: out[k] = v

buckets = [(0.0, 0.5, "≤0.50"), (0.5, 0.7, "0.50–0.70"), (0.7, 0.9, "0.70–0.90"), (0.9, 1.01, "≥0.90")]
def sd_of(rows): return statistics.fmean(r["sd_top"] for r in rows) if rows else float("nan")
def acc_of(rows):
    L = [r for r in rows if r["correct"] is not None]
    return (sum(r["correct"] for r in L) / len(L), len(L)) if L else (float("nan"), 0)

md = "# Variance and entropy at the decision boundary\n\n"
md += "Same 72 inputs for every backend: 24 judge pairs (2 options; 4 are identical-diff controls) and 48 labelled gate commands (3 options). Repeats per input as listed. Open models are read through the logprobs of the option letters (one token, temperature 0); Jev and open-jev through their native decision heads.\n\n"
md += "| backend | inputs | repeats | median ms | cost | mean SD of top prob | max SD | verdict flips | mean entropy (bits) | accuracy (labelled) | identical controls called tie |\n|---|---|---|---|---|---|---|---|---|---|---|\n"
for name, d in out.items():
    R = d["rows"]; ties = [r for r in R if r["tie"]]; a, n = acc_of(R)
    md += f"| {name} | {len(R)} | {d['repeats']} | {d['median_ms']:.0f} | ${d['usd']:.4f} | {sd_of(R):.4f} | {max(r['sd_top'] for r in R):.3f} | {sum(r['flip'] for r in R)} | {statistics.fmean(r['entropy_bits'] for r in R):.3f} | {a:.2f} ({n}) | {sum(1 for r in ties if r['mean_top'] <= 0.6)}/{len(ties)} |\n"

md += "\n## At each decision boundary\n\nRows bucket every input by its mean top probability. The interesting row is 0.50–0.70: the referee is nearly undecided there, so run-to-run noise can flip the verdict.\n\n"
chart = {"backends": {}, "buckets": [b[2] for b in buckets]}
for name, d in out.items():
    md += f"\n### {name}\n\n| mean top prob | inputs | mean SD | max SD | verdict flips | mean entropy | accuracy |\n|---|---|---|---|---|---|---|\n"
    chart["backends"][name] = []
    for lo, hi, lab in buckets:
        R = [r for r in d["rows"] if lo <= r["mean_top"] < hi]
        if not R:
            md += f"| {lab} | 0 | | | | | |\n"; chart["backends"][name].append(None); continue
        a, n = acc_of(R)
        md += f"| {lab} | {len(R)} | {sd_of(R):.4f} | {max(r['sd_top'] for r in R):.3f} | {sum(r['flip'] for r in R)} | {statistics.fmean(r['entropy_bits'] for r in R):.3f} | {a:.2f} ({n}) |\n"
        chart["backends"][name].append({"n": len(R), "sd": sd_of(R), "flips": sum(r["flip"] for r in R), "entropy": statistics.fmean(r["entropy_bits"] for r in R), "acc": a, "labelled": n})

md += "\n## Per-set split\n\n| backend | set | inputs | mean SD | flips | mean entropy | accuracy |\n|---|---|---|---|---|---|---|\n"
for name, d in out.items():
    for s in ("judge", "gate"):
        R = [r for r in d["rows"] if r["set"] == s]
        if not R: continue
        a, n = acc_of(R)
        md += f"| {name} | {s} | {len(R)} | {sd_of(R):.4f} | {sum(r['flip'] for r in R)} | {statistics.fmean(r['entropy_bits'] for r in R):.3f} | {a:.2f} ({n}) |\n"

md += "\nSD: population standard deviation of the top option's probability across repeats. Entropy: of the full distribution per call, averaged (max 1 bit for 2 options, 1.585 for 3). Accuracy: argmax of the mean distribution against the label; identical-diff controls count as correct when the top probability is at most 0.6.\n"
open(os.path.join(here, "REPORT.md"), "w").write(md)
# per-input scatter data for the page: mean_top vs sd_top per backend
chart["points"] = {name: [{"p": r["mean_top"], "sd": r["sd_top"], "ent": r["entropy_bits"], "ok": r["correct"], "set": r["set"]} for r in d["rows"]] for name, d in out.items()}
json.dump(chart, open(os.path.join(here, "chart.json"), "w"))
print(md)
