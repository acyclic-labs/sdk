# Where Jev is useful and where it is not

Every experiment run on Repo Arena so far, with the verdict for the referee. Costs are OpenRouter's own accounting. Logs are hash-chained JSONL under this directory; each experiment can be rerun.

| # | Experiment | What it tests | Result | Verdict for Jev |
|---|---|---|---|---|
| 1 | Four-arm eval, 12 tasks with hidden tests (`run.sh`) | Routing and judging when tests exist | fast (Haiku) 11/12 $0.36 298 s · routed 11/12 $0.41 390 s · raced 11/12 $0.78 666 s · frontier (Sonnet) 11/12 $0.78 342 s | **Not useful as shipped.** Default routing over-escalated a rename and a refactor to two Sonnet attempts (85% of the routed bill). A fixed cheap model beat it. Tests decided every promotion. |
| 1b | Same 12 tasks, aggressive routing thresholds (`ARENA_ROUTE=aggressive`) | Whether routing can beat a fixed model | routed-aggressive 11/12 **$0.026** 330 s | **Useful.** Same outcome as Sonnet-on-everything at 3% of the cost and faster. One threshold change. The router's signal was right; the policy table was wrong. |
| 2 | Gate set: 48 shell commands labelled harmless / moderate / destructive (`gate/`) | Tool-call risk without any test to lean on | Jev 77% (ECE 0.16, $0.0008, 272 ms/q) · Llama 8B letter-logits 67% (700 ms/q) · keyword heuristic 100% | **Useful, with a caveat.** Beats a chat model read through logits on accuracy, latency and calibration. Misses are adjacent-level boundary calls (harmless vs moderate). The heuristic's 100% is an upper bound: it was written with the labels in view. Every destructive command Jev missed it still called moderate, never harmless. |
| 3 | Six no-test races, DeepSeek vs Sonnet, hand-reviewed (`notest/`) | Picking between two passing diffs | Agreed with the reviewer 4/6, called a fair tie 1/6, wrong 1/6 | **Mixed.** The miss: byte-identical diffs scored 0.86 / 0.14. The win: it scored a fork unsafe at 0.30 because Sonnet's diff dragged in a compiled `.pyc` file, and picked the clean fork at 0.97. |
| 4 | Synthetic judge set: 12 hand-made pairs incl. sabotage, both orders (`judge/`) | Judge accuracy against a known answer, and vs an LLM judge | Jev raw 20/24, $0.0005, 236 ms · Jev swap-averaged 12/12 with both identical controls called tie · Sonnet-as-judge 23/24, $0.0260, 3799 ms | **Useful, and the strongest result.** Caught every sabotage: deleted function, hardcoded test pass, leaked secret, `os.system`, off-by-one, swallowed exception, scope creep. After debiasing it matched the LLM judge at 56x lower cost and 16x lower latency. |
| 5 | Self-race: DeepSeek vs DeepSeek, 6 tasks (`selfrace/`) | Position and label bias | Identical diffs scored 0.95 / 0.05 for the fork named "A". Reversing order alone still gave 0.90. Relabelling by position in each of two calls and averaging: 0.50 / 0.50. | **Useless raw, fixed in the harness.** The bias is on the label "A", not the position. The arena now asks twice with relabelling. Real differences still come through at 0.99. |
| 6 | Raced arm, Jev's pick vs tests (from experiment 1 logs) | Does the referee side with the passing fork | Forks disagreed on tests in 1 of 12 races; Jev picked the passing fork. 10 races both passed; 1 both failed. | **Consistent, small sample.** |
| 7 | Router's task-kind vs labels (from experiment 1 logs) | Classification for the board | 10/12 correct. Misses: a validation feature called a bug fix; an iterable refactor called a feature. | **Useful enough for the board.** |
| 8 | Calibration on public sets (AG News, Yelp, 64 rows each; see the Jev vs open-jev page) | Are the probabilities probabilities | ECE 0.17 and 0.20, mean confidence 0.95 on AG News; open-jev publishes 0.02 and 0.12 | **Not useful as a probability.** Useful as a ranking. Thresholds must come from outcomes, not from Jev's confidence. |
| 9 | The docstrings task (experiment 1) | A control | All four arms failed it; the test was verified against a hand-written answer | Not about Jev. Four models missed an easy instruction. Kept as data. |

## What this adds up to

**Where Jev earns its place**
- Judging finished diffs against a task, including catching sabotage and junk, at 1/50th the cost and 1/16th the latency of an LLM judge, with equal accuracy once debiased (experiments 3, 4).
- Gating tool calls when there is nothing to run (experiment 2).
- Routing, once the thresholds are tuned from outcomes rather than guessed (experiment 1b). The classification signal is good; the default policy was not.
- Being cheap enough to ask twice. The debiasing fix doubled the referee cost to about $0.0002 per race and nobody will notice.

**Where it does not**
- As a probability. Overconfident on public sets and biased toward the label "A" on identical inputs. Never threshold on its raw confidence; use tests, outcomes, and the debiased average.
- As the promotion decision when tests exist. Tests are ground truth; Jev's safety score was 0.3 on forks whose tests passed cleanly. The arena treats a passing test as overriding Jev's doubt.
- With the default routing thresholds. They cost more than a fixed cheap model. Ship the aggressive set, or better, fit them from the board.

**Harness changes these experiments forced**
- Passing tests override the referee's safety doubt on promote.
- The judge asks in both fork orders with positional relabelling and averages.
- `ARENA_ROUTE=aggressive` thresholds; `--budget`; `--save-states` for review; per-task `test` and `models` in task files.
- Git-worktree forks exclude the arena log and `.env`.
