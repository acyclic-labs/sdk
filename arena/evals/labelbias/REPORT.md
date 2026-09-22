# Jev is biased by the name and position of your options

*Repo Arena, experiment 11 · 22 September 2026 · `typesafe/jev-1.13` via OpenRouter · every number below cost $0.0012 in total to produce and reruns from one script*

Give TypeSafe's Jev two byte-identical code changes and ask which is better. It picks the one called "A" with probability 0.95. Rename them and it picks the earlier letter, the lower number, or whichever is listed first. The tilt is deterministic, repeatable, and has nothing to do with the code. It also disappears completely the moment you give the model a way to say "they are the same".

Put precisely, Jev's choice output breaks two symmetries a classifier over an unordered option set should have. **Order invariance:** permuting the options should permute the probabilities and nothing else; with the order-free names left and right, listing order alone swings the answer by 0.40. **Label invariance:** renaming the options should change nothing; the earlier name wins at 0.88 to 0.94 and keeps winning from second place. A classical classifier never sees its labels as input, so it has the second property trivially, and a per-option scorer such as open-jev's, which scores each option alone and softmaxes, has the first by construction. Jev has neither, which is only possible if it reads all the options together as one sequence and lets label text and slot position into the score. That is the strongest architectural inference these experiments support about a model whose internals are undisclosed. The violation shows only when the evidence cannot separate the options; on real differences the swapped orders agree to within 0.03.

## The results

Same two identical diffs every time. Only the option names change. P(first listed), three runs each, spread never above 0.04.

| Earlier name listed first | P(first) | Earlier name listed second | P(first) |
|---|---|---|---|
| fork A / fork B | **0.93** | fork B / fork A | 0.12 |
| fork X / fork Y | **0.94** | fork Y / fork X | 0.21 |
| fork 1 / fork 2 | **0.94** | fork 2 / fork 1 | 0.14 |
| alpha / bravo | **0.90** | bravo / alpha | 0.28 |
| fork B / fork C | **0.92** | fork C / fork B | 0.51 |
| left / right | **0.87** | right / left | 0.47 |

- **The earlier name wins.** A, X, 1, alpha, B-over-C. From second place it still wins at 0.72 to 0.88.
- **The first slot wins.** With order-free names, left takes 0.87; reversed, the two priors cancel to 0.47.
- **They add.** Together about 0.93. Opposed about 0.50. Each is worth roughly 0.4 on a question the evidence cannot decide.

Raw per-run numbers are in `results.json`.

**And the kill shot.** Add a third option, "they are the same". Three runs each; every run within 0.01 of the value shown.

| Options | P(first) | P(second) | P(same) |
|---|---|---|---|
| fork A / fork B / they are the same | 0.00 | 0.00 | **1.00** |
| fork X / fork Y / they are the same | 0.00 | 0.00 | **1.00** |

The bias does not shrink. It vanishes. Jev knew the forks were identical the whole time; it had no option that said so. On a forced binary choice with the true answer missing, it fell back on priors about the labels and committed to them at 0.95, exactly as hard as it commits to real evidence.


## The example

Two snippets of DeepSeek V4 Flash generated code. The only difference is the label.

```
=== fork A
-        if best is None or it.price_cents > best.price_cents:
+        if best is None or it.price_cents < best.price_cents:

=== fork B
-        if best is None or it.price_cents > best.price_cents:
+        if best is None or it.price_cents < best.price_cents:
```

*Which fork best completes the task and should be promoted?*

| | fork A | fork B |
|---|---|---|
| Which wins? | **0.95** | 0.05 |
| Safe to land? | 0.83 | 0.85 |
| How complete? | 1.00 | 1.00 |

Five repeats: 0.95, 0.96, 0.95, 0.96, 0.96. B listed first, names kept: A still 0.90.

## Reproduce it

```
cd arena/evals
export OPENROUTER_API_KEY=sk-or-...
python labelbias/run.py        # the twelve naming schemes and the third-option test, ~$0.001, ~2 minutes
python judge/run_judge.py      # the 12 sabotage pairs with Sonnet 5 as a comparison judge, ~$0.03
```

`labelbias/run.py` builds the identical-diff state, loops over the naming schemes, and calls OpenRouter's Decisions endpoint with `{"type": "choice", "instructions": ..., "criteria": {name: name}}`. Raw probabilities are written to `labelbias/results.json`. Swap in any diff you like; the effect only needs two options the state cannot separate.

To see it in a real race: `node ../packages/arena/dist/cli.js run selfrace/tasks.json --models deepseek/deepseek-v4-flash,deepseek/deepseek-v4-flash --forks git --save-states states` on a copy of `seed/`, then read the saved states.

## What the harness does about it

1. The winner question now always includes `no meaningful difference`. A probability of 0.5 or more on it is a tie.
2. The arena also asks twice with the forks relabelled by position and averages, which cancels any residual prior on close calls. Cost: about $0.0002 per race.
3. Ties fall through to the tests, then to the cheaper worker, so an identical pair lands the cheaper attempt rather than the one that happened to be called A.

Rerun of the self-race with these changes: the three identical pairs scored "no meaningful difference" at 1.00, 1.00, 1.00; the three different pairs still picked a winner at 0.99, 0.99, 0.76.

| Approach | Identical diffs | Sabotage set | Calls |
|---|---|---|---|
| Raw, two options | 0.95 / 0.05 | 20 / 24 | 1 |
| Ask twice, relabel, average | 0.50 / 0.50 | 12 / 12 pairs | 2 |
| Add "they are the same" | 0.00 / 0.00 / 1.00 | 24 / 24 | 1 |

Swap-averaging is the standard cure for option-order effects in language-model evals and it works here too, but its 0.50 means "I cancelled a bias", not "the model judged these equal". The third option's 1.00 means what it says.


## Does it generalise, or is this an edge case?

Identical diffs are the limit of a continuum, not a special case. Five more measurements, all on the arena branch.

### 1. People's names: the slot prior on its own

Names carry no sequence, so any tilt is position. Identical diffs, two repeats averaged (`names.py`, `names.json`).

| first / second | P(first) | P(second) |
|---|---|---|
| kevin / greg | 0.84 | 0.16 |
| greg / kevin | 0.83 | 0.17 |
| bob / sam | 0.83 | 0.17 |
| sam / bob | 0.82 | 0.17 |
| ram / hao | 0.82 | 0.18 |
| hao / ram | 0.81 | 0.18 |

Net name preference: 0.00 in every pair. Position alone is worth about +0.33. With six names and six identical diffs, whoever sits in slot 1 takes 0.83 to 0.94; mean by slot 0.91 / 0.02 / 0.02 / 0.01 / 0.01 / 0.03. A seventh option, "they are the same": 0.99.

### 2. Three lettered options: the label prior on its own

Identical diffs, names permuted across fixed slots.

| slots 1, 2, 3 | P(slot 1) | P(slot 2) | P(slot 3) |
|---|---|---|---|
| A / B / C | 0.97 | 0.01 | 0.02 |
| A / C / B | 0.95 | 0.02 | 0.03 |
| B / A / C | 0.07 | 0.90 | 0.03 |
| B / C / A | 0.02 | 0.01 | 0.97 |
| C / A / B | 0.07 | 0.91 | 0.02 |
| C / B / A | 0.04 | 0.01 | 0.95 |

Mean by name: A 0.94, B 0.03, C 0.03. Mean by slot: 0.35 / 0.31 / 0.34. With three lettered options position stops mattering; "fork A" wins from any slot. So the two-option picture decomposes cleanly: a slot prior of about 0.33 that any labels show, plus a sequence-name prior that letters and numbers add on top, and for letters the name prior is the stronger of the two.

### 3. Real, non-identical forks that both passed their tests

The six no-test races, each re-judged in both orders (`../notest/states`).

| race | same diff? | P(fork 1) as A | P(fork 1) as B | swing |
|---|---|---|---|---|
| 1 | no | 0.13 | 0.08 | 0.05 |
| 2 | no | 0.87 | 0.88 | 0.01 |
| 3 | no | 0.51 | 0.51 | 0.00 |
| 4 | yes | 0.92 | 0.09 | 0.83 |
| 5 | no | 0.95 | 0.92 | 0.03 |
| 6 | no, near-tie | 0.42 | 0.04 | 0.38, verdict flips |

### 4. A quality-gap ladder: when does the label flip a real advantage?

Same base fix in both forks; one fork gets a small extra. Judged with the better fork first and second, as A/B and as kevin/greg. "Evidence" is the mean over the two name orders, the referee's opinion with priors cancelled (`ladder.py`, `ladder.json`).

| the better fork also has... | as A, first | as B, second | as kevin, first | as greg, second | evidence | letter + slot swing | slot-only swing |
|---|---|---|---|---|---|---|---|
| nothing (identical) | 0.94 | 0.06 | 0.83 | 0.17 | 0.50 | 0.88 | 0.66 |
| a trailing comment | 0.27 | 0.20 | 0.26 | 0.45 | 0.35 | 0.07 | -0.20 |
| the loop variable renamed | 0.53 | 0.34 | 0.57 | 0.55 | 0.56 | **0.18, flips** | 0.02 |
| a docstring | 0.84 | 0.71 | 0.88 | 0.86 | 0.87 | 0.12 | 0.02 |
| type hints | 0.88 | 0.68 | 0.90 | 0.88 | 0.89 | 0.20 | 0.02 |
| an idiomatic `min()` | 0.96 | 0.79 | 0.92 | 0.86 | 0.89 | 0.17 | 0.06 |
| a test | 0.91 | 0.88 | 0.90 | 0.98 | 0.94 | 0.03 | -0.08 |
| correctness (other fork is wrong) | 1.00 | 1.00 | 0.99 | 1.00 | 0.99 | 0.00 | -0.01 |

Once there is anything real to judge, the slot prior is gone (0.02 to 0.06). The letter prior survives as a 0.12 to 0.20 discount on whichever fork is called B, and at a genuine toss-up, evidence 0.56, it flips the verdict: 0.53 as A, 0.34 as B. Strong evidence erases both.

### 5. It contaminated one of our own headlines

The original raced arm listed DeepSeek first in every race, so it was always "fork A". Rerun with the fixed judge, relabelled in both orders and with "no meaningful difference" available (`../results/first/raced-fixedjudge.jsonl`):

| task | original verdict, DeepSeek always A | debiased verdict | P(same) |
|---|---|---|---|
| bug fix | DeepSeek 1.00 | tie | 0.61 |
| bug fix | DeepSeek 0.88 | tie | 1.00 |
| bug fix | DeepSeek 0.88 | Sonnet 0.41 | 0.32 |
| bug fix | DeepSeek 0.85 | tie | 1.00 |
| rename | DeepSeek 0.81 | tie | 1.00 |
| new feature | DeepSeek 0.80 | DeepSeek 0.71 | 0.07 |
| new feature | DeepSeek 0.88 | tie | 1.00 |
| new feature | DeepSeek 1.00 | Sonnet 0.59 | 0.09 |
| docs | Sonnet 0.59 | DeepSeek 0.62 | 0.10 |
| new feature | Sonnet 0.95 | tie | 0.72 |
| new feature | DeepSeek 0.63 | Sonnet 0.47 | 0.43 |
| refactor | DeepSeek 0.67 | DeepSeek 0.35 | 0.32 |

**"DeepSeek won 10 of 12 verdicts" was mostly the label.** Debiased: DeepSeek 3, Sonnet 3, no meaningful difference 6. Both forks passed their tests in every race, so the cost headline, which was decided by tests and cost, stands unchanged: $0.026 routed against $0.78 on Sonnet. The verdict headline does not, and the experiment-6 row of the main report has been corrected.

### The general statement

- The prior is a fixed weight that evidence must outweigh: about 0.33 for the slot, more for a sequence label, both only visible where the evidence is thin.
- Thin evidence is the common case in real races, not the rare one: 6 of 12 head-to-heads between two passing forks were genuinely equivalent, and the referee's true answer was "same".
- Position is harmless once anything real distinguishes the options. The letter label is not: it discounts the disfavoured fork by 0.12 to 0.20 and can flip a true toss-up.
- So: always offer "no meaningful difference", never use sequence labels for a choice question without symmetrising, and keep the two-order relabelled average. The arena now does all three, and the tie is broken by tests then cost.

## Follow-up experiments

- **Does it hold on other question types?** The score and yes/no questions were not tilted here. Test whether "score fork A" and "score fork B" as separate questions drift with the label, and whether noul questions prefer "yes".
- **Does it hold on TypeSafe's direct API?** Everything here went through OpenRouter's Decisions endpoint. The wire format is the same; the serving stack may not be.
- **Does the third option ever hurt?** On the twelve pairs it never exceeded 0.04 when the forks differed. Test near-ties on purpose: two correct fixes with different style, where "same" is arguably right and arguably wrong.
- **Long states.** All of this was on 200-token diffs. The prior may weigh more, or less, when the evidence is 10k tokens of diff.
- **Other referees.** Run the same twelve schemes against open-jev's scorer and against a chat model read through letter logprobs. The letter-logit method is known to prefer "A"; whether a trained scoring head does too is the interesting comparison.
- **The same table against open-jev's scorer.** A per-option scorer should return the same number in every order. If it does and Jev does not, the architectural inference in the introduction is proven rather than inferred.
- **Use it as a feature.** A cheap identical-input control before every batch would detect any drift in the served model's priors over time. It costs a thousandth of a cent.


## A note on "calibrated"

TypeSafe's [launch post](https://typesafe.ai/blog/introducing-system-one-models-and-jev) says, verbatim: "All answers are accompanied with calibrated probabilities and confidence scores." "Always communicates confidence and uncertainty with every output." "Calibrated: higher confidence means higher accuracy." And, describing the problem Jev is meant to solve: "Even if prompted for a confidence estimate, models tend to be overconfident and inconsistent. If a model can do a task 95% of the time but doesn't say when it's in the 5%, it can't automate that task."

Calibrated means a 0.95 should be right about 95% of the time. The identical-diff result is the sharpest test of that claim there is: the model reported 0.95 on a question where the right answer was 0.50, did not say it was in the 5%, and did so deterministically.

How this applies:

- **The number is a ranking, not a probability.** Above 0.90 Jev was right 98% of the time on the labelled inputs; between 0.50 and 0.70 it was right 17% of the time. The order of its answers is trustworthy. The magnitude is not. On public classification sets its expected calibration error was 0.17 to 0.20, against 0.02 to 0.12 for the open reimplementation (see `../variance/REPORT.md` and the Jev-vs-open-jev comparison).
- **Calibration presupposes a complete option set.** A decision model puts all of its probability on the options it is given. When the true answer is missing, the probabilities cannot be calibrated because there is nothing correct to be calibrated against; they measure the model's priors about the labels instead. The 0.95 here is not a miscalibrated belief about the code, it is a confident answer to a question that had no right answer.
- **So the fix is on the caller's side.** Include "they are the same", "neither", or "not enough information" whenever it can be true, and the reported probabilities become meaningful again: 1.00 on "same" for identical diffs, and 24 of 24 on the sabotage set. Nothing about the model changed.
- **Never threshold on the raw number.** Treat anything within 0.06 as a tie, use tests as ground truth where they exist, and let outcomes rather than Jev's confidence set routing thresholds. That is how Repo Arena uses it, and under those rules the referee is cheap, fast, and right where it counts.

In fairness, TypeSafe's own docs describe their headline accuracy figure as "not empirical", and the sharpness that produces the overconfidence is the same property that makes the model useful when the evidence is real. The claim to take with care is the word calibrated, not the model.

The wider set of experiments is summarised in `../REPORT.md`; variance and entropy figures are in `../variance`.
