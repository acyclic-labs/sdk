# Jev is biased by the name and position of your options

*Repo Arena, experiment 11 · 22 September 2026 · `typesafe/jev-1.13` via OpenRouter · every number below cost $0.0012 in total to produce and reruns from one script*

Give TypeSafe's Jev two byte-identical code changes and ask which is better. It picks the one called "A" with probability 0.95. Rename them and it picks the earlier letter, the lower number, or whichever is listed first. The tilt is deterministic, repeatable, and has nothing to do with the code. It also disappears completely the moment you give the model a way to say "they are the same".

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

**And the kill shot.** Add a third option, "they are the same":

| Options | P(first) | P(second) | P(same) |
|---|---|---|---|
| fork A / fork B / they are the same | 0.00 | 0.00 | **1.00, 1.00, 1.00** |
| fork X / fork Y / they are the same | 0.00 | 0.00 | **1.00, 1.00, 0.99** |

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

## Follow-up experiments

- **How strong is the prior against weak evidence?** Take two forks that differ by one trivial line and shrink the difference until the label prior wins. That measures the prior in units of evidence, not just on identical input.
- **Does it hold on other question types?** The score and yes/no questions were not tilted here. Test whether "score fork A" and "score fork B" as separate questions drift with the label, and whether noul questions prefer "yes".
- **Does it hold on TypeSafe's direct API?** Everything here went through OpenRouter's Decisions endpoint. The wire format is the same; the serving stack may not be.
- **Does the third option ever hurt?** On the twelve pairs it never exceeded 0.04 when the forks differed. Test near-ties on purpose: two correct fixes with different style, where "same" is arguably right and arguably wrong.
- **Long states.** All of this was on 200-token diffs. The prior may weigh more, or less, when the evidence is 10k tokens of diff.
- **Other referees.** Run the same twelve schemes against open-jev's scorer and against a chat model read through letter logprobs. The letter-logit method is known to prefer "A"; whether a trained scoring head does too is the interesting comparison.
- **Three or more options.** With forks A, B, C, does the prior concentrate on A or spread down the alphabet? The arena races up to three; the answer decides whether relabelling matters there.
- **Use it as a feature.** A cheap identical-input control before every batch would detect any drift in the served model's priors over time. It costs a thousandth of a cent.

The wider set of experiments is summarised in `../REPORT.md`; variance and entropy figures are in `../variance`.
