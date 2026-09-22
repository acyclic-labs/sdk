# The "fork A" preference, replicated and explained

Identical diffs, only the option strings change, 3 repeats each, single call. Cost $0.0008.

| option names, first listed / second listed | P(first listed) | P(second) |
|---|---|---|
| fork A / fork B | 0.93 | 0.07 |
| fork B / fork A | 0.12 | **0.88** (A wins from second place) |
| fork X / fork Y | 0.94 | 0.06 |
| fork Y / fork X | 0.21 | **0.79** (X wins from second place) |
| fork 1 / fork 2 | 0.94 | 0.06 |
| fork 2 / fork 1 | 0.14 | **0.86** |
| left / right | 0.87 | 0.13 |
| right / left | 0.47 | 0.53 |
| alpha / bravo | 0.90 | 0.10 |
| bravo / alpha | 0.28 | 0.72 |
| fork B / fork C (no A) | 0.92 | 0.08 |
| fork C / fork B | 0.51 | 0.49 |

Two additive priors, not one:
1. **The earlier name wins**: A over B, X over Y, 1 over 2, alpha over bravo, B over C. When the names are swapped so the earlier name is listed second, it still wins (A 0.88, X 0.79, 1 0.86) or ties (C/B 0.51).
2. **The first listed position wins**: with neutral names (left/right) the first slot takes 0.87; reversed, it is a coin flip because "left" and first position now pull opposite ways.

When both priors agree the tilt is about 0.93; when they oppose, they roughly cancel. The earlier claim that "the bias is on the label, not the position" was half right.

## The real fix: give Jev the true answer as an option

With a third option, **"they are the same"**, on the identical diffs:

| options | result, 3 repeats |
|---|---|
| fork A / fork B / they are the same | 0.00 / 0.00 / **1.00** every time |
| fork X / fork Y / they are the same | 0.00 / 0.00 / **1.00** |

And on the full synthetic judge set (12 pairs, both orders, single call, no swap-averaging):

- **24/24 correct.** Every sabotage pair still picked the right fork at 0.90 to 1.00; both identical controls chose "they are the same" at 0.97 to 1.00; the "same" option was never chosen on a genuinely different pair (max 0.04).

The tilt was never a belief that A is better. It was a forced binary choice with the true answer missing from the option set, so the model fell back on priors about the labels. This is the defining property of a typed decision model: **the output space is the option set, so the option set must contain the truth.**

## What the arena does now

1. The winner question includes "no meaningful difference" whenever there are two or more forks. P(same) ≥ 0.5 is a tie.
2. It still asks twice with forks relabelled by position and averages, which cancels any residual name or position prior.
3. Ties fall through to tests, then worker cost.
