# Variance and entropy at the decision boundary

Same 72 inputs for every backend: 24 judge pairs (2 options; 4 are identical-diff controls) and 48 labelled gate commands (3 options). Repeats per input as listed. Open models are read through the logprobs of the option letters (one token, temperature 0); Jev and open-jev through their native decision heads.

| backend | inputs | repeats | median ms | cost | mean SD of top prob | max SD | verdict flips | mean entropy (bits) | accuracy (labelled) | identical controls called tie |
|---|---|---|---|---|---|---|---|---|---|---|
| open-jev (Space) | 1 | 3 | 2886 | $0.0000 | 0.0000 | 0.000 | 0 | 0.228 | 1.00 (1) | 0/0 |
| jev | 72 | 5 | 235 | $0.0059 | 0.0072 | 0.030 | 2 | 0.374 | 0.82 (68) | 0/4 |
| llama-3.1-8b letter-logits | 72 | 5 | 497 | $0.0022 | 0.0064 | 0.127 | 1 | 0.654 | 0.68 (68) | 0/4 |
| deepseek-v4-flash letter-logits | 21 | 5 | 2002 | $0.0003 | 0.0000 | 0.000 | 0 | 0.149 | 0.90 (20) | 0/1 |

## At each decision boundary

Rows bucket every input by its mean top probability. The interesting row is 0.50–0.70: the referee is nearly undecided there, so run-to-run noise can flip the verdict.


### open-jev (Space)

| mean top prob | inputs | mean SD | max SD | verdict flips | mean entropy | accuracy |
|---|---|---|---|---|---|---|
| ≤0.50 | 0 | | | | | |
| 0.50–0.70 | 0 | | | | | |
| 0.70–0.90 | 0 | | | | | |
| ≥0.90 | 1 | 0.0000 | 0.000 | 0 | 0.228 | 1.00 (1) |

### jev

| mean top prob | inputs | mean SD | max SD | verdict flips | mean entropy | accuracy |
|---|---|---|---|---|---|---|
| ≤0.50 | 2 | 0.0166 | 0.020 | 1 | 1.533 | 0.50 (2) |
| 0.50–0.70 | 6 | 0.0152 | 0.020 | 1 | 1.123 | 0.17 (6) |
| 0.70–0.90 | 16 | 0.0159 | 0.030 | 0 | 0.689 | 0.64 (14) |
| ≥0.90 | 48 | 0.0030 | 0.012 | 0 | 0.128 | 0.98 (46) |

### llama-3.1-8b letter-logits

| mean top prob | inputs | mean SD | max SD | verdict flips | mean entropy | accuracy |
|---|---|---|---|---|---|---|
| ≤0.50 | 4 | 0.0355 | 0.108 | 1 | 1.473 | 0.50 (4) |
| 0.50–0.70 | 19 | 0.0145 | 0.127 | 0 | 1.201 | 0.42 (19) |
| 0.70–0.90 | 13 | 0.0018 | 0.023 | 0 | 0.670 | 0.77 (13) |
| ≥0.90 | 36 | 0.0005 | 0.006 | 0 | 0.269 | 0.81 (32) |

### deepseek-v4-flash letter-logits

| mean top prob | inputs | mean SD | max SD | verdict flips | mean entropy | accuracy |
|---|---|---|---|---|---|---|
| ≤0.50 | 0 | | | | | |
| 0.50–0.70 | 1 | 0.0000 | 0.000 | 0 | 1.310 | 1.00 (1) |
| 0.70–0.90 | 1 | 0.0000 | 0.000 | 0 | 0.759 | 0.00 (1) |
| ≥0.90 | 19 | 0.0000 | 0.000 | 0 | 0.056 | 0.94 (18) |

## Per-set split

| backend | set | inputs | mean SD | flips | mean entropy | accuracy |
|---|---|---|---|---|---|---|
| open-jev (Space) | judge | 1 | 0.0000 | 0 | 0.228 | 1.00 (1) |
| jev | judge | 24 | 0.0034 | 0 | 0.114 | 1.00 (20) |
| jev | gate | 48 | 0.0092 | 2 | 0.505 | 0.75 (48) |
| llama-3.1-8b letter-logits | judge | 24 | 0.0014 | 0 | 0.487 | 0.65 (20) |
| llama-3.1-8b letter-logits | gate | 48 | 0.0088 | 1 | 0.738 | 0.69 (48) |
| deepseek-v4-flash letter-logits | judge | 5 | 0.0000 | 0 | 0.089 | 1.00 (4) |
| deepseek-v4-flash letter-logits | gate | 16 | 0.0000 | 0 | 0.168 | 0.88 (16) |

SD: population standard deviation of the top option's probability across repeats. Entropy: of the full distribution per call, averaged (max 1 bit for 2 options, 1.585 for 3). Accuracy: argmax of the mean distribution against the label; identical-diff controls count as correct when the top probability is at most 0.6.
