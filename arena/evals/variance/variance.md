# Variance and entropy at the decision boundary

72 inputs (24 judge pairs with 2 options, 48 gate commands with 3 options), 5 repeats each, same text every time.

| backend | inputs | median ms | cost | mean SD of top prob | max SD | inputs whose verdict flipped | mean entropy (bits) | accuracy on labelled | identical-pair controls called tie |
|---|---|---|---|---|---|---|---|---|---|
| jev | 72 | 235 | $0.0059 | 0.0072 | 0.030 | 2 | 0.374 | 56/68 | 0/4 |
| llama-3.1-8b letter-logits | 72 | 497 | $0.0022 | 0.0064 | 0.127 | 1 | 0.654 | 46/68 | 0/4 |
| deepseek-v4-flash letter-logits | 21 | 2002 | $0.0003 | 0.0000 | 0.000 | 0 | 0.149 | 18/20 | 0/1 |

## By confidence bucket (mean top probability across repeats)


### jev

| bucket | inputs | mean SD | verdict flips | mean entropy | accuracy |
|---|---|---|---|---|---|
| ≤0.50 (coin flip) | 2 | 0.0166 | 1 | 1.533 | 0.50 (2) |
| 0.50–0.70 (boundary) | 6 | 0.0152 | 1 | 1.123 | 0.17 (6) |
| 0.70–0.90 | 16 | 0.0159 | 0 | 0.689 | 0.64 (14) |
| ≥0.90 (confident) | 48 | 0.0030 | 0 | 0.128 | 0.98 (46) |

### llama-3.1-8b letter-logits

| bucket | inputs | mean SD | verdict flips | mean entropy | accuracy |
|---|---|---|---|---|---|
| ≤0.50 (coin flip) | 4 | 0.0355 | 1 | 1.473 | 0.50 (4) |
| 0.50–0.70 (boundary) | 19 | 0.0145 | 0 | 1.201 | 0.42 (19) |
| 0.70–0.90 | 13 | 0.0018 | 0 | 0.670 | 0.77 (13) |
| ≥0.90 (confident) | 36 | 0.0005 | 0 | 0.269 | 0.81 (32) |

### deepseek-v4-flash letter-logits

| bucket | inputs | mean SD | verdict flips | mean entropy | accuracy |
|---|---|---|---|---|---|
| ≤0.50 (coin flip) | 0 | | | | |
| 0.50–0.70 (boundary) | 1 | 0.0000 | 0 | 1.310 | 1.00 (1) |
| 0.70–0.90 | 1 | 0.0000 | 0 | 0.759 | 0.00 (1) |
| ≥0.90 (confident) | 19 | 0.0000 | 0 | 0.056 | 0.94 (18) |

SD is the population standard deviation of the top option's probability across repeats; entropy is of the full distribution (max 1 bit for 2 options, 1.585 for 3). Accuracy uses the mean distribution's argmax against the label; identical-pair controls count as correct when the top probability is at most 0.6.
