# Variance and entropy at the decision boundary

72 inputs (24 judge pairs with 2 options, 48 gate commands with 3 options), 3 repeats each, same text every time.

| backend | inputs | median ms | cost | mean SD of top prob | max SD | inputs whose verdict flipped | mean entropy (bits) | accuracy on labelled | identical-pair controls called tie |
|---|---|---|---|---|---|---|---|---|---|
| open-jev (Space) | 1 | 2886 | $0.0000 | 0.0000 | 0.000 | 0 | 0.228 | 1/1 | 0/0 |

## By confidence bucket (mean top probability across repeats)


### open-jev (Space)

| bucket | inputs | mean SD | verdict flips | mean entropy | accuracy |
|---|---|---|---|---|---|
| ≤0.50 (coin flip) | 0 | | | | |
| 0.50–0.70 (boundary) | 0 | | | | |
| 0.70–0.90 | 0 | | | | |
| ≥0.90 (confident) | 1 | 0.0000 | 0 | 0.228 | 1.00 (1) |

SD is the population standard deviation of the top option's probability across repeats; entropy is of the full distribution (max 1 bit for 2 options, 1.585 for 3). Accuracy uses the mean distribution's argmax against the label; identical-pair controls count as correct when the top probability is at most 0.6.
