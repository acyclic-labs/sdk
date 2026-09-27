# Retraining the scorer for commercial use

The published adapter inherits CC-BY-NC-4.0 from its customer-support-ticket training data. The base
model (Qwen3.5-4B-Base, Apache-2.0) and the author's training script are not the problem, so a
commercial-safe scorer is the same recipe with the ticket family removed.

`retrain.sh` does exactly that on a GPU box: it downloads the author's `system_one.py` at run time,
builds the dataset from the remaining public families, trains the LoRA plus scalar head for the same
2,200 steps, fits the temperature on the validation split, and evaluates on test. Check the Yelp terms
for your use, or set its size to 0.

Expect the ticket tasks (queue, priority, type, language) to be absent from the metrics, and expect the
adapter to be weaker on support-ticket-shaped states. Everything else on the model card should hold.

Afterwards:

```
SYSTEM_ONE_ADAPTER=/path/to/run SYSTEM_ONE_TEMPERATURE=1.9 jev serve --backend local
jev eval cases.jsonl --backend http://gpu:8788 --log evals.jsonl     # your own labelled cases
```

This has not been run: the machine this was written on has no GPU.
