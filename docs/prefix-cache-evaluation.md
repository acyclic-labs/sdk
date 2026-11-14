# Offline prefix-cache feature evaluation

Run the provider integration tests with output enabled:

```sh
cargo test --locked -p acyclic-harness-providers --test main prefix_cache::feature_evaluation -- --nocapture
```

Each `FEATURE_EVAL` JSON line pairs the same task with one optional feature off
and on, for Chat, Responses, Anthropic and Gemini. It reports completed task
successes, generation requests, input/output tokens, cache-read tokens and signed
on-minus-off deltas. Token units are deterministic serialized UTF-8 bytes from
the existing scripted transport and cache emulator. They measure request cost
and cache behavior; scripted answers do not establish live model quality.

The preserved optional-source guard from #3635/#3613 (landed through #3840)
checks exact isolated request identities and bytes. The paired guards extend it
to growing conversations: each execution-only preset input (UI, approvals,
configuration, sessions, evaluations, GOAL, plans and hooks), project instructions,
environment facts, skill catalogues, selected skill bodies and MCP tool listings
is selected, edited and removed
while its original context epoch remains pinned. Source changes append notices;
they do not replace earlier requests. Every case also encodes a rewritten system
instruction and requires both prefix comparison and cache-read checks to reject
it. Execution-only inputs must have zero success/token/cache-read deltas.

Compaction runs the stock executor with capacity admission on and off. The
enabled run must dispatch a separate summary, adopt its durable checkpoint,
remove older history, preserve the reusable provider header and read its cache.
The report includes the summary's request and tokens. Reopening either completed
turn must replay its answer without another dispatch. A scripted summary can
cost more input tokens overall even when the continuing worker request shrinks;
the report retains that cost rather than claiming a net model-quality gain.

The seed prompt and serialized preset size remain pinned in the existing guards.
Live Gemini acceptance requires a credentialed repeated request against a real
Gemini model, recording provider-reported cached tokens and preserved prefix
across a feature edit and compaction. This offline evaluation does not perform
that step.
