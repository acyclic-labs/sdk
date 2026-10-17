# Harness client hypotheses

Call `await HypothesisClient.initialize()` before construction.

Supply a trusted, pure `ClientDomain<Value, Assumption, Evidence>`. Its identity
pins the adapter implementation and revision; its basis pins authority, generation,
revision and content. `observe` must preserve operation correlation and reject
stale/conflicting observations using validated reducer/journal output.
