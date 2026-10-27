# @acyclic-labs/pi

`configurePiModel(registry, model, compaction, bridge)` registers a Pi model implementation in the Rust browser agent registry exported by `@acyclic-labs/harness/browser`. It is host configuration. Applications use the same public harness client for every backend.

The bridge projects canonical prepared request bytes plus the ordered file byte slices already authorized and verified by the Rust host. It does not receive a file resolver or implement conversation selection, file authorization, model accounting admission, task execution or a transport. Model capacity and token-count callbacks use the registry's generated Rust signatures; counting sees the exact request bytes and digest without invoking projection or generation.

Generation receives the original dispatch identity and cancellation signal, pulls the provider one event at a time, and projects provider events into the canonical generated ModelEvent type. Reconciliation receives the original dispatch and observed events; null means the outcome is unresolved. It never redispatches. The host owns event validation, terminal admission, cancellation and the retained attempt.

This replaces the legacy piProvider/ModelProvider task facade and the JavaScript projectPiRequest/file-resolution path. Model-wire projection belongs in the injected bridge; core request admission and verified content reads remain in the one Rust implementation. No frontend callback or secret is serialized to a remote backend.
