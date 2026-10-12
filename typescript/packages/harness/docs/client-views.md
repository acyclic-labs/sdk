# Harness client views

`@acyclic-labs/harness/client` binds the existing Rust `acyclic_harness::client`
kernel. It holds bounded demand and hypotheses; it performs no IO, admission,
domain reduction, rollback, cancellation or transport. Construction is synchronous
and effect-free after the explicit shared `await ClientViews.initialize()` step.

Supply a trusted, pure `ClientDomain<Value, Assumption, Evidence>`. Its identity
pins the adapter implementation and revision. Its basis must pin the original
authority, generation, revision and content identity. `observe` must consume the
host's existing validated reducer/journal output, preserve original operation
correlation and reject stale/conflicting observations. Raw predictions and remote
`verified: true` flags are not evidence. These are host obligations, rather than
authentication furnished by the generic kernel.
