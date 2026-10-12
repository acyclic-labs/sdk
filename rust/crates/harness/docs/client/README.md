# Harness client views

`Client::new` installs a typed `Domain` and finite limits. `observe` consumes
validated evidence; `begin` creates a hypothesis; `view` selects overlays with
provenance. `discard` invalidates descendants, `advance` applies logical retention
ticks, and `release` frees canonical demand after hypotheses are discarded.

Predictions do not authorize or admit effects. The domain validates evidence,
basis and correspondence against existing authoritative records. A conflicting
completion replaces the overlay and invalidates descendants. Restore requires
the installed domain's trusted validator and the durable sequence watermark.

`examples/client_views.rs` demonstrates these operations using Harness types.
