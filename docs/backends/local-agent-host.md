# Native agent host

Compose `AgentHost::hosting` with a `LocalStream`, an `AgentHandler`, the host clock and any turn participant composer. `actor_store(&provider)` keeps actor checkpoints, cursors, alarms and agent effects in the same Stream transaction. A native host may call the handler directly or install `acyclic_actors::guest::wire::Remote` around it.

Run `cargo test -p acyclic-agent-host-conformance --features local` on bb2. Both adapters execute the unchanged `agent_host::verify` vectors. Each crash case drops the host and provider and reopens the retained directory. Faults surround provider API calls; this does not simulate physical torn journal writes, which the LocalStream suite owns.

The suite uses a deterministic turn driver rather than a model account. Admission vectors use the real filesystem turn host and signed workload scopes, with separate counters for scheduler and admitted-turn IO. See [the suite](../../rust/crates/conformance/agent-host/README.md) for the exact assertions and guest facade boundary.

On bb2 (2026-10-26), both native adapters passed together in 1.49 seconds. The new crate also passed targeted Clippy with warnings denied.
