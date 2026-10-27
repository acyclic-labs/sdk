# Browser agent host

Compose `AgentHost::hosting` with `BrowserStream` and the canonical guest wire handler. Each suite case opens a fresh IndexedDB database; reopening uses a new provider session against that same database. No native filesystem or process adapter is pulled into this browser qualification crate.

Filesystem authority shares the agent Stream so the existing filesystem spawn participant can publish child volumes in the same transaction. Immutable objects stay in `filesystem_database`. Experimental profiles created with the older split authority layout need an explicit migration.

Run `bash scripts/qualify-agent-host.sh` on bb2 with Chrome and its driver configured. It executes both native adapters, checks browser test compilation and executes the unchanged suite in headless Chrome through the pinned wasm-bindgen test runner. The workspace web qualification runs the browser suite as well.

This qualifies the Rust AgentHost, IndexedDB streams and `acyclic_actors::guest::wire::Remote` boundary. The generated JavaScript `AgentGuest.open` artifact and its `AbortSignal` bridge have a separate harness artifact test (`browser-agent-guest.html`); these Rust vectors do not claim to execute that JavaScript facade. There is no production model call or live account in this run. See [the suite](../../rust/crates/conformance/agent-host/README.md) for fault boundaries and admission controls.

On bb2 (2026-10-27), the unchanged suite passed in Chrome 156 in 4.25 seconds, using wasm-bindgen 0.2.117. The normal packaged harness also passed all three artifact pages: prompt/history and exact replay, pause across reopen, cancel/archive, uncertain tool-effect reconciliation, and the real guest facade. The shared runtime releases pending follow I/O before reading actor state; settled conversation appends publish their consumed inbox cursor in the same commit. The run used a private Chrome copy with executable helper permissions; no vector, assertion or timeout was relaxed.

The hosted protocol vectors additionally compose `Preset::executors(..)` -> `Bound::admitted` -> `hosting::serve` over this backend's storage and handler boundary. They share their model/tool script and every assertion with the real Durable Objects endpoint: approval binding and decision replay, immutable replies, exact spawn cuts and replay, conflicting prompts, lazy children and typed refusals. The fixtures are public in `acyclic_agent_host_conformance::protocol` for client-protocol consumers.
