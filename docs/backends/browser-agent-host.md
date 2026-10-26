# Browser agent host

Compose `AgentHost::hosting` with `BrowserStream` and the canonical guest wire handler. Each suite case opens a fresh IndexedDB database; reopening uses a new provider session against that same database. No native filesystem or process adapter is pulled into this browser qualification crate.

Run `bash scripts/qualify-agent-host.sh` on bb2 with Chrome and its driver configured. It executes both native adapters, checks browser test compilation and executes the unchanged suite in headless Chrome through the pinned wasm-bindgen test runner. The workspace web qualification runs the browser suite as well.

This qualifies the Rust AgentHost, IndexedDB streams and `acyclic_actors::guest::wire::Remote` boundary. The generated JavaScript `AgentGuest.open` artifact and its `AbortSignal` bridge have a separate harness artifact test (`browser-agent-guest.html`); these Rust vectors do not claim to execute that JavaScript facade. There is no production model call or live account in this run. See [the suite](../../rust/crates/conformance/agent-host/README.md) for fault boundaries and admission controls.

On bb2 (2026-10-26), the unchanged suite passed in Chrome 156 in 2.36 seconds, using wasm-bindgen 0.2.117. The shared browser bundle lacked execute permission on `chrome_crashpad_handler`; the successful run used a private copy with that permission repaired. No vector or timeout was changed.
