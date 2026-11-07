# acyclic-mcp

Shared MCP protocol version, bounded JSON-line framing, and a native stdio tool server.
The `stdio` feature exposes the framing and server APIs. Tool providers retain their
own errors; serving distinguishes provider failures from transport failures.

Once serving observes input closure or failure, the provider's `closed` callback
retires owned work before queued replies drain. A slow output consumer cannot
postpone that callback. Callback failures remain provider failures; an original serving
failure is preserved, and an output failure retains transport-error precedence.

Run the protocol controls with `cargo test -p acyclic-mcp --features stdio`.
