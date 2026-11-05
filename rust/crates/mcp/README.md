# acyclic-mcp

Shared MCP protocol version, bounded JSON-line framing, and a native stdio tool server.
The `stdio` feature exposes the framing and server APIs. Tool providers retain their
own errors; serving distinguishes provider failures from transport failures.

Run the protocol controls with `cargo test -p acyclic-mcp --features stdio`.
