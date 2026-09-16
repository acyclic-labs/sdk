# acyclic-memory

Deterministic in-memory provider profile for SDK composition and tests. It is process-local and deliberately does not claim durable storage or a service boundary.

```sh
cargo add acyclic-memory
```

Start with `MemoryProfile::new()` and pass its providers into the SDK families you need. Use durable or hosted providers for production data. See the [Rust API](https://docs.rs/acyclic-memory/latest/acyclic_memory/) and [workspace overview](https://github.com/acyclic-labs/sdk#readme).
