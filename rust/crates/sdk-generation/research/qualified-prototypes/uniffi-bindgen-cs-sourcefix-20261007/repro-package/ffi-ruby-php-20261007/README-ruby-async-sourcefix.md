# Ruby maintained UniFFI 0.31 async source qualification

This artifact rebuilds the maintained Ruby backend from UniFFI 0.31.0 source with a narrow source patch. The patch imports the upstream scheduler pattern, attaches async future FFI definitions from metadata, and emits generic async calls for top-level functions and object methods. It does not add operation-specific wrappers or a shared semantic registry.

The current macOS arm64 producer is the recorded Rust-native dylib from the Go qualification cohort (`35C3BC984C8A8B3DCD101CFAA84570E2606E4F08F2EB0B17CBF54DCB84DB7A04`). The generated Ruby was run from an external task-owned GEM_HOME on ivar Ruby 2.6.10 with ffi 1.17.2.

The all-eight probe passed with authenticated TLS/HTTP2 fixture traffic and a `9007199254740993` cursor. A pending `InspectActor` was interrupted with `Thread#raise`; the shared helper's ensure path cancelled and freed the native future, and the fixture was released and closed cleanly. Wrong bearer credentials produced generated `BindingError::Service`; runtime negative checks rejected a non-string `ActorId` and an out-of-range u64.

The prior official 0.31.0 Ruby failure remains in `receipt.json`. This sourcefix receipt and the installable gem are separate evidence and do not relabel the historical failure.
