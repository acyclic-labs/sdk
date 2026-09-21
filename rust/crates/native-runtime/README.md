# `acyclic-native-runtime`

Small, platform-specific file primitives shared by Acyclic's local storage providers.

Most applications should depend on a higher-level crate such as
[`acyclic-fs`](https://docs.rs/acyclic-fs) instead. This crate is public so the
other Acyclic crates can be installed from crates.io without Git dependencies.

The implementation selects the native backend at compile time:

- Linux: `io_uring`-aware file operations.
- macOS: dispatch I/O.
- Windows: overlapped I/O.
- Other targets: the portable fallback.

The API documentation is available on
[`docs.rs`](https://docs.rs/acyclic-native-runtime).
