# acyclic-harness-filesystem

Explicit bridge from Harness resource references to versioned Filesystem workspaces. It keeps generation identities visible rather than pretending mutable paths are durable references.

```sh
cargo add acyclic-harness-filesystem
```

Choose an `acyclic-fs` authority and object store for the required durability boundary, then wire the adapter into Harness. See the [adapter API](https://docs.rs/acyclic-harness-filesystem/latest/acyclic_harness_filesystem/), [Filesystem guide](https://docs.rs/acyclic-fs/latest/acyclic_fs/), and [Harness guide](https://docs.rs/acyclic-harness/latest/acyclic_harness/).
