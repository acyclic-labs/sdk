# acyclic-sdk

An umbrella crate for one tested combination of the Acyclic Rust SDK families. Import a family through this crate or depend on that crate directly when you need a smaller graph.

```sh
cargo add acyclic-sdk
```

```rust
use acyclic_sdk::stream::StreamPath;
let path = StreamPath::new("runs/example").unwrap();
assert_eq!(path.as_str(), "runs/example");
```

The umbrella re-exports `harness`, `filesystem`, `stream`, `objects`, `machines`, `inference`, `memory`, and Harness adapters. It does not configure credentials or start services; each provider and transport retains its own requirements and assurance boundary. See the [workspace overview](https://github.com/acyclic-labs/sdk#readme) and the package-local guides for each family.
