# Acyclic Rust SDK

`acyclic-sdk` is the installable Rust facade for the public Acyclic service
families. The family crates remain independently usable; this crate gives a
Rust application one stable dependency and keeps the public surface grouped by
service family.

```toml
[dependencies]
acyclic-sdk = "0.2"
```

Use a family through its namespace:

```rust
use acyclic_sdk::{actors, objects, stream};

// Each family connection helper chooses the best transport available on the
// target and service endpoint. Applications do not select platform feature
// flags to obtain the native or browser implementation.
let _ = actors::connect;
let _ = objects::connect;
let _ = stream::connect;
```

The facade includes `actors`, `filesystem`, `harness`, `inference`,
`machines`, `objects`, `stream`, and `workers`. Their contracts, validation,
transport behavior, and documentation are implemented in the Rust family
crates and generated from the Rust-owned contract model where applicable.

Embedded behavior is exposed through the `filesystem` and `harness` modules;
remote behavior is exposed through each family's transport-neutral `connect`
helper and `Client` API. Cargo selects the target implementation as part of
normal compilation.
