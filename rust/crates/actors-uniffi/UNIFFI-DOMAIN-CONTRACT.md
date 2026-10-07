# Domain feature contract

The facade consumes the existing `acyclic-actors::domain` declarations
directly. The Actors crate owner needs to add an opt-in `uniffi` feature and
feature-gated UniFFI derives to those declarations; no facade copy of the 19
semantic records/enums is permitted.

The feature contract is:

- `acyclic-actors/uniffi` enables the UniFFI dependency only for the facade
  build;
- the existing domain records and enums derive the matching UniFFI `Record` or
  `Enum` metadata under `cfg_attr(feature = "uniffi", ...)`;
- fields exposed as UniFFI records are public to the derive or are exposed by
  the supported UniFFI record mechanism, while Rust constructors and client
  validators remain the semantic authority;
- the facade references the domain types in its exported signatures, so the
  generated bindings receive one type declaration from the producer; and
- the domain crate does not depend on the facade or on Kotlin/JVM tooling.

The facade's only owned UniFFI declarations are `BindingError`,
`CancellationHandle`, and the opaque `ActorsClient`. `ActorsClient` delegates
all eight operations and both connection functions to `acyclic_actors::client`
and uses its `run_with_cancellation` helper. The root workspace registration
is intentionally left to the workspace owner.

The producer contract also covers semantic roots used inside those records:
`ActorId`, `CodeSha256`, `PositiveU64`, and the true-only
`subscription_start::CurrentHeadMarker` are Rust-owned nominal values and must
expose feature-gated UniFFI custom-type conversions while retaining their
existing validation; `SubscriptionStart` must expose a feature-gated UniFFI
enum implementation. Plain `Record` derives on the tuple newtypes are
insufficient for UniFFI 0.31 because they do not provide the required `TypeId`,
`Lower`, and `Lift` implementations. These conversions belong beside the
declarations in the domain crate, so the facade continues to consume the
actual semantic types without mirrors.

The declarations and constructors live in `domain/nominal.rs`. The facade's
`validate_actor_id`, `validate_code_sha256`, `validate_positive_u64`, and
`validate_current_head` exports are unit-returning bridges for generated
nominal factories; each delegates to the corresponding Rust constructor or
conversion. Generated bindings must keep trusted Rust decode on a private
constructor path and route caller-created values through these bridges.

`src/nominal.rs::NOMINAL_METADATA` is the single producer metadata table for
nominal names and wire carriers. `src/bin/export-nominal-metadata.rs` emits
`uniffi.nominal.toml` from that table; language-specific configs and consumer
fixtures must use this generated projection rather than maintain a separate
type or validator registry.