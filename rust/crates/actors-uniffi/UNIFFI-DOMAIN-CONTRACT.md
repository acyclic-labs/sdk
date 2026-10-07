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
`ActorId`, `CodeSha256`, and `PositiveU64` are tuple newtypes and must expose
feature-gated UniFFI custom-type conversions while retaining their existing
validation; `SubscriptionStart` must expose a feature-gated UniFFI enum
implementation. Plain `Record` derives on the tuple newtypes are insufficient
for UniFFI 0.31 because they do not provide the required `TypeId`, `Lower`,
and `Lift` implementations. These conversions belong beside the declarations
in the domain crate, so the facade continues to consume the actual semantic
types without mirrors.
