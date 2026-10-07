# CurrentHead marker coordination and proof scope

Snapshot: 2026-10-07, canonical producer `Q:\sdk\work\sdkgen-main-port-current`, revision `0093da12d7d4205d56db1c25fb75d50b59de11d6`.

The canonical Rust producer still declares `SubscriptionStart::CurrentHead { current_head: bool }`, exposes `current_head(bool)`, and exposes `current_head_value() -> Option<bool>`. Its ingress conversion accepts the wire oneof only for `CurrentHead(true)` and rejects `CurrentHead(false)` and a missing oneof. The TypeScript semantic marker is already `value: true`, but the Rust semantic representation remains bool.

The coordinated foreign output audit is not a marker-change snapshot: the current Q `actors-uniffi/src/lib.rs` still declares `CurrentHead { current_head: bool }`; the checked Kotlin consumer output still has a Boolean payload; and the checked Swift nominal output has a Bool payload plus a producer validation call. These artifacts therefore cannot support a claim that false is semantically unrepresentable. A unit-like/nominal Rust marker must land and be source-frozen by the owning producer/binding work before that stronger theorem is asserted.

The new Kani theorem is deliberately limited to the current actual production ingress: for every symbolic wire `bool`, `TryFrom<wire::SubscriptionStart>` returns `Ok` iff the payload is true; successful conversion has no cursor payload and reports `Some(true)`; false returns `DomainError::InvalidSubscription`. The theorem invokes `contract::generated::domain::SubscriptionStart::try_from`, `cursor_value`, and `current_head_value` directly. It does not reimplement the validator or prove semantic construction unrepresentability.

The oneof round-trip theorem was rerun against this same source snapshot and proves every symbolic `u64` cursor and the true current-head oneof round-trip equal to the original production wire value. Both runs use Kani 0.68.0, CBMC 6.11.0, nightly-2026-08-21, offline mode, one build job, and task-local targets.

Checked foreign-source/output paths:

* `Q:\sdk\work\sdkgen-main-port-current\rust\crates\actors-uniffi\src\lib.rs` retains `CurrentHead { current_head: bool }`.
* `Q:\sdk\work\actors-uniffi-current-nominal-kotlin-20261007-v5\consumer-compile\src\main\kotlin\acyclic_actors\acyclic_actors.kt` retains a Boolean `CurrentHead` payload.
* `Q:\sdk\work\swift-actors-cancellation-nominal-final-20261007\generated-config2\acyclic_actors.swift` wraps the current-head case nominally but still stores a Bool payload and reads the wire Bool.
