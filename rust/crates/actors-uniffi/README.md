# Actors UniFFI facade

This standalone crate is the first Rust-owned JVM binding slice for the
current Actors source. It is deliberately outside the SDK workspace until a
generated Kotlin package is qualified.

The crate pins the complete UniFFI cohort to `0.31.0` and uses proc-macro
metadata only; there is no UDL or second contract. `ActorId`, `CodeSha256`, and
`PositiveU64` are opaque foreign objects whose constructors call the canonical
`acyclic_actors::domain` constructors. `connect_actors` and
`ActorsClient::inspect_actor` call the canonical Rust client and use its
`run_with_cancellation` helper through an explicit `CancellationHandle`.

The result records are adapter metadata over Rust-owned domain accessors. They
do not contain wire messages, transport code, retry logic, or validators.
Optional Actor observation presence is preserved. The current domain rejects
unknown enum values as typed semantic errors; an `Unknown { raw }` projection
remains a later compatibility decision if the Actors domain changes to retain
unknown values.

## Qualification boundary

The source authority is foundation revision `371bb4170e16aca973176b6756a261ee5add7297`.
The authoritative Actors domain hash used for this slice is
`79092881EC6B9434A3AC3AE6B810DCA46BB85A8E4C8C1E98191982AC5D82B4B2`.

This first slice intentionally leaves create/update/add/remove/resume/
checkpoint/invoke request records and package generation for the next step.
No native artifact or Kotlin package is claimed by this source-only change.
