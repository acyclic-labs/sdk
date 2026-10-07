# Shared Rust Stream producer audit r1

Date: 2026-10-07. Scope: independent audit for Dart/.NET reuse of the exact JVM Stream producer cohort. No product source was changed.

## Minimality result

The producer delegates append, read, follow, and children to `acyclic_stream::StreamClient` and carries the provider's `RecordStream`/`ChildStream` as opaque cursors. The remote bridge uses `GrpcStreamClient::connect_with_ca_certificate`; it does not translate recovery, replay, ordering, retries, or transport. The generated records preserve `u64` sequence/timestamp values and byte payloads.

The generated foreign contract is therefore reusable by Dart and .NET with the same operations and cursor types. Their adapters should only expose language-native async iteration and close the opaque cursor in cancellation/finally. They should not add a second operation registry, recovery algorithm, semantic record mirror, or custom native loader.

## Actionable cancellation findings

1. `RecordCursor::next` moves the provider `BoxStream` out of the cursor before awaiting it (`src/lib.rs:118-141`). Dropping a cancelled foreign future drops that local stream and therefore cancels the underlying gRPC request, but the cursor's qualification counter remains active until `close_cursor` or cursor drop. Kotlin's `Flow` adapter already closes in `finally`; Dart and .NET must make the same close guarantee and test server-side stream drop. Do not claim cleanup from future cancellation alone.
2. `ChildCursor::next` has the same moved-stream behavior (`src/lib.rs:209-231`) but no active counter. If child-stream cancellation is a production requirement, add an explicit server-observed child cleanup probe or keep child cleanup as a documented cursor-close obligation.
3. A future Rust hardening option is a drop guard around the moved stream that calls `close_now` when `next` is cancelled. That would make the active metric self-consistent even if a foreign adapter forgets its `finally`, but it is a producer change and should be coordinated with the shared Rust owner. Until then, generated Dart/.NET adapters must always call `close_cursor`.

## Production integration gaps

- `active_record_cursors` is a qualification metric, not semantic product API. Remove it from the production foreign surface after the installed cleanup proof, or gate it behind a test-only facade.
- `StreamClientBridge` over `MemoryStream` is a local test fixture. Production bindings should expose the maintained remote/provider constructor and retain local construction only in tests.
- Local and remote bridge methods intentionally duplicate the UniFFI object surface because UniFFI objects cannot be generic over provider type. Keep the duplication mechanical and generated from the same Rust declarations; do not hand-maintain language operation lists.
- Before Dart/.NET admission, generate their bindings from the same producer snapshot and compile a surface probe covering all public methods, full-width `u64` values, typed errors, cursor close, and cancellation. Their runtime probe must observe the same server-side abort marker used by the JVM proof.

The current JVM evidence proves the underlying behavior: final Mac JAR `FCFF21D468D7CF16EEE385567BDE3DB6419667C9533CED0740A9C881A4429546`, Mac raw receipt `6E40FB658024CDD75BC17FC73979C92FC28F0276A5F04A430785F76D5C302B46`, and cross-platform artifact archive `03A3915C864B3F74C8CF1856F708F260D081516AD936F36C90CC6069DD721A37`.
