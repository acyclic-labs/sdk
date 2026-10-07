# Ruby current-main generic external type qualification — 2026-10-07

This receipt qualifies the maintained UniFFI 0.31 Ruby generator patched for async futures, fieldless tuple enum fields, source-owned nominal custom types, and imported component RustBuffer converters. The producer is the exact current Actors candidate `Q:/sdk/work/sdkgen-actors-c8-minimal`, revision `737775737a54c31c1d50b11c677ca9ae900b45b2`; the historical PRIMARY Mac artifact remains unchanged.

The generated package contains `acyclic_actors.rb` and `acyclic_actors_uniffi.rb`, with generic `ActorId`, `CodeSha256`, `PositiveU64`, and `CurrentHeadMarker` wrappers sourced from UniFFI metadata. The generator emits the imported `AcyclicActors::RustBuffer` converter bridge from the component external-type map; no operation-specific Ruby facade or handwritten type mirror was added.

Mac ivar arm64 runtime evidence:

- `all8-terminal.txt`: connect-with-CA plus create, update, inspect, add, remove, resume, checkpoint, invoke all pass; the cursor preserves `9007199254740993`.
- `cancel-terminal.txt` and `pending-marker.txt`: a real pending `InspectActor` future returns typed `BindingError::Cancelled`; the fixture records `entered` then `aborted active=0`.
- `negative-terminal.txt`: invalid ActorId, short CodeSha256, zero PositiveU64, false CurrentHeadMarker, static wrapper type errors, true-only CurrentHeadMarker, and readonly guards pass.
- `installed-terminal.txt`: the task-local gem was installed and the external consumer passed all eight operations.

Windows and WSL are explicitly excluded from this receipt because this final generic generator binary was rebuilt on ivar arm64. Earlier Windows/WSL candidate receipts remain historical and were not relabeled.
