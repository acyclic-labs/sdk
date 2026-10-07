# Dart current production semantic audit (2026-10-07)

Producer component `acyclic_actors_uniffi` is generated with `--crate acyclic_actors_uniffi` and external package mapping `acyclic_actors = "acyclic_actors.dart"`. The canonical domain component is kept in `generated/acyclic_actors.dart`; the facade imports it and uses public `*FfiCodec` bridges. No domain fields are mirrored in the facade.

## Semantic roots

Generated canonical records (24): ActorLimits, ActorObservation, AddSubscriptionRequest, AddSubscriptionResponse, Binding, CheckpointActorRequest, CheckpointActorResponse, CreateActorRequest, CreateActorResponse, Header, InspectActorRequest, InspectActorResponse, InvokeActorRequest, InvokeActorResponse, RemoveSubscriptionRequest, RemoveSubscriptionResponse, ResumeSubscriptionRequest, ResumeSubscriptionResponse, ServiceError, SubscriptionObservation, SubscriptionSpec, SubscriptionStart, UpdateActorRequest, UpdateActorResponse.

Generated canonical enums and oneof: ActorState, ErrorCode, SubscriptionState, StartCursor, StartCurrentHead, and the sealed Start base. `SubscriptionStart.start` preserves the oneof presence shape.

## Public semantic checks

- Public UInt64 aliases and every generated UInt64 field/getter are `BigInt`; internal FFI lowering remains signed `int` only for the 64-bit bit pattern.
- All eight caller-supplied request/result operations are present: create, update, inspect, add, remove, resume, checkpoint, invoke.
- TLS CA connection uses caller-supplied `Uint8List?`; typed `BindingErrorException*` cases are generated.
- Runtime probes exercise zero, 2^63, 2^64-1, nullable checkpoint presence, `SubscriptionStart` oneof presence, semantic typed errors, and real cancellation/server abort cleanup.

## Static/runtime qualification

- Windows x86_64: all8 PASS; cancellation PASS (`BindingErrorExceptionCancelled`, server marker `active=0`).
- WSL Linux x86_64: all8 PASS; cancellation PASS (`BindingErrorExceptionCancelled`, server marker `active=0`).
- SSH host `ivar` macOS arm64: all8 PASS; cancellation PASS (`BindingErrorExceptionCancelled`, server marker `active=0`).
- Generator workspace unit tests: 225 passed. The 21 legacy golden tests have 19 expected failures from the maintained BigInt output change and 2 passes; no generated production package error occurred.

## Reproduction

- Generator source: `Q:\\sdk\\work\\uniffi-bindgen-dart-013-source`.
- Producer source: `Q:\\sdk\\work\\sdkgen-main-port-current`, commit recorded in the production receipt; the source tree was dirty and its exact status is retained.
- Installed archive: `Q:\\sdk\\work\\actors-uniffi-dart-production-current-20261007.zip`.
- Primary receipt directory: `C:\\Users\\varun\\.codex\\worktrees\\rust-source-foundation\\sdk\\rust\\crates\\sdk-generation\\research\\qualified-prototypes\\dart-actors-ffi-buffer`.
