# Swift-specific generator review

The generated Swift package is driven by the Rust producer source and pinned
UniFFI templates. The package consumers import `AcyclicActors`; they do not
declare a second set of operation signatures, object layouts, validators, or
wire metadata.

The generated surface inspected from the source-locked build contains these
Rust-owned semantic objects and constructors:

- `ActorId`, `CodeSha256`, and `ActorLimits` retain nominal identity and call
  the Rust constructors through generated FFI converters.
- `ActorObservation` preserves the typed fields `actorId`, `codeSha256`,
  `homeRegion`, `state`, `subscriptions`, `checkpointEpoch`, and
  `configurationRevision`.
- The client exposes the eight async operations plus the CA-aware connection
  function. Generated async signatures carry the existing optional
  `CancellationHandle`; the source consumer passes `nil` and relies on Swift
  `Task.cancel()`.

The only proven Swift-specific gap in the pinned UniFFI 0.31 templates is
structured-concurrency cancellation: `Async.swift` must pass the generated
`rust_future_cancel_*` symbol, wrap polling in
`withTaskCancellationHandler`, and retain deferred future freeing;
`macros.swift` must emit the cancel symbol; and `Helpers.swift` must map the
Rust cancelled status to `CancellationError()`. The Windows and macOS live
receipts exercise those exact generated paths.

No additional Swift semantic or signature gap was observed in the source-
locked package build. In particular, `CodeSha256` and `ActorLimits` are
producer-derived generated objects, not handwritten metadata mirrors. The
only handwritten C source in the lane is `ffi_anchor.c`, a one-line module
materialization anchor that includes the generated header.
