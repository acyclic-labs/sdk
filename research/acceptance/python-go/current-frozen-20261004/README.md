# Current frozen Python/Go remote qualification

All consumer runs in this directory use the Rust model authority `f7a2e0eedea058cf85b1746e8dd0af8716cb362e035c45938b0c1ddf3d4d9b2a` and the installed Python wheel SHA-256 `8496E7D67AF04F60E6197BAF9FF521F79D3B28F62B71A119541E2077F73505AD`. Every recorded scenario is marked `execution_mode: remote`, checks a real gRPC response, and includes serialization or streaming assertions.

The completed semantic runs are Python Actors/Stream 18/18, Harness 5/5, Inference mTLS 14/14, and Machines mTLS 19/19. Go completed the same Actors/Stream scenario, Inference mTLS behavior (generate, inspect, cancel, watch `[0,1]`), and all 19 Machines methods with nonempty watch output. The Go module archive used by the installed consumer is recorded in `qualification-summary.json`.

The reusable full73 Python probe reached 65/73 with eight stateful cases intentionally retained as failures: object upload state before `GetObject`, stream hierarchy/deadline state, filesystem generation state, and Harness replay/operation aggregate state. The specialized semantic receipts above close the Actors, Stream, and Harness cases with Rust scenario state. Objects and Filesystem still require dedicated stateful probes before the 73-method family gate can be marked complete.

The mTLS fixtures are pinned by binary SHA-256 in `qualification-summary.json`. Their fixture build source is recorded separately from the Rust model hash so the central gate cannot accidentally promote fixture ancestry as model ancestry.

No production deployment, registry publication, or merge to main was performed.
