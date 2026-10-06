# Inference and Machines wire qualification

This report records the source-owned checks for the two Rust scenario modules.
The scenarios are executable against repository code and use encoded protobuf
fixtures that can be consumed by another language binding. They do not assert
model, provider, isolation, durability, billing, or hosted endpoint
availability.

## Inference

Source: `src/inference_scenarios.rs` and
`../../inference/tests/run_watch_wire.rs`.

The fixtures cover:

- proto3 presence for `RunView.cancellation_requested` and the optional
  `RunView.result`;
- `ExactRational.numerator` as a signed zig-zag `sint64` value;
- contiguous event ordering and rejection of a gap or duplicate;
- cancellation progress followed by a `Cancelled` terminal event;
- terminal recovery from `last_sequence + 1`, which closes without replaying a
  duplicate terminal event.

The scenario receipt scope is `rust-wire-validation`. Passing these checks is
qualified local wire evidence only.

## Machines

Source: `src/machines_scenarios.rs` and
`../../machines/tests/wire_fixture.rs`.

The fixture preserves optional request message presence, enum values, a custom
image digest, network-policy digest, suspension policy, expiration policy, and
budgets. The executable path uses `SimulatedMachines` through the public
`Machines` facade for create, inspect, checkpoint, and fork. Its receipt scope
is `rust-process-local-simulation`, and its assurance is explicitly
`ProcessLocalSimulation`.

## Generation authority

`sdk-contract-wire` emits the descriptor and `.proto` artifacts from the Rust
model modules. The generator provenance test checks the emitted authority
manifest for Rust model inputs and rejects authored `proto/inference` or
`proto/machines` paths as generation sources. The compatibility descriptor
fixtures remain immutable comparison baselines.

## Validation record

Run the focused checks from the repository root:

```text
cargo test --locked -p acyclic-inference --test run_watch_wire
cargo test --locked --manifest-path rust/crates/machines/Cargo.toml --test wire_fixture
cargo test --locked --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --test generator_provenance
cargo test --locked --manifest-path rust/crates/sdk-examples/Cargo.toml inference_scenarios::tests --lib
```

The Machines scenario module has unit tests and is compiled in the examples
crate once the examples registry exports `machines_scenarios`; its public
tests were also executed through an isolated temporary harness during source
qualification.
