# Filesystem and Harness executable scenario handoff

The Rust-owned scenario sources are:

- [`filesystem_scenarios.rs`](../../rust/crates/sdk-examples/src/filesystem_scenarios.rs), scenario `filesystem-mounted-workspace`.
- [`harness_scenarios.rs`](../../rust/crates/sdk-examples/src/harness_scenarios.rs), scenario `harness-admission-recovery-cancel`.

The Filesystem runner opens the real local provider, creates durable and
ephemeral volumes, checks out both at the head with a private overlay, mounts
them at `/` and `/.scratch`, writes `/.scratch/tool-output.txt`, checkpoints
the workspace generation, and records the immutable mounted-view snapshot.
Its receipt is returned only after those provider operations succeed.

The Harness runner uses public `TaskGroup` admission, completion,
cancellation rejection, and fresh-group recovery. It also binds the canonical
typed custom executor to `HarnessBuilder` and records the builder's required
owner journal boundary. The custom executor stages and appends a typed model
event and preserves ref-only attachments, matching the existing
`rust/crates/harness/examples/custom_executor.rs` contract.

The examples registry owner must add the two modules and wire their source
constants, `QUICKSTART_SNIPPET` values, and receipt functions into the existing
Rust scenario and fixture pipeline. The modules intentionally do not edit the
shared registry. The required direct dependencies are `acyclic-fs` with its
`local` feature and `acyclic-harness` with default features disabled.

The isolated direct compile was attempted through a temporary crate importing
both modules. Dependency compilation stopped when the temporary target filled
the Windows system volume; no source error was emitted before the disk-full
failure. The shared examples owner should run the normal locked workspace
checks after adding the dependencies and registry exports.
