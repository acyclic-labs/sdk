# Central TypeScript fixture qualification

This directory is the checked-in evidence produced by
`consume-rust-fixture-receipt.mjs`. The adapter consumes the Rust-owned
fixture receipt for the 73 generated RPC calls, the RSA Inference transcript
for 14 calls, and the RSA Machines transcript for 19 calls. It resolves every
entry against `target/sdk-contract/rust-authority.json`, so the central log
contains exactly the 106 RPCs emitted by the Rust authority.

The package archives under `qualification/packages/` were packed from the
compiled `dist` trees with npm lifecycle scripts disabled. Their hashes,
versions, generated client hashes, and deterministic build nonces are recorded
in `typescript-rust-authority-central-metadata.json`. The per-RPC result files
under `qualification/consumers/` are the inputs to the Rust
`sdk-qualification-receipt` writer; the central metadata is an index over those
inputs, not a replacement for them.

Regenerate into an output directory outside the source checkout:

```text
node research/acceptance/typescript-review/consume-rust-fixture-receipt.mjs \
  --source-root C:/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk \
  --expected-source-root C:/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk \
  --authority target/sdk-contract/rust-authority.json \
  --runtime-receipt research/acceptance/typescript-review/rust-authority-runtime-receipt-20261004.json \
  --inference-receipt research/acceptance/typescript-review/inference-rsa-current-source-20261004.receipt.json \
  --machines-receipt research/acceptance/typescript-review/machines-rsa-current-source-20261004.receipt.json \
  --package-root <compiled-typescript-snapshot> \
  --output <central-generation-output>
```

The adapter requires each captured fixture revision to be an ancestor of the
current SDK revision, checks that the authority union is 106 methods, and
fails if any compiled package is missing its `dist` tree or Rust provenance.

The installed-consumer probe is
[`installed-rust-fixture-probe.mjs`](../installed-rust-fixture-probe.mjs). It
imports the eight package exports by their published names after npm installs
the archives from `qualification/packages/`, connects to the Rust fixture over
gRPC, and records `execution_mode: "remote"` on every result. Run it from an
isolated consumer directory with `FIXTURE_GRPC_ADDRESS` set to the fixture
server's gRPC endpoint. Its output is accepted only after all eight fixture
services are present and the 106-method result set has been converted to the
canonical per-RPC files above.

The central review artifacts are:

- [`typescript-rust-authority-central-metadata.json`](typescript-rust-authority-central-metadata.json)
- [`typescript-rust-authority-scenario-log.json`](typescript-rust-authority-scenario-log.json)
- [`qualification/consumers/typescript-rust-fixture.json`](qualification/consumers/typescript-rust-fixture.json)
- [`qualification/packages/`](qualification/packages/)
