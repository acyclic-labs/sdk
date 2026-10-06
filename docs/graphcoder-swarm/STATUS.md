# Harness-first local coding swarm status

This is the single current status and evidence index. The locked acceptance
matrix has 68 required rows in [requirements.json](requirements.json), with
the receipt contract in [qualification-receipt.schema.json](qualification-receipt.schema.json)
and the platform inventory in [platform-gates.json](platform-gates.json).

## Qualification state

Final qualification is **open**. This checkout contains no final receipt and
no source-bound installed native, package, PTY, WASM, or full recursive-runtime
evidence. A focused pass never promotes a matrix row by itself. Missing,
failed, skipped, flaky, stale, fixture-only, compile-only, or unbound evidence
keeps the final gate closed.

The historical checkpoint files were removed from the working tree because
their external logs and binaries are no longer retained. Their commits remain
in Git history. They must not be cited as current qualification evidence.
The machine-readable scenarios and contracts required by the runners remain:

- `graphcoder-native-scenarios.json`
- `graphcoder-installed-swarm-scenarios.json`
- `graphcoder-real-backend-scenarios.json`
- `graphcoder-transport-fault-scenarios.json`
- `platform-gates.json`
- `qualification-receipt.schema.json`
- `requirements.json`

## Retained Q evidence

The packaging slice retains only focused, source-bound checks with their
scope stated plainly:

- Native binding producer commit `9da098ca9`: five producer tests pass,
  including Cargo identity, exact `-j1` argv, retained logs, source mutation,
  and tamper rejection. No Cargo build was run by that check.
- Qualification validator commit `54162ce38`: twenty receipt and coverage
  tests pass, including shared-suite assertions and rejection of unknown
  requirement IDs. These are validator tests, not runtime qualification.
- The final required evidence must be regenerated from the final clean source
  after native, package, PTY, and WASM lanes complete.

## Final execution order

Run the platform manifest gates, produce the Windows native binding and its
causal receipt, run the package gate, consume fresh native and packed
artifacts, run PTY and transport-fault lanes, then assemble and validate the
68-row receipt. Every descriptor, transcript, artifact, source identity,
execution count, exit status, and digest must be fresh for that source.

Models remain mocked for this goal. Filesystem, storage, recursive agents,
approved process effects, and terminal interaction must be real. No sandbox is
implemented or implied by workspace routing.
