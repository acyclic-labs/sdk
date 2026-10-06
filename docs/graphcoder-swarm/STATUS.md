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

Retained native and tooling evidence is scoped to the source in each receipt.
It does not qualify later source revisions or the full matrix:

- Model-input conformance/rejection: nine cases at `1932e22aa`, recorded in
  [input contracts](checkpoint-q-input-contracts-2026-10-06.json).
- Persistent request admission: seven cases at `46852dc41`, recorded in
  [persistent input](checkpoint-q-persistent-input-pass-2026-10-06.json).
- Windows native process cleanup: eleven cases at `bdd5ea0a4`, recorded in
  [native processes](checkpoint-q-native-process-pass-2026-10-06.json).
- Qualification tooling: twenty-three cases at `e8d329944`, recorded in
  [qualification tooling](checkpoint-q-qualification-tooling-pass-2026-10-06.json).
- Both recursive and unified native scenarios at `af3945f43` failed with
  stack overflows; see [preflight failures](checkpoint-q-owned-task-preflight-failure-2026-10-06.json).
- The owned resolver task repair at `f195400b5` prepared and published two
  children without the prior overflow, but the scenario failed during the
  `default-message-child` tool exchange. See [communication failure](checkpoint-q-owned-task-communication-failure-2026-10-06.json).

- The unified native scenario at `f3d9612fa` completed workspace read and edit,
  then overflowed before its second batch publication. See
  [unified runtime failure](checkpoint-q-unified-edit-overflow-2026-10-06.json).
- Qualification tooling at `f3d9612fa`: thirty-two cases pass with no skips;
  see [tooling checks](checkpoint-q-tooling-32-pass-2026-10-06.json). These
  checks do not qualify runtime or installed artifacts.

- The completed-batch trace at `6f4e7ef13` locates the unified overflow before
  completed-batch entry, after the workspace edit succeeded; see
  [tool return failure](checkpoint-q-6f4e7ef13-completed-batch-trace-2026-10-06.json).
- The recursive scenario at `1f5baa1c8` overflowed during publisher seed
  boundary verification after physical preparations returned. It never
  reached message delivery; see
  [seed publication failure](checkpoint-q-1f5baa1c8-message-diagnostic-2026-10-06.json).

Packaging-side focused checks also remain distinct from actual artifact builds:

- Native binding producer commit `9da098ca9`: five producer tests pass,
  including Cargo identity, exact `-j1` argv, retained logs, source mutation,
  and tamper rejection. No Cargo build was run by that check.
- Qualification validator commit `54162ce38`: twenty receipt and coverage
  tests pass, including shared-suite assertions and rejection of unknown
  requirement IDs. These are validator tests, not runtime qualification.
- The final required evidence must be regenerated from the final clean source
  after native, package, PTY, and WASM lanes complete.

## Implementation and qualification checklist

- [x] Isolated branch from the pinned base; no merge into the user's checkout.
- [x] Locked 68-row matrix and source-bound evidence receipt contract.
- [ ] Exact input, attachment and recursive-prefix tests on current native/WASM artifacts.
- [ ] Complete recursive production workflow, communication, waits and restart.
- [ ] Session-wide budgets with durable effect accounting and one authority.
- [ ] Direct-parent integration, conflict/abort/rebase/discard and publication recovery.
- [ ] Exact process approval, uncertainty and cleanup across failure boundaries.
- [ ] Approved root writeback preserving concurrent edits and recovering partial restore.
- [ ] Thin terminal composition, lazy inspection, interactive/headless installed tests.
- [ ] Fresh bindings, packages, provider/platform regressions and complete 68-row qualification.
- [ ] Final clean committed source and artifact audit; goal completion.

## Final execution order

Run the platform manifest gates, produce the Windows native binding and its
causal receipt, run the package gate, consume fresh native and packed
artifacts, run PTY and transport-fault lanes, then assemble and validate the
68-row receipt. Every descriptor, transcript, artifact, source identity,
execution count, exit status, and digest must be fresh for that source.

Models remain mocked for this goal. Filesystem, storage, recursive agents,
approved process effects, and terminal interaction must be real. No sandbox is
implemented or implied by workspace routing.
