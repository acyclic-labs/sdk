# Qualification evidence index

These are checkpoint results, not final product qualification.

| Checkpoint | Suite | Result | Required scope remaining |
|---|---|---|---|
| 2f96c3c3 | Harness library, filesystem-local | 195 passed | Recursive runtime, persistent host, terminal, artifacts |
| Durable composition checkpoint | Harness library, filesystem-local | 197 passed, zero ignored | Recursive runtime, terminal, artifacts |

Command: cargo test -p acyclic-harness --locked --lib --features filesystem-local.
Windows native execution on 2026-10-03.

New evidence: model_input::tests::prefix_provider_rejects_before_downstream_dispatch;
filesystem::local::tests::reopen_recovers_completed_turn_without_dispatch.
The latter uses real LocalStream and LocalFs storage and reopens the providers.

Implementation iterations exposed an unbound conversation initialization check; it
was repaired by testing the bound agent, and the full 197-case suite rerun passed.
Final source/suite/artifact digests and full acceptance receipts remain pending.

## Completed-batch checkpoint

Harness library with filesystem-local: 198 passed, zero ignored.
Production model-input scenario: two edits, all ordered tool results, retained
assistant text, then three explicitly composed child levels through the same
StockExecutor and prefix guard. This does not yet exercise a model-facing fork tool.

Existing integration gates rerun on Windows:
- execution_journal: 6 passed.
- fork_preparer: 1 passed (lost reply reconciles without another child allocation).
- local_recursive_fork: 2 passed (native persistent providers, restart and parent controls).

Command: cargo test -p acyclic-harness --locked --features filesystem-local
--test fork_preparer --test local_recursive_fork --test execution_journal.

The journal regression's former four-record assertion was updated to assert the
fifth input-admission record and verify the persisted request digest. Its
exactly-once dispatch and private-content assertions remain intact.

## Recovery admission checkpoint

See checkpoint-recovery.json for source and descriptor hashes and exact gates.
Native library: 198 passed; journal integration: 6 passed; library lint: passed.
WASM: compilation passed, runtime/serialization parity still pending.

The full native Harness regression at f2fc0f4c passed 215 tests, zero ignored,
including 1,024 recursive forks and 32 sibling forks. It predates the final
recovery-admission/refusal changes and is not a final-source qualification claim.

Recovery tests reject identity-only prefix reconciliation, corrupt request
digests and changed inherited content. Tool refusal replay retains the same
journal, invokes no executor and sends no additional model request.

## Publication admission checkpoint

See checkpoint-publication.json for hashes and exact gates.
Final-source Windows native library: 200 passed; journal integration: 6 passed;
zero ignored. Native library lint passed. WASM compilation passed; no WASM
execution or serialization parity is claimed.

completed_batch_publication_blocks_next_request_until_reconciled exercises the
stock executor: lost publication response blocks the second model request, exact
completed call/result bytes survive recovery, and neither tool execution nor
publication is duplicated on replay. The second request equals the pinned boundary.

batch_publication_recovery_preserves_admission_and_retry_guarantee covers all
three effect guarantees, observed/unresolved outcomes, original admission identity,
and altered-boundary refusal. Unknown at-most-once/exactly-once outcomes remain
indeterminate; only the declared idempotent guarantee allows redispatch.

The concrete adapter to existing workspace forks and child task admission is
still pending. This checkpoint does not qualify the recursive swarm or terminal.

## Authoritative history checkpoint

See checkpoint-history.json for source/suite/descriptor hashes.
Windows native: 201 library, 6 execution-journal, 1 fork-preparer recovery and
2 model-fork-boundary tests passed, with zero ignored. Native library lint and
WASM compilation passed. WASM execution/parity is still unverified.

The native fork fixture supplies a publisher to the production stock loop.
It publishes a complete exchange through Harness, performs two real workspace
forks with FilesystemForkPreparer and StreamAggregate.spawn_from_report, then
runs child models using HarnessStorage.inherited_builder. Actual child provider
requests are captured and their inherited serialized message bytes compared to
the pinned parent prefix. Parent history includes exact whitespace/Unicode,
malformed-call feedback, an actual private-file effect and ordered results.
A follow-up request proves earlier assistant text is not duplicated.

The first run caught text-to-file representation drift and was repaired with
the versioned canonical model-text artifact. Negative artifact tests reject
noncanonical JSON, corruption, invalid encoding/type and render overflow.
A stale-boundary fault scenario interrupts publication, appends concurrent user
input and proves repeated refusal leaves the private workspace generation unchanged.

These are real native provider effects, but activation is supplied by the test
fixture. Production fork tools, task scheduling, complete recursive swarm recovery,
git facade, terminal, generated bindings and fresh package qualification remain
required and pending.
