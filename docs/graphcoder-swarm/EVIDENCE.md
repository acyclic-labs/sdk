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
