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
