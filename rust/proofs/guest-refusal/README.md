# Guest mutation preserves its refusal

Run `python3 rust/proofs/qualify.py guest-refusal` from the repository root.

The existing Actors Loom entry point includes the shipped `guest/state.rs`
module. The existing compiler wrapper selects Loom synchronization only for
workspace crates opting into `model-checking`; upstream runtimes retain their
normal configuration. No separate state machine implements the guest behavior.

The retained property calls `GuestState::with` recursively on the same state and
on another state. Each call must return `GuestError::NestedMutation` before
trying another lock. After leaving the outer mutation, a subsequent mutation
must succeed. The retained production mutation silently accepts a nested call
without executing it. The unchanged oracle must reject its incorrect success;
the qualifier then restores exact source bytes and requires the property to pass.
Compiler failures and empty test selections do not qualify.

This replaces the negative example that manually erased the held-lock marker
inside a test. The two-handler model and its lost-update negative remain. This
control proves the typed refusal on the mapped synchronous path, not arbitrary
handler termination, native scheduling, or actor delivery and indexing cost.
It adds no product operation, wait, allocation or synchronization mechanism.
