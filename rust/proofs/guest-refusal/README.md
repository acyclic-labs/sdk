# Guest mutation preserves its refusal

Run `node rust/proofs/qualify.mjs guest-refusal` from the repository root.

The Actors Loom target links the contract library and exercises its shipped
`GuestState` implementation. The build script selects Loom synchronization when
the contract opts into `model-checking` and `ACYCLIC_LOOM` is set.

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
