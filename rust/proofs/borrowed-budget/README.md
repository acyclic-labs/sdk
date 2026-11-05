# Original-budget verification

`performance::tests::native::borrowed::completed_kernel_work_borrows_the_original_budget_without_allocating`
calls the production `KernelAccounting::record_completed`. Its existing
observations require one verification against the original budget address,
zero allocator calls or bytes, and the exact completed-work receipt.

From the repository root:

```sh
python3 rust/proofs/qualify.py borrowed-budget
```

The retained mutation changes that actual standalone completion branch to pass
the budget by value. The unchanged oracle must fail with `verification copied
the original budget`. Compilation failure, empty selection and timeout do not
qualify. The shared driver restores exact source bytes and requires the selected
test to pass. This replaces the test's hand-written former implementation and
caught-panic comparison; no production behavior changes.

The address observation checks borrowing in this build, rather than counting
machine-level copies. Allocation counts cover the selected operation. Separate
generated checks retain full-width budget errors and priority. Borrowed native
contexts, all other operations and complete physical complexity remain separate
obligations. This synchronous path adds no wait, task or producer.
