# Pending admissions share one home binding

The allocation oracle calls the production `Admissions::bind_home` and
`interested` methods with 1, 1,000 and 100,000 original pending memberships.
After establishing enough home-string capacity, rebinding to a shorter name
must allocate neither objects nor bytes. Changing the home again must retain
every membership; repeated withdrawal must be idempotent and the final
withdrawal must end interest.

Run `python3 rust/proofs/qualify.py admission-rebind` from the repository root.
The kept patch inserts per-membership string allocation into the actual
`bind_home` method. The unchanged allocation assertion must fail with
`home rebinding allocated per pending membership`. A compile failure, empty
selection or timeout is not a negative result. The shared driver restores the
exact original production source before running the positive control.

This replaces a hand-written former-mechanism loop with a production mutation.
It proves allocation-free rebinding within existing string capacity, independent
of the tested membership populations. It does not detect an allocation-free
scan or prove all operation costs. The production method takes one mutex,
clears one shared string and copies the requested name; membership insertion
and removal still use a `BTreeMap`. Existing admission Loom models separately
exercise the real lock, registration and cancellation ordering.
