# Read completion retirement

`final_consumer_settles_original_read_credit_before_discarding_accounting`
reserves a read through `DemandWork::reserve_view_read`, drops the production
completion on another thread, and calls the same reconciliation function as
`reconcile_native_controls`. Both completed reads and abandoned admissions must
return the original finite credit before the last account is removed. The
completed case also records exactly one item; abandonment records none.

Only the reference-count observation is refined. An explicit relaxed atomic
models `std::sync::Arc::strong_count`; Loom's own Arc observation is stronger.
The initial count is checked against the actual original account, and its
release decrement follows the actual completion destructor. The observer must
settle final ownership before joining the producer, so thread join cannot mask
a missing acquire. A nonfinal observation retains the account for reconciliation
after that producer's exit.

From the repository root on Linux:

```sh
RUSTC_WORKSPACE_WRAPPER="$PWD/scripts/loom-rustc.sh" cargo test --locked -p acyclic-fs --lib --no-default-features --features native-mount,model-checking demand::admissions::loom_tests::final_consumer_settles_original_read_credit_before_discarding_accounting -- --exact --nocapture
```

`missing-acquire.patch` removes the production acquire before read completion
drain, leaving the later byte-retirement acquire intact. Apply it with `git apply`,
run the same command, reverse it with `git apply -R`, and rerun. The mutation must
fail at `final consumer stranded its original read reservation`; compilation
failure or another assertion does not qualify the control.

The extraction adds no allocation, copy, lock, syscall, round trip, durability
barrier or wait. Both entry points use one owner-count observation and the same
fixed set of accounting atomics. This model covers the original read reservation
and shared ledger; native resource refund failures, OS callbacks and mounted
kernel behavior require their own representative controls.
