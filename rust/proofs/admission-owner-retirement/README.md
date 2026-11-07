# Original capacity waiter retirement

Admission unlinks a borrowed waiter. The original caller retains that owner
until admission is unlocked and its successor has been notified. No extra
reference count, allocation, lock, syscall, durability barrier, round trip or
payload copy is added. Unlinking retains its fixed adjacent-node work; it
never traverses other waiters. Final owner destruction moves from inside the
admission lock to after successor notification.

`cancelled_original_submission_retires_waiter_outside_admission` occupies the
configured production workers and their real channel before polling public
`run_blocking_io` submissions. Cancelling the original pending submission must
retire its observer outside admission, preserve its own destructor panic,
notify the live successor before that panic, release the original funding
and leave the cancelled body unexecuted. Releasing the original worker gate
then lets that same successor complete. The existing exclusive-process entry
keeps whole-pool occupation from blocking unrelated tests.

The retained inverse restores both the complete admission implementation and
its original allocation-control callers from
`9430f6f64442e14d2e25a12d875d6630fd6de404`. The new public-entry property stays
unchanged. Compilation failures cannot qualify this negative control:

```sh
git apply rust/proofs/admission-owner-retirement/original-owner.patch
git diff --exit-code 9430f6f64442e14d2e25a12d875d6630fd6de404 -- rust/crates/native-runtime/src/workers/admission.rs rust/crates/native-runtime/src/tests/native_allocation.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(cancelled_original_submission_retires_waiter_outside_admission)'
git apply -R rust/proofs/admission-owner-retirement/original-owner.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(cancelled_original_submission_retires_waiter_outside_admission) | test(retired_waiter_)'
```

The original must fail with `original waiter retired under the admission lock`.
The existing allocation controls cover final notifier ownership and removal of
neighbor links. Registration replacement and notification snapshot cloning
are separate callback boundaries and remain outside this qualification.
