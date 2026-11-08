# Capacity observer transfer

Admission stores one optional observer per waiting owner. A capacity release
or head retirement takes that observer and wakes it after unlocking. A later
owner poll reinstalls an observer if its submission still cannot proceed.
Releases before that poll coalesce into the already delivered wake.

Registration prepares the incoming waker outside both admission and node
locks. It then attempts the original channel transfer again while holding
admission before installing that observer. This single additional transfer
closes the release interval during preparation; it is not a readiness loop.
The displaced observer retires after unlocking and after the pending job has
been restored, including when its destructor unwinds.

Accepted submissions and unchanged pending registrations retain one admission
lock and one channel transfer with zero observer clones. Cold registration or
replacement uses two admission locks and two channel transfers, with one
observer clone outside the locks. Notification and head retirement change from
one observer clone per delivered wake to zero. There are no added allocations,
syscalls, durability barriers, round trips, payload copies, queues or scans.
The existing funded node remains the sole waiter allocation. Adjacent-node
retirement is constant work; a complete broadcast still visits its live
waiters and is not claimed to take constant time.

The public `run_blocking_io` replacement property occupies the configured
workers and their real channel. It checks ordinary and panicking observer
retirement, original node/admission reentry, pending-job preservation, actual
completion and final credit return. The queue property replaces a stale test
that merely reset a wake counter: it now repolls the original submission
against a full channel inside a notification and releases that capacity
through the real receive path. The shared queue models include the production
admission, channel, owned jobs and terminal paths.

The complete inverse includes admission, job state and their changed callers
from `4bc8e89da2e23815b2f3b70833932c88ca15e8c2`. The new property remains:

```sh
git apply rust/proofs/admission-observer-transfer/original-observer.patch
git diff --exit-code 4bc8e89da2e23815b2f3b70833932c88ca15e8c2 -- rust/crates/native-runtime/src/workers/admission.rs rust/crates/native-runtime/src/workers/job.rs rust/crates/native-runtime/src/tests/native_allocation.rs rust/crates/native-runtime/src/testing/queue_model.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(original_submission_replacement_retires_observer_outside_admission)'
git apply -R rust/proofs/admission-observer-transfer/original-observer.patch
```

The original must fail with `replacement retained admission lock`. Compiler
errors do not qualify it. The retained production mutation removes the second
transfer; the unchanged real-queue model must detect `capacity wake lost`:

```sh
git apply rust/proofs/admission-observer-transfer/missing-capacity-rejoin.patch
node scripts/qualify-mcp-loom.mjs --native-capacity
git apply -R rust/proofs/admission-observer-transfer/missing-capacity-rejoin.patch
node scripts/qualify-mcp-loom.mjs --native-capacity
node scripts/qualify-mcp-loom.mjs --native-funding
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(original_submission_replacement_retires_observer_outside_admission) | test(capacity_release_reaches_a_repolled_original_submission) | test(cancelled_original_submission_retires_waiter_outside_admission) | test(retired_waiter_)'
```
