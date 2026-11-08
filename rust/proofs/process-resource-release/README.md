# Native resource release before accounting

`exit_watch_prepays_and_records_its_descriptor_release` uses an actual child
and its production exit watch. The original meter callback observes that
watch's descriptor with `fcntl(F_GETFD)` at the accounting boundary. It requires
`EBADF` before publishing cleanup, including when that same callback unwinds.
The existing denied-credit branch still requires reservation before native open.
No substitute resource, timer, polling or model is used.

Each retained patch is generated from production Git revisions. After applying
it, the source comparison below must pass before running the property. This
checks the complete original files, including later cleanup changes outside
the mutation hunk; patch applicability alone does not establish refinement.

The retained patch restores the five production source files exactly to
`727348691239c7912901b18287b493fb56e9675b`; it keeps the stronger original
property. That source records cleanup before the native owner is dropped in
exit watches, Windows events, Windows member handles and resume registrations.
The shared resource now drops the native owner first, then the prepaid cleanup
guard. A resume registration also remains ahead of its callback event.

Both layouts perform the same one native release and one accounting call.
Reservation/adoption and release add no native calls, allocations, copies,
round trips, durability barriers or waits; drop visits two fixed fields once.
Callback unwind cannot delay the already completed descriptor close. This
property qualifies the Unix descriptor path; Windows release and suspend/resume
controls require native Windows execution separately.

Run from the repository root on Linux or macOS:

```sh
git apply --check rust/proofs/process-resource-release/original-drop-order.patch
git apply rust/proofs/process-resource-release/original-drop-order.patch
git diff --exit-code 727348691239c7912901b18287b493fb56e9675b -- rust/crates/native-runtime/src/process_tree/cleanup.rs rust/crates/native-runtime/src/process_tree/exit_wait.rs rust/crates/native-runtime/src/process_tree/windows_event.rs rust/crates/native-runtime/src/process_tree/windows_job_wait.rs rust/crates/native-runtime/src/process_tree/windows_wait.rs
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::exit_wait::tests::exit_watch_prepays_and_records_its_descriptor_release -- --exact --nocapture
# Require failure: cleanup accounting preceded the original descriptor release.
git apply -R rust/proofs/process-resource-release/original-drop-order.patch
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::exit_wait::tests::exit_watch_prepays_and_records_its_descriptor_release -- --exact --nocapture
# Require a pass, including the original callback-unwind case.
```

Restore the patch even if a command fails. A compiler failure does not qualify
the negative control; require its exact semantic assertion before restoration.

## Raw Unix spawn objects

`closed_spawn_retires_native_objects_at_every_callback_cut` uses the public
`ProcessTree::spawn_closed_prepared` entry point with an actual `/bin/sh` child.
It cuts every original accounting callback for the seven native preparation
operations, spawn, and both native destroys, once with an error and once with
an unwind. The shared reservation records initialization/spawn attempts before
the native call: an error still executes it, while an unwind does not. The
property requires cleanup only for objects actually initialized by that cut,
exact original accounting counts, original child custody and release of the
original funded preparation frame. There are no timed probes.

The retained `original-spawn-retirement.patch` restores the complete `unix.rs`,
including its original argument-allocation body, exactly to
`fc6be5bd0676092c38f736f0410d78d532b45c14`, retaining this stronger property.
That source has no native-object owner during callback unwind. The fixed owner
uses the existing terminal delivery primitive to finish both cleanup slots.
Each native destroy runs before recording; a failed destroy is retained in the
original caller's tree before its accounting callback. Taking each prepaid
slot once prevents a second destroy or second accounting call during unwind.

The normal successful path still performs seven preparation calls, one spawn
and two destroys, with two prepaid cleanup reservations. No new syscall,
allocation, copy, round trip, barrier or wait is introduced. Retirement visits
exactly two slots, independent of history or process-tree size. Errors and
unwinds finish those same slots without retrying a callback. The original
source is the safety, progress and bounded-operation negative control.
Actual native destroy failures and Darwin execution remain separate coverage
obligations; successful system destroys do not qualify those failure branches.

```sh
git apply --check rust/proofs/process-resource-release/original-spawn-retirement.patch
git apply rust/proofs/process-resource-release/original-spawn-retirement.patch
git diff --exit-code fc6be5bd0676092c38f736f0410d78d532b45c14 -- rust/crates/native-runtime/src/process_tree/unix.rs
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::tests::closed_spawn_retires_native_objects_at_every_callback_cut -- --exact --nocapture
# Require failure: closed spawn stranded original native cleanup at cut 2.
git apply -R rust/proofs/process-resource-release/original-spawn-retirement.patch
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::tests::closed_spawn_retires_native_objects_at_every_callback_cut -- --exact --nocapture
# Require a pass at every error and unwind cut.
```

Restore the original-source patch even when qualification fails. Compilation
failure does not replace the required semantic negative-control assertion.

## Accounting during an existing unwind

The same original-descriptor property runs in an isolated copy of the real test
binary. It covers both normal return and caller unwind, with and without a
panic from the original cleanup-accounting callback. It requires the descriptor
to be physically closed and accounted exactly once in every case. When the
caller already unwinds, its original panic payload must survive cleanup.

`original-accounting-unwind.patch` restores `cleanup.rs` exactly to
`108093b46f0ad0f10d85e801e3bf6e74eb1aa4c5`, keeping the stronger property.
That source aborts when accounting panics during the caller's unwind. The
shared terminal-delivery primitive now completes the same single callback;
it propagates an ordinary callback panic and preserves an existing unwind.
There are still one native close and one accounting attempt, with no added
syscall, allocation, copy, round trip, barrier or wait. The fixed one-item
iterator neither retries the callback nor grows with process-tree size.

```sh
git apply --check rust/proofs/process-resource-release/original-accounting-unwind.patch
git apply rust/proofs/process-resource-release/original-accounting-unwind.patch
git diff --exit-code 108093b46f0ad0f10d85e801e3bf6e74eb1aa4c5 -- rust/crates/native-runtime/src/process_tree/cleanup.rs
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::exit_wait::tests::exit_watch_prepays_and_records_its_descriptor_release -- --exact --nocapture
# Require original cleanup callback failure and panic in a destructor during cleanup.
git apply -R rust/proofs/process-resource-release/original-accounting-unwind.patch
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::exit_wait::tests::exit_watch_prepays_and_records_its_descriptor_release -- --exact --nocapture
# Require a pass, including preservation of the original caller panic.
```

Restore the patch after any failure. The isolated process makes the original
abort finite; an unrelated compiler failure does not qualify this control.
This control checks release accounting. The unused-reservation control below
checks its separate refund boundary.

## Unused cleanup credit during admission unwind

The same property also cuts actual read-attempt accounting before native open.
Its original cleanup reservation must then be refunded once. A panic from that
real refund callback must preserve the original admission panic, with one
recorded read attempt, no recorded close and no unused cleanup credit.
The existing test meter observes its real refund after returning the credit;
no replacement resource or native operation is invented.

`original-refund-unwind.patch` restores `cleanup.rs` exactly to
`1b15b053e3b6088f0faaa3a674a1dfaed3308cae`, retaining this admission cut.
That source aborts in the unused cleanup token's destructor. The fixed owner
uses the same terminal-delivery primitive for this one original refund.
There is no native close for an unopened watch and no extra syscall,
allocation, copy, round trip, barrier or wait; the fixed iterator executes one
refund once. This does not qualify other native reservation mechanisms.

```sh
git apply --check rust/proofs/process-resource-release/original-refund-unwind.patch
git apply rust/proofs/process-resource-release/original-refund-unwind.patch
git diff --exit-code 1b15b053e3b6088f0faaa3a674a1dfaed3308cae -- rust/crates/native-runtime/src/process_tree/cleanup.rs
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::exit_wait::tests::exit_watch_prepays_and_records_its_descriptor_release -- --exact --nocapture
# Require original unused cleanup refund failure and panic in a destructor during cleanup.
git apply -R rust/proofs/process-resource-release/original-refund-unwind.patch
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::exit_wait::tests::exit_watch_prepays_and_records_its_descriptor_release -- --exact --nocapture
# Require the original admission panic, one refund and unchanged native counts.
```

Restore after any failure. Require the actual finite isolated-process abort;
a compilation failure does not qualify the negative control.
