# Native resource release before accounting

`exit_watch_prepays_and_records_its_descriptor_release` uses an actual child
and its production exit watch. The original meter callback observes that
watch's descriptor with `fcntl(F_GETFD)` at the accounting boundary. It requires
`EBADF` before publishing cleanup, including when that same callback unwinds.
The existing denied-credit branch still requires reservation before native open.
No substitute resource, timer, polling or model is used.

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

The retained `original-spawn-retirement.patch` restores `unix.rs` exactly to
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
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::tests::closed_spawn_retires_native_objects_at_every_callback_cut -- --exact --nocapture
# Require failure: closed spawn stranded original native cleanup at cut 2.
git apply -R rust/proofs/process-resource-release/original-spawn-retirement.patch
cargo test --locked -p acyclic-native-runtime --all-features --lib process_tree::tests::closed_spawn_retires_native_objects_at_every_callback_cut -- --exact --nocapture
# Require a pass at every error and unwind cut.
```

Restore the original-source patch even when qualification fails. Compilation
failure does not replace the required semantic negative-control assertion.
