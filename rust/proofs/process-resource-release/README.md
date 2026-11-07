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
