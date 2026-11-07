# Native completion and binding controls

These patches change the production statement while leaving the original
control unchanged. Apply one from the repository root with `git apply`, then
reverse it with `git apply --reverse` before selecting another.

| Patch | Exact acyclic-fs library test | Required failure |
| --- | --- | --- |
| `packet-closed-session-continues.patch` | `native_mount::fuse::tests::packets::failed_child_view_keeps_parent_packets_live_until_transport_failure` | The final request must observe `ENODEV`. |
| `packet-workers-early-startup.patch` | `native_mount::fuse::tests::packet_workers::callback_failure_and_panic_join_the_other_physical_worker` | `startup preceded physical worker creation` |
| `record-replacement-stale-bindings.patch` | `native_mount::lazy::tests::publication::source_hard_link_open_unlink_write_preserves_surviving_alias` | The original surviving-alias or closure binding assertion fails. |

Use `cargo test --locked -p acyclic-fs --all-features --lib <exact-test> -- --exact`
for each selected control. A missing test, compiler error, timeout or unrelated
panic is not evidence that the mutation was detected. Require the same test to
pass after restoring production source. The startup count is emitted by the
same recursion that creates and joins the physical worker handles.

The patches are retained controls; their presence or static applicability does
not establish runtime detection on a new source revision. Physical mounting,
crash recovery and platform conformance require their original native checks.
