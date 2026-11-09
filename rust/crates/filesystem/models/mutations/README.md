# Native completion and binding controls

These patches change the production statement while leaving the original
control unchanged. Apply one from the repository root with `git apply`, then
reverse it with `git apply --reverse` before selecting another.

| Patch | Exact acyclic-fs library test | Required failure |
| --- | --- | --- |
| `packet-closed-session-continues.patch` | `native_mount::fuse::tests::packets::failed_child_view_keeps_parent_packets_live_until_transport_failure` | The final request must observe `ENODEV`. |
| `packet-workers-early-startup.patch` | `native_mount::fuse::tests::packet_workers::callback_failure_and_panic_join_the_other_physical_worker` | `startup preceded physical worker creation` |
| `record-replacement-stale-bindings.patch` | `native_mount::lazy::tests::publication::source_hard_link_open_unlink_write_preserves_surviving_alias` | The original surviving-alias or closure binding assertion fails. |
| `recovery-early-release.patch` | `native_mount::disconnected::models::real_recovery_owner_keeps_fence_through_helper_exit_with_negative_control` | `recovery fence released before helper exit`, `remount acquired the fence before helper exit` or `helper detached a live remount after early fence release` |

Use `cargo test --locked -p acyclic-fs --all-features --lib <exact-test> -- --exact`
for each selected control. A missing test, compiler error, timeout or unrelated
panic is not evidence that the mutation was detected. Require the same test to
pass after restoring production source. The startup count is emitted by the
same recursion that creates and joins the physical worker handles.

The recovery control requires the production synchronization model:
`RUSTC_WORKSPACE_WRAPPER="$PWD/scripts/loom-rustc.sh" cargo test --locked -p acyclic-fs --all-features --lib native_mount::disconnected::models::real_recovery_owner_keeps_fence_through_helper_exit_with_negative_control -- --exact`.
Its mutation changes the production fence ordering; it does not add a model-only
release path or change the original custody assertions.

The patches are retained controls; their presence or static applicability does
not establish runtime detection on a new source revision. Physical mounting,
crash recovery and platform conformance require their original native checks.
