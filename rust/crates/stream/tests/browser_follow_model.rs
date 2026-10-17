//! Models the browser's shared production state and interest subscription path.
#![cfg(all(acyclic_loom, acyclic_stream_loom, feature = "local"))]

#[test]
fn checked_follow_rejects_replaced_history_without_lost_wakeup() {
    acyclic_stream::browser_follow_verification::rejects_replaced_history_without_lost_wakeup();
}

#[test]
fn negative_control_omitting_the_anchor_is_detected() {
    acyclic_stream::browser_follow_verification::negative_control_omitting_the_anchor_is_detected();
}
