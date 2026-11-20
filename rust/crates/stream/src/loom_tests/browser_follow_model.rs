//! Models the browser's shared production state and interest subscription path.

#[test]
fn checked_follow_rejects_replaced_history_without_lost_wakeup() {
    crate::browser_follow_verification::rejects_replaced_history_without_lost_wakeup();
}

#[test]
fn negative_control_omitting_the_anchor_is_detected() {
    crate::browser_follow_verification::negative_control_omitting_the_anchor_is_detected();
}

#[test]
fn creation_during_registration_retains_the_requested_cursor() {
    crate::browser_follow_verification::creation_during_registration_retains_the_requested_cursor();
}

#[test]
fn negative_control_refusing_initial_absence_is_detected() {
    crate::browser_follow_verification::negative_control_refusing_initial_absence_is_detected();
}
