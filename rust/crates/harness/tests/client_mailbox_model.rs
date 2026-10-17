//! Exhaustive ownership and wakeup checks over the production mailbox.
#![cfg(acyclic_harness_loom)]

#[test]
fn ownership_handoff_preserves_order_cancellation_and_wakeups() {
    acyclic_harness::client::verify_mailbox(false);
}
#[test]
#[should_panic(expected = "mailbox handoff lost its wakeup")]
fn negative_control_drops_the_handoff_wake() {
    acyclic_harness::client::verify_mailbox(true);
}
