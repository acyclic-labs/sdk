//! Real-path group boundary ordering, including a broken-gate control.
#![cfg(all(acyclic_loom, feature = "local"))]

#[test]
fn group_seals_follow_successful_predecessor_flushes() {
    acyclic_stream::group_seals_follow_successful_predecessor_flushes();
}

#[test]
#[should_panic(expected = "seal preceded predecessor flush")]
fn negative_control_group_seal_overtakes_flush() {
    acyclic_stream::negative_control_group_seal_overtakes_flush();
}
