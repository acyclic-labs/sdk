//! Run with RUSTFLAGS="--cfg acyclic_loom" and --features local,http.
#![cfg(all(acyclic_loom, feature = "local"))]

#[test]
fn checked_registration_races_accepted_path_publication() {
    acyclic_stream::verification::local_path_registration(false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_omits_the_accepted_path_wait() {
    acyclic_stream::verification::local_path_registration(true);
}
#[test]
fn cancellation_releases_only_its_owned_subscriptions() {
    acyclic_stream::verification::local_path_cancellation(false);
}
#[test]
#[should_panic(expected = "cancelled follow retained its failure subscription")]
fn negative_control_retains_a_cancelled_cursor() {
    acyclic_stream::verification::local_path_cancellation(true);
}
#[test]
fn terminal_failure_races_registration_and_wakes_checked_followers() {
    acyclic_stream::verification::local_poison_wakes_followers();
}
#[test]
fn accepted_append_reaches_a_parked_follower() {
    acyclic_stream::verification::local_persist_wakes_parked_followers(false);
}
#[test]
#[should_panic(expected = "persist omitted publication wake")]
fn negative_control_omits_a_parked_followers_path_wait() {
    acyclic_stream::verification::local_persist_wakes_parked_followers(true);
}
#[test]
fn discovery_observes_failure_without_waiting_for_durability() {
    acyclic_stream::discovery_verification::local_failure(false);
    acyclic_stream::discovery_verification::local_accepted(false);
}

#[test]
fn failure_borrows_end_before_visibility_waits() {
    acyclic_stream::verification::local_failure_lock_order(false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_holds_failure_borrow_across_visibility_wait() {
    acyclic_stream::verification::local_failure_lock_order(true);
}
