//! Runs the production session pool with crate-local Loom synchronization.
#![cfg(all(acyclic_stream_loom, feature = "grpc"))]

#[test]
fn latest_cursor_fencing_and_conflated_watermarks_use_the_real_pool() {
    acyclic_stream::session::verify_follow_data(0);
}
#[test]
#[should_panic(expected = "fenced LATEST must not advance its cursor")]
fn negative_control_latest_ignores_the_generation_fence() {
    acyclic_stream::session::verify_follow_data(1);
}
#[test]
#[should_panic(expected = "standalone watermark must be delivered")]
fn negative_control_drops_standalone_watermarks() {
    acyclic_stream::session::verify_follow_data(2);
}

#[test]
fn connection_wakes_are_atomic_with_waiter_registration() {
    acyclic_stream::session::verify_connections(false);
}
#[test]
#[should_panic(expected = "connection publication lost its wakeup")]
fn negative_control_drops_the_connection_wake() {
    acyclic_stream::session::verify_connections(true);
}

#[test]
fn cancelled_reconnect_cannot_capture_a_new_interest() {
    acyclic_stream::session::verify_abandoned_reconnect(false);
}

#[test]
#[should_panic(expected = "an abandoned dial must not fail a new interest")]
fn negative_control_late_credential_failure_terminates_the_new_interest() {
    acyclic_stream::session::verify_abandoned_reconnect(true);
}

#[test]
fn malformed_node_cannot_terminate_independent_routes_or_opens() {
    acyclic_stream::session::verify_link_isolation(false);
}

#[test]
#[should_panic(expected = "a peer fault must preserve the healthy route")]
fn negative_control_propagates_a_peer_fault_to_the_whole_pool() {
    acyclic_stream::session::verify_link_isolation(true);
}

#[test]
fn concurrent_initialization_publishes_one_pool() {
    acyclic_stream::session::verify_cache(false);
}
#[test]
#[should_panic(expected = "one shared pool must be published")]
fn negative_control_splits_pool_publication() {
    acyclic_stream::session::verify_cache(true);
}
#[test]
fn cancelling_a_waiter_preserves_initialization_and_last_interest_closes_links() {
    acyclic_stream::session::verify_pool_lifetime(0);
}
#[test]
#[should_panic(expected = "a cancelled waiter must preserve shared initialization")]
fn negative_control_cancels_initialization_with_its_first_waiter() {
    acyclic_stream::session::verify_pool_lifetime(1);
}
#[test]
#[should_panic(expected = "a cancelled waiter must preserve shared initialization")]
fn negative_control_ignores_an_open_waiting_for_dispatch() {
    acyclic_stream::session::verify_pool_lifetime(2);
}
#[test]
#[should_panic(expected = "the last interest must close cached links")]
fn negative_control_retains_idle_sessions() {
    acyclic_stream::session::verify_pool_lifetime(3);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn reservations_equal_the_live_cursor_fold(
        operations in proptest::collection::vec((0_u8..4, proptest::bool::ANY), 0..32)
    ) {
        acyclic_stream::session::verify_interest_accounting(operations);
    }
}

#[test]
fn registration_expiry_races_open_and_releases_the_real_cursor_guard() {
    acyclic_stream::session::verify_registration_deadline(false);
}
#[test]
#[should_panic(expected = "expired registration must complete")]
fn negative_control_forgets_the_logical_registration_deadline() {
    acyclic_stream::session::verify_registration_deadline(true);
}
