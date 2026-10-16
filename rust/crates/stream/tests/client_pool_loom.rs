//! Runs the production session pool with crate-local Loom synchronization.
#![cfg(all(acyclic_stream_loom, feature = "grpc"))]

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

#[test]
fn node_dial_cancellation_and_reconnect_use_real_cursor_fences() {
    acyclic_stream::session::verify_node_dials(0);
}
#[test]
#[should_panic(expected = "withdrawn node must cancel its dial")]
fn negative_control_preserves_an_ownerless_dial_while_another_node_is_active() {
    acyclic_stream::session::verify_node_dials(1);
}
#[test]
#[should_panic(expected = "reconnect must select a live interest before removal")]
fn negative_control_selects_a_cancelled_interest_before_removal() {
    acyclic_stream::session::verify_node_dials(2);
}

#[test]
fn an_unpolled_terminal_cursor_cannot_own_a_home_reconnect() {
    acyclic_stream::session::verify_terminal_cursor_dial(false);
}
#[test]
#[should_panic(expected = "a terminal cursor must not imply a pending Open")]
fn negative_control_infers_pending_opens_from_terminal_cursor_counts() {
    acyclic_stream::session::verify_terminal_cursor_dial(true);
}
#[test]
fn pending_opens_keep_their_node_when_home_binding_races_registration() {
    acyclic_stream::session::verify_admission_binding(false);
}
#[test]
#[should_panic(expected = "binding home must preserve each owned pending Open")]
fn negative_control_does_not_rebind_the_pending_home_entry() {
    acyclic_stream::session::verify_admission_binding(true);
}

#[test]
fn latest_consumption_and_move_fence_share_one_application_cut() {
    acyclic_stream::session::verify_latest_progress(false);
}
#[test]
#[should_panic(expected = "a stale delivery cannot advance the fenced resume cut")]
fn negative_control_checks_latest_generation_outside_the_cursor_lock() {
    acyclic_stream::session::verify_latest_progress(true);
}
