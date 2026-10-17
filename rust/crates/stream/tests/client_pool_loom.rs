//! Runs the production session pool with crate-local Loom synchronization.
#![cfg(all(acyclic_stream_loom, feature = "grpc"))]

#[test]
fn provider_source_publication_cannot_lose_its_wake() {
    acyclic_stream::session::verify_source_wakes(false);
}
#[test]
#[should_panic(expected = "provider source lost its publication wake")]
fn negative_control_omits_the_source_wake() {
    acyclic_stream::session::verify_source_wakes(true);
}

#[test]
fn cancelled_follow_is_removed_before_replacement_admission() {
    acyclic_stream::session::verify_cancel_before_open(false);
}
#[test]
#[should_panic(expected = "cancelled follow survived replacement admission")]
fn negative_control_new_open_overtakes_cancellation() {
    acyclic_stream::session::verify_cancel_before_open(true);
}

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

#[test]
fn source_deduplication_survives_pending_rechecks_during_drain() {
    acyclic_stream::session::verify_source_recheck(0);
}
#[test]
#[should_panic(expected = "provider recheck lost its wakeup")]
fn negative_control_clears_the_source_hint_after_rechecking() {
    acyclic_stream::session::verify_source_recheck(1);
}

#[test]
#[should_panic(expected = "coalesced wake lost its publication")]
fn negative_control_resets_the_source_hint_without_acquiring_coalesced_wakes() {
    acyclic_stream::session::verify_source_recheck(2);
}

#[test]
fn initial_eof_retry_yields_to_deadline_and_cancellation() {
    acyclic_stream::session::verify_initial_retry_yield(false);
}
#[test]
#[should_panic(expected = "initial retry must yield before another physical RPC")]
fn negative_control_initial_eof_retry_monopolizes_the_executor() {
    acyclic_stream::session::verify_initial_retry_yield(true);
}

#[test]
fn discovery_snapshot_delivery_bounds_status_admission() {
    acyclic_stream::session::verify_discovery_admission(false, false);
}
#[test]
#[should_panic(expected = "membership bypassed session delivery backpressure")]
fn negative_control_membership_snapshot_bypasses_discovery_credit() {
    acyclic_stream::session::verify_discovery_admission(false, true);
}

#[test]
fn discovery_live_membership_delivery_bounds_status_admission() {
    acyclic_stream::session::verify_discovery_admission(true, false);
}
#[test]
#[should_panic(expected = "membership bypassed session delivery backpressure")]
fn negative_control_live_membership_bypasses_discovery_credit() {
    acyclic_stream::session::verify_discovery_admission(true, true);
}
