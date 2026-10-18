//! Run with `RUSTFLAGS="--cfg acyclic_loom"` and `--features local,http`.
#![cfg(all(acyclic_loom, feature = "local"))]

#[test]
fn executor_shutdown_returns_unpolled_snapshot_ownership() {
    for (children, checked) in [(false, false), (false, true), (true, false)] {
        acyclic_stream::verification::snapshot_executor_shutdown(
            false, children, checked, false, false,
        );
    }
}

#[test]
fn executor_shutdown_returns_queued_or_granted_snapshot_ownership() {
    for (children, checked) in [(false, false), (false, true), (true, false)] {
        acyclic_stream::verification::snapshot_executor_shutdown(
            false, children, checked, false, true,
        );
    }
}

#[test]
fn consumer_cancellation_races_executor_shutdown() {
    for polled in [false, true] {
        acyclic_stream::verification::snapshot_executor_shutdown(false, false, false, true, polled);
    }
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_executor_shutdown_discards_snapshot_ownership() {
    acyclic_stream::verification::snapshot_executor_shutdown(true, false, false, false, true);
}

#[test]
fn snapshot_consumer_departures_return_every_reservation() {
    acyclic_stream::verification::snapshot_consumer_departures("none");
}

#[test]
#[should_panic(expected = "reservation")]
fn negative_control_abandoned_job_leaks_its_reservation() {
    acyclic_stream::verification::snapshot_consumer_departures("leak-reservation");
}

#[test]
#[should_panic(expected = "cancellation did not wake the queued job")]
fn negative_control_cancellation_skips_the_wake() {
    acyclic_stream::verification::snapshot_consumer_departures("no-wake");
}

#[test]
#[should_panic(expected = "a completed job was woken for cancellation")]
fn negative_control_completion_leaves_the_job_armed() {
    acyclic_stream::verification::snapshot_consumer_departures("armed-after-completion");
}

#[test]
fn memory_follow_cannot_park_state() {
    acyclic_stream::verification::memory_follow_parking(false, false, false);
    acyclic_stream::verification::memory_follow_parking(false, false, true);
}
#[test]
fn memory_children_cannot_park_state() {
    acyclic_stream::verification::memory_follow_parking(false, true, false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_memory_follow_parks_state() {
    acyclic_stream::verification::memory_follow_parking(true, false, false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_memory_children_parks_state() {
    acyclic_stream::verification::memory_follow_parking(true, true, false);
}

#[test]
fn lock_admission_and_cancellation_match_tokio() {
    acyclic_stream::verification::lock_parity(false);
}
#[test]
#[should_panic(expected = "granted reader excluded a compatible reader")]
fn negative_control_granted_reader_stalls_new_readers() {
    acyclic_stream::verification::lock_parity(true);
}

#[test]
fn unpolled_follow_cannot_park_a_memory_permit() {
    acyclic_stream::verification::local_follow_parking(false, false, true);
}
#[test]
fn unpolled_children_cannot_park_a_memory_permit() {
    acyclic_stream::verification::local_follow_parking(false, true, true);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_follow_retains_pending_memory_read() {
    acyclic_stream::verification::local_follow_parking(true, false, true);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_children_retains_pending_memory_read() {
    acyclic_stream::verification::local_follow_parking(true, true, true);
}

#[test]
fn unpolled_follow_cannot_park_a_publication_permit() {
    acyclic_stream::verification::local_follow_parking(false, false, false);
}
#[test]
fn unpolled_children_cannot_park_a_publication_permit() {
    acyclic_stream::verification::local_follow_parking(false, true, false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_follow_retains_pending_publication_read() {
    acyclic_stream::verification::local_follow_parking(true, false, false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_children_retains_pending_publication_read() {
    acyclic_stream::verification::local_follow_parking(true, true, false);
}

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

#[test]
fn accepted_reads_and_cancellation_progress_during_compaction_without_tearing_the_cut() {
    acyclic_stream::verification::local_compaction_visibility(false);
}
#[test]
#[should_panic(expected = "compaction must preserve accepted reads and the snapshot cut")]
fn negative_control_compaction_retains_the_publication_fence() {
    acyclic_stream::verification::local_compaction_visibility(true);
}

#[test]
#[should_panic(expected = "compaction must preserve accepted reads and the snapshot cut")]
fn negative_control_compaction_releases_mutation_serialization_early() {
    acyclic_stream::verification::local_compaction_cut();
}
