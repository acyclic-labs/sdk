//! Run with `ACYCLIC_LOOM=1` and `--features model-checking`.

#[test]
fn pending_publication_preserves_reads_and_owned_writer_order() {
    for cancel in [false, true] {
        crate::verification::pending_publication(false, false, cancel);
    }
}

#[test]
#[should_panic(expected = "pending storage blocked accepted read")]
fn negative_control_pending_storage_retains_visible_state() {
    crate::verification::pending_publication(true, false, false);
}

#[test]
#[should_panic(expected = "pending publication lost writer ownership")]
fn negative_control_pending_storage_releases_writer_order() {
    crate::verification::pending_publication(false, true, false);
}

#[test]
fn executor_shutdown_returns_unpolled_snapshot_ownership() {
    for checked in [false, true] {
        crate::verification::snapshot_executor_shutdown(false, checked, false, false);
    }
}

#[test]
fn executor_shutdown_ends_queued_snapshot_reads() {
    for checked in [false, true] {
        crate::verification::snapshot_executor_shutdown(false, checked, false, true);
    }
}

#[test]
fn consumer_cancellation_races_executor_shutdown() {
    for polled in [false, true] {
        crate::verification::snapshot_executor_shutdown(false, false, true, polled);
    }
}

#[test]
#[should_panic(expected = "lost snapshot producer left its public follow parked")]
fn negative_control_executor_shutdown_discards_snapshot_ownership() {
    crate::verification::snapshot_executor_shutdown(true, false, false, true);
}

#[test]
fn snapshot_consumer_departures_return_every_reservation() {
    crate::verification::snapshot_consumer_departures("none");
}

#[test]
#[should_panic(expected = "reservation")]
fn negative_control_abandoned_job_leaks_its_reservation() {
    crate::verification::snapshot_consumer_departures("leak-reservation");
}

#[test]
#[should_panic(expected = "cancellation did not wake the queued job")]
fn negative_control_cancellation_skips_the_wake() {
    crate::verification::snapshot_consumer_departures("no-wake");
}

#[test]
#[should_panic(expected = "a completed job was woken for cancellation")]
fn negative_control_completion_leaves_the_job_armed() {
    crate::verification::snapshot_consumer_departures("armed-after-completion");
}

#[test]
fn budget_parked_waiters_complete_after_one_publication() {
    for drop_parked in [false, true] {
        crate::verification::budget_parked_waiters(drop_parked, false);
    }
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_budget_yield_after_acquire_parks_state() {
    crate::verification::budget_parked_waiters(false, true);
}

#[test]
fn memory_follow_cannot_park_state() {
    crate::verification::memory_follow_parking(false, false);
    crate::verification::memory_follow_parking(false, true);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_memory_follow_parks_state() {
    crate::verification::memory_follow_parking(true, false);
}

#[test]
fn unpolled_follow_cannot_park_a_memory_permit() {
    crate::verification::local_follow_parking(false, true);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_follow_retains_pending_memory_read() {
    crate::verification::local_follow_parking(true, true);
}

#[test]
fn unpolled_follow_cannot_park_a_publication_permit() {
    crate::verification::local_follow_parking(false, false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_follow_retains_pending_publication_read() {
    crate::verification::local_follow_parking(true, false);
}

#[test]
fn checked_registration_races_accepted_path_publication() {
    crate::verification::local_path_registration(false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_omits_the_accepted_path_wait() {
    crate::verification::local_path_registration(true);
}
#[test]
fn cancellation_releases_only_its_owned_subscriptions() {
    crate::verification::local_path_cancellation(false);
}
#[test]
#[should_panic(expected = "cancelled follow retained its failure subscription")]
fn negative_control_retains_a_cancelled_cursor() {
    crate::verification::local_path_cancellation(true);
}
#[test]
fn terminal_failure_races_registration_and_wakes_checked_followers() {
    crate::verification::local_poison_wakes_followers();
}
#[test]
fn accepted_append_reaches_a_parked_follower() {
    crate::verification::local_persist_wakes_parked_followers(false);
}
#[test]
#[should_panic(expected = "persist omitted publication wake")]
fn negative_control_omits_a_parked_followers_path_wait() {
    crate::verification::local_persist_wakes_parked_followers(true);
}
#[test]
fn failure_borrows_end_before_visibility_waits() {
    crate::verification::local_failure_lock_order(false);
}
#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_holds_failure_borrow_across_visibility_wait() {
    crate::verification::local_failure_lock_order(true);
}

#[test]
fn accepted_reads_and_cancellation_progress_during_compaction_without_tearing_the_cut() {
    crate::verification::local_compaction_visibility(false);
}
#[test]
#[should_panic(expected = "compaction must preserve accepted reads and the snapshot cut")]
fn negative_control_compaction_retains_the_publication_fence() {
    crate::verification::local_compaction_visibility(true);
}

#[test]
#[should_panic(expected = "compaction must preserve accepted reads and the snapshot cut")]
fn negative_control_compaction_releases_mutation_serialization_early() {
    crate::verification::local_compaction_cut();
}

#[test]
fn concurrent_receipt_cuts_keep_their_flushed_snapshot() {
    crate::verification::local_receipt_cuts(false);
}
#[test]
#[should_panic(expected = "flushed snapshot lost its receipt")]
fn negative_control_receipt_cuts_mix_observations() {
    crate::verification::local_receipt_cuts(true);
}

#[test]
fn barrier_progress_cannot_certify_device_flush() {
    crate::verification::local_barrier_receipts(false);
}
#[test]
#[should_panic(expected = "barrier certified an unflushed record")]
fn negative_control_barrier_progress_certifies_device_flush() {
    crate::verification::local_barrier_receipts(true);
}
