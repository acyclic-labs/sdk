#![cfg(all(acyclic_harness_loom, acyclic_loom))]

#[test]
fn complete_prepared_identity_survives_unrelated_writer_with_negative_control() {
    loom::model(|| acyclic_harness::distributed::verify_prepared_journal_identity(true));
    let failure = std::panic::catch_unwind(|| {
        loom::model(|| acyclic_harness::distributed::verify_prepared_journal_identity(false))
    })
    .expect_err("changed conditions reused a cached identity");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("prepared retry reused a changed transaction identity"),
        "{text}"
    );
}

#[test]
fn cached_execution_summary_keeps_cursor_and_fence_atomic_with_negative_control() {
    loom::model(|| acyclic_harness::filesystem::verify_cached_summary_publication(false));
    let failure = std::panic::catch_unwind(|| {
        loom::model(|| acyclic_harness::filesystem::verify_cached_summary_publication(true))
    })
    .expect_err("split cache publication escaped the model");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("production journal append/refresh guard was released early"),
        "{text}"
    );
}

#[test]
fn task_wide_chunk_sequence_rechecks_reservations_with_negative_control() {
    loom::model(|| acyclic_harness::task_volume::verify_chunk_sequence_publication(false));
    let failure = std::panic::catch_unwind(|| {
        loom::model(|| acyclic_harness::task_volume::verify_chunk_sequence_publication(true))
    })
    .expect_err("unchecked publication escaped the sequence model");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("task chunk publication repeated its reserved sequence"),
        "{text}"
    );
}
