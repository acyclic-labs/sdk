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
        text.contains("cached execution cursor was published without its fence"),
        "{text}"
    );
}
