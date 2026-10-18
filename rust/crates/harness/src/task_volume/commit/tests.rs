use super::*;
use acyclic_stream::MemoryStream;
use futures::FutureExt as _;

async fn published_batch(count: usize) {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("batch-index-proof").unwrap();
    let volume = TaskVolume::open(provider.clone(), path.clone())
        .await
        .unwrap();
    let writes: Vec<_> = (0..count)
        .map(|index| {
            FileValue::new(
                FileKey::parse(&format!(".ui/config/image-{index}.json")).unwrap(),
                format!("{{\"image\":{index}}}").into_bytes(),
            )
        })
        .collect();
    let before = volume.head().await.unwrap();
    let committed = volume.append_many(before, &writes).await.unwrap();
    assert_eq!(committed.position, before.position + count as u64);
    // Refresh always authenticates one existing anchor. It must never decode an
    // already-encoded batch just to repair an omitted index acknowledgement.
    let (refreshed, examined) = crate::test_support::measure_task_volume_work(|| {
        volume
            .refresh()
            .now_or_never()
            .expect("in-memory refresh is ready")
    });
    refreshed.unwrap();
    assert_eq!(examined, 1, "acknowledged records were replayed");
    let cached = volume.snapshot(&Query::Root, count.max(1)).await.unwrap();
    let reopened = TaskVolume::open(provider, path).await.unwrap();
    let replayed = reopened.snapshot(&Query::Root, count.max(1)).await.unwrap();
    assert_eq!(cached.files, replayed.files);
    assert_eq!(cached.generation, replayed.generation);
    assert_eq!(cached.cursor, replayed.cursor);
    assert_eq!(cached.generation, committed);
    assert_eq!(cached.files, writes);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn batch_acknowledgement_never_replays_its_records(count in 0usize..8) {
        futures::executor::block_on(published_batch(count));
    }
}

#[test]
fn missing_batch_index_publication_is_detected_negative_control() {
    OMIT_INDEX.with(|flag| flag.set(true));
    let caught = std::panic::catch_unwind(|| futures::executor::block_on(published_batch(2)));
    OMIT_INDEX.with(|flag| flag.set(false));
    let panic = caught.expect_err("omitting the production index publication must fail");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("acknowledged records were replayed"),
        "{message}"
    );
}
