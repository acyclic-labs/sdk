use super::MemoryFs;
use acyclic_native_runtime::testing::Schedule;
use acyclic_objects::v1::{ObjectsProvider, wire};
use acyclic_stream::OwnedBytes;

async fn sample(seed: u64) -> (Vec<wire::ObjectInfo>, String) {
    let (schedule, _boundaries) = Schedule::seeded(seed);
    schedule
        .scope(async {
            // Exercise the provider created by the public filesystem constructor.
            let fs = MemoryFs::memory();
            let store = fs.objects();
            let mut objects = Vec::new();
            for key in ["first", "second"] {
                let object = store
                    .provider()
                    .put(
                        wire::PutObjectHeader {
                            bucket: Some(store.bucket().clone()),
                            object_key: key.into(),
                            ..Default::default()
                        },
                        OwnedBytes::from_static(b"body"),
                    )
                    .await
                    .unwrap();
                let time = object.last_modified.as_ref().unwrap();
                assert_eq!(time.seconds, 1_700_000_000, "object clock escaped its seed");
                assert_eq!(time.nanos, 0);
                objects.push(object);
            }
            let page = store
                .provider()
                .list(wire::ListObjectsRequest {
                    bucket: Some(store.bucket().clone()),
                    page_size: 1,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(page.entries.len(), 1);
            assert!(!page.continuation_token.is_empty());
            (objects, page.continuation_token)
        })
        .await
}

#[test]
fn memory_composition_retains_seeded_object_clock_and_cursor_entropy() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::pause();
        let first = sample(71).await;
        assert_eq!(
            first,
            sample(71).await,
            "seeded object inputs did not replay"
        );
        let different = sample(72).await;
        assert_eq!(first.0, different.0, "entropy changed the object clock");
        assert_ne!(
            first.1, different.1,
            "seed did not reach cursor authentication"
        );
    });
}
