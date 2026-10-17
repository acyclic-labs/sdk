use super::*;
use crate::{
    client::TaskFileClient,
    task_volume::{FileKey, FileValue, PlanName, Query, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::StreamExt as _;
use std::sync::Arc;

async fn model<const BOUND: bool>(updates: &[bool], length: usize) {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("frame-cut").unwrap(),
    )
    .await
    .unwrap();
    let authority = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: crate::TaskId::from_bytes([33; 16]).to_string(),
    };
    let mut client = TaskFileClient::observe(&volume, &authority, 128, 65536)
        .await
        .unwrap();
    let mut observed = volume.observe(Query::Plans, None, 128).await.unwrap();
    client
        .views_mut()
        .query_snapshot(Query::Plans, observed.snapshot.take().unwrap(), 128, 65536)
        .unwrap();
    let mut renderer = ClientRenderer::default();
    renderer.update_files(&client, &authority);
    let mut expected = 1;
    for (index, changed) in updates.iter().enumerate() {
        let before = client.presentation_revision();
        if *changed {
            let file = FileValue::new(
                FileKey::Plan(PlanName::new("work").unwrap()),
                format!("{} {index}", "x".repeat(length)).into_bytes(),
            );
            volume
                .append(volume.head().await.unwrap(), &file)
                .await
                .unwrap();
            client
                .apply_change(observed.changes.next().await.unwrap().unwrap())
                .unwrap();
            expected += 1;
            assert!(client.presentation_revision() > before);
        }
        if BOUND {
            renderer.update_files(&client, &authority);
        } else {
            renderer.update(client.views(), &authority);
        }
        assert_eq!(
            renderer.folded_views, expected,
            "unchanged frames must not refold client views"
        );
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn only_accepted_publications_refold_the_presentation(updates in proptest::collection::vec(proptest::prelude::any::<bool>(),1..16),length in 1usize..512) {
        futures::executor::block_on(model::<true>(&updates,length));
    }
}

#[test]
fn long_transcripts_and_scroll_frames_reuse_the_same_fold() {
    futures::executor::block_on(model::<true>(
        &[true, false, false, false, true, false],
        8192,
    ));
}

#[test]
#[should_panic(expected = "unchanged frames must not refold client views")]
fn raw_per_frame_fold_negative_control() {
    futures::executor::block_on(model::<false>(&[true, false], 8192));
}
