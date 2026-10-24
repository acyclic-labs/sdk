use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

// Production volume/provider synchronization uses query_sync's Loom locks.
const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn publish_peer(mut owner: CommandSession<MemoryStream>) {
    loom::thread::spawn(move || {
        loom::future::block_on(Box::pin(async move {
            owner
                .update_state(CommandFileRoot::Session, "layout.json", |layout| {
                    layout["transcript_back"] =
                        json!(layout["transcript_back"].as_u64().unwrap() + 4);
                    layout["peer"] = json!(true);
                    Ok(true)
                })
                .await
                .unwrap();
        }))
    })
    .join()
    .unwrap();
}

fn scroll_race<const CACHED: bool>(cut: usize) {
    loom::model(move || {
        loom::future::block_on(Box::pin(async {
            let volume = Arc::new(
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("scroll-model").unwrap(),
                )
                .await
                .unwrap(),
            );
            let id = uuid::Uuid::from_bytes([1; 16]);
            let mut seed = CommandSession::open(volume.clone(), id, 8192, 32)
                .await
                .unwrap();
            seed.write_state(
                CommandFileRoot::Session,
                "layout.json",
                &json!({"transcript_back":2}),
            )
            .await
            .unwrap();
            let mut owner = Some(
                CommandSession::open(volume.clone(), id, 8192, 32)
                    .await
                    .unwrap(),
            );
            // Task-volume models cover provider-internal schedules. At this
            // caller boundary a peer publication has four distinct cuts:
            // before observation, before edit, during edit before CAS, or after
            // accepted publication. Enumerate all four using the real writer
            // and locks, rather than multiplying equivalent storage schedules.
            if cut == 0 {
                publish_peer(owner.take().unwrap());
            }
            let mut input = CommandSession::open(volume.clone(), id, 8192, 32)
                .await
                .unwrap();
            if cut == 1 {
                publish_peer(owner.take().unwrap());
            }
            if CACHED {
                input
                    .write_state(
                        CommandFileRoot::Session,
                        "layout.json",
                        &json!({"transcript_back":5}),
                    )
                    .await
                    .expect("scroll must re-resolve the new cut");
            } else if cut == 2 {
                let mut edits = 0;
                input
                    .update_state(CommandFileRoot::Session, "layout.json", |layout| {
                        if let Some(owner) = owner.take() {
                            publish_peer(owner);
                        }
                        layout["transcript_back"] =
                            json!(layout["transcript_back"].as_u64().unwrap() + 3);
                        edits += 1;
                        Ok(true)
                    })
                    .await
                    .unwrap();
                assert_eq!(edits, 2, "stale edit must be recomputed exactly once");
            } else {
                crate::client::scroll_tui(&mut input, [3], 100)
                    .await
                    .unwrap();
            }
            if cut == 3 {
                publish_peer(owner.take().unwrap());
            }
            let replay = CommandSession::open(volume, id, 8192, 32).await.unwrap();
            let layout = replay
                .state(CommandFileRoot::Session, "layout.json")
                .unwrap()
                .unwrap();
            assert_eq!(layout["transcript_back"], 9);
            assert_eq!(layout["peer"], true);
        }));
    });
}

#[test]
fn scrolling_and_owner_layout_edits_preserve_both_changes() {
    for cut in 0..4 {
        scroll_race::<false>(cut);
    }
}

#[test]
#[should_panic(expected = "scroll must re-resolve the new cut")]
fn negative_control_cached_layout_fails_after_owner_publication() {
    scroll_race::<true>(1);
}
