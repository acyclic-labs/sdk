use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use acyclic_test_support::boxed_future as boxed;
use std::future::Future;

// Production volume/provider synchronization uses query_sync's Loom locks.
const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

// Construct each future in a separate frame, then poll it after that frame
// has returned. The largest production future is 7240 bytes; retaining its
// construction temporaries alongside the modeled operation's polling frames
// can still exhaust Loom's fixed coroutine stack. The factory keeps those
// lifetimes disjoint, without changing the scheduler or its stack size.
fn complete<F: Future>(operation: impl FnOnce() -> F) -> F::Output {
    loom::future::block_on(boxed(operation))
}

fn publish_peer(mut owner: CommandSession<MemoryStream>) {
    loom::thread::spawn(move || {
        complete(|| {
            owner.update_state(CommandFileRoot::Session, "layout.json", |layout| {
                let from: crate::tui::TranscriptAnchor =
                    serde_json::from_value(layout["transcript_top"].clone()).unwrap();
                layout["transcript_top"] = json!(scroll_anchor(from.position.line + 4));
                layout["peer"] = json!(true);
                Ok(true)
            })
        })
        .unwrap();
    })
    .join()
    .unwrap();
}

fn scroll_race<const CACHED: bool>(cut: usize) {
    loom::model(move || {
        let volume = Arc::new(
            complete(|| {
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("scroll-model").unwrap(),
                )
            })
            .unwrap(),
        );
        let id = uuid::Uuid::from_bytes([1; 16]);
        let mut seed = complete(|| CommandSession::open(volume.clone(), id, 8192, 32)).unwrap();
        let initial = json!({"transcript_top":scroll_anchor(2)});
        complete(|| seed.write_state(CommandFileRoot::Session, "layout.json", &initial)).unwrap();
        let mut owner =
            Some(complete(|| CommandSession::open(volume.clone(), id, 8192, 32)).unwrap());
        // Task-volume models cover provider-internal schedules. At this
        // caller boundary a peer publication has four distinct cuts:
        // before observation, before edit, during edit before CAS, or after
        // accepted publication. Enumerate all four using the real writer
        // and locks, rather than multiplying equivalent storage schedules.
        if cut == 0 {
            publish_peer(owner.take().unwrap());
        }
        let mut input = complete(|| CommandSession::open(volume.clone(), id, 8192, 32)).unwrap();
        if cut == 1 {
            publish_peer(owner.take().unwrap());
        }
        if CACHED {
            let stale = json!({"transcript_top":scroll_anchor(5)});
            complete(|| input.write_state(CommandFileRoot::Session, "layout.json", &stale))
                .expect("scroll must re-resolve the new cut");
        } else {
            let mut edits = 0;
            let move_view = |from: Option<crate::tui::TranscriptAnchor>, rows: isize| {
                if cut == 2 {
                    if let Some(owner) = owner.take() {
                        publish_peer(owner);
                    }
                }
                edits += 1;
                Some(scroll_anchor(
                    from.unwrap()
                        .position
                        .line
                        .saturating_add_signed(rows.saturating_neg()),
                ))
            };
            complete(|| crate::client::scroll_tui(&mut input, [3], 100, move_view)).unwrap();
            assert_eq!(
                edits,
                if matches!(cut, 1 | 2) { 2 } else { 1 },
                "stale edit must be recomputed exactly once at cut {cut}"
            );
        }
        if cut == 3 {
            publish_peer(owner.take().unwrap());
        }
        let replay = complete(|| CommandSession::open(volume, id, 8192, 32)).unwrap();
        let layout = replay
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap();
        assert_eq!(layout["transcript_top"], json!(scroll_anchor(9)));
        assert_eq!(layout["peer"], true);
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

fn scroll_anchor(line: usize) -> crate::tui::TranscriptAnchor {
    crate::tui::TranscriptAnchor {
        origin: Some(crate::tui::TranscriptOrigin {
            file: [41; 32],
            line: 0,
        }),
        position: crate::tui::RowPosition { line, row: 0 },
        ..Default::default()
    }
}
