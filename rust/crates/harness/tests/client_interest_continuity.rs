//! Snapshot anchors and standalone durability updates in the production driver.
use super::{path, runtime, status};
use acyclic_harness::{
    Error,
    client::{SwarmChange, SwarmClientLimits},
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, StreamProvider, wire};
use futures::StreamExt as _;
use std::{cell::RefCell, sync::Arc};

#[path = "support/client_resume.rs"]
mod resume;

proptest::proptest! {
    // Exact offset and anchor survive protobuf serialization; all malformed
    // offsets/anchor lengths are rejected rather than becoming unchecked follows.
    #[test]
    fn continuation_round_trip_preserves_the_complete_cut(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..256)) {
        resume::exercise(&bytes);
    }
}

#[test]
fn negative_control_dropping_the_snapshot_anchor_is_detected() {
    let original = acyclic_stream::wire_codec::session::record_cursor(
        7,
        acyclic_stream::CommitId::from_bytes([5; 32]),
    )
    .unwrap();
    resume::check(&original, 7, [5; 32]);
    let broken = wire::SessionCursor {
        next: original.next,
        ..Default::default()
    };
    assert!(std::panic::catch_unwind(|| resume::check(&broken, 7, [5; 32])).is_err());
}

fn limits() -> SwarmClientLimits {
    SwarmClientLimits {
        agents: 1,
        selected: 1,
        layout_bytes: 1024,
        config_files: 2,
        snapshot_files: 2,
    }
}

async fn put(volume: &TaskVolume<MemoryStream>, name: &str, bytes: &[u8]) {
    let cut = volume.snapshot(&Query::Root, 2).await.unwrap();
    volume
        .append(
            cut.generation,
            &Write::new(FileKey::parse(name).unwrap(), bytes.to_vec()),
        )
        .await
        .unwrap();
}

#[test]
fn snapshot_cannot_be_spliced_into_recovered_history_at_the_same_position() {
    futures::executor::block_on(async {
        let original = Arc::new(MemoryStream::default());
        let replacement = Arc::new(MemoryStream::default());
        let parent = TaskVolume::open(replacement.clone(), path("swarm/files"))
            .await
            .unwrap();
        let config = TaskVolume::open(original.clone(), path("config/files"))
            .await
            .unwrap();
        put(
            &config,
            "workspace/layout.json",
            br#"{"in_view":["swarm/a"]}"#,
        )
        .await;
        let old = TaskVolume::open(original.clone(), path("swarm/a/files"))
            .await
            .unwrap();
        let recovered = TaskVolume::open(replacement.clone(), path("swarm/a/files"))
            .await
            .unwrap();
        status::status(&replacement, "swarm/a", false).await;
        status::member(&parent, "a").await;
        put(&old, ".tasks/t/status.json", b"old").await;
        put(&recovered, ".tasks/t/status.json", b"replaced").await;
        let cut = old.heads(&Query::Root, None, 2).await.unwrap().cursor;
        assert_eq!(
            recovered
                .heads(&Query::Root, None, 2)
                .await
                .unwrap()
                .cursor
                .position,
            cut.position
        );
        put(&recovered, "GOAL.md", b"new history tail").await;
        // A deliberately unchecked follow accepts the replacement suffix.
        let mut unchecked = replacement
            .follow(path("swarm/a/files"), cut.position)
            .await
            .unwrap();
        assert_eq!(
            unchecked.next().await.unwrap().unwrap().sequence,
            cut.position
        );
        let (session, mut runner, mut observed) =
            runtime::open(replacement, Arc::new(|s| s), Arc::new(|s| s));
        let snapshots = RefCell::new(0);
        let client = super::client(limits()).unwrap();
        let views = client.views();
        let mut consumer = Box::pin(client.follow(
            &config,
            &parent,
            &session,
            |_, maximum| old.heads(&Query::Root, None, maximum),
            async |change| {
                match change {
                    SwarmChange::Snapshot { cursor, .. } => {
                        assert_eq!(cursor, cut);
                        *snapshots.borrow_mut() += 1;
                    }
                    SwarmChange::Data {
                        overview: false, ..
                    } => panic!("replacement tail escaped its anchor"),
                    _ => {}
                }
                Ok(())
            },
        ));
        let result = runner.until_idle(consumer.as_mut()).await;
        assert!(matches!(result, Some(Err(Error::Storage(_)))), "{result:?}");
        assert_eq!(*snapshots.borrow(), 1);
        assert_eq!(
            views
                .read(|v| v.cursor(&path("swarm/a")).unwrap().clone())
                .unwrap(),
            cut
        );
        let content: Vec<_> = std::iter::from_fn(|| observed.try_recv().ok()).filter_map(|frame| match frame.body {
            Some(wire::session_frame::Body::Interest(update)) => Some(update.upsert), _ => None,
        }).flatten().filter(|sub| matches!(&sub.kind, Some(wire::session_subscription::Kind::Follow(f)) if f.path == "swarm/a/files" && f.delivery == wire::SessionDelivery::All as i32)).collect();
        assert_eq!(content.len(), 1);
        let Some(wire::session_subscription::Kind::Follow(follow)) = &content[0].kind else {
            unreachable!()
        };
        assert_eq!(follow.preceding_commit_id.as_ref(), cut.commit);
    });
}

#[test]
fn standalone_watermarks_reach_status_and_content_without_another_data_frame() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
            .await
            .unwrap();
        let config = TaskVolume::open(provider.clone(), path("config/files"))
            .await
            .unwrap();
        put(
            &config,
            "workspace/layout.json",
            br#"{"in_view":["swarm/a"]}"#,
        )
        .await;
        let agent = TaskVolume::open(provider.clone(), path("swarm/a/files"))
            .await
            .unwrap();
        put(&agent, "GOAL.md", b"before").await;
        status::status(&provider, "swarm/a", false).await;
        status::member(&parent, "a").await;
        let responses: runtime::ResponseMap = Arc::new(|source| {
            Box::pin(source.flat_map(|item| {
            let watermark = match &item {
                Ok(frame) if matches!(&frame.body, Some(wire::session_frame::Body::Data(data)) if matches!(&data.item,
                    Some(wire::session_data::Item::LatestValue(_) | wire::session_data::Item::Record(_)))) => {
                    Some(Ok(wire::SessionFrame { body: Some(wire::session_frame::Body::Watermark(wire::SessionWatermark { accepted: 9, durable: 9 })), ..frame.clone() }))
                }
                _ => None,
            };
            futures::stream::iter(std::iter::once(item).chain(watermark))
        }))
        });
        let (session, mut runner, _) = runtime::open(provider, Arc::new(|s| s), responses);
        let seen = RefCell::new(Vec::new());
        let data = RefCell::new(std::collections::BTreeSet::new());
        let client = super::client(limits()).unwrap();
        let views = client.views();
        let mut consumer = Box::pin(client.follow(
            &config,
            &parent,
            &session,
            |_, maximum| {
                let agent = &agent;
                async move {
                    let snapshot = agent.heads(&Query::Root, None, maximum).await?;
                    put(agent, "GOAL.md", b"after").await;
                    Ok(snapshot)
                }
            },
            async |change| {
                match change {
                    SwarmChange::Watermark {
                        agent,
                        overview,
                        watermark,
                    } => {
                        assert_eq!(agent, path("swarm/a"));
                        assert_eq!((watermark.accepted, watermark.durable), (9, 9));
                        assert!(
                            data.borrow().contains(&overview),
                            "watermark preceded the last record"
                        );
                        seen.borrow_mut().push(overview);
                        if seen.borrow().len() == 2 {
                            return Err(Error::Unsupported("complete".into()));
                        }
                    }
                    SwarmChange::Data { overview, .. } => {
                        assert!(data.borrow_mut().insert(overview));
                    }
                    _ => {}
                }
                Ok(())
            },
        ));
        let result = runner.until_idle(consumer.as_mut()).await;
        assert!(
            matches!(result, Some(Err(Error::Unsupported(_)))),
            "{result:?}"
        );
        drop(consumer);
        let mut seen = seen.into_inner();
        seen.sort();
        assert_eq!(seen, [false, true]);
        views
            .read(|v| {
                let status = v.status(&path("swarm/a")).unwrap().watermarks();
                let content = v.cursor(&path("swarm/a")).unwrap();
                assert_eq!(
                    (
                        status.accepted,
                        status.durable,
                        content.accepted,
                        content.durable
                    ),
                    (9, 9, 9, 9)
                );
            })
            .unwrap();
    });
}
