//! Snapshot anchors and standalone durability updates in the production driver.
use super::path;
use acyclic_harness::{
    Error,
    client::{SwarmClient, SwarmClientLimits, SwarmUpdate},
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, StreamProvider, wire, wire_codec::session::follow_records};
use futures::{StreamExt as _, channel::mpsc};
use std::{cell::RefCell, rc::Rc, sync::Arc};

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
        put(&old, ".tasks/t/status.json", b"old").await;
        put(&recovered, ".tasks/t/status.json", b"replaced").await;
        let cut = old.heads(&Query::Root, None, 2).await.unwrap().cursor;
        assert_eq!(
            recovered
                .snapshot(&Query::Root, 2)
                .await
                .unwrap()
                .cursor
                .position,
            cut.position
        );
        put(&recovered, "GOAL.md", b"new history tail").await;
        // Negative control: a position-only follow silently returns that tail.
        let mut unchecked = replacement
            .follow(path("swarm/a/files"), cut.position)
            .await
            .unwrap();
        assert_eq!(
            unchecked.next().await.unwrap().unwrap().sequence,
            cut.position
        );
        let admitted = Rc::new(RefCell::new(0));
        let sink = Box::pin(futures::sink::unfold(
            (replacement, admitted.clone()),
            |(provider, admitted), update: wire::SessionInterest| async move {
                for subscription in update.upsert {
                    if let Some(wire::session_subscription::Kind::Follow(follow)) =
                        subscription.kind
                    {
                        if follow.delivery == wire::SessionDelivery::All as i32 {
                            *admitted.borrow_mut() += 1;
                            let result = follow_records(
                                provider.as_ref(),
                                &follow,
                                subscription.resume.as_ref().unwrap(),
                            )
                            .await;
                            assert!(matches!(
                                result,
                                Err(acyclic_stream::StreamError::PrefixNotRetained)
                            ));
                            return Err(Error::Storage("history replaced; reset required".into()));
                        }
                    }
                }
                Ok((provider, admitted))
            },
        ));
        let snapshots = RefCell::new(0);
        let result = SwarmClient::new(path("swarm"), limits())
            .unwrap()
            .follow(
                &MemoryStream::default(),
                &config,
                sink,
                futures::stream::pending(),
                |_, maximum| old.heads(&Query::Root, None, maximum),
                |update| {
                    if let SwarmUpdate::Snapshot { snapshot, .. } = update {
                        assert_eq!(snapshot.cursor, cut);
                        *snapshots.borrow_mut() += 1;
                    }
                    futures::future::ready(Ok(()))
                },
            )
            .await;
        assert!(
            matches!(result, Err(Error::Storage(message)) if message.contains("history replaced"))
        );
        assert_eq!(*snapshots.borrow(), 1);
        assert_eq!(*admitted.borrow(), 1);
    });
}

#[test]
fn standalone_watermarks_reach_status_and_content_without_another_data_frame() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
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
        let (frames, incoming) = mpsc::unbounded();
        let sink = Box::pin(futures::sink::unfold(
            frames,
            |frames, update: wire::SessionInterest| async move {
                for subscription in update.upsert {
                    if let Some(wire::session_subscription::Kind::Follow(_)) = subscription.kind {
                        frames
                            .unbounded_send(Ok(wire::SessionFrame {
                                channel_id: subscription.channel.unwrap().channel_id,
                                generation: 1,
                                body: Some(wire::session_frame::Body::Watermark(
                                    wire::SessionWatermark {
                                        accepted: 9,
                                        durable: 9,
                                    },
                                )),
                            }))
                            .unwrap();
                    }
                }
                Ok::<_, Error>(frames)
            },
        ));
        let seen = RefCell::new(Vec::new());
        let result = SwarmClient::new(path("swarm"), limits())
            .unwrap()
            .follow(
                provider.as_ref(),
                &config,
                sink,
                incoming,
                |_, maximum| agent.heads(&Query::Root, None, maximum),
                |update| {
                    let result = match update {
                        SwarmUpdate::Watermark {
                            agent,
                            overview,
                            watermark,
                        } => {
                            assert_eq!(agent, path("swarm/a"));
                            assert_eq!((watermark.accepted, watermark.durable), (9, 9));
                            seen.borrow_mut().push(overview);
                            if seen.borrow().len() == 2 {
                                Err(Error::Unsupported("complete".into()))
                            } else {
                                Ok(())
                            }
                        }
                        SwarmUpdate::Data { .. } => panic!("no data frames in this scenario"),
                        _ => Ok(()),
                    };
                    futures::future::ready(result)
                },
            )
            .await;
        assert!(matches!(result, Err(Error::Unsupported(_))));
        let mut seen = seen.into_inner();
        seen.sort();
        assert_eq!(seen, [false, true]);
    });
}
