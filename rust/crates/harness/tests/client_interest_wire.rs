//! The production swarm reducer over the shared Rust pool and real memory host.
use super::{path, runtime, status};
use acyclic_harness::{
    Error,
    client::{SwarmChange, SwarmClientLimits},
    task_volume::{Channel, FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, wire};
use futures::StreamExt as _;
use std::{cell::RefCell, sync::Arc};
use wire::{session_frame::Body, session_subscription::Kind};

#[derive(Clone, Copy)]
enum Fault {
    None,
    MissingDiscoveryCursor,
    OpaqueContentAnchor,
    EvenChannel,
    RequireOpaqueDataAnchor,
    DuplicateContent,
}

async fn chunk(volume: &TaskVolume<MemoryStream>, sequence: u64, bytes: &[u8]) {
    let mut write = Write::new(
        FileKey::parse(&format!(".tasks/t/turns/1/chunks/{sequence}")).unwrap(),
        bytes.to_vec(),
    );
    write.channel = Some(Channel::Text);
    volume
        .append(volume.head().await.unwrap(), &write)
        .await
        .unwrap();
}

struct Volumes {
    provider: Arc<MemoryStream>,
    parent: TaskVolume<MemoryStream>,
    config: TaskVolume<MemoryStream>,
    files: TaskVolume<MemoryStream>,
}
async fn seed(before: &[u8]) -> Volumes {
    let provider = Arc::new(MemoryStream::default());
    let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
        .await
        .unwrap();
    let config = TaskVolume::open(provider.clone(), path("config/files"))
        .await
        .unwrap();
    config
        .append(
            config.head().await.unwrap(),
            &Write::new(
                FileKey::parse("workspace/layout.json").unwrap(),
                br#"{"in_view":["swarm/a"]}"#.to_vec(),
            ),
        )
        .await
        .unwrap();
    let files = TaskVolume::open(provider.clone(), path("swarm/a/files"))
        .await
        .unwrap();
    chunk(&files, 1, before).await;
    status::member(&parent, "a").await;
    status::status(&provider, "swarm/a", false).await;
    Volumes {
        provider,
        parent,
        config,
        files,
    }
}
fn requests(fault: Fault) -> runtime::RequestMap {
    Arc::new(move |source| {
        Box::pin(source.map(move |mut frame| {
            if let Some(Body::Interest(update)) = &mut frame.body {
                for sub in &mut update.upsert {
                    match (&mut sub.kind, fault) {
                        (Some(Kind::Children(_)), Fault::MissingDiscoveryCursor) => {
                            sub.resume = None
                        }
                        (Some(Kind::Follow(follow)), Fault::OpaqueContentAnchor)
                            if follow.resource == "streams:/swarm/a/files"
                                && follow.delivery == wire::SessionDelivery::All as i32 =>
                        {
                            sub.resume.as_mut().unwrap().opaque =
                                std::mem::take(&mut follow.preceding_commit_id);
                        }
                        (Some(Kind::Follow(_)), Fault::EvenChannel) => {
                            sub.channel.as_mut().unwrap().channel_id -= 1
                        }
                        _ => {}
                    }
                }
            }
            frame
        }))
    })
}
fn responses(fault: Fault) -> runtime::ResponseMap {
    Arc::new(move |source| {
        Box::pin(source.flat_map(move |item| {
            let duplicate = matches!(fault, Fault::DuplicateContent)
                && matches!(
                    &item,
                    Ok(wire::SessionFrame {
                        body: Some(Body::Data(wire::SessionData {
                            item: Some(wire::session_data::Item::Record(_)),
                            ..
                        })),
                        ..
                    })
                );
            let mut frames = Vec::new();
            if duplicate {
                frames.push(Ok(item.as_ref().unwrap().clone()));
            }
            frames.push(item);
            futures::stream::iter(frames)
        }))
    })
}
fn content_follow(
    frames: impl Iterator<Item = wire::SessionFrame>,
    cursor: &acyclic_harness::task_volume::Cursor,
) {
    let subscriptions: Vec<_> = frames
        .filter_map(|frame| {
            if let Some(Body::Interest(update)) = frame.body {
                Some(update.upsert)
            } else {
                None
            }
        })
        .flatten()
        .collect();
    let content: Vec<_> = subscriptions.iter().filter(|sub| matches!(&sub.kind, Some(Kind::Follow(follow)) if follow.resource == "streams:/swarm/a/files" && follow.delivery == wire::SessionDelivery::All as i32)).collect();
    assert_eq!(content.len(), 1);
    let Some(Kind::Follow(follow)) = &content[0].kind else {
        unreachable!()
    };
    assert_eq!(content[0].resume.as_ref().unwrap().next, cursor.position);
    assert_eq!(follow.preceding_commit_id.as_ref(), cursor.commit);
}

async fn exercise(before: &[u8], after: &[u8], fault: Fault) {
    let Volumes {
        provider,
        parent,
        config,
        files,
    } = seed(before).await;
    let (session, mut runner, mut observed) =
        runtime::open(provider, requests(fault), responses(fault));
    let client = super::client(SwarmClientLimits {
        agents: 1,
        selected: 1,
        layout_bytes: 1024,
        config_files: 4,
        snapshot_files: 4,
    })
    .unwrap();
    let views = client.views();
    let snapshots = RefCell::new(Vec::new());
    let delivered = RefCell::new(Vec::new());
    let consume = client.follow(
        &config,
        &parent,
        &session,
        |_, maximum| {
            let files = &files;
            async move {
                let snapshot = files.heads(&Query::Root, None, maximum).await?;
                chunk(files, 2, after).await;
                Ok(snapshot)
            }
        },
        async |update| {
            match update {
                SwarmChange::Snapshot { agent, cursor } => {
                    let bytes: Vec<_> = views
                        .read(|v| {
                            v.files(&agent)
                                .flat_map(|f| f.segments.iter().flat_map(|s| s.iter().copied()))
                                .collect()
                        })
                        .unwrap();
                    snapshots.borrow_mut().push((cursor, bytes));
                }
                SwarmChange::Data {
                    overview: false,
                    data,
                    file,
                    ..
                } => {
                    if matches!(fault, Fault::RequireOpaqueDataAnchor) {
                        // Deliberately broken reader: delivery tokens are not file anchors.
                        acyclic_stream::wire_codec::session::record_anchor(
                            data.after.as_ref().unwrap(),
                        )
                        .map_err(|e| Error::Storage(e.to_string()))?;
                    }
                    let file = file.expect("content record");
                    assert_eq!(file.key.path(), ".tasks/t/turns/1/chunks/2");
                    assert_eq!(file.channel, Some(Channel::Text));
                    delivered.borrow_mut().push(file.bytes);
                }
                _ => {}
            }
            Ok(())
        },
    );
    let mut consume = Box::pin(consume);
    let outcome = runner.until_idle(consume.as_mut()).await;
    drop(consume);
    verify(
        fault,
        (before, after),
        &views,
        &snapshots.borrow(),
        &delivered.borrow(),
        &outcome,
        std::iter::from_fn(|| observed.try_recv().ok()),
    );
}

fn verify(
    fault: Fault,
    (before, after): (&[u8], &[u8]),
    views: &acyclic_harness::client::ClientViewHandle,
    snapshots: &[(acyclic_harness::task_volume::Cursor, Vec<u8>)],
    delivered: &[acyclic_stream::OwnedBytes],
    outcome: &Option<acyclic_harness::Result<()>>,
    frames: impl Iterator<Item = wire::SessionFrame>,
) {
    match fault {
        Fault::None | Fault::RequireOpaqueDataAnchor => {
            assert!(
                outcome.is_none(),
                "accepted platform delivery was rejected: {outcome:?}"
            );
            assert_eq!(snapshots.len(), 1);
            assert_eq!(snapshots[0].1, before);
            assert_eq!(delivered.len(), 1);
            assert_eq!(delivered[0].as_ref(), after);
            assert_eq!(
                views
                    .read(|v| v
                        .files(&path("swarm/a"))
                        .flat_map(|f| f.segments.iter().flat_map(|s| s.iter().copied()))
                        .collect::<Vec<_>>())
                    .unwrap(),
                [before, after].concat()
            );
            content_follow(frames, &snapshots[0].0);
        }
        Fault::DuplicateContent => {
            // A duplicate in the same connection violates ALL ordering. The pool
            // fails closed before the reducer can fold the same record twice.
            assert!(
                matches!(outcome, Some(Err(Error::Storage(_)))),
                "{outcome:?}"
            );
            assert!(delivered.len() <= 1);
            let text = views
                .read(|v| {
                    v.files(&path("swarm/a"))
                        .flat_map(|f| f.segments.iter().flat_map(|s| s.iter().copied()))
                        .collect::<Vec<_>>()
                })
                .unwrap();
            assert!(text == before || text == [before, after].concat());
        }
        _ => {
            assert!(delivered.is_empty(), "malformed subscription admitted");
            assert!(
                outcome.is_none() || matches!(outcome, Some(Err(Error::Storage(_)))),
                "{outcome:?}"
            );
        }
    }
}

#[test]
fn duplicate_delivery_after_the_known_cut_is_rejected() {
    futures::executor::block_on(async {
        exercise(b"before", b"after", Fault::None).await;
        exercise(b"before", b"after", Fault::DuplicateContent).await;
    });
}
#[test]
fn negative_control_treating_delivery_resume_as_a_record_anchor_is_detected() {
    let failure = std::panic::catch_unwind(|| {
        futures::executor::block_on(exercise(
            b"before",
            b"after",
            Fault::RequireOpaqueDataAnchor,
        ))
    })
    .expect_err("the broken reader passed");
    assert!(
        failure
            .downcast_ref::<String>()
            .unwrap()
            .contains("accepted platform delivery was rejected")
    );
}
#[test]
fn actual_session_rejects_malformed_subscriptions() {
    futures::executor::block_on(async {
        for fault in [
            Fault::None,
            Fault::MissingDiscoveryCursor,
            Fault::OpaqueContentAnchor,
            Fault::EvenChannel,
        ] {
            exercise(b"before", b"after", fault).await;
        }
    });
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn platform_snapshot_plus_follow_equals_the_complete_history(
        before in proptest::collection::vec(proptest::num::u8::ANY, 0..32),
        after in proptest::collection::vec(proptest::num::u8::ANY, 0..32),
    ) { futures::executor::block_on(exercise(&before, &after, Fault::None)); }
}
