//! Filesystem publications drive the actual client/session orchestration.

use acyclic_harness::{
    Error, Result,
    client::{InterestFiles, InterestTier, SwarmClient, SwarmClientLimits, SwarmUpdate},
    task_volume::{Channel, FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, StreamProvider, wire};
use futures::{FutureExt as _, SinkExt as _, StreamExt as _, channel::mpsc};
use std::{cell::RefCell, rc::Rc, sync::Arc};

#[path = "support/client_files.rs"]
mod client_files;
use client_files::Fixture;

use super::path;

async fn cut(fs: &Fixture) -> Result<InterestFiles> {
    let page = fs
        .reader
        .list_private_directory(&fs.root.volume, "", "", None, None, 16, None)?
        .await?;
    Ok(InterestFiles {
        root: fs.root.clone(),
        generation: page.generation,
    })
}

fn child(value: &str) -> wire::SessionFrame {
    wire::SessionFrame {
        channel_id: 1,
        generation: 1,
        body: Some(wire::session_frame::Body::Data(wire::SessionData {
            after: Some(wire::SessionCursor {
                next: 1,
                ..Default::default()
            }),
            accepted: 1,
            durable: 1,
            item: Some(wire::session_data::Item::Child(wire::Child {
                path: value.into(),
            })),
            ..Default::default()
        })),
    }
}

#[tokio::test]
async fn file_writes_change_session_interests_without_losing_the_snapshot_cut() -> Result<()> {
    let fs = Fixture::new(&[("layout.json", "{}"), ("open-views.json", "{}")]).await?;
    let provider = Arc::new(MemoryStream::default());
    let volume = Arc::new(
        TaskVolume::open(provider.clone(), path("swarm/a/files"))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?,
    );
    let initial = volume.snapshot(&Query::Root, 10).await.unwrap();
    let mut chunk = Write::new(
        FileKey::parse(".tasks/t/turns/1/chunks/1").unwrap(),
        b"before".to_vec(),
    );
    chunk.channel = Some(Channel::Text);
    volume.append(initial.generation, &chunk).await.unwrap();

    let client = SwarmClient::new(
        path("swarm"),
        fs.reader.clone(),
        SwarmClientLimits {
            agents: 10,
            selected: 2,
            layout_bytes: 1024,
            snapshot_files: 10,
        },
    )?;
    let (mut publications, files) = mpsc::channel(1);
    let (session, mut sent) = mpsc::channel::<wire::SessionInterest>(4);
    let (mut frames, incoming) = mpsc::channel::<Result<wire::SessionFrame>>(2);
    let (layouts, mut rendered) = mpsc::unbounded();
    let (data_seen, mut observed) = mpsc::unbounded();
    let snapshots = Rc::new(RefCell::new(Vec::new()));
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let consumer = client.follow(
        files.map(Ok),
        session.sink_map_err(|e| Error::Storage(e.to_string())),
        incoming,
        |agent, maximum| {
            assert_eq!(agent, path("swarm/a"));
            let volume = volume.clone();
            async move { volume.heads(&Query::Root, None, maximum).await }
        },
        |update| {
            let volume = volume.clone();
            let snapshots = snapshots.clone();
            let delivered = delivered.clone();
            match update {
                SwarmUpdate::Layout(layout) => layouts.unbounded_send(layout).unwrap(),
                SwarmUpdate::Data { overview, data, .. } => {
                    delivered.borrow_mut().push((overview, data));
                    data_seen.unbounded_send(()).unwrap();
                }
                SwarmUpdate::Snapshot { snapshot, .. } => {
                    snapshots.borrow_mut().push(snapshot);
                    // Publication between the snapshot and session subscription:
                    // follow must start at C, not at the now-newer tail.
                    return futures::future::Either::Left(async move {
                        let latest = volume.snapshot(&Query::Root, 10).await.unwrap();
                        let mut next = Write::new(
                            FileKey::parse(".tasks/t/turns/1/chunks/2").unwrap(),
                            b"after".to_vec(),
                        );
                        next.channel = Some(Channel::Text);
                        volume.append(latest.generation, &next).await.unwrap();
                        Ok(())
                    });
                }
            }
            futures::future::Either::Right(futures::future::ready(Ok(())))
        },
    );
    let writer = async {
        let discovery = sent.next().await.unwrap();
        assert_eq!(discovery.revision, 1);
        assert!(matches!(
            discovery.upsert[0].kind,
            Some(wire::session_subscription::Kind::Children(_))
        ));
        frames.send(Ok(child("swarm/unseen"))).await.unwrap();
        let overview = sent.next().await.unwrap();
        assert_eq!(overview.revision, 2);
        let Some(wire::session_subscription::Kind::Follow(status)) = &overview.upsert[0].kind
        else {
            panic!("status follow")
        };
        assert_eq!(status.path, "swarm/unseen/status");
        assert_eq!(status.delivery, wire::SessionDelivery::Latest as i32);
        let attention = wire::SessionData {
            after: Some(wire::SessionCursor {
                next: 42,
                ..Default::default()
            }),
            accepted: 42,
            durable: 41,
            item: Some(wire::session_data::Item::LatestValue(
                wire::SessionLatestValue {
                    key: b"approval".to_vec().into(),
                    value: b"waiting".to_vec().into(),
                    attention: true,
                },
            )),
            ..Default::default()
        };
        frames
            .send(Ok(wire::SessionFrame {
                channel_id: overview.upsert[0].channel.as_ref().unwrap().channel_id,
                // Platform can move a follow without changing its logical channel.
                generation: 2,
                body: Some(wire::session_frame::Body::Data(attention.clone())),
            }))
            .await
            .unwrap();
        observed.next().await.unwrap();
        assert_eq!(delivered.borrow()[0], (true, attention));

        fs.put("layout.json", r#"{"prefetch":["swarm/a"]}"#, "prefetch")
            .await?;
        publications.send(cut(&fs).await?).await.unwrap();
        let selected = sent.next().await.unwrap();
        assert_eq!(selected.revision, 3);
        let content = selected.upsert.iter().find(|sub| matches!(&sub.kind,
            Some(wire::session_subscription::Kind::Follow(follow)) if follow.delivery == wire::SessionDelivery::All as i32)).unwrap();
        let snapshot_cut = snapshots.borrow()[0].cursor.position;
        assert_eq!(content.resume.as_ref().unwrap().next, snapshot_cut);
        assert_eq!(
            snapshots.borrow()[0].files[0].segments[0].as_ref(),
            b"before"
        );
        assert_eq!(
            rendered.next().await.unwrap().tier(&path("swarm/a")),
            Some(InterestTier::Prefetch)
        );
        let mut tail = provider
            .follow(path("swarm/a/files"), snapshot_cut)
            .await
            .unwrap();
        let record = tail.next().await.unwrap().unwrap();
        assert_eq!(record.sequence, snapshot_cut);
        frames
            .send(Ok(wire::SessionFrame {
                channel_id: content.channel.as_ref().unwrap().channel_id,
                generation: 1,
                body: Some(wire::session_frame::Body::Data(wire::SessionData {
                    after: Some(wire::SessionCursor {
                        next: snapshot_cut + 1,
                        ..Default::default()
                    }),
                    accepted: snapshot_cut + 1,
                    durable: snapshot_cut + 1,
                    item: Some(wire::session_data::Item::Record(
                        acyclic_stream::wire_codec::record_wire(record),
                    )),
                    ..Default::default()
                })),
            }))
            .await
            .unwrap();
        observed.next().await.unwrap();

        fs.put("open-views.json", r#"{"in_view":["swarm/a"]}"#, "focus")
            .await?;
        publications.send(cut(&fs).await?).await.unwrap();
        assert_eq!(
            rendered.next().await.unwrap().tier(&path("swarm/a")),
            Some(InterestTier::InView)
        );
        assert!(
            sent.next().now_or_never().is_none(),
            "promotion must retain its follow"
        );
        assert_eq!(snapshots.borrow().len(), 1);

        fs.put("layout.json", "{}", "unprefetch").await?;
        fs.put("open-views.json", "{}", "close").await?;
        publications.send(cut(&fs).await?).await.unwrap();
        let removed = sent.next().await.unwrap();
        assert_eq!(
            removed.revision, 4,
            "render-only promotion sends no session revision"
        );
        assert_eq!(removed.remove, vec![content.channel.clone().unwrap()]);
        assert!(
            removed.upsert.is_empty(),
            "status and discovery remain subscribed"
        );
        assert_eq!(
            rendered.next().await.unwrap().tier(&path("swarm/a")),
            Some(InterestTier::Overview)
        );
        frames
            .send(Err(Error::Unsupported("end scenario".into())))
            .await
            .unwrap();
        Ok::<_, Error>(())
    };
    let (outcome, result) = futures::join!(consumer, writer);
    result?;
    assert!(matches!(outcome, Err(Error::Unsupported(_))));
    assert_eq!(delivered.borrow().len(), 2);
    assert!(!delivered.borrow()[1].0);
    Ok(())
}

proptest::proptest! {
    #[test]
    fn json_path_validation_equals_platform_validation(value in ".{0,100}") {
        let bytes = serde_json::to_vec(&serde_json::json!({"in_view": [&value]})).unwrap();
        let decoded = acyclic_harness::client::InterestDocument::decode(&bytes, bytes.len());
        proptest::prop_assert_eq!(decoded.is_ok(), acyclic_stream::StreamPath::new(&value).is_ok());
        if let Ok(document) = decoded {
            proptest::prop_assert_eq!(document.in_view[0].as_str(), value.as_str());
        }
    }

    #[test]
    fn layout_documents_round_trip_and_preserve_all_selections(
        visible in proptest::collection::vec(0u8..32, 0..32),
        nearby in proptest::collection::vec(0u8..32, 0..32),
    ) {
        let document = acyclic_harness::client::InterestDocument {
            in_view: super::selected(&visible).collect(),
            prefetch: super::selected(&nearby).collect(),
        };
        let bytes = serde_json::to_vec(&document).unwrap();
        let decoded = acyclic_harness::client::InterestDocument::decode(&bytes, bytes.len()).unwrap();
        proptest::prop_assert_eq!(decoded, document);
        proptest::prop_assert!(acyclic_harness::client::InterestDocument::decode(&bytes, bytes.len() - 1).is_err());
    }
}
