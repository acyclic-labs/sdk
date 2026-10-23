//! File publications drive the real shared session and the renderer's one fold.
use acyclic_harness::{
    Error, Result,
    client::{ClientViewLimits, QueryViewLimits, SwarmChange, SwarmClient, SwarmClientLimits},
    task_volume::{Channel, FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, StreamPath, wire};
use futures::{
    FutureExt as _, StreamExt as _,
    channel::{mpsc, oneshot},
};
use std::{rc::Rc, sync::Arc};

fn path(value: &str) -> StreamPath {
    StreamPath::new(value).unwrap()
}
async fn put(volume: &TaskVolume<MemoryStream>, key: &str, bytes: &[u8]) {
    volume
        .append(
            volume.head().await.unwrap(),
            &Write::new(FileKey::parse(key).unwrap(), bytes.to_vec()),
        )
        .await
        .unwrap();
}
async fn selected(
    sent: &mut mpsc::UnboundedReceiver<wire::SessionFrame>,
    agent: &str,
) -> wire::SessionSubscription {
    while let Some(frame) = sent.next().await {
        if let Some(wire::session_frame::Body::Interest(update)) = frame.body {
            if let Some(sub) = update.upsert.into_iter().find(|sub| matches!(&sub.kind,
                Some(wire::session_subscription::Kind::Follow(follow)) if follow.delivery == wire::SessionDelivery::All as i32 && follow.resource == format!("streams:/{agent}/files"))) { return sub; }
        }
    }
    panic!("interest stream ended before selecting {agent}")
}
async fn notice(changed: &mut mpsc::UnboundedReceiver<&'static str>, expected: &str) {
    while let Some(actual) = changed.next().await {
        if actual == expected {
            return;
        }
    }
    panic!("view notifications ended before {expected}")
}
#[test]
fn view_file_writes_update_subscriptions_and_the_same_file_fold() {
    futures::executor::block_on(exercise()).unwrap();
}
async fn exercise() -> Result<()> {
    let streams = Arc::new(MemoryStream::default());
    let parent = TaskVolume::open(streams.clone(), path("swarm/files"))
        .await
        .unwrap();
    let a = Rc::new(
        TaskVolume::open(streams.clone(), path("swarm/a/files"))
            .await
            .unwrap(),
    );
    let b = Rc::new(
        TaskVolume::open(streams.clone(), path("swarm/b/files"))
            .await
            .unwrap(),
    );
    super::status::status(&streams, "swarm/a", false).await;
    super::status::member(&parent, "a").await;
    super::status::status(&streams, "swarm/b", false).await;
    super::status::member(&parent, "b").await;
    let config = TaskVolume::open(Arc::new(MemoryStream::default()), path("client/config"))
        .await
        .unwrap();
    put(
        &config,
        "workspace/layout.json",
        br#"{"prefetch":["swarm/a"]}"#,
    )
    .await;
    let client = SwarmClient::new(
        path("swarm"),
        SwarmClientLimits {
            agents: 4,
            selected: 1,
            layout_bytes: 1024,
            config_files: 4,
            snapshot_files: 16,
        },
        ClientViewLimits {
            agents: 4,
            selected: 1,
            overview_values: 4,
            overview_bytes: 4096,
            query: QueryViewLimits {
                files: 16,
                segments: 16,
                bytes: 4096,
            },
        },
    )?;
    let views = client.views();
    let (session, mut runner, mut sent) =
        super::runtime::open(streams.clone(), Arc::new(|s| s), Arc::new(|s| s));
    let (notices, mut changed) = mpsc::unbounded();
    let (b_seen, b_ready) = oneshot::channel();
    let mut b_seen = Some(b_seen);
    let consumer = client.follow(&config, &parent, &session,
        |agent, maximum| { let volume = if agent == path("swarm/a") { a.clone() } else { assert_eq!(agent, path("swarm/b")); b.clone() };
            async move { volume.heads(&Query::Root, None, maximum).await } },
        async |change| {
            views.read(|v| v.layout().visible().count())?;
            let kind = match change {
                SwarmChange::Agent { .. } => "agent",
                SwarmChange::Retired { .. } => "retired",
                SwarmChange::Layout => "layout",
                SwarmChange::Snapshot { .. } => "snapshot",
                SwarmChange::Data { overview: false, .. } => "file",
                SwarmChange::Data { agent, overview: true, data, .. } => {
                    if agent == path("swarm/b") && matches!(data.item, Some(wire::session_data::Item::LatestValue(ref value)) if value.attention) {
                        if let Some(seen) = b_seen.take() { let _ = seen.send(()); }
                    }
                    "status"
                }
                SwarmChange::Watermark { .. } => "watermark",
            };
            notices.unbounded_send(kind).unwrap();
            Ok(())
        });
    let writer = async {
        let selected_a = selected(&mut sent, "swarm/a").await;
        notice(&mut changed, "snapshot").await;
        super::status::status(&streams, "swarm/b", true).await;
        b_ready.await.unwrap();
        views.read(|v| {
            assert_eq!(
                v.agents().cloned().collect::<Vec<_>>(),
                [path("swarm/a"), path("swarm/b")]
            );
            assert!(
                v.status(&path("swarm/b"))
                    .unwrap()
                    .values()
                    .any(|value| value.attention)
            );
        })?;
        let cut = views.read(|v| v.cursor(&path("swarm/a")).unwrap().clone())?;
        assert_eq!(selected_a.resume.as_ref().unwrap().next, cut.position);
        let mut write = Write::new(
            FileKey::parse(".tasks/t/turns/1/chunks/1").unwrap(),
            b"live".to_vec(),
        );
        write.channel = Some(Channel::Text);
        a.append(a.head().await.unwrap(), &write).await.unwrap();
        notice(&mut changed, "file").await;
        let key = FileKey::parse(".tasks/t/turns/1/text.md").unwrap();
        let (cursor, pointer) = views.read(|v| {
            assert_eq!(
                v.file(&path("swarm/a"), &key).unwrap().segments[0].as_ref(),
                b"live"
            );
            (
                v.cursor(&path("swarm/a")).unwrap().clone(),
                v.file(&path("swarm/a"), &key).unwrap().segments[0].as_ptr(),
            )
        })?;
        put(
            &config,
            "workspace/open-views.json",
            br#"{"in_view":["swarm/a"]}"#,
        )
        .await;
        notice(&mut changed, "layout").await;
        views.read(|v| {
            assert_eq!(v.cursor(&path("swarm/a")), Some(&cursor));
            assert_eq!(
                v.file(&path("swarm/a"), &key).unwrap().segments[0].as_ptr(),
                pointer
            );
        })?;
        // Removing prefetch must retain the open view and its existing follow.
        put(&config, "workspace/layout.json", b"{}").await;
        notice(&mut changed, "layout").await;
        views.read(|v| {
            assert_eq!(v.cursor(&path("swarm/a")), Some(&cursor));
            assert_eq!(
                v.file(&path("swarm/a"), &key).unwrap().segments[0].as_ptr(),
                pointer
            );
        })?;
        put(&config, "workspace/open-views.json", b"{}").await;
        notice(&mut changed, "layout").await;
        put(
            &config,
            "workspace/layout.json",
            br#"{"in_view":["swarm/b"]}"#,
        )
        .await;
        selected(&mut sent, "swarm/b").await;
        notice(&mut changed, "snapshot").await;
        views.read(|v| {
            assert!(v.cursor(&path("swarm/a")).is_none());
            assert!(v.cursor(&path("swarm/b")).is_some());
        })?;
        Ok::<_, Error>(())
    };
    let scenario = async {
        futures::pin_mut!(consumer, writer);
        futures::select! {
            outcome = consumer.fuse() => Err(Error::Storage(format!("live view ended: {outcome:?}"))),
            outcome = writer.fuse() => outcome,
        }
    };
    let mut scenario = Box::pin(scenario);
    assert!(
        matches!(runner.until_idle(scenario.as_mut()).await, Some(Ok(()))),
        "scenario stalled before its last publication"
    );
    Ok(())
}

#[test]
fn pending_snapshot_cannot_stall_unfollow_status_or_cancellation() {
    futures::executor::block_on(async {
        use std::cell::Cell;
        struct Dropped(Rc<Cell<bool>>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let streams = Arc::new(MemoryStream::default());
        let parent = TaskVolume::open(streams.clone(), path("swarm/files"))
            .await
            .unwrap();
        let a = Rc::new(
            TaskVolume::open(streams.clone(), path("swarm/a/files"))
                .await
                .unwrap(),
        );
        super::status::status(&streams, "swarm/a", false).await;
        super::status::member(&parent, "a").await;
        super::status::status(&streams, "swarm/b", false).await;
        super::status::member(&parent, "b").await;
        let config = TaskVolume::open(Arc::new(MemoryStream::default()), path("client/config"))
            .await
            .unwrap();
        put(
            &config,
            "workspace/layout.json",
            br#"{"in_view":["swarm/a"]}"#,
        )
        .await;
        let client = SwarmClient::new(
            path("swarm"),
            SwarmClientLimits {
                agents: 2,
                selected: 2,
                layout_bytes: 1024,
                config_files: 2,
                snapshot_files: 8,
            },
            ClientViewLimits {
                agents: 2,
                selected: 2,
                overview_values: 2,
                overview_bytes: 4096,
                query: QueryViewLimits {
                    files: 8,
                    segments: 8,
                    bytes: 4096,
                },
            },
        )
        .unwrap();
        let views = client.views();
        let (session, mut runner, mut sent) =
            super::runtime::open(streams.clone(), Arc::new(|s| s), Arc::new(|s| s));
        let (notices, mut changed) = mpsc::unbounded();
        let (started, ready) = oneshot::channel();
        let mut started = Some(started);
        let dropped = Rc::new(Cell::new(false));
        let cancelled = dropped.clone();
        let consumer = client.follow(&config, &parent, &session, |agent, maximum| {
            let a = a.clone();
            let started = if agent == path("swarm/b") { started.take() } else { None };
            let dropped = dropped.clone();
            async move {
                if let Some(started) = started {
                    let _pending = Dropped(dropped);
                    let _ = started.send(());
                    futures::future::pending().await
                } else { a.heads(&Query::Root, None, maximum).await }
            }
        }, async |change| {
            let kind = match change {
                SwarmChange::Snapshot { .. } => "snapshot",
                SwarmChange::Data { overview: true, data, .. } if matches!(data.item, Some(wire::session_data::Item::LatestValue(ref value)) if value.attention) => "attention",
                _ => "other",
            };
            notices.unbounded_send(kind).unwrap();
            Ok(())
        });
        let writer = async {
            let first = selected(&mut sent, "swarm/a").await;
            notice(&mut changed, "snapshot").await;
            put(
                &config,
                "workspace/layout.json",
                br#"{"in_view":["swarm/a","swarm/b"]}"#,
            )
            .await;
            ready.await.unwrap();
            put(&config, "workspace/layout.json", b"{}").await;
            let channel = first.channel.unwrap();
            loop {
                let frame = sent.next().await.unwrap();
                if matches!(frame.body, Some(wire::session_frame::Body::Interest(update)) if update.remove.contains(&channel))
                {
                    break;
                }
            }
            assert!(cancelled.get(), "obsolete snapshot I/O remained retained");
            super::status::status(&streams, "swarm/b", true).await;
            notice(&mut changed, "attention").await;
            views.read(|v| {
                assert_eq!(v.layout().full_fidelity().count(), 0);
                assert!(v.cursor(&path("swarm/a")).is_none());
                assert!(
                    v.status(&path("swarm/b"))
                        .unwrap()
                        .values()
                        .any(|v| v.attention)
                );
            })?;
            Ok::<_, Error>(())
        };
        let scenario = async {
            futures::pin_mut!(consumer, writer);
            futures::select! {
                outcome = consumer.fuse() => panic!("view ended: {outcome:?}"),
                outcome = writer.fuse() => outcome,
            }
        };
        let mut scenario = Box::pin(scenario);
        assert!(
            matches!(runner.until_idle(scenario.as_mut()).await, Some(Ok(()))),
            "pending snapshot blocked a ready layout or status publication"
        );
    });
}
