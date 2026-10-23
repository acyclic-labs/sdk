//! Complete discovery and fail-closed lifecycle through the shared platform pool.
use super::{path, runtime, status};
use acyclic_harness::{
    Error,
    client::{SwarmChange, SwarmClientLimits},
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, wire};
use futures::StreamExt as _;
use std::{cell::Cell, collections::BTreeSet, sync::Arc};

fn limits(agents: usize) -> SwarmClientLimits {
    SwarmClientLimits {
        agents,
        selected: agents,
        layout_bytes: 4096,
        config_files: 10,
        snapshot_files: 1,
    }
}
async fn write(config: &TaskVolume<MemoryStream>, name: &str, bytes: &[u8]) {
    config
        .append(
            config.head().await.unwrap(),
            &Write::new(FileKey::parse(name).unwrap(), bytes.to_vec()),
        )
        .await
        .unwrap();
}
fn status_paths(updates: &[wire::SessionInterest]) -> Option<BTreeSet<String>> {
    let mut found = BTreeSet::new();
    for update in updates {
        for subscription in &update.upsert {
            if let Some(wire::session_subscription::Kind::Follow(follow)) = &subscription.kind {
                if follow.resource == "streams:/swarm/files" {
                    assert_eq!(follow.delivery, wire::SessionDelivery::All as i32);
                    assert_eq!(follow.preceding_commit_id.len(), 32);
                    continue;
                }
                if follow.delivery != wire::SessionDelivery::Latest as i32
                    || !found.insert(follow.resource.strip_prefix("streams:/")?.to_owned())
                {
                    return None;
                }
            }
        }
    }
    Some(found)
}
fn check_statuses(updates: &[wire::SessionInterest], count: usize) -> bool {
    status_paths(updates)
        == Some(
            (0..count)
                .map(|id| format!("swarm/a{id:05}/status"))
                .collect(),
        )
}
async fn discover(count: usize) -> Vec<wire::SessionInterest> {
    discover_with_creation(count, None).await
}
async fn discover_with_creation(
    count: usize,
    new_child: Option<usize>,
) -> Vec<wire::SessionInterest> {
    let provider = Arc::new(MemoryStream::default());
    let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
        .await
        .unwrap();
    let config = TaskVolume::open(provider.clone(), path("config/files"))
        .await
        .unwrap();
    for id in 0..count {
        status::status(&provider, &format!("swarm/a{id:05}"), false).await;
        status::member(&parent, &format!("a{id:05}")).await;
    }
    let parent = Arc::new(parent);
    let live_parent = parent.clone();
    let injected = provider.clone();
    let requests: runtime::RequestMap = Arc::new(move |source| {
        let provider = injected.clone();
        let parent = live_parent.clone();
        Box::pin(source.scan(false, move |created, frame| {
            let create = !*created && new_child.is_some() && matches!(&frame.body,
                Some(wire::session_frame::Body::Interest(update)) if update.upsert.iter().any(|sub| matches!(sub.kind, Some(wire::session_subscription::Kind::Follow(_)))));
            *created |= create;
            let provider = provider.clone();
            let parent = parent.clone();
            async move {
                if create {
                    let name = match new_child.unwrap() {
                        usize::MAX => "other/created".to_owned(),
                        id if id == usize::MAX - 1 => "swarm/00000".to_owned(),
                        id => format!("swarm/a{id:05}"),
                    };
                    // Creation while the initial children snapshot is being consumed.
                    status::status(&provider, &name, false).await;
                    if let Some(reference) = name.strip_prefix("swarm/") { status::member(&parent, reference).await; }
                }
                Some(frame)
            }
        }))
    });
    let (session, mut runner, mut observed) = runtime::open(provider, requests, Arc::new(|s| s));
    let client = super::client(limits(
        (count + usize::from(new_child.is_some_and(|id| id != usize::MAX))).max(1),
    ))
    .unwrap();
    let mut consumer = Box::pin(client.follow(
        &config,
        &parent,
        &session,
        |_, _| futures::future::ready(Err(acyclic_harness::task_volume::Error::InvalidPath)),
        async |_| Ok(()),
    ));
    let settled = runner.until_idle(consumer.as_mut()).await;
    assert!(
        settled.is_none(),
        "discovery count {count} must remain live: {settled:?}"
    );
    std::iter::from_fn(|| observed.try_recv().ok())
        .filter_map(|frame| match frame.body {
            Some(wire::session_frame::Body::Interest(update)) => Some(update),
            _ => None,
        })
        .collect()
}
#[test]
fn hierarchy_creation_between_pages_restarts_without_duplicate_status_follows() {
    for new_child in [usize::MAX, usize::MAX - 1, acyclic_stream::MAX_ITEMS + 1] {
        let count = acyclic_stream::MAX_ITEMS + 1;
        let updates = futures::executor::block_on(discover_with_creation(count, Some(new_child)));
        let mut expected: BTreeSet<_> = (0..count)
            .map(|id| format!("swarm/a{id:05}/status"))
            .collect();
        if new_child == usize::MAX - 1 {
            expected.insert("swarm/00000/status".into());
        } else if new_child != usize::MAX {
            expected.insert(format!("swarm/a{new_child:05}/status"));
        }
        assert_eq!(status_paths(&updates), Some(expected));
        assert!(
            updates
                .iter()
                .enumerate()
                .all(|(index, update)| update.revision == index as u64 + 1)
        );
    }
}
#[test]
fn discovery_follows_every_page_and_status_once() {
    for count in [
        0,
        1,
        acyclic_stream::MAX_ITEMS,
        acyclic_stream::MAX_ITEMS + 1,
        acyclic_stream::MAX_ITEMS * 2 + 1,
    ] {
        let updates = futures::executor::block_on(discover(count));
        assert!(check_statuses(&updates, count), "agent count {count}");
        assert!(
            updates
                .iter()
                .enumerate()
                .all(|(index, update)| update.revision == index as u64 + 1)
        );
    }
}
#[test]
fn negative_control_truncated_discovery_is_rejected() {
    let mut updates = futures::executor::block_on(discover(2));
    assert!(check_statuses(&updates, 2));
    updates.last_mut().unwrap().upsert.pop();
    assert!(!check_statuses(&updates, 2));
}

#[derive(Clone, Copy)]
enum Closed {
    None,
    Control,
    Discovery,
    Status,
    Content,
    Inactive,
}
#[test]
fn config_capacity_is_independent_and_closed_active_channels_fail() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
            .await
            .unwrap();
        let config = TaskVolume::open(provider.clone(), path("config/files"))
            .await
            .unwrap();
        let agent = TaskVolume::open(provider.clone(), path("swarm/a/files"))
            .await
            .unwrap();
        status::status(&provider, "swarm/a", false).await;
        status::member(&parent, "a").await;
        write(
            &config,
            "workspace/layout.json",
            br#"{"in_view":["swarm/a"]}"#,
        )
        .await;
        write(&config, "workspace/open-views.json", b"{}").await;
        for id in 0..6 {
            write(&config, &format!("commands/{id}.md"), b"unrelated").await;
        }
        write(&agent, ".tasks/t/status.json", b"active").await;
        for closed in [
            Closed::None,
            Closed::Control,
            Closed::Discovery,
            Closed::Status,
            Closed::Content,
            Closed::Inactive,
        ] {
            let responses: runtime::ResponseMap = Arc::new(move |source| {
                Box::pin(source.flat_map(move |item| {
                    let mut output = Vec::new();
                    if let Ok(frame) = &item {
                        if let Some(wire::session_frame::Body::Data(data)) = &frame.body {
                            use wire::session_data::Item;
                            let target = matches!(
                                (closed, &data.item),
                                (Closed::Control | Closed::Inactive, _)
                                    | (Closed::Discovery, Some(Item::Child(_)))
                                    | (Closed::Status, Some(Item::LatestValue(_)))
                                    | (Closed::Content, Some(Item::Record(_)))
                            );
                            if target {
                                let mut failure = frame.clone();
                                failure.channel_id = match closed {
                                    Closed::Control => 0,
                                    Closed::Inactive => u64::MAX - 1,
                                    _ => failure.channel_id,
                                };
                                failure.body =
                                    Some(wire::session_frame::Body::Closed(wire::SessionClosed {
                                        code: "access_denied".into(),
                                        message: "closed".into(),
                                    }));
                                output.push(Ok(failure));
                                if !matches!(closed, Closed::Inactive) {
                                    return futures::stream::iter(output);
                                }
                            }
                        }
                    }
                    output.push(item);
                    futures::stream::iter(output)
                }))
            });
            let (session, mut runner, _) =
                runtime::open(provider.clone(), Arc::new(|s| s), responses);
            let snapshots = Cell::new(0);
            let client = super::client(limits(1)).unwrap();
            let views = client.views();
            let mut consumer = Box::pin(client.follow(
                &config,
                &parent,
                &session,
                |_, maximum| {
                    let agent = &agent;
                    async move {
                        assert_eq!(maximum, 1);
                        let snapshot = agent.heads(&Query::Root, None, maximum).await?;
                        let next = if snapshot.files[0].segments[0].as_ref() == b"active" {
                            b"updated".as_slice()
                        } else {
                            b"active".as_slice()
                        };
                        write(agent, ".tasks/t/status.json", next).await;
                        Ok(snapshot)
                    }
                },
                async |change| {
                    if let SwarmChange::Snapshot { agent, .. } = change {
                        assert_eq!(views.read(|v| v.files(&agent).count()).unwrap(), 1);
                        snapshots.set(snapshots.get() + 1);
                    }
                    Ok(())
                },
            ));
            let result = runner.until_idle(consumer.as_mut()).await;
            if matches!(closed, Closed::None | Closed::Inactive) {
                assert!(
                    result.is_none(),
                    "inactive channel ended a live follow: {result:?}"
                );
                assert_eq!(snapshots.get(), 1);
            } else {
                assert!(
                    matches!(result, Some(Err(Error::Storage(_)))),
                    "active channel did not fail: {result:?}"
                );
            }
        }
    });
}
proptest::proptest! {
    // Every admitted direct child gets exactly one latest-value follow. Hierarchy
    // page boundaries and in-flight creation never change this set property.
    #[test]
    fn discovery_preserves_the_complete_status_set(count in 0usize..64) {
        let updates = futures::executor::block_on(discover(count));
        proptest::prop_assert!(check_statuses(&updates, count));
    }
}

#[test]
fn ready_membership_cannot_register_before_discovery_is_delivered() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
            .await
            .unwrap();
        let config = TaskVolume::open(provider.clone(), path("config/files"))
            .await
            .unwrap();
        let count = acyclic_stream::MAX_ITEMS * 2 + 1;
        for id in 0..count {
            status::status(&provider, &format!("swarm/a{id:05}"), false).await;
            status::member(&parent, &format!("a{id:05}")).await;
        }
        let (release, waiting) = futures::channel::oneshot::channel::<()>();
        let waiting = std::sync::Mutex::new(Some(waiting));
        let responses: runtime::ResponseMap = Arc::new(move |source| {
            let mut waiting = waiting.lock().unwrap().take();
            Box::pin(source.then(move |frame| {
                let waiting = if matches!(&frame, Ok(frame) if matches!(&frame.body,
                    Some(wire::session_frame::Body::Data(data)) if matches!(data.item, Some(wire::session_data::Item::Child(_))))) {
                    waiting.take()
                } else { None };
                async move { if let Some(waiting) = waiting { waiting.await.unwrap(); } frame }
            }))
        });
        let (session, mut runner, mut observed) =
            runtime::open(provider, Arc::new(|s| s), responses);
        let client = super::client(limits(count)).unwrap();
        let mut consumer = Box::pin(client.follow(
            &config,
            &parent,
            &session,
            |_, _| futures::future::ready(Err(acyclic_harness::task_volume::Error::InvalidPath)),
            async |_| Ok(()),
        ));
        assert!(runner.until_idle(consumer.as_mut()).await.is_none());
        let interests = std::iter::from_fn(|| observed.try_recv().ok())
            .filter_map(|frame| match frame.body {
                Some(wire::session_frame::Body::Interest(change)) => Some(change),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            status_paths(&interests).unwrap().len(),
            0,
            "snapshot bypassed the discovery flow-control boundary"
        );
        release.send(()).unwrap();
        let settled = runner.until_idle(consumer.as_mut()).await;
        assert!(settled.is_none(), "paced discovery terminated: {settled:?}");
        let interests = std::iter::from_fn(|| observed.try_recv().ok())
            .filter_map(|frame| match frame.body {
                Some(wire::session_frame::Body::Interest(change)) => Some(change),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(check_statuses(&interests, count));
    });
}

#[test]
fn live_and_republished_membership_wait_for_shared_session_delivery() {
    for initial in [false, true] {
        futures::executor::block_on(async {
            use acyclic_harness::{
                TaskId,
                task_volume::{ChildState, ChildStatus},
            };
            let provider = Arc::new(MemoryStream::default());
            let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
                .await
                .unwrap();
            let config = TaskVolume::open(provider.clone(), path("config/files"))
                .await
                .unwrap();
            let count = acyclic_stream::MAX_ITEMS * 2 + 1;
            if initial {
                for id in 0..count {
                    status::status(&provider, &format!("swarm/a{id:05}"), false).await;
                    status::member(&parent, &format!("a{id:05}")).await;
                }
            }
            let (release, waiting) = futures::channel::oneshot::channel::<()>();
            let waiting = std::sync::Mutex::new(Some(waiting));
            let responses: runtime::ResponseMap = Arc::new(move |source| {
                let mut waiting = waiting.lock().unwrap().take();
                Box::pin(source.then(move |frame| {
                    let held = matches!(&frame, Ok(frame) if matches!(&frame.body,
                        Some(wire::session_frame::Body::Data(data)) if if initial {
                            matches!(data.item, Some(wire::session_data::Item::Child(_)))
                        } else { matches!(data.item, Some(wire::session_data::Item::Record(_))) }));
                    let waiting = if held { waiting.take() } else { None };
                    async move {
                        if let Some(waiting) = waiting {
                            waiting.await.unwrap();
                        }
                        frame
                    }
                }))
            });
            let (session, mut runner, mut observed) =
                runtime::open(provider.clone(), Arc::new(|s| s), responses);
            let client = super::client(limits(count)).unwrap();
            let views = client.views();
            let mut consumer = Box::pin(client.follow(
                &config,
                &parent,
                &session,
                |_, _| {
                    futures::future::ready(Err(acyclic_harness::task_volume::Error::InvalidPath))
                },
                async |_| Ok(()),
            ));
            assert!(runner.until_idle(consumer.as_mut()).await.is_none());
            while observed.try_recv().is_ok() {}
            for id in 0..count {
                if !initial {
                    status::status(&provider, &format!("swarm/a{id:05}"), false).await;
                }
                let value = ChildStatus {
                    task: TaskId::from_bytes([1; 16]),
                    state: ChildState::Running,
                    settled: false,
                    inputs: 2,
                };
                write(
                    &parent,
                    &format!("agents/a{id:05}/status.json"),
                    &serde_json::to_vec(&value).unwrap(),
                )
                .await;
            }
            let settled = runner.until_idle(consumer.as_mut()).await;
            assert!(
                settled.is_none(),
                "membership burst initial={initial} terminated: {settled:?}"
            );
            assert_eq!(
                views.read(|v| v.agents().count()).unwrap(),
                0,
                "live membership bypassed held session delivery"
            );
            let pending = std::iter::from_fn(|| observed.try_recv().ok())
                .filter_map(|frame| match frame.body {
                    Some(wire::session_frame::Body::Interest(change)) => Some(change),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(status_paths(&pending).unwrap().len(), 0);
            release.send(()).unwrap();
            let settled = runner.until_idle(consumer.as_mut()).await;
            assert!(
                settled.is_none(),
                "membership burst failed after release: {settled:?}"
            );
            assert_eq!(views.read(|v| v.agents().count()).unwrap(), count);
            let admitted = std::iter::from_fn(|| observed.try_recv().ok())
                .filter_map(|frame| match frame.body {
                    Some(wire::session_frame::Body::Interest(change)) => Some(change),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert!(check_statuses(&admitted, count));
        });
    }
}
