//! Complete discovery and fail-closed lifecycle over the real swarm driver.

use super::path;
use acyclic_harness::{
    Error,
    client::{SwarmClient, SwarmClientLimits, SwarmUpdate},
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{AppendRequest, MemoryStream, StreamProvider, wire};
use futures::{FutureExt as _, SinkExt as _, channel::mpsc};
use std::{cell::RefCell, collections::BTreeSet, rc::Rc, sync::Arc};

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
    let cut = config.snapshot(&Query::Inputs, 10).await.unwrap();
    config
        .append(
            cut.generation,
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
                if follow.delivery != wire::SessionDelivery::Latest as i32
                    || !found.insert(follow.path.clone())
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
    let config = TaskVolume::open(provider.clone(), path("config/files"))
        .await
        .unwrap();
    for id in 0..count {
        provider
            .append(AppendRequest {
                path: path(&format!("swarm/a{id:05}/status")),
                records: vec![b"idle".to_vec().into()],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await
            .unwrap();
    }
    let updates = Rc::new(RefCell::new(Vec::new()));
    let sink = Box::pin(futures::sink::unfold(
        (updates.clone(), false, provider.clone()),
        |(updates, mut created, provider), update: wire::SessionInterest| async move {
            // Actual hierarchy publication between the first and second page.
            // No injected HierarchyChanged error and no timing/polling assumptions.
            if !created
                && update.upsert.iter().any(|sub| {
                    matches!(sub.kind, Some(wire::session_subscription::Kind::Follow(_)))
                })
            {
                if let Some(id) = new_child {
                    let name = match id {
                        usize::MAX => "other/created".to_owned(),
                        id if id == usize::MAX - 1 => "swarm/00000/status".to_owned(),
                        _ => format!("swarm/a{id:05}/status"),
                    };
                    provider
                        .append(AppendRequest {
                            path: path(&name),
                            records: vec![b"created".to_vec().into()],
                            if_tail: Some(0),
                            idempotency_key: None,
                        })
                        .await
                        .unwrap();
                    created = true;
                }
            }
            updates.borrow_mut().push(update);
            Ok::<_, Error>((updates, created, provider))
        },
    ));
    let client = SwarmClient::new(
        path("swarm"),
        limits((count + usize::from(new_child.is_some_and(|id| id != usize::MAX))).max(1)),
    )
    .unwrap();
    let result = client
        .follow(
            provider.as_ref(),
            &config,
            sink,
            futures::stream::pending(),
            |_, _| futures::future::ready(Err(acyclic_harness::task_volume::Error::InvalidPath)),
            |update| {
                futures::future::ready(match update {
                    SwarmUpdate::Layout(_) => Err(Error::Unsupported("complete".into())),
                    _ => Ok(()),
                })
            },
        )
        .await;
    assert!(matches!(result, Err(Error::Unsupported(_))));
    let result = updates.borrow().clone();
    result
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

#[test]
fn config_capacity_is_independent_and_closed_active_channels_fail() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let config = TaskVolume::open(provider.clone(), path("config/files"))
            .await
            .unwrap();
        let agent = TaskVolume::open(provider.clone(), path("swarm/a/files"))
            .await
            .unwrap();
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
        let cut = agent.snapshot(&Query::Root, 1).await.unwrap();
        agent
            .append(
                cut.generation,
                &Write::new(
                    FileKey::parse(".tasks/t/status.json").unwrap(),
                    b"active".to_vec(),
                ),
            )
            .await
            .unwrap();
        // Client channels are odd: control, discovery, status, then full content.
        // Channel 2 is deliberately inactive and must leave the follow pending.
        for channel_id in [0, 1, 3, 5, 2] {
            let (frames, incoming) = mpsc::unbounded();
            let snapshots = RefCell::new(0);
            let client = SwarmClient::new(path("swarm"), limits(1)).unwrap();
            let result = client
                .follow(
                    provider.as_ref(),
                    &config,
                    futures::sink::drain().sink_map_err(|never| match never {}),
                    incoming,
                    |_, maximum| {
                        assert_eq!(maximum, 1);
                        agent.heads(&Query::Root, None, maximum)
                    },
                    |update| {
                        match update {
                            SwarmUpdate::Snapshot { snapshot, .. } => {
                                assert_eq!(snapshot.files.len(), 1);
                                *snapshots.borrow_mut() += 1;
                            }
                            SwarmUpdate::Layout(_) => frames
                                .unbounded_send(Ok(wire::SessionFrame {
                                    channel_id,
                                    generation: 1,
                                    body: Some(wire::session_frame::Body::Closed(
                                        wire::SessionClosed {
                                            code: "permission_revoked".into(),
                                            message: "closed".into(),
                                        },
                                    )),
                                }))
                                .unwrap(),
                            _ => {}
                        }
                        futures::future::ready(Ok(()))
                    },
                )
                .now_or_never();
            assert_eq!(*snapshots.borrow(), 1);
            if channel_id == 2 {
                assert!(result.is_none(), "inactive channel must not end the follow");
                continue;
            }
            let Some(Err(Error::Storage(message))) = result else {
                panic!("closed channel {channel_id} must fail immediately")
            };
            assert!(message.contains(&format!(
                "subscription {channel_id} closed: permission_revoked"
            )));
        }
    });
}

proptest::proptest! {
    // Every admitted direct child gets exactly one latest-value follow, including
    // an empty hierarchy; page boundaries never change this set property.
    #[test]
    fn discovery_preserves_the_complete_status_set(count in 0usize..64) {
        let updates = futures::executor::block_on(discover(count));
        proptest::prop_assert!(check_statuses(&updates, count));
    }
}
