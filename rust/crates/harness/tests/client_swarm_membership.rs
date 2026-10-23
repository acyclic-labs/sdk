//! Archive follows confirmed child membership, without deleting retained history.
use super::{runtime, status};
use acyclic_harness::{
    client::{ClientViewLimits, QueryViewLimits, SwarmClient, SwarmClientLimits},
    task_volume::{ChildState, ChildStatus, FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, StreamPath, wire};
use std::sync::Arc;
fn path(s: &str) -> StreamPath {
    StreamPath::new(s).unwrap()
}
#[test]
fn settled_archive_reclaims_capacity_and_keeps_history_readable() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let parent = TaskVolume::open(provider.clone(), path("swarm/files"))
            .await
            .unwrap();
        let config = TaskVolume::open(provider.clone(), path("config"))
            .await
            .unwrap();
        let a = TaskVolume::open(provider.clone(), path("swarm/a/files"))
            .await
            .unwrap();
        let publication = acyclic_harness::OperationId::from_bytes([9; 16]);
        let mut goal = Write::new(FileKey::Goal, b"retained history".to_vec());
        goal.publication = Some(publication);
        a.append(a.head().await.unwrap(), &goal).await.unwrap();
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
        for reference in ["a", "b"] {
            status::status(&provider, &format!("swarm/{reference}"), false).await;
            status::member(&parent, reference).await;
        }
        let client = SwarmClient::new(
            path("swarm"),
            SwarmClientLimits {
                agents: 2,
                selected: 1,
                layout_bytes: 128,
                config_files: 1,
                snapshot_files: 1,
            },
            ClientViewLimits {
                agents: 2,
                selected: 1,
                overview_values: 2,
                overview_bytes: 64,
                query: QueryViewLimits {
                    files: 1,
                    segments: 1,
                    bytes: 128,
                },
            },
        )
        .unwrap();
        let views = client.views();
        let (session, mut runner, mut observed) =
            runtime::open(provider.clone(), Arc::new(|s| s), Arc::new(|s| s));
        let mut consumer = Box::pin(client.follow(
            &config,
            &parent,
            &session,
            |agent, maximum| {
                assert_eq!(agent, path("swarm/a"));
                a.heads(&Query::Root, None, maximum)
            },
            async |_| Ok(()),
        ));
        assert!(runner.until_idle(consumer.as_mut()).await.is_none());
        views
            .read(|v| {
                assert_eq!(v.agents().count(), 2);
                assert!(v.cursor(&path("swarm/a")).is_some());
                assert_eq!(v.file(&path("swarm/a"), &FileKey::Goal).unwrap().publication, Some(publication));
                assert!(v.goals().any(|view| matches!(view.goal, acyclic_harness::client::GoalFileView::Present { goal } if goal.markdown == "retained history")));
            })
            .unwrap();
        let mut owned = Vec::new();
        while let Ok(frame) = observed.try_recv() {
            if let Some(wire::session_frame::Body::Interest(change)) = frame.body {
                for sub in change.upsert {
                    if matches!(&sub.kind, Some(wire::session_subscription::Kind::Follow(f)) if f.resource == "streams:/swarm/a/status" || f.resource == "streams:/swarm/a/files")
                    {
                        owned.push(sub.channel.unwrap().channel_id);
                    }
                }
            }
        }
        assert_eq!(owned.len(), 2);
        // A requested archive is not evidence that all child effects have stopped.
        parent
            .append(
                parent.head().await.unwrap(),
                &Write::new(
                    FileKey::parse("agents/a/control").unwrap(),
                    b"archive".to_vec(),
                ),
            )
            .await
            .unwrap();
        assert!(runner.until_idle(consumer.as_mut()).await.is_none());
        assert_eq!(views.read(|v| v.agents().count()).unwrap(), 2);
        let archived = ChildStatus {
            task: acyclic_harness::TaskId::from_bytes([1; 16]),
            state: ChildState::Archived,
            settled: true,
            inputs: 1,
        };
        parent
            .append(
                parent.head().await.unwrap(),
                &Write::new(
                    FileKey::parse("agents/a/status.json").unwrap(),
                    serde_json::to_vec(&archived).unwrap(),
                ),
            )
            .await
            .unwrap();
        assert!(runner.until_idle(consumer.as_mut()).await.is_none());
        views
            .read(|v| {
                assert_eq!(v.agents().count(), 1);
                assert!(v.status(&path("swarm/a")).is_none());
                assert!(v.cursor(&path("swarm/a")).is_none());
                assert_eq!(v.files(&path("swarm/a")).count(), 0);
            })
            .unwrap();
        let mut removed = Vec::new();
        while let Ok(frame) = observed.try_recv() {
            if let Some(wire::session_frame::Body::Interest(change)) = frame.body {
                removed.extend(change.remove.into_iter().map(|c| c.channel_id));
            }
        }
        assert!(
            owned.iter().all(|id| removed.contains(id)),
            "retired pool cursors remained subscribed"
        );
        status::status(&provider, "swarm/c", false).await;
        status::member(&parent, "c").await;
        // Old immutable history and its latest status cannot reactivate the archived row.
        status::status(&provider, "swarm/a", true).await;
        assert!(runner.until_idle(consumer.as_mut()).await.is_none());
        assert_eq!(
            views
                .read(|v| v
                    .agents()
                    .map(StreamPath::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>())
                .unwrap(),
            ["swarm/b", "swarm/c"]
        );
        let snapshot = parent.snapshot(&Query::ActiveAgents, 2).await.unwrap();
        assert_eq!(
            snapshot.files.len(),
            2,
            "archived history must not consume active snapshot capacity"
        );
        assert_eq!(
            a.snapshot(&Query::Root, 1).await.unwrap().files[0]
                .bytes
                .as_ref(),
            b"retained history"
        );
    });
}
