//! Independent lifecycle oracle for the canonical gc-agents status file codec.
use acyclic_harness::task_volume::{ChildFile, ChildState, ChildStatus, Component, FileKey, Query};

pub fn encoded(task: [u8; 16], selected: u8, inputs: u64, settled: bool) -> ChildStatus {
    ChildStatus {
        task: acyclic_harness::TaskId::from_bytes(task),
        inputs,
        state: [
            ChildState::Running,
            ChildState::Paused,
            ChildState::Interrupted,
            ChildState::Cancelled,
            ChildState::Archived,
        ][selected as usize % 5],
        settled,
    }
}
pub fn check(status: ChildStatus, broken: bool) {
    let bytes = serde_json::to_vec(&status).unwrap();
    let valid = status.inputs > 0
        && (status.settled || matches!(status.state, ChildState::Running | ChildState::Paused));
    let parsed = ChildStatus::parse(&bytes);
    assert_eq!(
        parsed.is_ok(),
        valid,
        "status codec accepted invalid quiescence"
    );
    if let Ok(parsed) = parsed {
        assert_eq!(parsed, status);
        let expected = !matches!(status.state, ChildState::Archived | ChildState::Cancelled);
        let actual = if broken { true } else { parsed.active() };
        assert_eq!(actual, expected, "archived membership was retained");
        assert_eq!(serde_json::to_vec(&parsed).unwrap(), bytes);
    }
    let reference = Component::new("worker").unwrap();
    for file in [
        ChildFile::Request,
        ChildFile::Inbox(Component::new("mail").unwrap()),
        ChildFile::Control,
        ChildFile::Status,
        ChildFile::Result(1),
    ] {
        let key = FileKey::Child {
            reference: reference.clone(),
            file,
        };
        assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
        assert_eq!(
            Query::ActiveAgents.matches(&key),
            matches!(
                key,
                FileKey::Child {
                    file: ChildFile::Status,
                    ..
                }
            )
        );
    }
}
pub fn exercise(bytes: &[u8]) {
    if let Ok(status) = ChildStatus::parse(bytes) {
        check(status, false);
    }
    if let Some((&selected, rest)) = bytes.split_first() {
        let mut task = [0; 16];
        for (target, byte) in task.iter_mut().zip(rest) {
            *target = *byte;
        }
        check(encoded(task, selected, 1, selected % 2 == 0), false);
    }
}

/// The actual snapshot/follow fold must equal active membership after every cut.
pub fn query(commands: &[u8], drop_removals: bool) {
    futures::executor::block_on(async {
        use acyclic_harness::{
            client::FileQueryView,
            task_volume::{TaskVolume, Write},
        };
        use acyclic_stream::{MemoryStream, StreamPath};
        use futures::StreamExt as _;
        use std::{collections::BTreeSet, sync::Arc};
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("members").unwrap(),
        )
        .await
        .unwrap();
        let observation = volume.observe(Query::ActiveAgents, None, 2).await.unwrap();
        let mut fold = FileQueryView::from_snapshot(
            Query::ActiveAgents,
            observation.snapshot.unwrap(),
            2,
            4096,
        )
        .unwrap();
        let mut changes = observation.changes;
        let mut active = BTreeSet::new();
        let mut next = 0u32;
        for command in commands.iter().take(32) {
            let (reference, state) = if command % 2 == 0 && active.len() < 2 {
                let reference = format!("agent-{next}");
                next += 1;
                active.insert(reference.clone());
                (reference, ChildState::Running)
            } else if command % 2 == 1
                && let Some(reference) = active.pop_first()
            {
                let key = FileKey::parse(&format!("agents/{reference}/control")).unwrap();
                volume
                    .append(
                        volume.head().await.unwrap(),
                        &Write::new(key, b"archive".to_vec()),
                    )
                    .await
                    .unwrap();
                (reference, ChildState::Archived)
            } else {
                continue;
            };
            let status = ChildStatus {
                task: acyclic_harness::TaskId::from_bytes([1; 16]),
                state,
                settled: state == ChildState::Archived,
                inputs: 1,
            };
            let key = FileKey::parse(&format!("agents/{reference}/status.json")).unwrap();
            let cut = volume
                .append(
                    volume.head().await.unwrap(),
                    &Write::new(key, serde_json::to_vec(&status).unwrap()),
                )
                .await
                .unwrap();
            while fold.cursor().position < cut.position {
                let mut change = changes.next().await.unwrap().unwrap();
                if drop_removals {
                    change.removed.clear();
                }
                fold.apply(change).unwrap();
            }
            let expected: BTreeSet<_> = active
                .iter()
                .map(|id| FileKey::parse(&format!("agents/{id}/status.json")).unwrap())
                .collect();
            assert_eq!(
                fold.files().keys().cloned().collect::<BTreeSet<_>>(),
                expected,
                "archive/removal source retained a member"
            );
            let snapshot = volume.snapshot(&Query::ActiveAgents, 2).await.unwrap();
            assert_eq!(
                snapshot
                    .files
                    .into_iter()
                    .map(|f| f.key)
                    .collect::<BTreeSet<_>>(),
                expected,
                "snapshot disagrees with the live membership fold"
            );
            assert_eq!(fold.cursor().commit, cut.commit);
        }
    });
}

pub fn paths(reference: &str, message: &str, turn: u64) {
    let reference = Component::new(reference).unwrap();
    for file in [
        ChildFile::Request,
        ChildFile::Inbox(Component::new(message).unwrap()),
        ChildFile::Control,
        ChildFile::Status,
        ChildFile::Result(turn),
    ] {
        let key = FileKey::Child {
            reference: reference.clone(),
            file,
        };
        assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
    }
}
