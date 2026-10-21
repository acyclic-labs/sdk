use super::*;
use crate::AgentId;
use crate::task_volume::FileQuerySource;
use acyclic_stream::{MemoryStream, StreamError};
use proptest::prelude::*;

use super::fixtures::{authority, volume};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A request's original owner cannot be supplied by an unfenced writer or
    // replaced by a different task, including an otherwise identical replay.
    #[test]
    fn request_owner_is_fenced_and_immutable(task in any::<[u8; 16]>()) {
        futures::executor::block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let volume = volume(AgentId::from_bytes([1; 16]));
            let (verifier, scope) = authority(
                Some(AgentId::from_bytes([1; 16])), &volume,
                &[VolumeOperation::Read, VolumeOperation::Write],
            );
            let files = TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 4096).await.unwrap();
            let parent = StreamPath::new("request-parent").unwrap();
            provider.append(acyclic_stream::AppendRequest {
                path: parent.clone(), records: vec![b"running".to_vec().into()],
                if_tail: Some(0), idempotency_key: None,
            }).await.unwrap();
            let condition = acyclic_stream::CommitCondition::Tail { path: parent, expected: 1 };
            let cut = files.inner.head().await.unwrap();
            let requester = crate::TaskId::from_bytes(task);
            let request = FileValue::new(FileKey::parse("agents/worker/request.md").unwrap(), b"work".to_vec());
            let mut forged = request.clone();
            forged.requester = Some(requester);
            prop_assert!(matches!(files.write(cut, &forged).await, Err(Error::Unauthorized)));
            prop_assert_eq!(files.inner.head().await.unwrap(), cut);
            let cut = files.write_if(cut, &request, requester, condition.clone(), None).await.unwrap();
            let snapshot = files.inner.snapshot(&Query::Children, 8).await.unwrap();
            prop_assert_eq!(snapshot.files[0].requester(), Some(requester));
            prop_assert_eq!(&snapshot.files[0].bytes, &request.bytes);
            prop_assert_eq!(files.write_if(cut, &request, requester, condition.clone(), None).await.unwrap(), cut);
            let mut other = task;
            other[0] ^= 1;
            prop_assert!(matches!(
                files.write_if(cut, &request, crate::TaskId::from_bytes(other), condition, None).await,
                Err(Error::ImmutableConflict(_))
            ));
            prop_assert_eq!(files.inner.head().await.unwrap(), cut);
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn opening_requires_the_exact_private_owner_and_both_volume_grants() {
    let agent = AgentId::from_bytes([1; 16]);
    let volume = volume(agent);
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new(format!(
        "harness/task-volumes/{}",
        volume.storage_name(&mut None).unwrap()
    ))
    .unwrap();
    let both = [VolumeOperation::Read, VolumeOperation::Write];
    for (owner, grants) in [
        (agent, &[VolumeOperation::Read][..]),
        (agent, &[VolumeOperation::Write][..]),
        (agent, &[][..]),
        (AgentId::from_bytes([3; 16]), &both[..]),
    ] {
        let (verifier, scope) = authority(Some(owner), &volume, grants);
        assert!(matches!(
            TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 4096).await,
            Err(crate::Error::Unauthorized(_))
        ));
        assert!(matches!(
            provider.tail(path.clone()).await,
            Err(StreamError::NotFound)
        ));
    }
    let (verifier, scope) = authority(Some(agent), &volume, &both);
    assert!(
        TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 4096)
            .await
            .is_ok()
    );
    assert_eq!(provider.tail(path).await.unwrap(), 1);
}

#[tokio::test]
async fn every_owner_scope_pair_is_checked_before_storage() {
    let agent = AgentId::from_bytes([1; 16]);
    let other = AgentId::from_bytes([2; 16]);
    let template = volume(agent);
    let both = [VolumeOperation::Read, VolumeOperation::Write];
    let mut broken_acceptances = 0;
    for owner in [
        VolumeOwner::Project("project".into()),
        VolumeOwner::Agent(agent),
        VolumeOwner::Session("session".into()),
    ] {
        for subject in [None, Some(agent), Some(other)] {
            let volume = VolumeRef::new(
                template.provider().clone(),
                "owner-table".into(),
                VolumeClass::AgentPrivate,
                owner.clone(),
                &mut None,
            )
            .unwrap();
            let (verifier, scope) = authority(subject, &volume, &both);
            let expected = owner == VolumeOwner::Agent(agent) && subject == Some(agent);
            // Negative control: the former grants-only boundary accepts all
            // six Project/Session cases. Signed grants do not prove ownership.
            let grants_only = both.into_iter().all(|operation| {
                ContentGrant::authorize_volume(
                    &verifier, &scope, &volume, operation, 4096, &mut None,
                )
                .is_ok()
            });
            broken_acceptances += usize::from(grants_only && !expected);
            let provider = Arc::new(MemoryStream::default());
            let path = StreamPath::new(format!(
                "harness/task-volumes/{}",
                volume.storage_name(&mut None).unwrap()
            ))
            .unwrap();
            let opened = TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 4096).await;
            assert_eq!(opened.is_ok(), expected, "{owner:?}, {subject:?}");
            if expected {
                assert_eq!(provider.tail(path).await.unwrap(), 1);
            } else {
                assert!(matches!(opened, Err(crate::Error::Unauthorized(_))));
                assert!(matches!(
                    provider.tail(path).await,
                    Err(StreamError::NotFound)
                ));
            }
        }
    }
    assert_eq!(
        broken_acceptances, 6,
        "broken ownership control was not detected"
    );
}

fn files() -> Vec<(FileKey, bool)> {
    use crate::task_volume::{Channel, Component, InputPath, TaskFile, ToolFile};
    let id = Component::new("id").unwrap();
    let mut files = vec![
        (FileKey::Goal, true),
        (
            FileKey::Plan(crate::task_volume::PlanName::new(id.as_str()).unwrap()),
            true,
        ),
        (
            FileKey::Input(InputPath::try_from("workspace/note.txt".to_owned()).unwrap()),
            true,
        ),
        (FileKey::Agent(id.clone()), false),
    ];
    let task_files = [
        (TaskFile::Request(id.clone()), true),
        (TaskFile::Status, false),
        (
            TaskFile::ModelTurn {
                execution: id.clone(),
                step: 0,
            },
            false,
        ),
        (TaskFile::TurnRequest(0), false),
        (TaskFile::Completion(0), false),
        (
            TaskFile::Chunk {
                turn: 0,
                sequence: 0,
            },
            false,
        ),
        (
            TaskFile::TextHead {
                turn: 0,
                channel: Channel::Text,
            },
            false,
        ),
        (
            TaskFile::TextHead {
                turn: 0,
                channel: Channel::Reasoning,
            },
            false,
        ),
        (TaskFile::ApprovalRequest(id.clone()), false),
        (
            TaskFile::ApprovalDecision {
                request: id.clone(),
                principal: id.clone(),
            },
            false,
        ),
        (TaskFile::Context(id.clone()), false),
        (TaskFile::Notification(id.clone()), false),
    ];
    files.extend(task_files.into_iter().map(|(file, allowed)| {
        (
            FileKey::Task {
                task: id.clone(),
                file,
            },
            allowed,
        )
    }));
    files.extend(
        [
            ToolFile::Request,
            ToolFile::Arguments(0),
            ToolFile::Stdout(0),
            ToolFile::Stderr(0),
            ToolFile::Result,
        ]
        .into_iter()
        .map(|file| {
            (
                FileKey::Task {
                    task: id.clone(),
                    file: TaskFile::Tool {
                        turn: 0,
                        call: id.clone(),
                        file,
                    },
                },
                false,
            )
        }),
    );
    files
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // The only accepted owner/scope state is Agent with exact identity equality.
    #[test]
    fn private_ownership_requires_exact_agent_identity(owner in any::<[u8;16]>(), subject in prop::option::of(any::<[u8;16]>())) {
        let agent = AgentId::from_bytes(owner);
        let template = volume(agent);
        for owner in [VolumeOwner::Project("project".into()), VolumeOwner::Agent(agent), VolumeOwner::Session("session".into())] {
            let volume = VolumeRef::new(template.provider().clone(), "owner-property".into(), VolumeClass::AgentPrivate, owner.clone(), &mut None).unwrap();
            let subject = subject.map(AgentId::from_bytes);
            prop_assert_eq!(owns_volume(&volume, subject), owner == VolumeOwner::Agent(agent) && subject == Some(agent));
        }
        // Always include the matching case, not just independently random IDs.
        prop_assert!(owns_volume(&template, Some(agent)));
    }

    // Exhaust every file role per generated byte/limit pair. Caller writes
    // cannot produce harness/approval records, even with a whole-volume grant.
    #[test]
    fn every_file_role_preserves_owner_separation_and_the_exact_byte_bound(bytes in any::<u64>(), maximum in any::<u64>()) {
        for (key, caller_owned) in files() {
            let result = authorize_write(&key, bytes, maximum);
            prop_assert_eq!(result.is_ok(), caller_owned && bytes <= maximum);
            if !caller_owned { prop_assert!(matches!(result, Err(Error::Unauthorized))); }
        }
    }
}

#[tokio::test]
async fn authorized_queries_bound_snapshot_and_follow_content() {
    use futures::StreamExt as _;
    let agent = AgentId::from_bytes([4; 16]);
    let volume = volume(agent);
    let (verifier, scope) = authority(
        Some(agent),
        &volume,
        &[VolumeOperation::Read, VolumeOperation::Write],
    );
    let files = TaskFiles::open(
        Arc::new(MemoryStream::default()),
        &volume,
        &verifier,
        &scope,
        4096,
    )
    .await
    .unwrap();
    let generation = files.snapshot(&Query::Root, 8).await.unwrap().generation;
    files
        .write(generation, &FileValue::new(FileKey::Goal, b"one".to_vec()))
        .await
        .unwrap();
    let query = Query::File(FileKey::Goal);
    assert!(matches!(
        files.observe_files(query.clone(), None, 1, 2).await,
        Err(Error::LimitExceeded)
    ));
    let mut selected = files.observe_files(query, None, 1, 3).await.unwrap();
    let snapshot = selected.snapshot.take().unwrap();
    assert_eq!(snapshot.files[0].bytes.as_ref(), b"one");
    files
        .write(
            snapshot.generation,
            &FileValue::new(FileKey::Goal, b"four".to_vec()),
        )
        .await
        .unwrap();
    assert!(matches!(
        selected.changes.next().await,
        Some(Err(Error::LimitExceeded))
    ));
}

#[tokio::test]
async fn terminal_child_is_read_only_and_stale_controls_cannot_replace_it() {
    use crate::task_volume::{ChildFile, ChildState, ChildStatus, Component, Control};
    let agent = AgentId::from_bytes([1; 16]);
    let volume = volume(agent);
    let (verifier, scope) = authority(
        Some(agent),
        &volume,
        &[VolumeOperation::Read, VolumeOperation::Write],
    );
    let files = TaskFiles::open(
        Arc::new(MemoryStream::default()),
        &volume,
        &verifier,
        &scope,
        4096,
    )
    .await
    .unwrap();
    for (reference, terminal) in [
        ("cancelled", Control::Cancel),
        ("archived", Control::Archive),
    ] {
        let reference = Component::new(reference).unwrap();
        let key = |file| FileKey::Child {
            reference: reference.clone(),
            file,
        };
        let snapshot = files
            .observe(Query::Children, None, 16)
            .await
            .unwrap()
            .snapshot
            .unwrap();
        let request = FileValue::new(key(ChildFile::Request), b"work".to_vec());
        let running = files.write(snapshot.generation, &request).await.unwrap();
        let status = ChildStatus {
            task: crate::TaskId::from_bytes([7; 16]),
            state: ChildState::Running,
            settled: false,
            inputs: 1,
        };
        let status_file =
            |status| FileValue::new(key(ChildFile::Status), serde_json::to_vec(&status).unwrap());
        let running = files
            .inner
            .append(running, &status_file(status))
            .await
            .unwrap();
        let control = FileValue::new(key(ChildFile::Control), terminal.bytes().to_vec());
        let frozen = files.write(running, &control).await.unwrap();
        assert_eq!(files.write(frozen, &control).await.unwrap(), frozen);
        let resume = FileValue::new(key(ChildFile::Control), Control::Resume.bytes().to_vec());
        assert!(matches!(
            files.write(running, &resume).await,
            Err(Error::Conflict(_))
        ));
        for write in [
            resume,
            request,
            FileValue::new(
                key(ChildFile::Inbox(Component::new("late").unwrap())),
                b"wake".to_vec(),
            ),
        ] {
            assert!(matches!(
                files.write(frozen, &write).await,
                Err(Error::ImmutableConflict(_))
            ));
        }
        let terminal_status = ChildStatus {
            state: ChildState::Running.apply(terminal),
            settled: true,
            ..status
        };
        for invalid in [
            ChildStatus {
                settled: true,
                ..status
            },
            ChildStatus {
                settled: false,
                ..terminal_status
            },
            ChildStatus {
                task: crate::TaskId::from_bytes([8; 16]),
                ..terminal_status
            },
            ChildStatus {
                inputs: 2,
                ..terminal_status
            },
        ] {
            assert!(
                files
                    .inner
                    .append(frozen, &status_file(invalid))
                    .await
                    .is_err()
            );
        }
        let acknowledged = status_file(terminal_status);
        let frozen = files.inner.append(frozen, &acknowledged).await.unwrap();
        assert_eq!(
            files.inner.append(frozen, &acknowledged).await.unwrap(),
            frozen
        );
        assert!(
            files
                .inner
                .append(frozen, &status_file(status))
                .await
                .is_err()
        );
        let snapshot = files
            .observe(Query::Child(reference), None, 16)
            .await
            .unwrap()
            .snapshot
            .unwrap();
        assert_eq!(snapshot.files.len(), 3);
        assert!(snapshot.files.contains(&control));
        assert!(snapshot.files.contains(&acknowledged));
    }
}
