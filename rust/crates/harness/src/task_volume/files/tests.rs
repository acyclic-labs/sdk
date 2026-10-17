use super::*;
use crate::AgentId;
use crate::task_volume::FileQuerySource;
use acyclic_stream::{MemoryStream, StreamError};
use proptest::prelude::*;

use super::fixtures::{authority, volume};

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

#[cfg(feature = "tui")]
#[tokio::test]
async fn composed_ui_uses_authorized_writes_and_cannot_claim_execution() {
    use crate::client::{CommandFileRoot, CommandSession};
    struct NoFiles;
    impl crate::conversation::ContentResidencyVerifier for NoFiles {}
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
        65_536,
    )
    .await
    .unwrap();
    let files = Arc::new(files);
    let identity = uuid::Uuid::from_bytes([5; 16]);
    let mut commands = CommandSession::open_authorized(files.clone(), identity, 65_536, 64)
        .await
        .unwrap();
    commands.submit("/diff", &NoFiles).await.unwrap();
    assert_eq!(
        commands
            .state(CommandFileRoot::Config, "settings.json")
            .unwrap()
            .unwrap()["diff_mode"],
        "unified"
    );
    crate::client::file_tui_input(
        &mut commands,
        &NoFiles,
        crate::tui::InputEffect::Submit {
            text: "ordinary request".into(),
            queued: false,
            edit: None,
        },
        10,
        100,
    )
    .await
    .unwrap();
    let request = commands.take_request().unwrap();
    let before = files.snapshot(&Query::Root, 64).await.unwrap().generation;
    let result = commands
        .consume(&request, |_, _, _| async {
            panic!("a UI must never acquire execution ownership")
        })
        .await;
    let error = result.unwrap_err();
    assert!(matches!(
        &error,
        crate::Error::TaskFiles(source) if matches!(source.as_ref(), Error::Unauthorized)
    ));
    assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
    assert_eq!(
        files.snapshot(&Query::Root, 64).await.unwrap().generation,
        before
    );
    drop(commands);
    let reopened = CommandSession::open_authorized(files, identity, 65_536, 64)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .state(CommandFileRoot::Config, "settings.json")
            .unwrap()
            .unwrap()["diff_mode"],
        "unified"
    );
}

#[cfg(all(feature = "tui", feature = "terminal"))]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Property: authorized GOAL/plans snapshots plus push changes equal reopening
    // at the final cut, in both ClientViews and the composed TestBackend cells.
    #[test]
    fn document_binding_snapshot_follow_equals_reopen(events in prop::collection::vec((0u8..2, ".{0,20}"), 1..7), cut in 0usize..7) {
        use crate::{client::document_files, wire};
        use futures::{StreamExt, FutureExt};
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let agent = AgentId::from_bytes([1;16]);
            let volume = volume(agent);
            let (verifier, scope) = authority(Some(agent), &volume, &[VolumeOperation::Read, VolumeOperation::Write]);
            let files = TaskFiles::open(Arc::new(MemoryStream::default()), &volume, &verifier, &scope, 65_536).await.unwrap();
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: crate::TaskId::from_bytes([2;16]).to_string() };
            let new_session=crate::test_support::empty_client_views;
            let cut = cut.min(events.len());
            let mut generation = files.snapshot(&Query::Root, 4).await.unwrap().generation;
            for (kind, text) in &events[..cut] {
                let key = if *kind == 0 { FileKey::Goal } else { FileKey::Plan(super::super::PlanName::new("layout").unwrap()) };
                generation = files.write(generation, &FileValue::new(key, text.as_bytes().to_vec())).await.unwrap();
            }
            let mut live = new_session();
            let mut source = document_files(&files, &mut live, 4, 65_536).await.unwrap();
            for (kind, text) in &events[cut..] {
                let key = if *kind == 0 { FileKey::Goal } else { FileKey::Plan(super::super::PlanName::new("layout").unwrap()) };
                for _ in 0..2 {
                    let before = generation;
                    generation = files.write(generation, &FileValue::new(key.clone(), text.as_bytes().to_vec())).await.unwrap();
                    let next = tokio::task::unconstrained(source.next()).now_or_never();
                    if generation == before {
                        prop_assert!(next.is_none());
                    } else {
                        live.query_change(next.expect("accepted cut must be immediately followed").unwrap().unwrap()).unwrap();
                        live.query_change(tokio::task::unconstrained(source.next()).now_or_never().expect("both query cursors must advance").unwrap().unwrap()).unwrap();
                    }
                }
            }
            let mut replayed = new_session();
            let _reopened = document_files(&files, &mut replayed, 4, 65_536).await.unwrap();
            let documents=|views: &crate::client::ClientViews| views.selections().flat_map(|(_,selection)|selection.files().values())
                .filter(|file|matches!(file.key,FileKey::Goal|FileKey::Plan(_)))
                .map(|file|(file.key.clone(),file.segments.clone())).collect::<std::collections::BTreeMap<_,_>>();
            prop_assert_eq!(documents(&live),documents(&replayed));
            let mut pictures = Vec::new();
            for session in [&live, &replayed] {
                let mut renderer = crate::tui::ClientRenderer::default();
                renderer.update(session, &owner);
                let mut terminal = crate::tui::ratatui::Terminal::new(crate::tui::ratatui::backend::TestBackend::new(80, 20)).unwrap();
                terminal.draw(|frame| { renderer.screen().render(frame, &crate::tui::PromptInput::default(), &mut crate::tui::Viewport::default(), 0, 0); }).unwrap();
                pictures.push(terminal.backend().buffer().clone());
            }
            prop_assert_eq!(&pictures[0], &pictures[1]);
            Ok(())
        })?;
    }
}
