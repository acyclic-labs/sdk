//! Composed-UI properties of task files: the command session and document
//! views write and read only through the authorized task-file handle.
use crate::{
    AgentId, Capabilities, TaskId,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, AuthorityVerifier, Scope},
    resources::ProviderRef,
    task_volume::{Error, Query, TaskFiles},
};
use acyclic_stream::MemoryStream;
use std::sync::Arc;

// Same fixtures as `acyclic_harness_core::task_volume::files` unit tests.
fn authority(
    agent: Option<AgentId>,
    volume: &VolumeRef,
    operations: &[VolumeOperation],
) -> (AuthorityVerifier, Scope) {
    let issuer = AuthorityIssuer::new(
        "task-files",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: TaskId::from_bytes([2; 16]).to_string(),
        },
    );
    let grants: Vec<_> = operations
        .iter()
        .map(|operation| volume.capability(*operation, &mut None).unwrap())
        .collect();
    let scope = issuer
        .issue("caller", &Capabilities::new(grants), agent, None, &mut None)
        .unwrap();
    (issuer.verifier(&mut None).unwrap(), scope)
}

fn volume(agent: AgentId) -> VolumeRef {
    VolumeRef::new(
        ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
        "task-files".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )
    .unwrap()
}

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

#[cfg(feature = "terminal")]
use crate::task_volume::{FileKey, FileValue};
#[cfg(feature = "terminal")]
use proptest::prelude::*;

#[cfg(feature = "terminal")]
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
                let key = if *kind == 0 { FileKey::Goal } else { FileKey::Plan(crate::task_volume::PlanName::new("layout").unwrap()) };
                generation = files.write(generation, &FileValue::new(key, text.as_bytes().to_vec())).await.unwrap();
            }
            let mut live = new_session();
            let mut source = document_files(&files, &mut live, 4, 65_536).await.unwrap();
            for (kind, text) in &events[cut..] {
                let key = if *kind == 0 { FileKey::Goal } else { FileKey::Plan(crate::task_volume::PlanName::new("layout").unwrap()) };
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
