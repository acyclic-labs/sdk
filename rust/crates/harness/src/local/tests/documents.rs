use super::*;
use crate::{
    TaskId,
    client::{ClientViews, local_documents},
    conversation::ContentPublisher,
    filesystem::FilesystemContentPublisher,
    task_volume::{FileKey, FileValue, Query},
    tui::{
        ClientRenderer, PromptInput, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    },
    wire,
};
use futures::StreamExt as _;

fn picture(session: &ClientViews, owner: &wire::Authority) -> String {
    let mut renderer = ClientRenderer::default();
    renderer.update(session, owner);
    let mut terminal = Terminal::new(TestBackend::new(100, 24)).unwrap();
    terminal
        .draw(|frame| {
            renderer.screen().render(
                frame,
                &PromptInput::default(),
                &mut Viewport::default(),
                0,
                0,
            );
        })
        .unwrap();
    terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}

async fn apply_next(
    source: &mut crate::task_volume::ChangeStream,
    session: &mut ClientViews,
) -> Result<()> {
    let update = tokio::time::timeout(std::time::Duration::from_secs(2), source.next())
        .await
        .map_err(super::super::storage)?
        .ok_or_else(|| Error::NotFound("document update".into()))??;
    session.query_change(update).map(|_| ())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(4))]
    // Property: snapshot and follow display the real private SDK publication,
    // independently of a conflicting GOAL.md in the execution-log volume.
    #[test]
    fn private_sdk_documents_follow_into_the_composed_panels(text in "[a-z]{1,12}") {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(async {
                let directory = tempfile::tempdir().map_err(super::super::storage)?;
                let harness = LocalHarness::open(LocalHarnessConfig::new(
                    directory.path(), (model()?, scripted([])),
                )).await?;
                assert!(harness.scratch_files(acyclic_fs::WorkBudget::UNBOUNDED).is_err());
                let publisher = FilesystemContentPublisher::new(
                    harness.files.clone(), harness.scratch(), &harness.verifier,
                    &harness.signed, harness.limits.file_bytes, None,
                )?;
                for (path, body) in [
                    ("GOAL.md", format!("# Scratch goal {text}")),
                    ("plans/release notes.md", format!("- [ ] Scratch plan {text}")),
                ] {
                    publisher.stage_once(OperationId::new(), path, body.as_bytes(),
                        "text/markdown", path.rsplit('/').next().unwrap(), None)?.await?;
                }
                let cut = harness.task_files().snapshot(&Query::File(FileKey::Goal), 1).await?;
                harness.task_files().write(cut.generation,
                    &FileValue::new(FileKey::Goal, b"Wrong execution-log goal".to_vec())).await?;
                let owner = wire::Authority { kind: wire::AggregateKind::Task as i32,
                    id: TaskId::new().to_string() };
                let mut session = crate::test_support::empty_client_views();
                let mut source = local_documents(&harness, &mut session).await?;
                let initial = picture(&session, &owner);
                assert!(initial.contains("Scratch goal"), "{initial}");
                assert!(initial.contains("Scratch plan"), "{initial}");
                assert!(!initial.contains("Wrong execution-log goal"));
                let updated = format!("# Updated goal {text}");
                publisher.stage_once(OperationId::new(), "GOAL.md", updated.as_bytes(),
                    "text/markdown", "GOAL.md", None)?.await?;
                for _ in 0..2 {apply_next(&mut source, &mut session).await?;}
                let followed = picture(&session, &owner);
                assert!(followed.contains("Updated goal"), "{followed}");
                assert!(!followed.contains("Scratch goal"));
                assert!(followed.contains("Scratch plan"));
                Ok::<(), Error>(())
            }).unwrap();
    }
}
