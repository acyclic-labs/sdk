use super::*;
use crate::{
    context::{ContextDiscoveryLimits, ContextManifest},
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef},
    filesystem::FilesystemContentPublisher,
};
pub(super) mod crash_cuts;
mod generation;

async fn instruction(harness: &LocalHarness, name: &str, text: &str) -> Result<()> {
    FilesystemContentPublisher::new(
        harness.files.clone(),
        harness.project(),
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
        None,
    )?
    .stage(
        OperationId::new(),
        name,
        text.as_bytes(),
        "text/markdown",
        name,
        None,
    )?
    .await?;
    Ok(())
}

pub(super) async fn manifest(
    harness: &LocalHarness,
    operation: OperationId,
) -> Result<(FileRef, ContextManifest)> {
    let file = harness.retained_context_manifest(operation).await?;
    let bytes = harness
        .context_reader()?
        .read(&file, harness.limits.file_bytes, None)?
        .await?;
    let manifest = ContextManifest::decode(&bytes, harness.limits)?;
    Ok((file, manifest))
}

pub(super) fn admitted(
    admission: Admission<crate::runtime::RuntimeTask<crate::executor::TurnOutput>>,
) -> Result<crate::TaskId> {
    match admission {
        Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) => Ok(task_id),
        _ => Err(Error::Conflict(
            "context fixture was not durably admitted".into(),
        )),
    }
}

async fn check_instructions(
    harness: &LocalHarness,
    provider: &ScriptedModelProvider,
    index: usize,
    agents: &str,
    claude: &str,
    absent: &[&str],
) -> Result<()> {
    let requests = provider.requests();
    let mut rendered = String::new();
    let reader = harness.context_reader()?;
    for message in &requests[index].messages {
        if let crate::model::ModelContent::Part(crate::model::ModelContentPart::File {
            file,
            policy: crate::model::FileProjectionPolicy::ProjectInstructions,
        }) = &message.content
        {
            assert_eq!(message.role, crate::model::ModelRole::User);
            let bytes = reader
                .read(file, file.descriptor().byte_length(), None)?
                .await?;
            file.descriptor().verify(&bytes, &mut None)?;
            rendered.push_str(&crate::model::InstructionSource::new(file).render(&bytes, None)?);
        }
    }
    for (name, text) in [("AGENTS.md", agents), ("CLAUDE.md", claude)] {
        assert!(rendered.contains(&format!(
            "# {name} instructions for .\n\n<INSTRUCTIONS>\n{text}\n</INSTRUCTIONS>"
        )));
    }
    let agents = rendered
        .find(agents)
        .expect("AGENTS.md instructions missing");
    let claude = rendered
        .find(claude)
        .expect("CLAUDE.md instructions missing");
    assert!(
        agents < claude,
        "instruction precedence changed: {rendered}"
    );
    for value in absent {
        assert!(
            !rendered.contains(value),
            "unselected instructions reached the model: {rendered}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn volume_instructions_are_pinned_before_input_and_refreshed_for_the_next_turn() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("second".into()),
    ]);
    let harness = LocalHarness::open(volume_config(
        directory.path(),
        project.clone(),
        provider.clone(),
    )?)
    .await?;
    instruction(&harness, "AGENTS.md", "agents before capture").await?;
    instruction(&harness, "CLAUDE.md", "claude before capture").await?;
    instruction(&harness, "unselected.md", "unselected context marker").await?;
    let operation = OperationId::new();
    let reference = harness.stage_context_manifest(operation).await?;
    assert!(
        harness
            .input_publisher()?
            .observe_staged_once(operation, harness.limits.file_bytes, None)?
            .await?
            .0
            .is_none()
    );
    instruction(&harness, "AGENTS.md", "agents after capture").await?;
    instruction(&harness, "CLAUDE.md", "claude after capture").await?;
    drop(harness);
    let harness =
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
    let task = admitted(harness.submit(operation, "first prompt").await?)?;
    let (retained, captured) = manifest(&harness, operation).await?;
    assert_eq!(reference, retained);
    assert_eq!(captured.tools, harness.tools().definitions(None)?);
    assert!(
        captured
            .discovery
            .instructions
            .iter()
            .all(|file| file.volume() == harness.project())
    );
    assert_eq!(
        captured.discovery.instructions[0].version(),
        captured.discovery.instructions[1].version()
    );
    assert!(captured.sources.is_empty());
    assert_eq!(
        captured
            .discovery
            .instructions
            .iter()
            .map(|file| file.display_name())
            .collect::<Vec<_>>(),
        ["AGENTS.md", "CLAUDE.md"]
    );
    assert!(provider.requests().is_empty());
    assert_eq!(harness.resume(task).await?.text, "first");
    check_instructions(
        &harness,
        &provider,
        0,
        "agents before capture",
        "claude before capture",
        &[
            "agents after capture",
            "claude after capture",
            "unselected context marker",
        ],
    )
    .await?;
    assert_eq!(harness.run("second prompt").await?.text, "second");
    check_instructions(
        &harness,
        &provider,
        1,
        "agents after capture",
        "claude after capture",
        &[
            "agents before capture",
            "claude before capture",
            "unselected context marker",
        ],
    )
    .await?;
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn checkout_instructions_survive_source_edits_and_reopen_without_authored_masks() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join("AGENTS.md"),
        "checkout agents original",
    )
    .map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join("CLAUDE.md"),
        "checkout claude original",
    )
    .map_err(super::super::storage)?;
    let provider = scripted([
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("second".into()),
    ]);
    let config =
        crate::test_support::local_harness_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path());
    let harness = LocalHarness::open(config).await?;
    let operation = OperationId::new();
    let task = admitted(harness.submit(operation, "original request").await?)?;
    let (reference, captured) = manifest(&harness, operation).await?;
    assert_eq!(captured.discovery.instructions.len(), 2);
    assert!(
        captured
            .discovery
            .instructions
            .iter()
            .all(|file| file.volume() == harness.journal_volume()
                && file.path().starts_with("inputs/context/"))
    );
    std::fs::write(
        directory.path().join("AGENTS.md"),
        "checkout agents replacement",
    )
    .map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join("CLAUDE.md"),
        "checkout claude replacement",
    )
    .map_err(super::super::storage)?;
    drop(harness);
    let config =
        crate::test_support::local_harness_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path());
    let reopened = LocalHarness::open(config).await?;
    assert_eq!(manifest(&reopened, operation).await?.0, reference);
    assert_eq!(reopened.resume(task).await?.text, "first");
    check_instructions(
        &reopened,
        &provider,
        0,
        "checkout agents original",
        "checkout claude original",
        &["checkout agents replacement", "checkout claude replacement"],
    )
    .await?;
    assert_eq!(reopened.run("new request").await?.text, "second");
    check_instructions(
        &reopened,
        &provider,
        1,
        "checkout agents replacement",
        "checkout claude replacement",
        &["checkout agents original", "checkout claude original"],
    )
    .await?;
    let next = OperationId::new();
    reopened.stage_context_manifest(next).await?;
    let (_, before) = manifest(&reopened, next).await?;
    std::fs::write(directory.path().join("unrelated.txt"), "an unrelated edit")
        .map_err(super::super::storage)?;
    let next = OperationId::new();
    reopened.stage_context_manifest(next).await?;
    let (_, after) = manifest(&reopened, next).await?;
    assert_eq!(before.discovery.instructions, after.discovery.instructions);
    std::fs::remove_file(directory.path().join("CLAUDE.md")).map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join("AGENTS.md"),
        "same instance latest agents",
    )
    .map_err(super::super::storage)?;
    let next = OperationId::new();
    reopened.stage_context_manifest(next).await?;
    let (_, deleted) = manifest(&reopened, next).await?;
    assert_eq!(deleted.discovery.instructions.len(), 1);
    assert_eq!(
        deleted.discovery.instructions[0].display_name(),
        "AGENTS.md"
    );
    let bytes = reopened
        .context_reader()?
        .read(
            &deleted.discovery.instructions[0],
            reopened.limits.file_bytes,
            None,
        )?
        .await?;
    assert_eq!(bytes.as_ref(), b"same instance latest agents");
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn invalid_instructions_reject_before_input_publication_or_model_dispatch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let maximum = ContextDiscoveryLimits::default().instruction_bytes as usize;
    let provider = text();
    let mut harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider.clone()),
    )?)
    .await?;
    for (case, bytes) in [
        vec![b'x'; maximum + 1],
        vec![0xff],
        b"invalid\0instructions".to_vec(),
        b"valid instructions".to_vec(),
    ]
    .into_iter()
    .enumerate()
    {
        std::fs::write(directory.path().join("CLAUDE.md"), bytes).map_err(super::super::storage)?;
        if case == 3 {
            harness.limits.path_bytes = 64;
        }
        let operation = OperationId::new();
        let error = harness
            .submit(operation, "bounded capture")
            .await
            .err()
            .expect("invalid context was admitted");
        if case == 0 {
            assert!(
                matches!(error, Error::NativeSource { source, .. } if matches!(*source, acyclic_fs::LazyWorkspaceError::TooLarge))
            );
        } else {
            assert!(matches!(error, Error::Invalid(_)), "{error:?}");
        }
        assert!(
            harness
                .input_publisher()?
                .observe_staged_once(operation, harness.limits.file_bytes, None)?
                .await?
                .0
                .is_none()
        );
        assert!(
            harness
                .runtime
                .harness()
                .reconcile_admission(operation)
                .await?
                .is_none()
        );
        assert!(provider.requests().is_empty());
    }
    Ok(())
}
