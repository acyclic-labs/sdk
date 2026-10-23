use super::*;
use crate::context::{
    ContextDiscovery, ContextDiscoveryLimits, ContextDiscoveryPolicy, ContextRoot, InstructionScope,
};
use crate::filesystem::WorkspaceMutation;

const SOURCES: [(&str, &str); 5] = [
    ("AGENTS.md", "ORIGINAL AGENTS"),
    ("CLAUDE.md", "ORIGINAL CLAUDE"),
    (
        "skills/inspect/SKILL.md",
        "---\nname: inspect\ndescription: Inspect sources\n---\nORIGINAL SKILL",
    ),
    (
        ".claude/settings.json",
        r#"{"hooks":{"PreToolUse":[{"matcher":"Bash","hooks":[{"type":"command","command":"original-guard"}]}]}}"#,
    ),
    (
        ".mcp.json",
        r#"{"mcpServers":{"original":{"command":"original-server"}}}"#,
    ),
];

pub(super) async fn publish_sources(fixture: &Fixture) -> Result<FileRef> {
    let publisher = FilesystemContentPublisher::new(
        fixture.filesystem.clone(),
        &fixture.volume,
        &fixture.issuer.verifier(&mut None)?,
        &fixture.signed,
        65_536,
        None,
    )?;
    let mut sources = Vec::new();
    for (index, (path, text)) in SOURCES.iter().enumerate() {
        let (file, _) = ContentPublisher::stage(
            publisher.as_ref(),
            OperationId::from_bytes([130 + index as u8; 16]),
            path,
            text.as_bytes(),
            if path.ends_with(".json") {
                "application/json"
            } else {
                "text/markdown"
            },
            path.rsplit('/').next().unwrap(),
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
        if path.ends_with(".json") {
            sources.push(file);
        }
    }
    let root = ContextRoot {
        volume: fixture.volume.clone(),
        directory: String::new(),
    };
    let discovery = ContextDiscovery {
        instructions: vec![InstructionScope {
            root: root.clone(),
            active_directory: String::new(),
        }],
        skills: vec![ContextRoot {
            directory: "skills".into(),
            ..root
        }],
        policy: ContextDiscoveryPolicy::default(),
        limits: ContextDiscoveryLimits::default(),
    }
    .capture(reader(fixture)?.as_ref())
    .await?;
    let mut manifest = ContextManifest::new(discovery, vec!["inspect".into()], &fixture.tools)?;
    manifest.sources = sources;
    manifest
        .publish(
            publisher.as_ref(),
            OperationId::from_bytes([141; 16]),
            "context/manifest.json",
            Limits::default(),
        )
        .await
}

pub(super) async fn mutate_sources(fixture: &Fixture, delete: bool) -> Result<()> {
    let workspace = crate::filesystem::fork_preparer_io::content_workspace(
        fixture.volume.provider(),
        &fixture.volume,
        None,
    )?;
    let mutations: Vec<_> = SOURCES
        .iter()
        .map(|(path, _)| {
            if delete {
                WorkspaceMutation::Remove {
                    path: format!("/{path}"),
                }
            } else {
                WorkspaceMutation::PutFile {
                    path: format!("/{path}"),
                    bytes: acyclic_stream::OwnedBytes::from(b"REPLACED".to_vec()),
                }
            }
        })
        .chain([WorkspaceMutation::PutFile {
            // A new current manifest must never replace the admitted file revision.
            path: "/context/manifest.json".into(),
            bytes: acyclic_stream::OwnedBytes::from(b"{}".to_vec()),
        }])
        .collect();
    fixture
        .filesystem
        .apply(
            &workspace,
            None,
            &mutations,
            &IdempotencyKey::new(format!("mutate-{delete}"))?,
        )
        .await?;
    Ok(())
}

async fn pending_command(
    fixture: &Fixture,
    runtime: &Runtime,
    task: TaskId,
    fence: &LeaseFence,
) -> Result<(WorkflowCommand, Value)> {
    let session = runtime.open_task(task, fence.clone()).await?;
    let mut transition = session.latest_transition(None).await?.0.unwrap();
    assert_eq!(transition.commands.len(), 1);
    let command = transition.commands.remove(0);
    let bytes = reader(fixture)?
        .read(&command.payload, 65_536, None, None)?
        .await?;
    Ok((command, serde_json::from_slice(&bytes).unwrap()))
}

async fn recover_after_source_changes(delete: bool) -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    // Admission stores the manifest's exact path, version and digest in the
    // command payload, before the first workflow transition or model dispatch.
    let lease = fixture
        .admit_command(
            &runtime,
            MODEL_TASK_COMMAND_KIND,
            serde_json::to_value(command(publish_sources(&fixture).await?)).unwrap(),
        )
        .await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    drop(runtime);
    // No manifest, source references, command payload or command host survives.
    mutate_sources(&fixture, delete).await?;
    change_live_revision(&mut fixture, 2)?;
    let runtime = fixture.runtime(None).await?;
    let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
        "done".into(),
    )]));
    let commands = runtime.commands().with_model(
        &model()?,
        provider.clone(),
        &CompactionPolicy::Disabled,
        None,
    )?;
    assert!(provider.requests().is_empty());
    match runtime.resume_task(lease, &commands, 1, None).await {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. }) => {}
        TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
        _ => {
            return Err(Error::Invalid(
                "fixture did not retain recovered outbox".into(),
            ));
        }
    }
    let (command, payload) = pending_command(&fixture, &runtime, task, &fence).await?;
    assert!(matches!(
        commands
            .execute(
                runtime.task_context(task, fence.clone()).await?,
                fence.clone(),
                command,
                payload,
            )
            .await?,
        TaskCommandProgress::Ready(_)
    ));
    // Simulate the next crash before the workflow records this command result.
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tools[0].revision, "1");
    let mut texts = Vec::new();
    for message in &requests[0].messages {
        match &message.content {
            ModelContent::Part(crate::model::ModelContentPart::File { file, .. }) => {
                texts.push(
                    String::from_utf8(
                        reader(&fixture)?
                            .read(file, 65_536, None, None)?
                            .await?
                            .to_vec(),
                    )
                    .unwrap(),
                );
            }
            ModelContent::Text(text) => texts.push(text.clone()),
            _ => {}
        }
    }
    assert!(texts.iter().any(|text| text == "ORIGINAL AGENTS"));
    assert!(texts.iter().any(|text| text == "ORIGINAL CLAUDE"));
    assert!(texts.iter().any(|text| text.contains("ORIGINAL SKILL")));
    assert!(texts.iter().all(|text| !text.contains("REPLACED")
        && !text.contains("original-guard")
        && !text.contains("original-server")));
    let session = runtime.open_task(task, fence.clone()).await?;
    let transition = session.latest_transition(None).await?.0.unwrap();
    let bytes = reader(&fixture)?
        .read(&transition.commands[0].payload, 65_536, None, None)?
        .await?;
    let retained: ModelTaskCommand = serde_json::from_slice(&bytes).unwrap();
    let manifest_file = retained.context_manifest;
    assert_eq!(manifest_file.path(), "context/manifest.json");
    let bytes = reader(&fixture)?
        .read(&manifest_file, 65_536, None, None)?
        .await?;
    manifest_file.descriptor().verify(&bytes, &mut None)?;
    let manifest = ContextManifest::decode(&bytes, Limits::default())?;
    for source in manifest.sources {
        let expected = SOURCES
            .iter()
            .find(|(path, _)| *path == source.path())
            .unwrap()
            .1;
        assert_eq!(
            reader(&fixture)?
                .read(&source, 65_536, None, None)?
                .await?
                .as_ref(),
            expected.as_bytes()
        );
    }
    drop(commands);
    drop(runtime);
    change_live_revision(&mut fixture, 3)?;
    let runtime = fixture.runtime(None).await?;
    let commands = runtime.commands().with_model(
        &model()?,
        provider.clone(),
        &CompactionPolicy::Disabled,
        None,
    )?;
    let (command, payload) = pending_command(&fixture, &runtime, task, &fence).await?;
    assert!(matches!(
        commands
            .execute(
                runtime.task_context(task, fence.clone()).await?,
                fence,
                command,
                payload,
            )
            .await?,
        TaskCommandProgress::Ready(_)
    ));
    assert_eq!(
        provider.requests(),
        requests,
        "replay must not resend or use current context"
    );
    Ok(())
}

#[tokio::test]
async fn crash_after_admission_recovers_edited_sources() -> Result<()> {
    recover_after_source_changes(false).await
}

#[tokio::test]
async fn crash_after_admission_recovers_deleted_sources() -> Result<()> {
    recover_after_source_changes(true).await
}
