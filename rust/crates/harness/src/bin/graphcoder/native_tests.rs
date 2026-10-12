use super::*;
use acyclic_harness::{AgentId, Capabilities, Outcome,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner}, core::AggregateKind};

async fn composition(root: &std::path::Path) -> Result<Config> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    #[cfg(feature = "native-execution")]
    acyclic_fs::initialize_native_mount_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("terminal-native-regression", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("terminal-native-regression", "stream", "2")?;
    let agent = AgentId::from_bytes([21; 16]);
    let volume = VolumeRef::new(provider.clone(), "private", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
    let authority = Authority { kind: AggregateKind::Task, id: "terminal-native-owner".into() };
    let issuer = AuthorityIssuer::new("terminal-native-regression", [22; 32], authority.clone());
    let scope = issuer.root_for_agent(agent, "owner", Capabilities::new([
        "operation:declare".to_owned(), "operation:observe".to_owned(),
        "operation:cancel".to_owned(), "operation:wake".to_owned(),
        "model:generate".to_owned(), "task:spawn:acyclic.stock_turn@1".to_owned(),
        volume.capability(VolumeOperation::Read)?, volume.capability(VolumeOperation::Write)?,
    ]));
    let filesystem_root = root.join("filesystem");
    let files = FilesystemHost::new(
        acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&filesystem_root)).await
            .map_err(|error| Error::Storage(error.to_string()))?, provider.clone(),
    )?;
    files.create_volume(&volume).await?;
    drop(files);
    Ok(Config {
        filesystem_root, stream_root: root.join("stream"), filesystem_provider: provider,
        stream_provider, private_volume: volume,
        owner: AuthorityBinding { authority, issuer: "terminal-native-regression".into(), key: [22; 32], scope },
        observed_authorities: vec![], generation: "terminal-native-generation-1".into(),
        model_invocation_ledger: root.join("model-invocations.jsonl"),
        limits: Limits::default(), run_limits: TaskRunLimits::default(),
        session_limits: SessionLimits { active_tasks: 4, total_tasks: 4, depth: 3, model_steps: 4 },
        concurrency: 4, deterministic_delay_ms: 0,
        fork_sources: vec![],
        tools: Default::default(),
        native_publications: vec![],
        project_source_root: None,
    })
}

#[tokio::test]
async fn prompt_reference_retains_exact_publication_across_reopen_and_rejects_rebinding() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let config = composition(directory.path()).await?;
    let session = NativeSession::open(config.clone()).await?;
    let command_operation = OperationId::from_bytes([66; 16]);
    let original = session.prompt_command("retained original prompt", command_operation).await?;
    assert_eq!(original.operation_id, command_operation);
    assert_eq!(original.kind, acyclic_harness::filesystem::MODEL_TASK_COMMAND_KIND);
    assert!(original.payload.descriptor().byte_length() <= MAX_FRAME_BYTES as u64);
    let repeated = session.prompt_command("retained original prompt", command_operation).await?;
    assert_eq!(original.payload, repeated.payload);
    assert!(session.prompt_command("different prompt at same operation", command_operation).await.is_err());
    assert!(matches!(session.prompt_command(
        &"x".repeat(MAX_FRAME_BYTES + 1), OperationId::from_bytes([67; 16]),
    ).await, Err(Error::Invalid(_))));
    drop(session);
    let session = NativeSession::open(config).await?;
    let reopened = session.prompt_command("retained original prompt", command_operation).await?;
    assert_eq!(original.payload, reopened.payload);
    let task = match session.queue(OperationId::from_bytes([68; 16]), original, None).await? {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => task_id,
        Admission::Accepted(RuntimeTask::Live(_)) => return Err(Error::Conflict("native task was not durable".into())),
        Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
        Admission::Indeterminate { operation_id } => return Err(Error::Indeterminate(operation_id)),
    };
    let worker = Worker {
        id: "terminal-reference-worker".into(), available: Default::default(),
        labels: Default::default(),
    };
    let WorkPull::Claimed(lease) = session.claim(task, &worker).await? else {
        return Err(Error::Conflict("queued task was not claimed".into()));
    };
    session.resume(lease).await?;
    assert!(matches!(session.runtime.task_host().outcome(task).await?, Some(Outcome::Succeeded(_))));
    Ok(())
}
