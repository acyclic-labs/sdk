use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::{AgentId, Capabilities, Outcome,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner}, core::AggregateKind};

async fn composition(root: &std::path::Path) -> Result<Config> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    #[cfg(feature = "native-execution")]
    acyclic_fs::initialize_native_mount_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("terminal-native-regression".into(), "filesystem".into(), "2".into(), &mut None)?;
    let stream_provider = ProviderRef::new("terminal-native-regression".into(), "stream".into(), "2".into(), &mut None)?;
    let agent = AgentId::from_bytes([21; 16]);
    let volume = VolumeRef::new(provider.clone(), "private".into(), VolumeClass::AgentPrivate, VolumeOwner::Agent(agent), &mut None)?;
    let authority = Authority { kind: AggregateKind::Task, id: "terminal-native-owner".into() };
    let issuer = AuthorityIssuer::new("terminal-native-regression", [22; 32], authority.clone());
    let scope = issuer.issue("owner", &Capabilities::new([
        "operation:declare".to_owned(), "operation:observe".to_owned(),
        "operation:cancel".to_owned(), "operation:wake".to_owned(),
        "model:generate".to_owned(), "task:spawn:acyclic.stock_turn@1".to_owned(),
        volume.capability(VolumeOperation::Read, &mut None)?, volume.capability(VolumeOperation::Write, &mut None)?,
    ]), Some(agent), None, &mut None).expect("scope proof");
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
        tools: Default::default(), context: Default::default(),
        native_publications: vec![],
        project_source_root: None,
    })
}

#[tokio::test]
async fn prompt_reference_retains_exact_publication_across_reopen_and_rejects_rebinding() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let mut config = composition(directory.path()).await?;
    let role = ContextAttribute::typed("graphcoder.role".into(), "1".into(),
        serde_json::json!({"type":"string"}), "reviewer-1".into(), &"reviewer")?;
    config.context.attributes.push(role);
    let input = acyclic_harness::context::ContextInput {
        input: ModelContent::Text("review".into()), selected_context: None,
        step: 0, prior_messages: vec![],
    };
    let initial = config.context.pipeline()?.run(&input).await?;
    assert!(matches!(initial.messages.first().map(|message| &message.content),
        Some(ModelContent::Text(text)) if text.contains("reviewer-1") && text.contains("reviewer")));
    let mut update = config.context.clone();
    update.attributes[0].state_revision = "implementor-2".into();
    update.attributes[0].value = serde_json::json!("implementor");
    update.attribute_mode = Some(ContextRenderMode::Update);
    let changed = update.pipeline()?.run(&input).await?;
    assert!(matches!(changed.messages.last().map(|message| &message.content),
        Some(ModelContent::Text(text)) if text.starts_with("Update:") && text.contains("implementor")));
    assert_ne!(initial, changed);
    let session = NativeSession::open(config.clone()).await?;
    session.configure_context(update).await?;
    let retained = session.config().context;
    let mut invalid = retained.clone();
    invalid.attributes[0].value = serde_json::json!(7);
    assert!(session.configure_context(invalid).await.is_err());
    assert_eq!(session.config().context.attributes, retained.attributes);
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
    config = session.config();
    drop(session);
    let session = NativeSession::open(config).await?;
    assert_eq!(session.config().context.attributes, retained.attributes);
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
    session.resume(lease.clone()).await?;
    use acyclic_harness::executor::{ExecutionEvent, ExecutionJournal as _};
    use acyclic_harness::conversation::ContentResidencyVerifier as _;
    let invocations = std::fs::read_to_string(&session.config.model_invocation_ledger)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let row: serde_json::Value = serde_json::from_str(invocations.lines().next()
        .ok_or_else(|| Error::NotFound("model invocation".into()))?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let execution: OperationId = serde_json::from_value(row["operation_id"].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let owner = session.runtime.task_host().journal_owner(task,
        LeaseFence::from(&lease.reservation)).await?;
    let journal = acyclic_harness::filesystem::FilesystemExecutionJournal::for_task(owner,
        execution, command_operation, session.files.clone(), &session.config.private_volume,
        &session.config.owner.verifier()?, &session.config.owner.scope, MAX_FRAME_BYTES as u64, None)?;
    let records = journal.replay(execution, 0, 16).await?;
    let request = records.into_iter().find_map(|record| match record.event {
        ExecutionEvent::ModelStarted { request, .. } => Some(request), _ => None,
    }).ok_or_else(|| Error::NotFound("admitted model request".into()))?;
    let bytes = journal.read(&request).await?;
    let actual = std::str::from_utf8(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(actual.contains("implementor-2") && actual.contains("implementor"));
    assert!(!actual.contains("reviewer-1"));
    assert!(matches!(session.runtime.task_host().outcome(task).await?, Some(Outcome::Succeeded(_))));
    Ok(())
}
