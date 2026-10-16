use super::*;
use crate::{
    context::{CompactionPolicy, ContextManifest, DiscoveredContext},
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef},
    executor::{ExecutionEvent, ExecutionJournal},
    filesystem::{FilesystemContentVerifier, MODEL_TASK_COMMAND_KIND, ModelTaskCommand},
    model::{ModelContent, ModelRequest},
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
use proptest::prelude::*;

fn model() -> Result<Model> {
    Model::new("test".into(), "manifest".into(), "1".into(), Value::Null)
}

fn command() -> ModelTaskCommand {
    ModelTaskCommand {
        input: ModelContent::Text("use pinned context".into()),
        selected_context: None,
        max_steps: 1,
        max_output_tokens: None,
        mailbox_operation: None,
        selected_output_volume: None,
    }
}

fn reader(
    fixture: &Fixture,
) -> Result<Arc<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>>> {
    FilesystemContentVerifier::new(
        fixture.filesystem.clone(),
        &fixture.issuer.verifier(&mut None)?,
        &fixture.signed,
        65_536,
        None,
    )
}

async fn manifest(
    fixture: &Fixture,
    selected: bool,
    text: &str,
) -> Result<(FileRef, Vec<ToolDefinition>)> {
    let tools = if selected {
        vec![
            fixture
                .tools
                .get("test.deadline", Some("1"), None)?
                .unwrap()
                .definition
                .clone(),
        ]
    } else {
        Vec::new()
    };
    let manifest = ContextManifest {
        conventions: vec![text.into()],
        discovery: DiscoveredContext::default(),
        selected_skills: Vec::new(),
        tools: tools.clone(),
    };
    let publisher = FilesystemContentPublisher::new(
        fixture.filesystem.clone(),
        &fixture.volume,
        &fixture.issuer.verifier(&mut None)?,
        &fixture.signed,
        65_536,
        None,
    )?;
    let reference = manifest
        .publish(
            publisher.as_ref() as &dyn ContentPublisher,
            OperationId::from_bytes([122; 16]),
            "context/manifest.json",
            Limits::default(),
        )
        .await?;
    Ok((reference, tools))
}

fn change_live_revision(fixture: &mut Fixture, revision: u8) -> Result<()> {
    let mut tool = fixture
        .tools
        .get("test.deadline", Some("1"), None)?
        .unwrap()
        .clone();
    tool.definition.revision = revision.to_string();
    tool.definition.description = "changed live catalog".into();
    fixture.tools.register((tool, None), &mut None, None)?;
    fixture
        .tools
        .select_model_version("test.deadline", &revision.to_string())
}

async fn retained_command(
    fixture: &Fixture,
    runtime: &Runtime,
) -> Result<(TaskId, LeaseFence, WorkflowCommand, Value)> {
    let lease = fixture
        .admit_command(
            runtime,
            MODEL_TASK_COMMAND_KIND,
            serde_json::to_value(command()).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    match runtime
        .resume_task(lease, &runtime.commands(), 1, None)
        .await
    {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. }) => {}
        TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
        _ => return Err(Error::Invalid("fixture did not commit its outbox".into())),
    }
    let session = runtime.open_task(task, fence.clone()).await?;
    let mut transition = session.latest_transition(None).await?.0.unwrap();
    assert_eq!(transition.commands.len(), 1);
    let command = transition.commands.remove(0);
    let bytes = reader(fixture)?
        .read(&command.payload, 65_536, None)?
        .await?;
    let payload =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok((task, fence, command, payload))
}

// Compile registry schemas once, retaining fresh volume and stream state for
// every generated case. No task, file, or admission survives between cases.
async fn isolated_fixture(template: &Fixture) -> Result<Fixture> {
    let filesystem = FilesystemHost::new(Fs::memory(), template.volume.provider(), None)?;
    filesystem.create_volume(&template.volume).await?;
    Ok(Fixture {
        stream: Arc::new(MemoryStream::default()),
        filesystem,
        volume: template.volume.clone(),
        issuer: template.issuer.clone(),
        signed: template.signed.clone(),
        scope: template.scope.clone(),
        tasks: template.tasks.clone(),
        machines: template.machines.clone(),
        tools: template.tools.clone(),
        clock: template.clock.clone(),
        tool: template.tool.clone(),
        machine: template.machine.clone(),
    })
}

// Property: arbitrary live revisions and context text cannot change the
// selected manifest's complete tool catalog or its leading instructions.
#[test]
fn command_requests_use_manifest_not_live_catalog() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let template = runtime.block_on(Fixture::new()).unwrap();
    let config = proptest::test_runner::Config {
        source_file: Some(file!()),
        test_name: Some(concat!(
            module_path!(),
            "::command_requests_use_manifest_not_live_catalog"
        )),
        ..Default::default()
    };
    let cases = config.cases;
    assert!(cases > 0);
    let workers = std::thread::available_parallelism()
        .unwrap()
        .get()
        .min(cases as usize);
    let mut seeds = proptest::test_runner::TestRunner::new(config.clone());
    let strategy = (
        any::<bool>(),
        2u8..32,
        proptest::string::string_regex("[a-zé猫]{1,32}").unwrap(),
    )
        .sboxed();
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for index in 0..workers {
            let mut config = config.clone();
            config.max_shrink_iters = config.max_shrink_iters();
            config.cases = cases / workers as u32 + u32::from(index < cases as usize % workers);
            // Split from one root RNG, including when PROPTEST_RNG_SEED is set:
            // independent workers must not repeat the same generated cases.
            let rng = seeds.new_rng();
            let strategy = strategy.clone();
            let template = &template;
            handles.push(scope.spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let executed = std::cell::Cell::new(0u64);
                let mut runner = proptest::test_runner::TestRunner::new_with_rng(config, rng);
                runner
                    .run(&strategy, |(selected, revision, text)| {
                        executed.set(executed.get() + 1);
                        let (request, tools) = runtime.block_on(async {
                            let mut fixture = isolated_fixture(template).await?;
                            change_live_revision(&mut fixture, revision)?;
                            let runtime = fixture.runtime(None).await?;
                            let provider =
                                Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
                                    "done".into(),
                                )]));
                            let (reference, tools) = manifest(&fixture, selected, &text).await?;
                            let reader = reader(&fixture)?;
                            let commands = runtime
                                .commands()
                                .with_model(
                                    &model()?,
                                    provider.clone(),
                                    &ContextPipeline::default(),
                                    &CompactionPolicy::Disabled,
                                    None,
                                )?
                                .with_context_manifest(
                                    reference,
                                    reader.as_ref(),
                                    Limits::default(),
                                )
                                .await?;
                            let (task, fence, command, payload) =
                                retained_command(&fixture, &runtime).await?;
                            let context = runtime.task_context(task, fence.clone()).await?;
                            assert!(matches!(
                                commands.execute(context, fence, command, payload).await?,
                                TaskCommandProgress::Ready(_)
                            ));
                            let requests = provider.requests();
                            assert_eq!(requests.len(), 1);
                            Ok::<_, Error>((requests.into_iter().next().unwrap(), tools))
                        })?;
                        prop_assert_eq!(request.tools, tools);
                        prop_assert_eq!(&request.messages[0].content, &ModelContent::Text(text));
                        Ok(())
                    })
                    .unwrap();
                executed.get()
            }));
        }
        let executed: u64 = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum();
        assert!(executed >= u64::from(cases));
        eprintln!("manifest property: {executed} cases on {workers} workers");
    });
}

#[tokio::test]
async fn reopened_commands_keep_the_manifest_and_reconcile_without_resending() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    change_live_revision(&mut fixture, 2)?;
    let runtime = fixture.runtime(None).await?;
    let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
        "done".into(),
    )]));
    let (reference, tools) = manifest(&fixture, true, "retained instructions").await?;
    let reader = reader(&fixture)?;
    let (task, fence, command, payload) = retained_command(&fixture, &runtime).await?;
    let commands = runtime
        .commands()
        .with_model(
            &model()?,
            provider.clone(),
            &ContextPipeline::default(),
            &CompactionPolicy::Disabled,
            None,
        )?
        .with_context_manifest(reference.clone(), reader.as_ref(), Limits::default())
        .await?;
    let context = runtime.task_context(task, fence.clone()).await?;
    assert!(matches!(
        commands
            .execute(context, fence.clone(), command.clone(), payload.clone())
            .await?,
        TaskCommandProgress::Ready(_)
    ));
    drop(commands);
    drop(runtime);
    change_live_revision(&mut fixture, 3)?;
    let reopened = fixture.runtime(None).await?;
    let commands = reopened
        .commands()
        .with_model(
            &model()?,
            provider.clone(),
            &ContextPipeline::default(),
            &CompactionPolicy::Disabled,
            None,
        )?
        .with_context_manifest(reference, reader.as_ref(), Limits::default())
        .await?;
    let context = reopened.task_context(task, fence.clone()).await?;
    assert!(matches!(
        commands.execute(context, fence, command, payload).await?,
        TaskCommandProgress::Ready(_)
    ));
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tools, tools);
    assert_eq!(
        requests[0].messages[0].content,
        ModelContent::Text("retained instructions".into())
    );
    Ok(())
}

#[tokio::test]
async fn native_request_uses_manifest_and_original_ledger() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    change_live_revision(&mut fixture, 2)?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    let (reference, tools) = manifest(&fixture, true, "native pinned instructions").await?;
    let reader = reader(&fixture)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let runtime = fixture.runtime(Some(&io)).await?;
    let (lease, custody) = fixture
        .native_lease_command(
            &runtime,
            MODEL_TASK_COMMAND_KIND,
            serde_json::to_value(command()).map_err(|error| Error::Invalid(error.to_string()))?,
            &work,
        )
        .await?;
    let original = lease.clone();
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let provider = Arc::new(PendingModel {
        clock: fixture.clock.clone(),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let commands = runtime
        .commands()
        .with_model(
            &model()?,
            provider.clone(),
            &ContextPipeline::default(),
            &CompactionPolicy::Disabled,
            None,
        )?
        .with_context_manifest(reference, reader.as_ref(), Limits::default())
        .await?;
    let (attempt, custody) = runtime
        .resume_task_with_native_work(lease, custody, &commands, 2, &work)
        .await?;
    match attempt {
        TaskWorkerAttempt::Unresolved {
            lease,
            error: Error::Indeterminate(_),
        } => assert_eq!(lease, original),
        TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
        _ => {
            return Err(Error::Invalid(
                "native pending provider unexpectedly completed".into(),
            ));
        }
    }
    assert_eq!(provider.generated.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(custody.work().shares_ledger(&work));
    // Inspecting the retained request is a separate readonly query, with its own ledger.
    let inspection =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&inspection)?;
    let (journal, operation) = runtime
        .readonly_execution_journal_with_native_work(
            task,
            OperationId::from_bytes([105; 16]),
            &inspection,
        )
        .await?;
    let records = journal.replay(operation, 0, 64, Some(&io))?.await?;
    let reference = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted { request, .. } => Some(request),
            _ => None,
        })
        .expect("native model request must be retained");
    let bytes = journal.load(reference, Some(&io))?.await?;
    let request: ModelRequest =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(request.tools, tools);
    assert_eq!(
        request.messages[0].content,
        ModelContent::Text("native pinned instructions".into())
    );
    Ok(())
}

#[tokio::test]
async fn rejects_unbound_duplicate_and_funded_manifest_replacement() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let (reference, _) = manifest(&fixture, true, "pinned").await?;
    let reader = reader(&fixture)?;
    let provider = Arc::new(ScriptedModelProvider::new([]));
    assert!(matches!(
        runtime
            .commands()
            .with_context_manifest(reference.clone(), reader.as_ref(), Limits::default())
            .await,
        Err(Error::Unsupported(_))
    ));
    let commands = runtime
        .commands()
        .with_model(
            &model()?,
            provider.clone(),
            &ContextPipeline::default(),
            &CompactionPolicy::Disabled,
            None,
        )?
        .with_context_manifest(reference.clone(), reader.as_ref(), Limits::default())
        .await?;
    assert!(matches!(
        commands
            .with_context_manifest(reference.clone(), reader.as_ref(), Limits::default())
            .await,
        Err(Error::Conflict(_))
    ));
    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let commands = runtime.commands().with_model(
        &model()?,
        provider.clone(),
        &ContextPipeline::default(),
        &CompactionPolicy::Disabled,
        Some(&io),
    )?;
    let before = work.receipt();
    assert!(matches!(
        commands
            .with_context_manifest(reference, reader.as_ref(), Limits::default())
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert!(work.receipt().backend_read_operations == before.backend_read_operations);
    assert!(provider.requests().is_empty());
    Ok(())
}
