use super::*;
use acyclic_harness_core::{
    Admission, AgentId, Capabilities, OperationId,
    context::{CompactionPolicy, ContextManifest},
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{SchedulerPayloadStore, WorkPull, Worker},
    durable_tool::ResumableToolRegistry,
    filesystem::{
        FilesystemContentPublisher, FilesystemHost, FilesystemTaskRuntime, MODEL_TASK_COMMAND_KIND,
        ModelTaskCommand, PreparationIo, TaskWorkerAttempt, TaskWorkerOutcome,
    },
    resources::ProviderRef,
    runtime::{RuntimeScope, TaskDefinition, TaskRegistry},
    scheduler::{ResourceRequest, SessionLimits},
    store::{HistoryIo, HistoryWork},
    tool::ToolRegistry,
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
        WorkflowCommand,
    },
};
use acyclic_stream::{MemoryStream, UnixMillisClock};

struct Clock;
impl UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1
    }
}

struct OneCommand {
    identity: MachineIdentity,
    schema: serde_json::Value,
    command: WorkflowCommand,
}
impl ResumableMachine for OneCommand {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &serde_json::Value {
        &self.schema
    }
    fn initialize(&self, input: &serde_json::Value) -> Result<serde_json::Value> {
        let _ = input;
        Ok(serde_json::Value::Bool(false))
    }
    fn transition(
        &self,
        _: &serde_json::Value,
        _: &serde_json::Value,
    ) -> Result<MachineTransition> {
        panic!("stock native fixture must use original-ledger machine callbacks")
    }
    fn supports_native_work(&self) -> bool {
        true
    }
    fn identity_with_native_work<'a>(
        &'a self,
        work: &NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        work.remaining()?;
        Ok(&self.identity)
    }
    fn initialize_with_native_work(
        &self,
        input: &serde_json::Value,
        work: &NativeMaterializationWork,
    ) -> Result<(serde_json::Value, acyclic_fs::NativeAllocation)> {
        let _ = input;
        Ok((
            serde_json::Value::Bool(false),
            work.claim(
                std::mem::size_of::<serde_json::Value>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?,
        ))
    }
    fn transition_with_native_work(
        &self,
        state: &serde_json::Value,
        input: serde_json::Value,
        work: &NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        drop(input);
        let mut history = HistoryWork::new(PreparationIo::from_native_work(work)?);
        history.retain_owned(std::mem::size_of::<MachineTransition>() as u64)?;
        let transition = if state == &serde_json::Value::Bool(true) {
            MachineTransition {
                state: serde_json::Value::Bool(true),
                commands: Vec::new(),
                status: MachineStatus::Completed {
                    value: serde_json::Value::Null,
                },
            }
        } else {
            let slot = std::mem::size_of::<WorkflowCommand>() as u64;
            history.retain_owned(slot)?;
            history.allocation_attempt(slot)?;
            let mut commands = Vec::new();
            commands.try_reserve_exact(1)?;
            if commands.capacity() > 1 {
                history.retain_owned((commands.capacity() as u64 - 1) * slot)?;
            }
            let kind = &self.command.kind;
            history.retain_owned(kind.len() as u64)?;
            history.allocation_attempt(kind.len() as u64)?;
            work.charge(acyclic_fs::WorkCounters {
                bytes_copied: kind.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED
            })?;
            let kind = kind.to_owned();
            let payload = self.command.payload.copy(&mut Some(&mut history))?;
            commands.push(WorkflowCommand {
                operation_id: self.command.operation_id,
                kind,
                payload,
            });
            MachineTransition {
                state: serde_json::Value::Bool(true),
                commands,
                status: MachineStatus::Suspended,
            }
        };
        let custody = history.into_custody()?;
        Ok((transition, custody))
    }
}

#[tokio::test]
async fn every_adapter_runs_a_stock_turn_on_the_issued_native_task() -> Result<()> {
    for (adapter, response) in [
        (
            Adapter::Chat,
            "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n",
        ),
        (
            Adapter::Anthropic,
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":5}}}\n\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
        ),
        (
            Adapter::Responses,
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        ),
        (
            Adapter::Gemini,
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"done\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":1}}\n\n",
        ),
    ] {
        let transport = Arc::new(Fixture::new(response));
        let provider = if adapter == Adapter::Responses {
            let mut registry = Registry::with_transport(
                Credentials::explicit(BTreeMap::from([(
                    "openai".into(),
                    "fixture-api-key".into(),
                )])),
                transport.clone(),
            );
            let catalog = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog"))?;
            std::fs::write(catalog.path(), include_str!("fixtures/luna-catalog.json"))
                .map_err(|_| invalid("fixture catalog"))?;
            registry.load_catalog(catalog.path())?;
            let selected = registry.resolve("openai/gpt-6-luna")?;
            assert_eq!(
                selected.context_capacity(selected.model())?.context_tokens,
                8192
            );
            selected
        } else {
            let (mut provider, _) = provider(adapter, 200);
            provider.transport =
                crate::deadline::Bounded::new(transport.clone(), crate::TimeoutSpec::DEFAULT);
            provider
        };
        let outcome = run_stock_turn(
            provider,
            adapter,
            CompactionPolicy::Disabled,
            acyclic_fs::Fs::memory(),
        )
        .await?;
        assert!(
            matches!(outcome, acyclic_harness_core::Outcome::Succeeded(_)),
            "{adapter:?}: {outcome:?}"
        );
        assert_eq!(transport.sent.load(Ordering::SeqCst), 1, "{adapter:?}");
    }
    Ok(())
}

pub(crate) async fn run_stock_turn<O: acyclic_fs::AsyncObjectStore + 'static>(
    provider: crate::ResolvedModel,
    adapter: Adapter,
    policy: CompactionPolicy,
    filesystem: acyclic_fs::Fs<acyclic_fs::MemoryAuthorityBackend, O>,
) -> Result<acyclic_harness_core::Outcome<serde_json::Value>> {
    let model = provider.model().clone();
    let stages = Arc::new(probe::Stages::default());
    let provider = Arc::new(probe::Probe {
        provider,
        stages: stages.clone(),
    });
    let storage_provider =
        ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &storage_provider, None)?;
    let agent = AgentId::from_bytes([2; 16]);
    let volume = VolumeRef::new(
        storage_provider,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "fixture",
        [47; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        },
    );
    let signed = issuer.issue(
        "owner",
        &Capabilities::new([
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "task:spawn:model@1".into(),
            "model:generate".into(),
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?;
    let work = work(&CancellationToken::new())?;
    let io = PreparationIo::from_native_work(&work)?;
    let operation = OperationId::from_bytes([1; 16]);
    let payloads = FilesystemContentPublisher::new(
        host.clone(),
        &volume,
        &issuer.verifier(&mut None)?,
        &signed,
        65_536,
        None,
    )?;
    let command = ModelTaskCommand {
        summary_source: None,
        context_manifest: ContextManifest::default()
            .publish(
                payloads.as_ref(),
                OperationId::from_bytes([10; 16]),
                "manifest.json",
                Limits::default(),
            )
            .await?,
        input: ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: 1,
        max_output_tokens: None,
        selected_output_volume: None,
        mailbox_operation: None,
    };
    let payload = payloads
        .stage(
            operation,
            "command",
            &serde_json::to_vec(&command).expect("command fixture"),
            None,
        )?
        .await?;
    let machine = Arc::new(OneCommand {
        identity: MachineIdentity {
            name: "model".into(),
            version: "1".into(),
            digest: [48; 32],
        },
        schema: json!({"type":"boolean"}),
        command: WorkflowCommand {
            operation_id: OperationId::from_bytes([3; 16]),
            kind: MODEL_TASK_COMMAND_KIND.into(),
            payload,
        },
    });
    let mut tasks = TaskRegistry::default();
    tasks.register(
        TaskDefinition::<serde_json::Value, serde_json::Value>::resumable(
            machine.clone(),
            json!({"type":"boolean"}),
            json!({"type":"null"}),
        )?
        .publish(&work)?,
    )?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let runtime = FilesystemTaskRuntime::open(
        Arc::new(MemoryStream::default()),
        host.clone(),
        &volume,
        // Callers may arm threshold compaction, whose summaries need the
        // private volume.
        Some(&volume),
        &issuer.verifier(&mut None)?,
        &signed,
        &scope,
        &tasks,
        &machines,
        &ToolRegistry::default(),
        SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 1,
            model_steps: 1,
        },
        1,
        65_536,
        Arc::new(Clock),
        None,
        &[],
        &ResumableToolRegistry::default(),
        Some(&io),
    )
    .await?;
    let opened = work.receipt().items_examined;
    let definition =
        tasks.get_version::<serde_json::Value, serde_json::Value>("model", "1", None)?;
    assert!(matches!(
        runtime
            .harness()
            .admit(operation, &definition, serde_json::Value::Bool(false), None)
            .await?,
        Admission::Accepted(_)
    ));
    let admitted = work.receipt().items_examined;
    let mut history = HistoryWork::new(PreparationIo::from_native_work(&work)?);
    history.retain_owned(
        std::mem::size_of::<WorkPull>().max(std::mem::size_of::<TaskWorkerAttempt>()) as u64,
    )?;
    let pulled = runtime
        .task_host()
        .pull_work(
            &Worker {
                id: "fixture".into(),
                available: ResourceRequest::default(),
                labels: BTreeMap::new(),
            },
            Some(operation),
            &mut Some(&mut history),
        )
        .await?;
    let WorkPull::Claimed(lease) = pulled else {
        panic!("native lease")
    };
    let recovery = history.into_custody()?;
    let commands = runtime
        .commands()
        .with_model(&model, provider, &policy, Some(&io))?;
    let leased = work.receipt().items_examined;
    let result = runtime
        .resume_task_with_native_work(lease, recovery, &commands, 2, &work)
        .await;
    eprintln!(
        "{adapter:?} items: open={opened}, admission={}, lease={}, run={}, provider={stages:?}",
        admitted - opened,
        leased - admitted,
        work.receipt().items_examined - leased
    );
    let (attempt, custody) = result?;
    if let TaskWorkerAttempt::Unresolved { error, .. } = attempt {
        return Err(error);
    }
    let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task }) = attempt else {
        panic!("{adapter:?}: the stock turn did not reach a terminal outcome");
    };
    use acyclic_harness_core::runtime::TaskStateProvider as _;
    let outcome = runtime
        .task_host()
        .outcome(task)
        .await?
        .expect("a completed task has an outcome");
    assert!(custody.work().shares_allocation_context(&work));
    eprintln!(
        "{adapter:?} total_items={} remaining_items={}",
        work.receipt().items_examined,
        work.remaining()?.items_examined,
    );
    Ok(outcome)
}
