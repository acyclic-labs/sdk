//! Real task admission and portable file-result replay under attenuated authority.
//! Reopens composition over retained memory providers; process restart, task-owned
//! lease journal, cancellation and native/browser execution are separate gates.
#![cfg(feature = "filesystem")]

use acyclic_fs::Fs;
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, InteractionId, OperationId, Outcome, Result, TaskId,
    conversation::{
        ContentPublisher, FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    durable_tool::DurableToolRunner,
    executor::{ExecutionEvent, ExecutionJournal, ExecutionRecord},
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
        FilesystemHost, FilesystemTaskRuntime,
    },
    interaction::{Interaction, InteractionOutcome},
    resources::ProviderRef,
    runtime::{
        AgentHarness, ContentBindings, RuntimeScope, TaskDefinition, TaskRegistry, ToolContext,
    },
    scheduler::SessionLimits,
    tool::{
        ToolExecutor, ToolInvocation, ToolRegistry, ToolResult, files,
        schema::ProjectionMode,
        text::{ReadOptions, SearchOptions},
        text_files,
    },
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamClient};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct AdmissionMachine {
    identity: MachineIdentity,
    schema: Value,
}

impl ResumableMachine for AdmissionMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, _: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Completed {
                value: state.clone(),
            },
        })
    }
}

// Instrument the actual factory executor; this wrapper supplies no file behavior.
struct FileExecutorSpy {
    inner: Arc<dyn ToolExecutor>,
    executions: AtomicUsize,
    reconciliations: AtomicUsize,
}

impl ToolExecutor for FileExecutorSpy {
    fn authorize(&self, scope: Option<&RuntimeScope>, invocation: &ToolInvocation) -> Result<()> {
        self.inner.authorize(scope, invocation)
    }
    fn authorize_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        self.inner.authorize_with_context(context, invocation)
    }
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.inner.execute(invocation)
    }
    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.inner.execute_with_context(context, invocation)
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        self.inner.reconcile(invocation)
    }
    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        self.inner.reconcile_with_context(context, invocation)
    }
}

struct JournalSpy {
    inner: Arc<dyn ExecutionJournal>,
    reads: AtomicUsize,
    writes: AtomicUsize,
}

impl ExecutionJournal for JournalSpy {
    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<Vec<ExecutionRecord>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.replay(operation, after, maximum)
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: String,
        event: ExecutionEvent,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.append(operation, key, event)
    }
    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: String,
        event: ExecutionEvent,
    ) -> BoxProviderFuture<'a, Result<bool>> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.append_if_tail(operation, tail, key, event)
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.stage(operation, key, bytes, media_type)
    }
    fn load<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<Vec<u8>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.load(file)
    }
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.open_interaction(id, interaction)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxProviderFuture<'a, Result<Option<InteractionOutcome>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.interaction_outcome(id)
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one retained admission and replay scenario keeps pre-I/O negative controls with its positive result"
)]
async fn portable_read_replay_checks_original_source_authority_before_journal_io() -> Result<()> {
    let provider = ProviderRef::new("file-replay", "filesystem", "2")?;
    let filesystem = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::from_bytes([81; 16]);
    let volume = VolumeRef::new(
        provider,
        "private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    filesystem.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "file-replay",
        [82; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "file-replay".into(),
        },
    );
    let read_grant = volume.capability(VolumeOperation::Read)?;
    let signed = issuer.root_for_agent(
        agent,
        "owner",
        Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "task:spawn:fixture.file_replay@1".to_owned(),
            "tool:call:acyclic.read_file".to_owned(),
            "tool:call:acyclic.read_file_range".to_owned(),
            "tool:call:acyclic.search_file".to_owned(),
            "tool:call:acyclic.write_file".to_owned(),
            "tool:call:acyclic.edit_file".to_owned(),
            "tool:call:acyclic.patch_file".to_owned(),
            read_grant.clone(),
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?;
    let writer = Arc::new(FilesystemContentPublisher::new(
        filesystem.clone(),
        volume.clone(),
        &issuer.verifier(),
        &signed,
        65_536,
    )?);
    let text = "original\r\n🦀 exact bytes\r\n";
    let source = writer
        .stage(
            OperationId::new(),
            "source.txt",
            text.as_bytes(),
            "text/plain",
            "source.txt",
        )
        .await?;
    let mut tool = files::read_file()?;
    let executor = Arc::new(FileExecutorSpy {
        inner: tool.executor.clone(),
        executions: AtomicUsize::new(0),
        reconciliations: AtomicUsize::new(0),
    });
    tool.executor = executor.clone();
    let definition = tool.definition.clone();
    let mut tools = ToolRegistry::new();
    tools.register(tool)?;
    let mutations = [
        (
            files::write_file()?,
            json!({"path":"destination.txt","text":"body","media_type":"text/plain","display_name":"destination.txt"}),
        ),
        (
            files::edit_file()?,
            json!({"file":source,"old_text":"original","new_text":"changed"}),
        ),
        (
            files::patch_file(4096, 16)?,
            json!({"file":source,"diff":"@@\n-original\n+changed\n"}),
        ),
    ];
    let write_executor = Arc::new(FileExecutorSpy {
        inner: mutations[0].0.executor.clone(),
        executions: AtomicUsize::new(0),
        reconciliations: AtomicUsize::new(0),
    });
    for (index, (tool, _)) in mutations.iter().enumerate() {
        let mut tool = tool.clone();
        if index == 0 {
            tool.executor = write_executor.clone();
        }
        tools.register(tool)?;
    }
    let mut bounded_text = [
        (
            text_files::read_file_range(
                ReadOptions {
                    maximum_input_bytes: 4096,
                    maximum_text_bytes: 32,
                },
                4096,
                ProjectionMode::Reference,
            )?,
            json!({"file":source,"range":{"start":0,"end":8}}),
        ),
        (
            text_files::search_file(
                SearchOptions {
                    maximum_input_bytes: 4096,
                    maximum_query_bytes: 16,
                    maximum_work: 4096,
                    maximum_matches: 2,
                },
                4096,
                ProjectionMode::Full,
            )?,
            json!({"file":source,"query":"exact"}),
        ),
    ];
    let mut bounded_executors = Vec::new();
    for (tool, _) in &mut bounded_text {
        let executor = Arc::new(FileExecutorSpy {
            inner: tool.executor.clone(),
            executions: AtomicUsize::new(0),
            reconciliations: AtomicUsize::new(0),
        });
        tool.executor = executor.clone();
        tools.register(tool.clone())?;
        bounded_executors.push(executor);
    }
    let mut retained_text_results = Vec::new();
    let machine: Arc<dyn ResumableMachine> = Arc::new(AdmissionMachine {
        identity: MachineIdentity {
            name: "fixture.file_replay".into(),
            version: "1".into(),
            digest: [83; 32],
        },
        schema: json!({"type":"integer"}),
    });
    let task_definition = TaskDefinition::<u64, u64>::resumable(
        machine.clone(),
        json!({"type":"integer"}),
        json!({"type":"integer"}),
    )?;
    let mut tasks = TaskRegistry::default();
    tasks.register(task_definition)?;
    let task_definition = tasks.get_version::<u64, u64>("fixture.file_replay", "1")?;
    let mut machines = MachineRegistry::default();
    machines.register(machine)?;
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let admission = OperationId::from_bytes([84; 16]);
    let task = TaskId::from_bytes(admission.into_bytes());
    let call = OperationId::from_bytes([85; 16]);
    let arguments = json!({"file":source});

    for reopened in [false, true] {
        // This reopens the real task host and reconstructs scope from its retained
        // admission. There is no test TaskStateProvider or fabricated resumed scope.
        let runtime = FilesystemTaskRuntime::open(
            stream.clone(),
            filesystem.clone(),
            volume.clone(),
            issuer.verifier(),
            signed.clone(),
            scope.clone(),
            tasks.clone(),
            machines.clone(),
            tools.clone(),
            SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 1,
                model_steps: 1,
            },
            1,
            65_536,
        )
        .await?;
        if !reopened {
            assert!(matches!(
                runtime
                    .harness()
                    .admit(admission, &task_definition, 7, None)
                    .await?,
                Admission::Accepted(_)
            ));
        }
        let context = runtime.harness().durable_context(task, admission).await?;
        // Explicit low-level composition reuses the real admitted task host and
        // original owner-bound writer. This is not the default lease-owned writer.
        let writable = AgentHarness::with_policy(
            tasks.clone(),
            tools.clone(),
            scope.clone(),
            1,
            Some(runtime.task_host().clone()),
            None,
            Some(ContentBindings {
                reader: Arc::new(FilesystemContentVerifier::new(
                    filesystem.clone(),
                    issuer.verifier(),
                    signed.clone(),
                    65_536,
                )?),
                writer: Some(writer.clone()),
            }),
            None,
            None,
        )?
        .durable_context(task, admission)
        .await?;
        // This runner regression uses an explicit real FS journal, not the
        // runtime's lease-owned default execution entry point.
        let journal = Arc::new(JournalSpy {
            inner: Arc::new(FilesystemExecutionJournal::new(
                stream.clone(),
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                signed.clone(),
                65_536,
            )?),
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        });
        let runner = DurableToolRunner::new(tools.clone(), journal.clone());
        assert_eq!(
            runner
                .run_with_context(
                    task,
                    call,
                    definition.clone(),
                    arguments.clone(),
                    ToolContext::new(context.clone(), call, call.to_string())?
                )
                .await?,
            Outcome::Succeeded(json!(text))
        );
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        assert_eq!(executor.reconciliations.load(Ordering::SeqCst), 0);
        assert!(journal.reads.load(Ordering::SeqCst) > 0);
        if reopened {
            assert_eq!(journal.writes.load(Ordering::SeqCst), 0);
        }
        for (index, (tool, arguments)) in bounded_text.iter().enumerate() {
            let operation = OperationId::from_bytes([if index == 0 { 87 } else { 88 }; 16]);
            let invocation = ToolInvocation {
                operation_id: operation,
                call_id: operation.to_string(),
                name: tool.definition.name.clone(),
                arguments: arguments.clone(),
            };
            let original = ToolContext::new(context.clone(), operation, operation.to_string())?;
            let reads = journal.reads.load(Ordering::SeqCst);
            let writes = journal.writes.load(Ordering::SeqCst);
            // A valid retained scope cannot replace the originally admitted call context.
            assert!(matches!(
                tool.executor.authorize(Some(context.scope()), &invocation),
                Err(Error::Unauthorized(_))
            ));
            tool.executor
                .authorize_with_context(&original, &invocation)?;
            for wrong in [
                ToolContext::new(context.clone(), OperationId::new(), operation.to_string())?,
                ToolContext::new(context.clone(), operation, "different-call")?,
            ] {
                assert!(matches!(
                    tool.executor.authorize_with_context(&wrong, &invocation),
                    Err(Error::Unauthorized(_))
                ));
            }
            assert_eq!(journal.reads.load(Ordering::SeqCst), reads);
            assert_eq!(journal.writes.load(Ordering::SeqCst), writes);
            let Outcome::Succeeded(value) = runner
                .run_with_context(
                    task,
                    operation,
                    tool.definition.clone(),
                    arguments.clone(),
                    original,
                )
                .await?
            else {
                panic!("admitted bounded text must complete")
            };
            assert_eq!(
                bounded_executors[index].executions.load(Ordering::SeqCst),
                1
            );
            assert_eq!(
                bounded_executors[index]
                    .reconciliations
                    .load(Ordering::SeqCst),
                0
            );
            assert_eq!(value.get("file"), Some(&json!(source)));
            if index == 0 {
                assert_eq!(value["selection"]["text"], "original");
                assert_eq!(value["selection"]["range"], json!({"start":0,"end":8}));
            } else {
                assert_eq!(value["matches"]["matches"], json!([{"start":15,"end":20}]));
                assert_eq!(value["matches"]["total_matches"], 1);
                assert_eq!(value["matches"]["omitted_matches"], 0);
            }
            if reopened {
                assert_eq!(Some(&value), retained_text_results.get(index));
                assert_eq!(journal.writes.load(Ordering::SeqCst), writes);
            } else {
                retained_text_results.push(value);
            }
        }
        let write_call = OperationId::from_bytes([86; 16]);
        let write_result = runner
            .run_with_context(
                task,
                write_call,
                mutations[0].0.definition.clone(),
                mutations[0].1.clone(),
                ToolContext::new(writable.clone(), write_call, write_call.to_string())?,
            )
            .await?;
        let Outcome::Succeeded(write_value) = write_result else {
            panic!("write must complete")
        };
        let published: files::FileResult = serde_json::from_value(write_value.clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(published.file.path(), "destination.txt");
        assert_eq!(writable.read_file(&published.file).await?, b"body");
        assert_eq!(write_executor.executions.load(Ordering::SeqCst), 1);
        assert_eq!(write_executor.reconciliations.load(Ordering::SeqCst), 0);
        if reopened {
            assert_eq!(journal.writes.load(Ordering::SeqCst), 0);
        }
        let write_records = journal.inner.replay(write_call, 0, 16).await?;
        let records = journal.inner.replay(call, 0, 16).await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. }))
                .count(),
            1
        );
        let before_reads = journal.reads.load(Ordering::SeqCst);
        let before_writes = journal.writes.load(Ordering::SeqCst);
        let workspace = acyclic_harness::filesystem::workspace_ref(
            volume.provider().clone(),
            &volume.storage_name()?,
        )?;
        let before_rejected_head = filesystem.resolve(&workspace).await?.generation;
        // This admitted runtime intentionally has a reader and no original writer.
        // A scope's write capability cannot supply that missing provider binding.
        for (tool, arguments) in &mutations {
            let operation = OperationId::new();
            assert!(matches!(
                runner
                    .run_with_context(
                        task,
                        operation,
                        tool.definition.clone(),
                        arguments.clone(),
                        ToolContext::new(context.clone(), operation, operation.to_string())?
                    )
                    .await,
                Err(Error::Unauthorized(_))
            ));
        }
        let write_grant = volume.capability(VolumeOperation::Write)?;
        let no_write = writable.scoped(
            scope.grants().without(&Capabilities::new([write_grant])),
            scope.limits(),
        )?;
        for (index, (tool, arguments)) in mutations.iter().enumerate() {
            // The write case replays an already completed operation. Edit/patch
            // are fresh controls with valid original read grant and input schema.
            let operation = if index == 0 {
                write_call
            } else {
                OperationId::new()
            };
            assert!(matches!(
                runner
                    .run_with_context(
                        task,
                        operation,
                        tool.definition.clone(),
                        arguments.clone(),
                        ToolContext::new(no_write.clone(), operation, operation.to_string())?
                    )
                    .await,
                Err(Error::Unauthorized(_))
            ));
        }
        assert_eq!(
            journal.inner.replay(write_call, 0, 16).await?,
            write_records
        );
        let restricted = context.scoped(
            scope
                .grants()
                .without(&Capabilities::new([read_grant.clone()])),
            scope.limits(),
        )?;
        assert!(matches!(
            runner
                .run_with_context(
                    task,
                    call,
                    definition.clone(),
                    arguments.clone(),
                    ToolContext::new(restricted, call, call.to_string())?
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        let restricted = context.scoped(
            scope.grants().clone(),
            Limits {
                render_bytes: 4,
                ..scope.limits()
            },
        )?;
        assert!(matches!(
            runner
                .run_with_context(
                    task,
                    call,
                    definition.clone(),
                    arguments.clone(),
                    ToolContext::new(restricted, call, call.to_string())?
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(journal.reads.load(Ordering::SeqCst), before_reads);
        assert_eq!(journal.writes.load(Ordering::SeqCst), before_writes);
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        assert_eq!(executor.reconciliations.load(Ordering::SeqCst), 0);
        assert_eq!(journal.inner.replay(call, 0, 16).await?, records);
        assert_eq!(
            filesystem.resolve(&workspace).await?.generation,
            before_rejected_head
        );
        assert_eq!(write_executor.executions.load(Ordering::SeqCst), 1);
        assert_eq!(write_executor.reconciliations.load(Ordering::SeqCst), 0);
        if !reopened {
            let later_write = writer
                .stage(
                    OperationId::new(),
                    "destination.txt",
                    b"later destination edit",
                    "text/plain",
                    "destination.txt",
                )
                .await?;
            assert_ne!(published.file, later_write);
            let later = writer
                .stage(
                    OperationId::new(),
                    "source.txt",
                    b"later user edit",
                    "text/plain",
                    "source.txt",
                )
                .await?;
            assert_ne!(source, later);
        }
    }
    Ok(())
}
