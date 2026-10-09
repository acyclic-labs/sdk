use super::*;
use crate::filesystem::NativeProcessStopKind;
use crate::{
    conversation::{ContentGrant, ContentPublisher, VolumeOperation},
    core::{EffectGuarantee, EffectStatus, SchemaRegistry},
    effect_host::{ConversationEffectHost, TaskEffectPlan, task_effect_id},
    effects::{EffectDispatch, EffectProvider, EffectRegistry},
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
        FilesystemHost, FilesystemTaskRuntime, NATIVE_PROCESS_EFFECT_KIND, NativeProcessAuthority,
        NativeProcessProvider, NativeProcessRequest, NativeProcessResult, NativeViewOptions,
        NativeVolumeBinding, NativeVolumeView, workspace_ref,
    },
    interaction::InteractionResponse,
    workflow::WorkflowCommand,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Fs, MemoryAuthorityBackend, MemoryObjectBackend,
};

#[tokio::test]
async fn native_owner_guard_exhausts_phase_cancel_and_fence_conditions() -> Result<()> {
    use crate::scheduler::{OperationPhase, require_execution_owner};
    let fixture = process_fixture("write", true).await?;
    let operation = fixture
        .owner
        .host
        .coordinator
        .lock()
        .await
        .scheduler()
        .operation(fixture.owner.operation_id())
        .cloned()
        .ok_or_else(|| Error::NotFound("admitted native operation".into()))?;
    let (_, original_fence) = fixture.owner.task_binding();
    let mut checked = 0;
    for phase in [
        OperationPhase::WaitingForDependencies,
        OperationPhase::WaitingForCapacity,
        OperationPhase::Admitted,
        OperationPhase::Running,
        OperationPhase::WaitingForChildren,
        OperationPhase::Suspended,
        OperationPhase::Reconciling,
        OperationPhase::Terminal,
    ] {
        for cancelled in [false, true] {
            for settlement in [false, true] {
                for reserved in [false, true] {
                    for same_id in [false, true] {
                        for same_placement in [false, true] {
                            let mut state = operation.clone();
                            state.phase = phase;
                            state.cancellation_requested = cancelled;
                            if !reserved {
                                state.reservation = None;
                            }
                            let mut fence = original_fence.clone();
                            if !same_id {
                                fence.reservation_id.push_str("-stale");
                            }
                            if !same_placement {
                                fence.placement.push_str("-foreign");
                            }
                            let phase_allowed = match phase {
                                OperationPhase::Running => true,
                                OperationPhase::Reconciling => settlement,
                                _ => false,
                            };
                            let expected = phase_allowed
                                && reserved
                                && same_id
                                && same_placement
                                && (!cancelled || settlement);
                            assert_eq!(
                                require_execution_owner(&state, &fence, settlement).is_ok(),
                                expected,
                                "{phase:?}, cancelled={cancelled}, settlement={settlement}, reserved={reserved}, same_id={same_id}, same_placement={same_placement}"
                            );
                            checked += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(checked, 256);
    // This calls the same guard used by TaskJournalOwner; no process has run.
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    Ok(())
}

type Files<A = MemoryAuthorityBackend, O = MemoryObjectBackend> = FilesystemHost<A, O>;

#[tokio::test]
async fn result_storage_cannot_mutate_a_pinned_native_view() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    let mut authority = fixture.authority();
    authority.results = Arc::new(FilesystemContentPublisher::new(
        fixture.files.clone(),
        fixture.destination.clone(),
        &fixture.issuer.verifier(),
        &fixture.scope,
        65_536,
    )?);
    assert!(matches!(
        Process::new(
            fixture.owner.clone(),
            fixture.command,
            fixture.plan.clone(),
            fixture.view.clone(),
            fixture.approval,
            authority,
        )
        .await,
        Err(Error::Unsupported(_))
    ));
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert!(
        fixture
            .files
            .read(&destination, None, "/native-process/result.json", 64)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn readonly_schema_storage_does_not_require_result_write_authority() -> Result<()> {
    let mut fixture = process_fixture("write", true).await?;
    let setup = fixture.issuer.root_for_agent(
        AgentId::from_bytes([61; 16]),
        "schema-setup",
        Capabilities::new([fixture.source.capability(VolumeOperation::Write)?]),
    );
    let grant = ContentGrant::verify(
        &fixture.issuer.verifier(),
        &setup,
        &fixture.source,
        VolumeOperation::Write,
    )?;
    fixture.plan.result_schema = fixture
        .files
        .put_content(
            &fixture.source,
            &grant,
            "schema.json",
            b"{\"type\":\"object\"}",
            "application/schema+json",
            "schema.json",
            128,
            &IdempotencyKey::new("readonly-native-schema")?,
        )
        .await?;
    assert!(
        fixture
            .owner
            .require_volume_grant(&fixture.source, VolumeOperation::Write)
            .is_err()
    );
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name()?,
    )?;
    let before = fixture.files.resolve(&source).await?.generation;
    let status = fixture
        .effects(fixture.provider().await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = status else {
        return Err(Error::Storage(format!(
            "readonly schema did not complete: {status:?}"
        )));
    };
    assert_eq!(result.volume(), fixture.results.volume());
    assert_ne!(result.volume(), fixture.plan.result_schema.volume());
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    Ok(())
}
type Process<P = MemoryStream, A = MemoryAuthorityBackend, O = MemoryObjectBackend> =
    NativeProcessProvider<P, A, O>;
type Journal<P = MemoryStream, A = MemoryAuthorityBackend, O = MemoryObjectBackend> =
    FilesystemExecutionJournal<P, A, O>;
type NativeRuntime<P = MemoryStream, A = MemoryAuthorityBackend, O = MemoryObjectBackend> =
    FilesystemTaskRuntime<P, A, O>;

struct ProcessFixture<
    P: StreamProvider = MemoryStream,
    A = MemoryAuthorityBackend,
    O = MemoryObjectBackend,
> {
    directory: tempfile::TempDir,
    task: NativeRuntime<P, A, O>,
    lease: crate::distributed::WorkLease,
    owner: Arc<TaskJournalOwner<P>>,
    files: Arc<Files<A, O>>,
    issuer: AuthorityIssuer,
    scope: Scope,
    source: VolumeRef,
    destination: VolumeRef,
    second_destination: VolumeRef,
    content: Arc<FilesystemContentVerifier<A, O>>,
    results: Arc<FilesystemContentPublisher<A, O>>,
    approvals: Arc<Journal<P, A, O>>,
    view: Arc<NativeVolumeView<A, O>>,
    command: OperationId,
    approval: InteractionId,
    plan: TaskEffectPlan,
}

impl<P, A, O> ProcessFixture<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn request(&self) -> Result<NativeProcessRequest> {
        crate::contract::json_from_slice(&self.content.read(&self.plan.request).await?)
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn replace_request(&mut self, request: &NativeProcessRequest) -> Result<()> {
        self.plan.request = self
            .results
            .stage(
                self.command,
                "process/changed-request.json",
                &crate::contract::canonical_json_bytes(request)?,
                "application/json",
                "changed-request.json",
            )
            .await?;
        Ok(())
    }

    fn authority(&self) -> NativeProcessAuthority {
        NativeProcessAuthority {
            verifier: self.issuer.verifier(),
            schemas: SchemaRegistry::new(),
            content: self.content.clone(),
            results: self.results.clone(),
            approvals: self.approvals.clone(),
        }
    }

    async fn provider(&self) -> Result<Arc<Process<P, A, O>>> {
        Ok(Arc::new(
            Process::new(
                self.owner.clone(),
                self.command,
                self.plan.clone(),
                self.view.clone(),
                self.approval,
                self.authority(),
            )
            .await?,
        ))
    }

    fn effects(&self, process: Arc<Process<P, A, O>>) -> Result<ConversationEffectHost<P>> {
        let mut registry = EffectRegistry::default().with_result_resolver(self.content.clone());
        registry.register(process)?;
        ConversationEffectHost::new(
            self.owner.stream(),
            self.issuer.verifier().audience().clone(),
            self.issuer.clone(),
            self.scope.clone(),
            SchemaRegistry::new(),
            self.content.clone(),
            registry,
        )
    }
}

async fn process_fixture(mode: &str, approve: bool) -> Result<ProcessFixture> {
    process_fixture_on(
        StreamClient::new(Arc::new(MemoryStream::default())),
        mode,
        approve,
    )
    .await
}

async fn process_fixture_on<P: StreamProvider + 'static>(
    stream: StreamClient<P>,
    mode: &str,
    approve: bool,
) -> Result<ProcessFixture<P>> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    process_fixture_with_filesystems(stream, directory, Fs::memory(), Fs::memory(), mode, approve)
        .await
}

async fn process_fixture_with_filesystems<P, A, O>(
    stream: StreamClient<P>,
    directory: tempfile::TempDir,
    filesystem: Fs<A, O>,
    auxiliary_filesystem: Fs<A, O>,
    mode: &str,
    approve: bool,
) -> Result<ProcessFixture<P, A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let provider = ProviderRef::new("native-test", "filesystem", "1")?;
    let files = Arc::new(FilesystemHost::new(filesystem, provider.clone())?);
    let agent = AgentId::from_bytes([61; 16]);
    let auxiliary_provider = ProviderRef::new("native-test-aux", "filesystem", "1")?;
    let auxiliary_host = Arc::new(FilesystemHost::new(
        auxiliary_filesystem,
        auxiliary_provider.clone(),
    )?);
    let auxiliary = VolumeRef::new(
        auxiliary_provider,
        "auxiliary",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let source = VolumeRef::new(
        provider.clone(),
        "source",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let destination = VolumeRef::new(
        provider.clone(),
        "destination",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let second_destination = VolumeRef::new(
        provider.clone(),
        "second-destination",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let result_volume = VolumeRef::new(
        provider,
        "results",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let extra_sources = (0..if mode == "many-bash" { 16 } else { 0 })
        .map(|index| {
            VolumeRef::new(
                source.provider().clone(),
                format!("extra-{index}"),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let command = OperationId::from_bytes([62; 16]);
    let approval = task_interaction_id(TaskId::from_bytes([53; 16]), command);
    let conversation = Authority {
        kind: AggregateKind::Conversation,
        id: "native-test-conversation".into(),
    };
    let issuer = AuthorityIssuer::new("native-test", [63; 32], conversation);
    let grants = Capabilities::new(
        [
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "task:spawn:test.effect@1".into(),
            source.capability(VolumeOperation::Read)?,
            auxiliary.capability(VolumeOperation::Read)?,
            destination.capability(VolumeOperation::Read)?,
            destination.capability(VolumeOperation::Write)?,
            second_destination.capability(VolumeOperation::Read)?,
            second_destination.capability(VolumeOperation::Write)?,
            result_volume.capability(VolumeOperation::Read)?,
            result_volume.capability(VolumeOperation::Write)?,
            "effect:plan".into(),
            "effect:run".into(),
            "effect:provider:test.native".into(),
            "interaction:open".into(),
            "interaction:resolve".into(),
            capability::interaction_respond(approval),
            "interaction:route".into(),
            "conversation:bind".into(),
        ]
        .into_iter()
        .chain(
            extra_sources
                .iter()
                .map(|volume| volume.capability(VolumeOperation::Read))
                .collect::<Result<Vec<_>>>()?,
        ),
    );
    let scope = issuer.root_for_agent(agent, "owner", grants.clone());
    files.create_volume(&source).await?;
    files.create_volume(&destination).await?;
    files.create_volume(&second_destination).await?;
    files.create_volume(&result_volume).await?;
    auxiliary_host.create_volume(&auxiliary).await?;
    for volume in &extra_sources {
        files.create_volume(volume).await?;
    }
    let task = native_task_fixture(
        stream,
        files.clone(),
        result_volume.clone(),
        &issuer,
        &scope,
        grants,
    )
    .await?;
    let task_id = TaskId::from_bytes([53; 16]);
    let crate::distributed::WorkPull::Claimed(lease) = task
        .task_host()
        .pull_work(&crate::distributed::Worker {
            id: "native-worker".into(),
            available: Default::default(),
            labels: BTreeMap::new(),
        })
        .await?
    else {
        return Err(Error::NotFound("native lease".into()));
    };
    task.task_host().start_task(&lease).await?;
    let owner = Arc::new(
        task.task_host()
            .journal_owner(
                task_id,
                crate::scheduler::LeaseFence::from(&lease.reservation),
            )
            .await?,
    );
    let mut aggregate = crate::store::StreamAggregate::open(
        &owner.stream(),
        issuer.verifier().audience().clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?;
    aggregate
        .execute(crate::core::Command {
            operation_id: OperationId::from_bytes([64; 16]),
            idempotency_key: IdempotencyKey::new("native-conversation-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: crate::core::Action::BindConversation { agent },
        })
        .await?;
    let setup_scope = issuer.root_for_agent(
        agent,
        "setup",
        Capabilities::new(
            [
                source.capability(VolumeOperation::Write)?,
                auxiliary.capability(VolumeOperation::Write)?,
            ]
            .into_iter()
            .chain(
                extra_sources
                    .iter()
                    .map(|volume| volume.capability(VolumeOperation::Write))
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
    );
    let setup_grant = ContentGrant::verify(
        &issuer.verifier(),
        &setup_scope,
        &source,
        VolumeOperation::Write,
    )?;
    files
        .put_content(
            &source,
            &setup_grant,
            "input.txt",
            b"pinned input",
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new("source-setup")?,
        )
        .await?;
    let auxiliary_grant = ContentGrant::verify(
        &issuer.verifier(),
        &setup_scope,
        &auxiliary,
        VolumeOperation::Write,
    )?;
    auxiliary_host
        .put_content(
            &auxiliary,
            &auxiliary_grant,
            "input.txt",
            b"auxiliary input",
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new("auxiliary-setup")?,
        )
        .await?;
    let auxiliary_generation = auxiliary_host
        .resolve(&workspace_ref(
            auxiliary.provider().clone(),
            &auxiliary.storage_name()?,
        )?)
        .await?
        .generation;
    let source_generation = files
        .resolve(&workspace_ref(
            source.provider().clone(),
            &source.storage_name()?,
        )?)
        .await?
        .generation;
    let destination_generation = files
        .resolve(&workspace_ref(
            destination.provider().clone(),
            &destination.storage_name()?,
        )?)
        .await?
        .generation;
    let second_generation = files
        .resolve(&workspace_ref(
            second_destination.provider().clone(),
            &second_destination.storage_name()?,
        )?)
        .await?
        .generation;
    let root = directory.path().join("view");
    std::fs::create_dir(&root).map_err(|error| Error::Storage(error.to_string()))?;
    let mut work = serde_json::to_value(acyclic_fs::WorkBudget::UNBOUNDED)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    for allowance in work
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("work counters".into()))?
        .values_mut()
    {
        *allowance = serde_json::json!(10_000_000);
    }
    let work = serde_json::from_value(work).map_err(|error| Error::Invalid(error.to_string()))?;
    let mut bindings = vec![
        NativeVolumeBinding {
            host: files.clone(),
            volume: source.clone(),
            generation: source_generation,
            path: "src".into(),
            writable: false,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
        NativeVolumeBinding {
            host: files.clone(),
            volume: destination.clone(),
            generation: destination_generation,
            path: "dst".into(),
            writable: true,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
        NativeVolumeBinding {
            host: auxiliary_host,
            volume: auxiliary,
            generation: auxiliary_generation,
            path: "aux".into(),
            writable: false,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        },
    ];
    if mode == "partial-publication" {
        bindings.push(NativeVolumeBinding {
            host: files.clone(),
            volume: second_destination.clone(),
            generation: second_generation,
            path: "second-dst".into(),
            writable: true,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        });
    }
    for (index, volume) in extra_sources.into_iter().enumerate() {
        let grant = ContentGrant::verify(
            &issuer.verifier(),
            &setup_scope,
            &volume,
            VolumeOperation::Write,
        )?;
        files
            .put_content(
                &volume,
                &grant,
                "input.txt",
                b"extra input",
                "text/plain",
                "input.txt",
                128,
                &IdempotencyKey::new(format!("extra-source-{index}"))?,
            )
            .await?;
        let generation = files
            .resolve(&workspace_ref(
                volume.provider().clone(),
                &volume.storage_name()?,
            )?)
            .await?
            .generation;
        bindings.push(NativeVolumeBinding {
            host: files.clone(),
            volume,
            generation,
            path: format!("extra-{index}"),
            writable: false,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        });
    }
    let maximum_volumes =
        u32::try_from(bindings.len()).map_err(|error| Error::Invalid(error.to_string()))?;
    let view = Arc::new(
        NativeVolumeView::prepare(
            &owner,
            NativeViewOptions {
                root,
                maximum_volumes,
                work_per_volume: work,
            },
            bindings,
        )
        .await?,
    );
    let mut request = NativeProcessRequest {
        executable: std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?,
        argv: vec![
            "--exact".into(),
            "durable_host::tests::native_process_tests::native_process_child".into(),
            "--nocapture".into(),
        ],
        cwd: view.manifest().options.root.clone(),
        environment: BTreeMap::from([
            ("NATIVE_TEST_MODE".into(), mode.into()),
            ("NATIVE_TEST_TOKEN".into(), "approved-env".into()),
        ]),
        timeout_ms: if mode == "timeout" { 300 } else { 10_000 },
        control_timeout_ms: if mode == "timeout" { 50 } else { 250 },
        cancellation_poll_ms: 10,
        maximum_output_bytes: 8192,
        maximum_result_bytes: 65_536,
        mcp_stdio: None,
        view: view.manifest().clone(),
    };
    if mode == "mcp-approval" {
        request.mcp_stdio = Some(crate::mcp::stdio::McpStdioRequest {
            initialization: OperationId::from_bytes([71; 16]),
            operation: OperationId::from_bytes([72; 16]),
            method: crate::mcp::stdio::McpStdioMethod::CallTool,
            params: serde_json::json!({"name":"echo","arguments":{}}),
            maximum_bytes: 4096,
        });
    }
    if mode == "bash" || mode == "mutate-source-bash" || mode == "many-bash" {
        request.executable = if cfg!(windows) {
            "C:/Program Files/Git/bin/bash.exe"
        } else {
            "/bin/bash"
        }
        .into();
        request.argv = vec!["--noprofile".into(), "--norc".into(), "-c".into(),
            "IFS= read -r input < src/input.txt; printf '%s' \"$input\" > dst/output.txt; IFS= read -r aux < aux/input.txt; printf '%s' \"$aux\" > dst/auxiliary.txt; printf '%s' \"$0|$1|$2\"; printf '%s' \"$NATIVE_TEST_TOKEN\" >&2".into(),
            "approved-bash".into(), "first arg".into(), "second arg".into()];
        if mode == "mutate-source-bash" {
            request.argv[3].push_str("; printf 'unauthorized source edit' > src/input.txt");
        }
        if mode == "many-bash" {
            request.argv[3].push_str("; for index in {0..15}; do IFS= read -r value < extra-$index/input.txt; printf '%s|' \"$value\"; done > dst/many.txt");
        }
    }
    let content = Arc::new(FilesystemContentVerifier::new(
        files.clone(),
        issuer.verifier(),
        scope.clone(),
        1_000_000,
    )?);
    let results = Arc::new(FilesystemContentPublisher::new(
        files.clone(),
        result_volume.clone(),
        &issuer.verifier(),
        &scope,
        1_000_000,
    )?);
    let request_ref = results
        .stage(
            command,
            "process/request.json",
            &crate::contract::canonical_json_bytes(&request)?,
            "application/json",
            "request.json",
        )
        .await?;
    let schema = results
        .stage(
            command,
            "process/schema.json",
            b"{\"type\":\"object\"}",
            "application/schema+json",
            "schema.json",
        )
        .await?;
    let plan = TaskEffectPlan {
        provider: "test.native".into(),
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: NATIVE_PROCESS_EFFECT_KIND.into(),
        request: request_ref,
        result_schema: schema,
    };
    let approvals = Arc::new(FilesystemExecutionJournal::new(
        owner.stream(),
        files.clone(),
        result_volume,
        issuer.verifier(),
        scope.clone(),
        1_000_000,
    )?);
    approvals
        .open_interaction(
            approval,
            Interaction::approval(
                "Run the exact admitted process",
                command,
                request.approval_digest(owner.task_binding().0, command)?,
            )?,
        )
        .await?;
    approvals
        .resolve_interaction(
            approval,
            InteractionResponse::Approval {
                approved: approve,
                reason: None,
            },
            &scope,
        )
        .await?;
    Ok(ProcessFixture {
        directory,
        task,
        lease,
        owner,
        files,
        issuer,
        scope,
        source,
        destination,
        second_destination,
        content,
        results,
        approvals,
        view,
        command,
        approval,
        plan,
    })
}

async fn native_task_fixture<P, A, O>(
    stream: StreamClient<P>,
    files: Arc<Files<A, O>>,
    volume: VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
    grants: Capabilities,
) -> Result<NativeRuntime<P, A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let implementation = Arc::new(NativeEffectMachine {
        identity: MachineIdentity {
            name: "test.effect".into(),
            version: "1".into(),
            digest: [52; 32],
        },
        schema: serde_json::json!({"type":"integer"}),
    });
    let definition = TaskDefinition::<Value, Value>::resumable(
        implementation.clone(),
        serde_json::json!({"type":"integer"}),
        serde_json::json!({"type":"object"}),
    )?;
    let identity = definition.identity().clone();
    let machine = implementation.identity.clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(implementation)?;
    let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
    let runtime = FilesystemTaskRuntime::open(
        stream,
        files,
        volume,
        issuer.verifier(),
        scope.clone(),
        runtime_scope.clone(),
        tasks,
        machines,
        crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        },
        1,
        1_000_000,
    )
    .await?;
    runtime
        .task_host()
        .admit(TaskAdmissionRecord {
            operation_id: OperationId::from_bytes([53; 16]),
            task: identity,
            machine,
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type":"integer"}),
            output_schema: serde_json::json!({"type":"object"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
    Ok(runtime)
}

struct NativeEffectMachine {
    identity: MachineIdentity,
    schema: Value,
}

#[derive(Default)]
struct ReceiptFaultStream {
    inner: MemoryStream,
    fault: Mutex<Option<(&'static str, bool)>>,
    hide_next_inspection: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl StreamProvider for ReceiptFaultStream {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        if self
            .hide_next_inspection
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, acyclic_stream::StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        self.inner.append(request).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        self.inner.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        self.inner.children(request).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, acyclic_stream::StreamError> {
        self.inner.children_page(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.inner.read_commit(id).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        let mut armed = self.fault.lock().await;
        let selected = armed.as_ref().is_some_and(|(kind, _)| request.mutations.iter().any(|mutation| {
            matches!(mutation, acyclic_stream::CommitMutation::Append { path, records }
                if path.as_str().starts_with("harness/v2/native-process/") && records.iter().any(|record| serde_json::from_slice::<Value>(record).is_ok_and(|value| value["kind"] == *kind)))
        }));
        let fault = if selected { armed.take() } else { None };
        drop(armed);
        if fault.is_some_and(|(_, lost_reply)| !lost_reply) {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        let result = self.inner.commit(request).await?;
        if fault.is_some() {
            // Hide the immediate keyed observation as well, so this is
            // an unknown commit rather than a recovered lost reply.
            self.hide_next_inspection
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        Ok(result)
    }
}

#[tokio::test]
async fn receipt_commit_faults_never_replay_an_unknown_native_attempt() -> Result<()> {
    for (kind, lost_reply) in [
        ("launch", false),
        ("launch", true),
        ("observed", false),
        ("observed", true),
    ] {
        let stream = Arc::new(ReceiptFaultStream::default());
        let fixture = process_fixture_on(StreamClient::new(stream.clone()), "write", true).await?;
        *stream.fault.lock().await = Some((kind, lost_reply));
        let effects = fixture.effects(fixture.provider().await?)?;
        assert!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await
                .is_err()
        );
        assert!(
            stream.fault.lock().await.is_none(),
            "fault was not exercised"
        );
        let recovered = Arc::new(
            Process::<ReceiptFaultStream>::recover(
                fixture.owner.clone(),
                fixture.command,
                fixture.plan.clone(),
                fixture.approval,
                fixture.authority(),
            )
            .await?,
        );
        let effects = fixture.effects(recovered)?;
        let status = effects
            .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
            .await?;
        if kind == "observed" && lost_reply {
            assert!(matches!(status, EffectStatus::Succeeded { .. }));
        } else {
            assert_eq!(status, EffectStatus::Indeterminate);
        }
        let root = &fixture.view.manifest().options.root;
        assert_eq!(root.join("dst/output.txt").exists(), kind == "observed");
        if kind == "observed" {
            std::fs::remove_file(root.join("dst/output.txt"))
                .map_err(|error| Error::Storage(error.to_string()))?;
        }
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            status
        );
        assert!(!root.join("dst/output.txt").exists());
        let destination = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&destination, None, "/output.txt", 64)
                .await
                .is_ok(),
            kind == "observed"
        );
    }
    Ok(())
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn receipt_reconciliation_reopens_all_local_stores_after_provider_objects_drop() -> Result<()>
{
    let storage = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let filesystem = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("filesystem"),
    ))
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let auxiliary = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("auxiliary"),
    ))
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let local = Arc::new(
        acyclic_stream::LocalStream::open(storage.path().join("coordinator"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let weak_stream = Arc::downgrade(&local);
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fixture = process_fixture_with_filesystems(
        StreamClient::new(local),
        directory,
        filesystem,
        auxiliary,
        "write",
        true,
    )
    .await?;
    let seed = NativeRestart::from_fixture(&fixture);
    let auxiliary = fixture.view.manifest().volumes[2].clone();
    let weak_files = Arc::downgrade(&fixture.files);
    let process = fixture.provider().await?;
    let weak_process = Arc::downgrade(&process);
    let status = fixture
        .effects(process.clone())?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    drop(process);
    assert!(weak_process.upgrade().is_none());
    let ProcessFixture {
        directory,
        task,
        lease: _,
        owner,
        files,
        issuer: _,
        scope: _,
        source: _,
        destination: _,
        second_destination: _,
        content,
        results,
        approvals,
        view,
        command: _,
        approval: _,
        plan: _,
    } = fixture;
    let native_output = seed.root.join("dst/output.txt");
    drop((task, owner, approvals, content, results, view, files));
    assert!(weak_stream.upgrade().is_none());
    assert!(weak_files.upgrade().is_none());
    let LocalNativeRecovery {
        files,
        effects,
        owner,
        ..
    } = reopen_native(storage.path(), &seed).await?;
    assert_eq!(
        effects
            .reconcile_task_effect(&owner, seed.command, &seed.plan)
            .await?,
        status
    );
    std::fs::remove_file(&native_output).map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&owner, seed.command, seed.plan.clone())
            .await?,
        status
    );
    assert!(!native_output.exists());
    let destination = workspace_ref(
        seed.destination.provider().clone(),
        &seed.destination.storage_name()?,
    )?;
    assert_eq!(
        files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let auxiliary_files = FilesystemHost::new(
        Fs::local(acyclic_fs::LocalOptions::new(
            storage.path().join("auxiliary"),
        ))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?,
        auxiliary.volume.provider().clone(),
    )?;
    let auxiliary_ref = workspace_ref(
        auxiliary.volume.provider().clone(),
        &auxiliary.volume.storage_name()?,
    )?;
    assert_eq!(
        auxiliary_files
            .read(
                &auxiliary_ref,
                Some(&auxiliary.generation),
                "/input.txt",
                64
            )
            .await?
            .as_ref(),
        b"auxiliary input"
    );
    assert!(directory.path().exists());
    Ok(())
}

#[cfg(feature = "filesystem-local")]
#[derive(serde::Serialize, serde::Deserialize)]
struct NativeRestart {
    task: TaskId,
    fence: crate::scheduler::LeaseFence,
    scope: Scope,
    command: OperationId,
    approval: InteractionId,
    plan: TaskEffectPlan,
    result_volume: VolumeRef,
    destination: VolumeRef,
    root: std::path::PathBuf,
}

#[cfg(feature = "filesystem-local")]
impl NativeRestart {
    fn from_fixture<P, A, O>(fixture: &ProcessFixture<P, A, O>) -> Self
    where
        P: StreamProvider + 'static,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let (task, fence) = fixture.owner.task_binding();
        Self {
            task,
            fence,
            scope: fixture.scope.clone(),
            command: fixture.command,
            approval: fixture.approval,
            plan: fixture.plan.clone(),
            result_volume: fixture.results.volume().clone(),
            destination: fixture.destination.clone(),
            root: fixture.view.manifest().options.root.clone(),
        }
    }
}

#[cfg(feature = "filesystem-local")]
type LocalFiles = Files<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
#[cfg(feature = "filesystem-local")]
type LocalNativeRuntime = NativeRuntime<
    acyclic_stream::LocalStream,
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
>;
#[cfg(feature = "filesystem-local")]
type LocalNativeProcess = Process<
    acyclic_stream::LocalStream,
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
>;

#[cfg(feature = "filesystem-local")]
struct LocalNativeRecovery {
    files: Arc<LocalFiles>,
    task: LocalNativeRuntime,
    owner: Arc<TaskJournalOwner<acyclic_stream::LocalStream>>,
    process: Arc<LocalNativeProcess>,
    effects: ConversationEffectHost<acyclic_stream::LocalStream>,
}

#[cfg(feature = "filesystem-local")]
async fn reopen_native(
    storage: &std::path::Path,
    seed: &NativeRestart,
) -> Result<LocalNativeRecovery> {
    let issuer = AuthorityIssuer::new(
        "native-test",
        [63; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "native-test-conversation".into(),
        },
    );
    let files = Arc::new(FilesystemHost::new(
        Fs::local(acyclic_fs::LocalOptions::new(storage.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        ProviderRef::new("native-test", "filesystem", "1")?,
    )?);
    let content = Arc::new(FilesystemContentVerifier::new(
        files.clone(),
        issuer.verifier(),
        seed.scope.clone(),
        1_000_000,
    )?);
    let results = Arc::new(FilesystemContentPublisher::new(
        files.clone(),
        seed.result_volume.clone(),
        &issuer.verifier(),
        &seed.scope,
        1_000_000,
    )?);
    let reopened = Arc::new(
        acyclic_stream::LocalStream::open(storage.join("coordinator"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let task = native_task_fixture(
        StreamClient::new(reopened),
        files.clone(),
        seed.result_volume.clone(),
        &issuer,
        &seed.scope,
        seed.scope.capabilities().clone(),
    )
    .await?;
    let owner = Arc::new(
        task.task_host()
            .journal_owner(seed.task, seed.fence.clone())
            .await?,
    );
    let approvals = Arc::new(FilesystemExecutionJournal::new(
        owner.stream(),
        files.clone(),
        seed.result_volume.clone(),
        issuer.verifier(),
        seed.scope.clone(),
        1_000_000,
    )?);
    let process = Arc::new(
        LocalNativeProcess::recover(
            owner.clone(),
            seed.command,
            seed.plan.clone(),
            seed.approval,
            NativeProcessAuthority {
                verifier: issuer.verifier(),
                schemas: SchemaRegistry::new(),
                content: content.clone(),
                results,
                approvals,
            },
        )
        .await?,
    );
    let mut registry = EffectRegistry::default().with_result_resolver(content.clone());
    registry.register(process.clone())?;
    let effects = ConversationEffectHost::new(
        owner.stream(),
        issuer.verifier().audience().clone(),
        issuer,
        seed.scope.clone(),
        SchemaRegistry::new(),
        content,
        registry,
    )?;
    Ok(LocalNativeRecovery {
        files,
        task,
        owner,
        process,
        effects,
    })
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn native_host_child() -> Result<()> {
    let Some(storage) = std::env::var_os("NATIVE_HOST_STORAGE") else {
        return Ok(());
    };
    let storage = std::path::PathBuf::from(storage);
    let filesystem = Fs::local(acyclic_fs::LocalOptions::new(storage.join("filesystem")))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let auxiliary = Fs::local(acyclic_fs::LocalOptions::new(storage.join("auxiliary")))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(storage.join("coordinator"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let directory =
        tempfile::tempdir_in(&storage).map_err(|error| Error::Storage(error.to_string()))?;
    let fixture = process_fixture_with_filesystems(
        StreamClient::new(stream),
        directory,
        filesystem,
        auxiliary,
        "host-death",
        true,
    )
    .await?;
    // Host-only restart metadata; it never enters the model's process request.
    std::fs::write(
        storage.join("restart.json"),
        crate::contract::canonical_json_bytes(&NativeRestart::from_fixture(&fixture))?,
    )
    .map_err(|error| Error::Storage(error.to_string()))?;
    fixture
        .effects(fixture.provider().await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    Err(Error::Storage(
        "host-death controller failed to terminate host".into(),
    ))
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn actual_host_termination_reopens_launch_only_receipt_without_replay_or_slot_release()
-> Result<()> {
    use std::process::{Command, Stdio};
    let storage = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let mut child =
        Command::new(std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?)
            .args([
                "--exact",
                "durable_host::tests::native_process_tests::native_host_child",
                "--nocapture",
            ])
            .env_clear()
            .env("NATIVE_HOST_STORAGE", storage.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| Error::Storage(error.to_string()))?;
    let ready = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Ok(bytes) = std::fs::read(storage.path().join("restart.json"))
                && let Ok(seed) = crate::contract::json_from_slice::<NativeRestart>(&bytes)
                && seed.root.join("host-death-ready").exists()
            {
                break seed;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    // Always stop and reap the exact host child, including a setup timeout.
    child
        .kill()
        .map_err(|error| Error::Storage(error.to_string()))?;
    child
        .wait()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let seed = ready
        .map_err(|error| Error::Storage(format!("native host did not reach launch: {error}")))?;
    tokio::time::sleep(std::time::Duration::from_millis(3200)).await;
    let LocalNativeRecovery {
        files,
        task,
        owner,
        process,
        effects,
    } = reopen_native(storage.path(), &seed).await?;
    let attempt =
        crate::EffectAttemptId::from_bytes(task_effect_id(seed.task, seed.command)?.into_bytes());
    let path = acyclic_stream::StreamPath::new(format!("harness/v2/native-process/{attempt}"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        owner
            .stream()
            .bounds(path.as_str())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .tail,
        1
    );
    assert_eq!(
        effects
            .run_task_effect(&owner, seed.command, seed.plan.clone())
            .await?,
        EffectStatus::Indeterminate
    );
    assert_eq!(process.output(attempt).await?, None);
    let output = seed.root.join("dst/output.txt");
    if output.exists() {
        std::fs::remove_file(&output).map_err(|error| Error::Storage(error.to_string()))?;
    }
    task.task_host().cancel(seed.task).await?;
    assert!(owner.verify(false).await.is_err());
    assert!(owner.verify(true).await.is_ok());
    assert_eq!(
        effects
            .reconcile_task_effect(&owner, seed.command, &seed.plan)
            .await?,
        EffectStatus::Indeterminate
    );
    assert_eq!(task.task_host().outcome(seed.task).await?, None);
    assert_eq!(
        owner
            .stream()
            .bounds(path.as_str())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .tail,
        1
    );
    assert!(!output.exists());
    let destination = workspace_ref(
        seed.destination.provider().clone(),
        &seed.destination.storage_name()?,
    )?;
    assert!(
        files
            .read(&destination, None, "/output.txt", 64)
            .await
            .is_err()
    );
    let coordinator = owner.host.coordinator.lock().await;
    let operation = coordinator
        .scheduler()
        .operation(owner.operation_id())
        .ok_or_else(|| Error::NotFound("native operation after host death".into()))?;
    assert!(operation.reservation.is_some());
    assert_ne!(operation.phase, crate::scheduler::OperationPhase::Terminal);
    Ok(())
}

impl ResumableMachine for NativeEffectMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if state == &serde_json::json!(0) {
            let command: WorkflowCommand = serde_json::from_value(input.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            Ok(MachineTransition {
                state: serde_json::json!(1),
                commands: vec![command],
                status: MachineStatus::Suspended,
            })
        } else {
            Ok(MachineTransition {
                state: serde_json::json!(2),
                commands: Vec::new(),
                status: MachineStatus::Completed {
                    value: input.clone(),
                },
            })
        }
    }
}

#[test]
fn native_process_child() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let Ok(mode) = std::env::var("NATIVE_TEST_MODE") else {
        return Ok(());
    };
    assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_none());
    if mode == "grandchild" {
        std::thread::sleep(std::time::Duration::from_secs(2));
        std::fs::write("escaped", b"escaped")?;
        return Ok(());
    }
    assert_eq!(std::env::var("NATIVE_TEST_TOKEN")?, "approved-env");
    if mode == "host-death" {
        std::fs::write("host-death-ready", b"ready")?;
        // An orphan on Unix finishes within this bound even after its host dies.
        std::thread::sleep(std::time::Duration::from_secs(3));
    }
    if mode == "wait" || mode == "timeout" {
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "durable_host::tests::native_process_tests::native_process_child",
                "--nocapture",
            ])
            .env_clear()
            .env("NATIVE_TEST_MODE", "grandchild")
            .spawn()?;
        std::fs::write("ready", b"ready")?;
        std::thread::sleep(std::time::Duration::from_secs(30));
    } else {
        let input = std::fs::read("src/input.txt")?;
        std::fs::write("dst/output.txt", input)?;
        std::fs::write("dst/auxiliary.txt", std::fs::read("aux/input.txt")?)?;
        println!("approved stdout");
        eprintln!("approved stderr");
        if mode == "pause-write" || mode == "partial-publication" {
            if mode == "partial-publication" {
                std::fs::write("second-dst/output.txt", b"second edit")?;
            }
            std::fs::write("publication-ready", b"ready")?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !std::path::Path::new("publication-release").exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "publication race controller did not release child"
                );
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }
        if mode == "flood" {
            use std::io::Write as _;
            std::io::stdout().write_all(&[255; 65_536])?;
        }
        if mode == "nonzero" {
            std::process::exit(22);
        }
    }
    Ok(())
}

#[tokio::test]
async fn changed_invocations_cannot_use_the_original_approval() -> Result<()> {
    for change in [
        "argv",
        "cwd",
        "environment",
        "bounds",
        "authority",
        "volume-order",
        "mcp-initialization",
        "mcp-operation",
        "mcp-method",
        "mcp-params",
        "mcp-bytes",
    ] {
        let mut fixture = process_fixture(
            if change.starts_with("mcp-") {
                "mcp-approval"
            } else {
                "write"
            },
            true,
        )
        .await?;
        drop(fixture.provider().await?);
        let mut request = fixture.request().await?;
        match change {
            "argv" => request.argv.swap(0, 1),
            "cwd" => request.cwd = request.view.volumes[0].path.clone(),
            "environment" => {
                request
                    .environment
                    .insert("NEW_VARIABLE".into(), "changed".into());
            }
            "bounds" => request.timeout_ms += 1,
            "authority" => request.view.volumes[0].authority_revision[0] ^= 1,
            "volume-order" => request.view.volumes.swap(0, 1),
            change if change.starts_with("mcp-") => {
                let exchange = request
                    .mcp_stdio
                    .as_mut()
                    .ok_or_else(|| Error::NotFound("MCP descriptor".into()))?;
                match change {
                    "mcp-initialization" => {
                        exchange.initialization = OperationId::from_bytes([73; 16]);
                    }
                    "mcp-operation" => exchange.operation = OperationId::from_bytes([74; 16]),
                    "mcp-method" => exchange.method = crate::mcp::stdio::McpStdioMethod::ListTools,
                    "mcp-params" => {
                        exchange.params = serde_json::json!({"name":"changed","arguments":{}});
                    }
                    "mcp-bytes" => exchange.maximum_bytes += 1,
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        let original = fixture.request().await?;
        assert_ne!(
            original.approval_digest(fixture.owner.task_binding().0, fixture.command)?,
            request.approval_digest(fixture.owner.task_binding().0, fixture.command)?
        );
        fixture.replace_request(&request).await?;
        assert!(
            fixture.provider().await.is_err(),
            "accepted changed {change}"
        );
        assert!(
            !fixture
                .view
                .manifest()
                .options
                .root
                .join("dst/output.txt")
                .exists()
        );
    }
    Ok(())
}

#[tokio::test]
async fn absent_stdio_preserves_legacy_approval_and_stopped_mcp_remains_uncertain() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    let mut request = fixture.request().await?;
    let legacy =
        serde_json::to_value(&request).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(legacy.get("mcp_stdio").is_none());
    let decoded: NativeProcessRequest = serde_json::from_value(legacy.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(decoded.mcp_stdio.is_none());
    assert_eq!(
        request.approval_digest(fixture.owner.task_binding().0, fixture.command)?,
        crate::contract::canonical_json_digest(&(
            NATIVE_PROCESS_EFFECT_KIND,
            fixture.owner.task_binding().0,
            fixture.command,
            legacy
        ))?
    );
    let initialization = OperationId::from_bytes([75; 16]);
    let operation = OperationId::from_bytes([76; 16]);
    request.mcp_stdio = Some(crate::mcp::stdio::McpStdioRequest {
        initialization,
        operation,
        method: crate::mcp::stdio::McpStdioMethod::CallTool,
        params: serde_json::json!({"name":"echo","arguments":{}}),
        maximum_bytes: 4096,
    });
    let value = serde_json::json!({"content":[],"structuredContent":{"sequence":u64::MAX}});
    let mut stdout = crate::contract::canonical_json_bytes(&serde_json::json!({"jsonrpc":"2.0",
        "id":initialization.to_string(),"result":{"protocolVersion":crate::mcp::PROTOCOL_VERSION,
        "capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}}))?;
    stdout.push(b'\n');
    stdout.extend(crate::contract::canonical_json_bytes(
        &serde_json::json!({"jsonrpc":"2.0",
        "id":operation.to_string(),"result":value}),
    )?);
    stdout.push(b'\n');
    let mut result = NativeProcessResult {
        success: false,
        exit_code: Some(1),
        stdout,
        stderr: vec![],
        stop: None,
    };
    assert_eq!(request.mcp_response(&result)?, value);
    result.stop = Some(crate::filesystem::NativeProcessStop {
        kind: NativeProcessStopKind::Timeout,
        message: "stopped capture".into(),
        cleanup_completed: true,
    });
    assert!(
        matches!(request.mcp_response(&result), Err(Error::Indeterminate(id)) if id == operation)
    );
    Ok(())
}

#[tokio::test]
#[cfg(windows)]
async fn native_view_rejects_physical_aliases_before_second_materialization() -> Result<()> {
    let fixture = process_fixture("write", true).await?;
    for names in [["same", "SAME"], ["parent", "PARENT/child"]] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let mut options = fixture.view.manifest().options.clone();
        options.root = directory.path().to_path_buf();
        let bindings = fixture.view.manifest().volumes[..2]
            .iter()
            .zip(names)
            .map(|(volume, path)| NativeVolumeBinding {
                host: fixture.files.clone(),
                volume: volume.volume.clone(),
                generation: volume.generation.clone(),
                path: path.into(),
                writable: volume.writable,
                verifier: fixture.issuer.verifier(),
                scope: fixture.scope.clone(),
                publication: acyclic_fs::PublicationPermit::Unrestricted,
            })
            .collect();
        assert!(matches!(
            NativeVolumeView::prepare(&fixture.owner, options, bindings).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(
            std::fs::read(directory.path().join(names[0]).join("input.txt"))
                .map_err(|error| Error::Storage(error.to_string()))?,
            b"pinned input"
        );
    }
    Ok(())
}

#[tokio::test]
async fn publication_races_preserve_output_and_do_not_claim_joint_volume_success() -> Result<()> {
    for mode in ["pause-write", "partial-publication"] {
        let fixture = process_fixture(mode, true).await?;
        let process = fixture.provider().await?;
        let effects = Arc::new(fixture.effects(process.clone())?);
        let running = {
            let effects = effects.clone();
            let owner = fixture.owner.clone();
            let command = fixture.command;
            let plan = fixture.plan.clone();
            tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await })
        };
        let root = &fixture.view.manifest().options.root;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !root.join("publication-ready").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        let changed = if mode == "pause-write" {
            &fixture.destination
        } else {
            &fixture.second_destination
        };
        let grant = ContentGrant::verify(
            &fixture.issuer.verifier(),
            &fixture.scope,
            changed,
            VolumeOperation::Write,
        )?;
        fixture
            .files
            .put_content(
                changed,
                &grant,
                "concurrent.txt",
                b"concurrent writer",
                "text/plain",
                "concurrent.txt",
                128,
                &IdempotencyKey::new("concurrent-destination")?,
            )
            .await?;
        std::fs::write(root.join("publication-release"), b"release")
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            running
                .await
                .map_err(|error| Error::Storage(error.to_string()))??,
            EffectStatus::Indeterminate
        );
        let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
        let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
        let output = process
            .output(attempt)
            .await?
            .ok_or_else(|| Error::Storage("publication fault lost native output".into()))?;
        let output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(&output).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(output.success && output.stop.is_none());
        assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
        let first = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&first, None, "/output.txt", 64)
                .await
                .is_ok(),
            mode == "partial-publication"
        );
        let changed_ref = workspace_ref(changed.provider().clone(), &changed.storage_name()?)?;
        assert_eq!(
            fixture
                .files
                .read(&changed_ref, None, "/concurrent.txt", 64)
                .await?
                .as_ref(),
            b"concurrent writer"
        );
        assert!(
            fixture
                .files
                .read(&changed_ref, None, "/output.txt", 64)
                .await
                .is_err()
        );
        std::fs::remove_file(root.join("dst/output.txt"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            EffectStatus::Indeterminate
        );
        assert!(!root.join("dst/output.txt").exists());
        assert_eq!(
            fixture
                .task
                .task_host()
                .outcome(fixture.owner.task_binding().0)
                .await?,
            None
        );
        assert!(fixture.owner.verify(false).await.is_ok());
    }
    Ok(())
}

#[tokio::test]
async fn timeout_and_output_overflow_retain_cleanup_failure_without_publication() -> Result<()> {
    for mode in ["timeout", "flood"] {
        let fixture = process_fixture(mode, true).await?;
        let process = fixture.provider().await?;
        let effects = fixture.effects(process)?;
        let status = effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?;
        let EffectStatus::Succeeded { result } = &status else {
            return Err(Error::Storage(format!(
                "stopped capture unobserved: {status:?}"
            )));
        };
        let output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(result).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(!output.success);
        let stop = output
            .stop
            .ok_or_else(|| Error::Storage("missing stopped-capture reason".into()))?;
        assert!(stop.cleanup_completed);
        assert_eq!(
            stop.kind,
            if mode == "timeout" {
                NativeProcessStopKind::Timeout
            } else {
                NativeProcessStopKind::OutputLimit
            }
        );
        assert!(output.stdout.len() + output.stderr.len() <= 8192);
        if mode == "flood" {
            assert_eq!(output.stdout.len() + output.stderr.len(), 8192);
        }
        let destination = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert!(
            fixture
                .files
                .read(&destination, None, "/output.txt", 64)
                .await
                .is_err()
        );
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            status
        );
        if mode == "timeout" {
            tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
            assert!(
                !fixture
                    .view
                    .manifest()
                    .options
                    .root
                    .join("escaped")
                    .exists()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn approved_bash_preserves_ordered_arguments_and_selected_environment() -> Result<()> {
    let fixture = process_fixture("bash", true).await?;
    assert_eq!(fixture.view.manifest().volumes.len(), 3);
    assert_ne!(
        fixture.view.manifest().volumes[0].volume.provider(),
        fixture.view.manifest().volumes[2].volume.provider()
    );
    let effects = fixture.effects(fixture.provider().await?)?;
    let status = effects
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = status else {
        return Err(Error::Storage(format!("Bash did not complete: {status:?}")));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&result).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        output.success,
        "Bash exit {:?}: {}",
        output.exit_code,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"approved-bash|first arg|second arg");
    assert_eq!(output.stderr, b"approved-env");
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/auxiliary.txt", 64)
            .await?
            .as_ref(),
        b"auxiliary input"
    );
    Ok(())
}

#[tokio::test]
async fn bash_source_mutation_preserves_sdk_sources_and_rejects_all_publication() -> Result<()> {
    let fixture = process_fixture("mutate-source-bash", true).await?;
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name()?,
    )?;
    let before = fixture.files.resolve(&source).await?.generation;
    let process = fixture.provider().await?;
    let effects = fixture.effects(process.clone())?;
    assert_eq!(
        effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        EffectStatus::Indeterminate
    );
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    assert_eq!(
        fixture
            .files
            .read(&source, None, "/input.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await
            .is_err()
    );
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes(),
    );
    let output = process
        .output(attempt)
        .await?
        .ok_or_else(|| Error::Storage("source mutation lost observed output".into()))?;
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&output).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(output.success);
    std::fs::remove_file(fixture.view.manifest().options.root.join("dst/output.txt"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        EffectStatus::Indeterminate
    );
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn approved_bash_uses_nineteen_independently_selected_volumes() -> Result<()> {
    let fixture = process_fixture("many-bash", true).await?;
    assert_eq!(fixture.view.manifest().volumes.len(), 19);
    let status = fixture
        .effects(fixture.provider().await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/many.txt", 512)
            .await?
            .as_ref(),
        "extra input|".repeat(16).as_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn stock_task_commands_select_existing_effect_registry_explicitly() -> Result<()> {
    use crate::filesystem::{EFFECT_TASK_COMMAND_KIND, TaskCommandHost as _, TaskCommandProgress};
    let fixture = process_fixture("write", true).await?;
    let (task, fence) = fixture.owner.task_binding();
    let context = fixture
        .task
        .harness()
        .durable_context(task, OperationId::from_bytes(task.into_bytes()))
        .await?;
    let payload = crate::contract::canonical_json_bytes(&fixture.plan)?;
    let payload_ref = fixture
        .results
        .stage(
            fixture.command,
            "process/effect-command.json",
            &payload,
            "application/json",
            "effect-command.json",
        )
        .await?;
    let command = WorkflowCommand {
        operation_id: fixture.command,
        kind: EFFECT_TASK_COMMAND_KIND.into(),
        payload: payload_ref,
    };
    let value =
        serde_json::to_value(&fixture.plan).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        fixture
            .task
            .commands()
            .execute(
                context.clone(),
                fence.clone(),
                command.clone(),
                value.clone()
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let commands = fixture
        .task
        .commands()
        .with_effects(Arc::new(fixture.effects(fixture.provider().await?)?));
    let TaskCommandProgress::Ready(result) = commands
        .execute(
            context.clone(),
            fence.clone(),
            command.clone(),
            value.clone(),
        )
        .await?
    else {
        return Err(Error::Storage("native command was not ready".into()));
    };
    let Outcome::Succeeded(file) = serde_json::from_value::<Outcome<FileRef>>(result.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?
    else {
        return Err(Error::Storage("native command was not successful".into()));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&file).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(output.success);
    let TaskCommandProgress::Ready(replayed) = commands
        .execute(context, fence.clone(), command.clone(), value)
        .await?
    else {
        return Err(Error::Storage("native command replay was not ready".into()));
    };
    assert_eq!(result, replayed);
    let mut session = fixture.task.open_task(task, fence).await?;
    session
        .step(
            OperationId::from_bytes([66; 16]),
            IdempotencyKey::new("native-workflow-command")?,
            serde_json::to_value(command).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .await?;
    assert!(
        matches!(fixture.task.run_task(fixture.lease.clone(), &commands, 2).await?, crate::filesystem::TaskWorkerOutcome::Completed { task: completed } if completed == task)
    );
    assert!(
        matches!(fixture.task.task_host().outcome(task).await?, Some(Outcome::Succeeded(value)) if value["commands"][0]["value"] == result)
    );
    Ok(())
}

#[tokio::test]
async fn nonzero_exit_preserves_output_and_is_not_implicitly_replayed() -> Result<()> {
    let fixture = process_fixture("nonzero", true).await?;
    let process = fixture.provider().await?;
    let effects = fixture.effects(process)?;
    let status = effects
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(Error::Storage(format!(
            "nonzero invocation unobserved: {status:?}"
        )));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(result).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(!output.success);
    assert_eq!(output.exit_code, Some(22));
    assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("approved stderr"));
    std::fs::remove_file(fixture.view.manifest().options.root.join("dst/output.txt"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        status
    );
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn real_process_requires_approval_and_durable_dispatch_then_reconciles_receipt() -> Result<()>
{
    let denied = process_fixture("write", false).await?;
    assert!(matches!(
        denied.provider().await,
        Err(Error::Unauthorized(_))
    ));
    assert!(
        !denied
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let fixture = process_fixture("write", true).await?;
    let process = fixture.provider().await?;
    let effect_id = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect_id.into_bytes());
    let unadmitted = EffectDispatch {
        provider: fixture.plan.provider.clone(),
        effect_id,
        attempt_id: attempt,
        effect_kind: fixture.plan.effect_kind.clone(),
        request: fixture.plan.request.clone(),
        guarantee: fixture.plan.guarantee,
        request_digest: [0; 32],
    };
    assert!(matches!(
        process.dispatch(unadmitted).await,
        Err(Error::Unauthorized(_))
    ));
    let status = fixture
        .effects(process.clone())?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(Error::Storage(format!(
            "native process did not complete: {status:?}"
        )));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(result).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(output.success);
    assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("approved stderr"));
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&source, None, "/input.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let weak = Arc::downgrade(&process);
    drop(process);
    assert!(weak.upgrade().is_none());
    let recovered: Process = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        fixture.plan.clone(),
        fixture.approval,
        fixture.authority(),
    )
    .await?;
    assert_eq!(
        recovered
            .reconcile(attempt)
            .await?
            .map(|receipt| receipt.status),
        Some(status)
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_after_actual_provider_drop_keeps_receipt_and_cleans_descendants() -> Result<()>
{
    let fixture = process_fixture("wait", true).await?;
    let process = fixture.provider().await?;
    let weak = Arc::downgrade(&process);
    let effects = fixture.effects(process.clone())?;
    let owner = fixture.owner.clone();
    let plan = fixture.plan.clone();
    let command = fixture.command;
    let dispatch =
        tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await });
    let root = fixture.view.manifest().options.root.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    dispatch.abort();
    let _ = dispatch.await;
    drop(process);
    assert!(weak.upgrade().is_none());
    fixture
        .task
        .task_host()
        .cancel(fixture.owner.task_binding().0)
        .await?;
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        fixture.plan.clone(),
        fixture.approval,
        fixture.authority(),
    )
    .await?;
    let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
    let path = format!("harness/v2/native-process/{attempt}");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while fixture.owner.stream().bounds(&path).await?.tail != 2 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Result::Ok(())
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))??;
    let captured = recovered
        .output(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing cancellation output".into()))?;
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&captured).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(!output.success);
    assert!(
        matches!(output.stop, Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed)
    );
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(!root.join("escaped").exists());
    assert!(fixture.owner.verify(false).await.is_err());
    assert!(fixture.owner.verify(true).await.is_ok());
    let known = fixture
        .effects(Arc::new(recovered))?
        .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
        .await?;
    assert_eq!(known, EffectStatus::Succeeded { result: captured });
    let (task, fence) = fixture.owner.task_binding();
    fixture
        .task
        .task_host()
        .settle_task(task, fence, Outcome::Cancelled)
        .await?;
    assert_eq!(
        fixture.task.task_host().outcome(task).await?,
        Some(Outcome::Cancelled)
    );
    assert!(fixture.owner.verify(true).await.is_err());
    assert!(fixture.directory.path().exists());
    Ok(())
}
