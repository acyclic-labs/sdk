//! Real admitted compaction captured into a child-owned fork environment.
use super::*;
use crate::context::{Context, ModelContextCapacity, ModelTokenCount};
use crate::distributed::DistributedCoordinator;
use crate::durable_host::CoordinatorTaskHost;
use crate::filesystem::workspace_ref;
use crate::filesystem::{FilesystemForkPreparer, FilesystemForkVerifier};
use crate::fork::{
    CompositeForkVerifier, ForkHistoryPolicy, ForkPreparation, ForkRequest, ForkSeedVerifier,
    ForkSelection, ResourceRevision, StreamHistoryForkVerifier, SummaryForkSelection,
    prepare_summary_fork_context,
};
use crate::model::PreparedModelRequest;
use crate::model::{Model, ModelProvider};
use crate::resources::StreamRef;
use crate::runtime::TaskSpawner;
use crate::runtime::{TaskAdmissionRecord, TaskDefinition, TaskRegistry};
use crate::store::{HistoryReadLimits, HistoryReader};
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
use crate::workflow::{
    MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
};

fn summary_model(turns: usize) -> ScriptedModelProvider {
    summary_model_turns(std::iter::repeat_n(
        ScriptedTurn::Text("admitted summary or answer".into()),
        turns,
    ))
}

fn summary_model_turns(turns: impl IntoIterator<Item = ScriptedTurn>) -> ScriptedModelProvider {
    ScriptedModelProvider::new(turns)
        .with_context_capacity(ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: Some(4_096),
        })
        .with_token_counter(|request| {
            let message_tokens = request
                .request()
                .messages
                .iter()
                .map(|message| {
                    let bytes = message.content.file_refs().try_fold(0_u64, |bytes, file| {
                        bytes
                            .checked_add(file.descriptor().byte_length())
                            .ok_or_else(|| Error::Invalid("synthetic accounting overflow".into()))
                    })?;
                    u32::try_from(bytes + 512).map_err(|error| Error::Invalid(error.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                512,
                message_tokens,
            ))
        })
}

struct SummaryLeaseMachine {
    identity: MachineIdentity,
    schema: Value,
}

impl ResumableMachine for SummaryLeaseMachine {
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
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Completed {
                value: input.clone(),
            },
        })
    }
}

/// Authenticates a real ordinary task lease through the public owner API.
async fn sealed_summary_preparation(
    storage: &MemoryHarnessStorage,
    preparer: &FilesystemForkPreparer<MemoryAuthorityBackend, MemoryObjectBackend>,
    fork: &ForkRequest,
    maximum_work: u64,
    maximum_bytes: u64,
) -> Result<(
    crate::resource_preparation::TaskResourcePreparation,
    crate::filesystem::default_tools::InvocationCancellation,
)> {
    let owner_id = OperationId::new();
    let authority = Authority {
        kind: AggregateKind::Task,
        id: format!("summary-preparation-{owner_id}"),
    };
    let issuer = AuthorityIssuer::new("summary-preparation", [173; 32], authority.clone());
    let agent = storage
        .scope
        .agent()
        .ok_or_else(|| Error::Invalid("Summary parent agent missing".into()))?;
    let payload_volume = VolumeRef::new(
        storage.volume.provider().clone(),
        format!("summary-preparation-payloads-{owner_id}"),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    storage.host.create_volume(&payload_volume).await?;
    let mut grants = storage
        .scope
        .capabilities()
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    grants.extend([
        "operation:declare".into(),
        "operation:observe".into(),
        "operation:cancel".into(),
        "task:spawn:test.summary-preparation@1".into(),
        "fork:publish".into(),
        payload_volume.capability(VolumeOperation::Read, &mut None)?,
        payload_volume.capability(VolumeOperation::Write, &mut None)?,
    ]);
    for selection in &fork.selections {
        if let ResourceRevision::Project { volume, .. }
        | ResourceRevision::PrivateVolume { volume, .. } = &selection.revision
        {
            grants.push(volume.capability(VolumeOperation::Read, &mut None)?);
        }
    }
    for grant in preparer.required_reference_grants(fork, None).await? {
        if grant.reader == fork.child_agent {
            grants.push(grant.file.read_capability(&mut None)?);
        }
    }
    let grants = Capabilities::new(grants);
    let signed = issuer
        .issue("summary-preparation", &grants, Some(agent), None, &mut None)
        .expect("scope proof");
    let runtime_scope = RuntimeScope::new(grants, Limits::default())?.with_run_limits(
        crate::runtime::TaskRunLimits {
            concurrency: Some(4),
            max_steps: Some(4),
            deadline_epoch_ms: None,
        },
        &mut None,
    )?;
    let payloads = crate::filesystem::FilesystemContentPublisher::new(
        storage.host.clone(),
        &(payload_volume),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &signed,
        2_097_152,
        None,
    )?;
    let reader = FilesystemContentVerifier::new(
        storage.host.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &signed,
        2_097_152,
        None,
    )?;
    let schema = serde_json::json!({"type":"object"});
    let implementation = Arc::new(SummaryLeaseMachine {
        identity: MachineIdentity {
            name: "test.summary-preparation".into(),
            version: "1".into(),
            digest: *blake3::hash(include_str!("mod.rs").as_bytes()).as_bytes(),
        },
        schema: schema.clone(),
    });
    let definition = TaskDefinition::<Value, Value>::resumable(
        implementation.clone(),
        schema.clone(),
        schema.clone(),
    )?;
    let identity = definition.identity().clone();
    let machine = implementation.identity.clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(implementation, &mut None)?;
    let task_stream = Arc::new(MemoryStream::default());
    let host = Arc::new(
        CoordinatorTaskHost::new(
            DistributedCoordinator::open(&task_stream, reader.clone(), None, &mut None).await?,
            task_stream,
            payloads.clone(),
            reader,
            authority,
            signed,
            issuer.verifier(&mut None).expect("verifier copy"),
            runtime_scope.clone(),
            tasks,
            machines,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
        )?
        .with_session_limits(crate::scheduler::SessionLimits {
            active_tasks: 4,
            total_tasks: 4,
            depth: 2,
            model_steps: 4,
        })?
        .with_resource_creation_providers(
            &[Arc::new(
                crate::filesystem::FilesystemResourceCreationProvider::new(
                    storage.host.clone(),
                    &issuer,
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                )?,
            )],
            None,
        )?,
    );
    let admission = |operation_id, parent, input| TaskAdmissionRecord {
        resource_proofs: Vec::new(),
        operation_id,
        task: identity.clone(),
        machine: machine.clone(),
        input,
        input_schema: schema.clone(),
        output_schema: schema.clone(),
        parent,
        grants: runtime_scope.grants().clone(),
        limits: runtime_scope.limits(),
        run_limits: runtime_scope.run_limits(),
        policy: None,
        extensions: None,
        execution: None,
    };
    let root_op = OperationId::new();
    let root = crate::TaskId::from_bytes(root_op.into_bytes());
    assert!(
        matches!(host.admit(admission(root_op, None, serde_json::json!({}))).await?,
        crate::Admission::Accepted(id) if id == root)
    );
    let worker = crate::distributed::Worker {
        id: "summary-preparation-worker".into(),
        available: Default::default(),
        labels: BTreeMap::new(),
    };
    let crate::distributed::WorkPull::Claimed(root_lease) =
        host.pull_work(&worker, None, &mut None).await?
    else {
        return Err(Error::NotFound("Summary root lease missing".into()));
    };
    host.start_task(&root_lease, &mut None).await?;
    let prep_op = OperationId::new();
    let prep_task = crate::TaskId::from_bytes(prep_op.into_bytes());
    let proofs = preparer.creation_allocations(fork, prep_task, None)?;
    let child = admission(OperationId::new(), Some(root), serde_json::json!({}))
        .with_resource_proofs(proofs)?;
    let command_payload = crate::distributed::SchedulerPayloadStore::stage(
        payloads.as_ref(),
        fork.operation_id,
        "summary-fork-request",
        &crate::contract::canonical_json_bytes(fork)?,
        None,
    )?
    .await?;
    let request = crate::resource_preparation::ResourcePreparationRequest {
        provider: storage.volume.provider().clone(),
        child,
        creation_grant: "fork:publish".into(),
        maximum_resources: 2,
        maximum_work,
        maximum_bytes,
        payload: serde_json::to_value(fork).map_err(|error| Error::Invalid(error.to_string()))?,
        command: Some(crate::workflow::WorkflowCommand {
            operation_id: fork.operation_id,
            kind: crate::filesystem::FORK_PREPARE_TASK_COMMAND_KIND.into(),
            payload: command_payload,
        }),
        source_checkpoint: None,
        input_files: Vec::new(),
        read_authorization: fork.authorized_source.clone(),
    };
    assert!(matches!(host.admit_owned(admission(prep_op, Some(root),
        serde_json::to_value(&request).map_err(|error| Error::Invalid(error.to_string()))?),
        (&root_lease.reservation).into()).await?, crate::Admission::Accepted(id) if id == prep_task));
    let crate::distributed::WorkPull::Claimed(lease) =
        host.pull_work(&worker, None, &mut None).await?
    else {
        return Err(Error::NotFound("Summary preparation lease missing".into()));
    };
    host.start_task(&lease, &mut None).await?;
    let invocation = crate::filesystem::default_tools::InvocationCancellation(
        acyclic_fs::CancellationToken::new(),
    );
    let owner = Arc::new(
        host.journal_owner(prep_task, &((&lease.reservation).into()), None)
            .await?
            .with_invocation_cancellation(&invocation.0),
    );
    Ok((
        owner.resource_preparation(request, &mut None).await?,
        invocation,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
mod accounting;
mod compaction;
mod publication;
mod retirement;

struct ColdSummaryStream {
    client: Arc<MemoryStream>,
    reads: std::sync::atomic::AtomicUsize,
    maximum: std::sync::atomic::AtomicU32,
    fail_read: std::sync::atomic::AtomicUsize,
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl acyclic_stream::StreamProvider for ColdSummaryStream {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.client.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        _: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, acyclic_stream::StreamError> {
        self.client.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        self.client.bounds(path).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        use std::sync::atomic::Ordering;
        let number = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(request.limit, Ordering::SeqCst);
        if number == self.fail_read.load(Ordering::SeqCst) {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.client.read(request).await
    }
    async fn read_commit(
        &self,
        commit: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.client.read_commit(commit).await
    }
    async fn append(
        &self,
        _: acyclic_stream::AppendRequest,
    ) -> std::result::Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn fork(
        &self,
        _: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn commit(
        &self,
        _: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn follow(
        &self,
        _: acyclic_stream::StreamPath,
        _: u64,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
    async fn children(
        &self,
        _: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        Err(acyclic_stream::StreamError::Unsupported)
    }
}

async fn append_later_summary_parent_messages(
    storage: &MemoryHarnessStorage,
    parent: &StreamAggregate<MemoryStream>,
    count: usize,
) -> Result<()> {
    let content = parent
        .history_reader()?
        .latest_conversation_message(262_144)
        .await?
        .ok_or_else(|| Error::Invalid("missing parent message".into()))?
        .content;
    let mut writer = StreamAggregate::open(
        &storage.stream,
        (
            storage.conversation.clone(),
            storage.verifier(),
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(storage.content_verifier.clone());
    for index in 0..count {
        let sequence = writer
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("missing conversation".into()))?
            .logical_revision()
            + 1;
        writer
            .execute(
                Command {
                    operation_id: OperationId::new(),
                    idempotency_key: IdempotencyKey::new(format!("later-summary-parent:{index}"))?,
                    expected_revision: writer.reducer().revision(),
                    scope: storage.scope.clone(),
                    causal_parent: None,
                    action: Action::AppendConversationMessage {
                        message: Box::new(ConversationMessage {
                            id: Uuid::new_v4(),
                            sequence,
                            kind: MessageKind::System,
                            content: content.clone(),
                            attachments: crate::conversation::ReferencedAttachments::Inline {
                                items: Vec::new(),
                            },
                            reply_to: None,
                            tool_call_id: None,
                            extensions: BTreeMap::new(),
                        }),
                    },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one cold import with exact byte/work bounds, read failure and retained-history scaling"
)]
async fn assert_cold_summary_reads(
    storage: &MemoryHarnessStorage,
    parent: &StreamAggregate<MemoryStream>,
    seed: &crate::fork::ForkSeed,
    issuer: &AuthorityIssuer,
    scope: &Scope,
    reader: Arc<dyn ContentResidencyVerifier>,
    limits: Limits,
) -> Result<crate::context::PinnedContextStage> {
    use std::sync::atomic::Ordering;
    let parent_history = parent.history_reader()?;
    let child_history = HistoryReader::new(
        &storage.stream,
        &seed.child,
        &(issuer.verifier(&mut None).expect("verifier copy")),
        &mut None,
    )?;
    let binding = child_history
        .read_page(
            &child_history.pin(0, None).await?,
            HistoryReadLimits {
                maximum_events: 1,
                maximum_bytes: 262_144,
            },
            None,
        )
        .await?
        .events
        .into_iter()
        .next()
        .ok_or_else(|| Error::Invalid("missing binding".into()))?;
    let (_, parent_bytes) = parent_history
        .operation_event(seed.operation_id, 262_144, &mut None)
        .await?;
    let (_, binding_bytes) = child_history
        .operation_event(binding.operation_id, 262_144, &mut None)
        .await?;
    let history_limits = HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: parent_bytes + binding_bytes,
    };
    let mut stage = None;
    for later_messages in [0, 1_000] {
        append_later_summary_parent_messages(storage, parent, later_messages).await?;
        let observed = Arc::new(ColdSummaryStream {
            client: storage.stream.clone(),
            reads: 0.into(),
            maximum: 0.into(),
            fail_read: 0.into(),
        });
        let client = observed.clone();
        let cold_parent =
            HistoryReader::new(&client, &seed.parent, &(storage.verifier()), &mut None)?;
        let cold_child = HistoryReader::new(
            &client,
            &seed.child,
            &(issuer.verifier(&mut None).expect("verifier copy")),
            &mut None,
        )?;
        if later_messages == 0 {
            for allowance in [
                HistoryReadLimits {
                    maximum_events: 1,
                    ..history_limits
                },
                HistoryReadLimits {
                    maximum_bytes: 0,
                    ..history_limits
                },
            ] {
                assert!(matches!(
                    cold_child
                        .summary_fork_stage(
                            &cold_parent,
                            seed,
                            scope,
                            reader.clone(),
                            limits,
                            allowance
                        )
                        .await,
                    Err(Error::Invalid(_))
                ));
                assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
            }
            assert!(matches!(
                cold_child
                    .summary_fork_stage(
                        &cold_parent,
                        seed,
                        scope,
                        reader.clone(),
                        limits,
                        HistoryReadLimits {
                            maximum_bytes: history_limits.maximum_bytes - 1,
                            ..history_limits
                        }
                    )
                    .await,
                Err(Error::Invalid(_))
            ));
            assert_eq!(observed.reads.load(Ordering::SeqCst), 4);
            observed.reads.store(0, Ordering::SeqCst);
            observed.fail_read.store(3, Ordering::SeqCst);
            assert!(
                cold_child
                    .summary_fork_stage(
                        &cold_parent,
                        seed,
                        scope,
                        reader.clone(),
                        limits,
                        history_limits
                    )
                    .await
                    .is_err()
            );
            assert_eq!(observed.reads.load(Ordering::SeqCst), 3);
            observed.reads.store(0, Ordering::SeqCst);
            observed.fail_read.store(0, Ordering::SeqCst);
        }
        let cold_parent =
            HistoryReader::new(&client, &seed.parent, &(storage.verifier()), &mut None)?;
        let cold_child = HistoryReader::new(
            &client,
            &seed.child,
            &(issuer.verifier(&mut None).expect("verifier copy")),
            &mut None,
        )?;
        stage = Some(
            cold_child
                .summary_fork_stage(
                    &cold_parent,
                    seed,
                    scope,
                    reader.clone(),
                    limits,
                    history_limits,
                )
                .await?,
        );
        assert_eq!(observed.reads.load(Ordering::SeqCst), 4);
        assert_eq!(observed.maximum.load(Ordering::SeqCst), 1);
    }
    stage.ok_or_else(|| Error::Invalid("cold Summary fixture did not run".into()))
}
