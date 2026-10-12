//! Real admitted compaction captured into a child-owned fork environment.
use super::*;
use crate::filesystem::{PreparationIo, workspace_ref};
use crate::context::{Context, ModelContextCapacity, ModelTokenCount};
use crate::filesystem::{FilesystemForkPreparer, FilesystemForkVerifier};
use crate::fork::{
    CompositeForkVerifier, ForkHistoryPolicy, ForkPreparation, ForkRequest, ForkSeedVerifier,
    ForkSelection, ResourceRevision, StreamHistoryForkVerifier, SummaryForkSelection,
    prepare_summary_fork_context,
};
use crate::model::{ModelDispatch, ModelEvent, PreparedModelRequest};
use crate::resources::StreamRef;
use crate::store::{HistoryReadLimits, HistoryReader};
use std::sync::Mutex;
use acyclic_stream::StreamProvider as _;
use crate::distributed::{DistributedCoordinator, SchedulerPayloadStore as _};
use crate::durable_host::CoordinatorTaskHost;
use crate::runtime::{DurableTaskHost as _, TaskAdmissionRecord, TaskDefinition, TaskRegistry};
use crate::workflow::{MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine};

#[derive(Default)]
struct SummaryModel(Mutex<Vec<PreparedModelRequest>>);

impl ModelProvider for SummaryModel {
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 4_096,
        })
    }
    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        let message_tokens = request
            .request()
            .messages
            .iter()
            .map(|message| {
                let bytes = message
                    .content
                    .file_refs()
                    .try_fold(0_u64, |bytes, file| {
                        bytes
                            .checked_add(file.descriptor().byte_length())
                            .ok_or_else(|| Error::Invalid("synthetic accounting overflow".into()))
                    })?;
                u32::try_from(bytes + 512).map_err(|error| Error::Invalid(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ModelTokenCount {
            request_digest: request.request_digest(),
            fixed_tokens: 512,
            message_tokens,
        })
    }
    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        assert_eq!(dispatch.request_digest, request.request_digest());
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content {
                delta: "admitted summary or answer".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }
    fn reconcile<'a>(&'a self, _dispatch: crate::model::ModelDispatch, _observed: &'a [crate::model::ModelEvent]) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fixture attempts settle immediately".into(),
            ))
        })
    }
}

const FINITE_SUMMARY_BUDGET: acyclic_fs::WorkBudget = acyclic_fs::performance::work_budget(1_000_000, 134_217_728);

struct SummaryLeaseMachine {
    identity: MachineIdentity,
    schema: Value,
}

impl ResumableMachine for SummaryLeaseMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> { Ok(input.clone()) }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(), commands: Vec::new(),
            status: MachineStatus::Completed { value: input.clone() },
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
) -> Result<(crate::resource_preparation::TaskResourcePreparation, crate::filesystem::InvocationCancellation)> {
    let owner_id = OperationId::new();
    let authority = Authority { kind: AggregateKind::Task, id: format!("summary-preparation-{owner_id}") };
    let issuer = AuthorityIssuer::new("summary-preparation", [173; 32], authority.clone());
    let agent = storage.scope.agent().ok_or_else(|| Error::Invalid("Summary parent agent missing".into()))?;
    let payload_volume = VolumeRef::new(storage.volume.provider().clone(),
        format!("summary-preparation-payloads-{owner_id}"), VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent))?;
    storage.host.create_volume(&payload_volume).await?;
    let mut grants = storage.scope.capabilities().iter().map(str::to_owned).collect::<Vec<_>>();
    grants.extend([
        "operation:declare".into(), "operation:observe".into(), "operation:cancel".into(),
        "task:spawn:test.summary-preparation@1".into(), "fork:publish".into(),
        payload_volume.capability(VolumeOperation::Read)?,
        payload_volume.capability(VolumeOperation::Write)?,
    ]);
    for selection in &fork.selections {
        if let ResourceRevision::Project { volume, .. } | ResourceRevision::PrivateVolume { volume, .. } = &selection.revision {
            grants.push(volume.capability(VolumeOperation::Read)?);
        }
    }
    let grants = Capabilities::new(grants);
    let signed = issuer.issue("summary-preparation", &grants, Some(agent), None, &mut None).expect("scope proof");
    let runtime_scope = RuntimeScope::new(grants, Limits::default())?
        .with_run_limits(crate::runtime::TaskRunLimits {
            concurrency: Some(4), max_steps: Some(4), deadline_unix_millis: None,
        })?;
    let payloads = Arc::new(crate::filesystem::FilesystemSchedulerPayloadStore::new(storage.host.clone(), &(payload_volume), &issuer.verifier(&mut None).expect("verifier copy"), &signed, 2_097_152, None)?);
    let reader = Arc::new(FilesystemContentVerifier::new(storage.host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &signed, 2_097_152, None)?);
    let schema = serde_json::json!({"type":"object"});
    let implementation = Arc::new(SummaryLeaseMachine {
        identity: MachineIdentity { name: "test.summary-preparation".into(), version: "1".into(),
            digest: *blake3::hash(include_str!("memory_summary_tests.rs").as_bytes()).as_bytes() },
        schema: schema.clone(),
    });
    let definition = TaskDefinition::<Value, Value>::resumable(implementation.clone(), schema.clone(), schema.clone())?;
    let identity = definition.identity().clone();
    let machine = implementation.identity.clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(implementation)?;
    let host = Arc::new(CoordinatorTaskHost::new(
        DistributedCoordinator::open(&storage.stream, payloads.clone()).await?,
        storage.stream.clone(), payloads.clone(), reader, authority, signed,
        issuer.verifier(&mut None).expect("verifier copy"), runtime_scope.clone(), tasks, machines,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
    )?.with_session_limits(crate::scheduler::SessionLimits {
        active_tasks: 4, total_tasks: 4, depth: 2, model_steps: 4,
    })?.with_resource_creation_providers(vec![Arc::new(
        crate::filesystem::FilesystemResourceCreationProvider::new(storage.host.clone(), &issuer, #[cfg(not(target_arch = "wasm32"))] None)?)])?);
    let admission = |operation_id, parent, input| TaskAdmissionRecord {
        resource_proofs: Vec::new(), operation_id, task: identity.clone(), machine: machine.clone(),
        input, input_schema: schema.clone(), output_schema: schema.clone(), parent,
        grants: runtime_scope.grants().clone(), limits: runtime_scope.limits(),
        run_limits: runtime_scope.run_limits(), policy: None, extensions: None, execution: None,
    };
    let root_op = OperationId::new();
    let root = crate::TaskId::from_bytes(root_op.into_bytes());
    assert!(matches!(host.admit(admission(root_op, None, serde_json::json!({}))).await?,
        crate::Admission::Accepted(id) if id == root));
    let worker = crate::distributed::Worker {
        id: "summary-preparation-worker".into(), available: Default::default(), labels: BTreeMap::new(),
    };
    let crate::distributed::WorkPull::Claimed(root_lease) = host.pull_work(&worker).await? else {
        return Err(Error::NotFound("Summary root lease missing".into()));
    };
    host.start_task(&root_lease).await?;
    let prep_op = OperationId::new();
    let prep_task = crate::TaskId::from_bytes(prep_op.into_bytes());
    let (resources, proofs) = preparer.creation_allocations(fork, prep_task, None)?;
    let child = admission(OperationId::new(), Some(root), serde_json::json!({})).with_resource_proofs(proofs)?;
    let command_payload = payloads.stage(fork.operation_id, "summary-fork-request",
        &crate::contract::canonical_json_bytes(fork)?).await?;
    let request = crate::resource_preparation::ResourcePreparationRequest {
        provider: storage.volume.provider().clone(), child,
        resources, creation_grant: "fork:publish".into(),
        maximum_resources: 2, maximum_work, maximum_bytes,
        payload: serde_json::to_value(fork).map_err(|error| Error::Invalid(error.to_string()))?,
        command: Some(crate::workflow::WorkflowCommand {
            operation_id: fork.operation_id, kind: crate::filesystem::FORK_PREPARE_TASK_COMMAND_KIND.into(),
            payload: command_payload,
        }),
        source_checkpoint: None, input_files: Vec::new(), read_authorization: fork.authorized_source.clone(),
    };
    assert!(matches!(host.admit_owned(admission(prep_op, Some(root),
        serde_json::to_value(&request).map_err(|error| Error::Invalid(error.to_string()))?),
        (&root_lease.reservation).into()).await?, crate::Admission::Accepted(id) if id == prep_task));
    let crate::distributed::WorkPull::Claimed(lease) = host.pull_work(&worker).await? else {
        return Err(Error::NotFound("Summary preparation lease missing".into()));
    };
    host.start_task(&lease).await?;
    let invocation = crate::filesystem::InvocationCancellation(acyclic_fs::CancellationToken::new());
    let owner = Arc::new(host.journal_owner(prep_task, (&lease.reservation).into()).await?
        .with_invocation_cancellation(&invocation.0));
    Ok((owner.resource_preparation(request).await?, invocation))
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one actual parent compaction, child capture, retry and publication scenario"
)]
async fn summary_fork_captures_private_payloads_and_binds_publication() -> Result<()> {
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 262_144).await?;
    let model = Arc::new(SummaryModel::default());
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let bundle = storage
        .builder()
        .model(
            Model::new("synthetic", "summary-fork", "1", Value::Null)?,
            model.clone(),
        )
        .grant("model:generate")
        .limits(limits)
        .build()?;
    let mut last = OperationId::new();
    for index in 0..2 {
        last = OperationId::new();
        let input = storage
            .stage(
                last,
                &format!("turns/{index}/input.txt"),
                "q".repeat(60_000).as_bytes(),
                "text/plain",
                "input.txt",
            )
            .await?;
        storage
            .run_conversation(&bundle, last, input, Vec::new(), 1)
            .await?;
    }
    assert_eq!(
        model
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        3
    );
    {
        let aggregate = storage.open_conversation(limits).await?;
        let conversation = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
        assert_eq!(conversation.logical_revision(), 4);
        assert_eq!(conversation.resident_after_sequence(), 3);
        assert_eq!(conversation.messages().len(), 1);
        assert_eq!(conversation.messages()[0].sequence, 4);
    }
    let reference =
        crate::executor::canonical_checkpoint_for_operation(storage.journal.as_ref(), last, limits, None)
            .await?
            .ok_or_else(|| Error::Invalid("missing admitted checkpoint".into()))?;
    let later = OperationId::new();
    let later_input = storage.stage(later, "turns/later/input.txt",
        "r".repeat(60_000).as_bytes(), "text/plain", "later-input.txt").await?;
    storage.run_conversation(&bundle, later, later_input, Vec::new(), 1).await?;
    let later_checkpoint = crate::executor::canonical_checkpoint_for_operation(
        storage.journal.as_ref(), later, limits, None).await?
        .ok_or_else(|| Error::Invalid("missing later admitted checkpoint".into()))?;
    assert_ne!(later_checkpoint, reference);
    let mut aggregate = StreamAggregate::open(
        &storage.stream,
        storage.conversation.clone(),
        storage.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    )
    .await?
    .with_content_verifier(storage.content_verifier.clone());
    let conversation = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
    let selection = crate::conversation::ModelContextSelection {
        conversation_revision: conversation.logical_revision(),
        message_ids: conversation
            .messages()
            .iter()
            .map(|message| message.id)
            .collect(),
        checkpoint: Some(reference.clone()),
    };
    aggregate
        .execute(Command {
            operation_id: OperationId::new(),
            idempotency_key: IdempotencyKey::new("summary-parent-complete-cut")?,
            expected_revision: aggregate.reducer().revision(),
            scope: storage.scope.clone(),
            causal_parent: None,
            action: Action::SelectModelContext { selection },
        }, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    aggregate.compact_conversation_projection()?;
    let retired = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
    assert_eq!(retired.logical_revision(), 4);
    assert_eq!(retired.resident_after_sequence(), 4);
    assert!(retired.messages().is_empty());

    let history = HistoryReader::new(&storage.stream, &storage.conversation, storage.verifier(&mut None).expect("verifier copy"))?;
    let cursor = history.pin(0, None).await?;
    let original_scope = RuntimeScope::new(storage.scope.capabilities().clone(), limits)?;
    let prepared = prepare_summary_fork_context(
        aggregate.reducer(),
        SummaryForkSelection {
            checkpoint: reference,
            limits,
            history_limits: HistoryReadLimits {
                maximum_events: 16,
                maximum_bytes: 2_097_152,
            },
        },
        storage.journal.as_ref(),
        &original_scope,
        &history,
        &cursor,
        storage.content_verifier.as_ref(),
    )
    .await?;
    let proof: crate::context::CompactionReference = crate::contract::json_from_slice(
        &storage
            .journal
            .load(&prepared.checkpoint().compaction)
            .await?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(proof.summary.is_some());
    let source = prepared.context().clone();
    let selection = prepared.selection().clone();
    drop(prepared);
    let provider = storage.volume.provider().clone();
    let project = VolumeRef::new(
        provider.clone(),
        "summary-parent-project",
        VolumeClass::Project,
        VolumeOwner::Project("summary-project".into()),
    )?;
    let project_head = storage.host.create_volume(&project).await?;
    let grants = Capabilities::new(
        storage
            .scope
            .capabilities()
            .iter()
            .map(str::to_owned)
            .chain([
                "fork:publish".to_owned(),
                project.capability(VolumeOperation::Read)?,
            ]),
    );
    let scope = storage
        .issuer.issue("summary-parent", &grants, Some(agent), None, &mut None).expect("scope proof");
    let stream_provider = ProviderRef::new("local", "stream", "2")?;
    let archived = history.fork_history(
        aggregate.reducer(), 4,
        HistoryReadLimits { maximum_events: 4, maximum_bytes: 2_097_152 },
     &mut None).await?;
    let preparer = FilesystemForkPreparer::new(
        storage.host.clone(),
        aggregate.reducer().clone(),
        storage.verifier(&mut None).expect("verifier copy"),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        storage.content_verifier.clone(),
    )?
    .with_history_context(archived)?
    .with_summary_history(selection.clone(), storage.journal(), original_scope, history, cursor, None)?;
    let child_agent = AgentId::new();
    let child_private = VolumeRef::new(
        provider.clone(),
        "summary-child-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
    )?;
    let request = ForkRequest {
        authorized_source: None,
        operation_id: OperationId::new(),
        parent: storage.conversation.clone(),
        parent_revision: aggregate.reducer().revision(),
        child: Authority {
            kind: AggregateKind::Conversation,
            id: "summary-child".into(),
        },
        child_agent,
        attached_agents: Vec::new(),
        preparation: ForkPreparation {
            child_result_volume: None,
            summary: None,
            child_project_volume: VolumeRef::new(
                provider,
                "summary-child-project",
                VolumeClass::Project,
                VolumeOwner::Project("summary-project".into()),
            )?,
            child_private_volume: child_private.clone(),
            inherited_through_sequence: 0,
            maximum_inherited_messages: 16,
            maximum_inherited_bytes: 262_144,
            maximum_inherited_references: 64,
        },
        selections: vec![
            ForkSelection {
                required: true,
                revision: ResourceRevision::PrivateVolume {
                    volume: storage.volume.clone(),
                    generation: storage
                        .host
                        .resolve(&crate::filesystem::workspace_ref(
                            storage.volume.provider().clone(),
                            &storage.volume.storage_name()?,
                        )?)
                        .await?
                        .generation,
                    paths: Vec::new(),
                },
            },
            ForkSelection {
                required: true,
                revision: ResourceRevision::History(StreamRef::new(
                    stream_provider.clone(),
                    storage.conversation.stream_path(&mut None)?.into_bytes(),
                    Some(aggregate.reducer().revision().to_string()),
                )?),
            },
            ForkSelection {
                required: true,
                revision: ResourceRevision::Project {
                    volume: project,
                    generation: project_head.generation,
                },
            },
        ],
        boundary: None,
    }
    .with_history_policy(
        aggregate.reducer(),
        ForkHistoryPolicy::Summary(Box::new(selection)), &mut None,
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let proof_io = PreparationIo::from_budget(FINITE_SUMMARY_BUDGET, &cancellation)?;
    let proof_hold;
    let finite = preparer.prepare_bound_summary(&request, Some(&proof_io)).await?
        .ok_or_else(|| Error::Invalid("finite Summary source proof missing".into()))?;
    proof_hold = proof_io.guard_owned(proof_io.owned_bytes()?)?;
    assert_eq!(finite.context(), &source);
    assert_eq!(finite.selection(), request.preparation.summary.as_ref().unwrap());
    assert!(proof_io.receipt().bytes_encoded > 0);
    assert!(proof_io.receipt().bytes_hashed > 0);
    assert!(proof_io.receipt().items_examined > 0);
    drop(finite);
    drop(proof_hold);
    assert_eq!(proof_io.owned_bytes()?, 0);
    let small_io = PreparationIo::from_budget(acyclic_fs::WorkBudget::default(), &cancellation)?;
    assert!(preparer.prepare_bound_summary(&request, Some(&small_io)).await.is_err());
    assert_eq!(small_io.owned_bytes()?, 0);
    let cancelled_io = PreparationIo::from_budget(FINITE_SUMMARY_BUDGET, &cancellation)?;
    cancellation.cancel();
    assert!(preparer.prepare_bound_summary(&request, Some(&cancelled_io)).await.is_err());
    assert_eq!(cancelled_io.receipt(), acyclic_fs::WorkCounters::default());
    let (quota_preparation, _quota_invocation) =
        sealed_summary_preparation(&storage, &preparer, &request, 1, 134_217_728).await?;
    assert!(preparer.prepare_for_task(request.clone(), &quota_preparation).await.is_err());
    let child_private_workspace = workspace_ref(provider.clone(), &child_private.storage_name()?)?;
    assert!(matches!(storage.host.resolve(&child_private_workspace).await, Err(Error::NotFound(_))));
    let (cancelled_preparation, cancelled_invocation) =
        sealed_summary_preparation(&storage, &preparer, &request, 1_000_000, 134_217_728).await?;
    cancelled_invocation.0.cancel();
    assert!(preparer.prepare_for_task(request.clone(), &cancelled_preparation).await.is_err());
    assert!(matches!(storage.host.resolve(&child_private_workspace).await, Err(Error::NotFound(_))));
    let (task_preparation, _successful_invocation) =
        sealed_summary_preparation(&storage, &preparer, &request, 1_000_000, 134_217_728).await?;
    let lost_acknowledgement = preparer.prepare_for_task(request.clone(), &task_preparation).await?;
    let preparer = preparer.with_task_preparation(Arc::new(task_preparation))?;
    let report = aggregate.prepare_fork(&preparer, request.clone()).await?;
    assert_eq!(report, lost_acknowledgement);
    assert_eq!(
        aggregate.prepare_fork(&preparer, request.clone()).await?,
        report
    );
    assert_eq!(
        aggregate.reconcile_fork(&preparer, request.clone()).await?,
        Some(report.clone())
    );
    let capture = report
        .summary
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing Summary capture".into()))?;
    assert!(!capture.payloads.is_empty());
    assert_eq!(report.inherited_through_sequence, 6);
    let bytes = storage.host.read_pinned(&capture.context, 262_144).await?;
    let captured: Context = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(captured.messages.len(), source.messages.len());
    assert_eq!(captured.current_input_index, None);
    let originals = source
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs())
        .chain(source.metadata.values())
        .map(|file| {
            Ok((
                hex::encode(crate::contract::canonical_json_digest(file)?),
                file.clone(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    for copy in &capture.payloads {
        let key = copy
            .path()
            .rsplit('/')
            .next()
            .ok_or_else(|| Error::Invalid("missing copy key".into()))?;
        let original = originals
            .get(key)
            .ok_or_else(|| Error::Invalid("copy is not source-bound".into()))?;
        assert_eq!(copy.volume(), &child_private);
        assert_eq!(copy.descriptor(), original.descriptor());
        assert_eq!(
            storage.host.read_pinned(copy, 262_144).await?.as_ref(),
            storage.journal.load(original).await?.as_ref()
        );
    }
    let seed = report.into_seed(&mut None)?;
    assert!(seed.history_witness.is_some());
    for mutation in 0..4 {
        let mut changed = seed.clone();
        match mutation {
            0 => { changed.history_witness = None; }
            1 => { changed.reference_grants.pop(); }
            2 => { changed.attached_agents.push(AgentId::new()); }
            _ => { changed.inherited_through_sequence -= 1; }
        }
        let mut reducer = aggregate.reducer().clone();
        assert!(reducer.apply(Command {
            operation_id: changed.operation_id,
            idempotency_key: IdempotencyKey::new(format!("summary-witness-mutation-{mutation}"))?,
            expected_revision: reducer.revision(),
            scope: scope.clone(),
            causal_parent: None,
            action: Action::PublishFork { seed: Box::new(changed) },
        }).is_err());
    }
    let fs_verifier = Arc::new(FilesystemForkVerifier::new(storage.host.clone(), 262_144)?);
    fs_verifier.verify(&seed).await?;
    assert!(matches!(
        storage
            .host
            .claim_fork_seed(&seed, aggregate.reducer(), &storage.verifier(&mut None).expect("verifier copy"), &scope)
            .await,
        Err(Error::Unsupported(_))
    ));
    let mut changed = seed.clone();
    changed
        .summary
        .as_mut()
        .ok_or_else(|| Error::Invalid("missing capture".into()))?
        .selection
        .history_limits
        .maximum_events += 1;
    assert!(matches!(
        fs_verifier.verify(&changed).await,
        Err(Error::Conflict(_))
    ));
    aggregate = aggregate.with_fork_verifier(Arc::new(CompositeForkVerifier::new(&[
        fs_verifier,
        Arc::new(StreamHistoryForkVerifier::new(&stream_provider, &mut None)?),
    ], &mut None)?));
    aggregate
        .execute(Command {
            operation_id: seed.operation_id,
            idempotency_key: IdempotencyKey::new("publish-summary-fork")?,
            expected_revision: aggregate.reducer().revision(),
            scope,
            causal_parent: None,
            action: Action::PublishFork {
                seed: Box::new(seed.clone()),
            },
        }, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    assert_eq!(aggregate.reducer().fork(&seed.child), Some(&seed));
    assert_summary_child_import(&storage, &aggregate, &seed, &captured, limits).await?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one child binding/import/admission with denied scope and narrowed bounds"
)]
async fn assert_summary_child_import(
    storage: &MemoryHarnessStorage,
    parent: &StreamAggregate<MemoryStream>,
    seed: &crate::fork::ForkSeed,
    captured: &Context,
    limits: Limits,
) -> Result<()> {
    use crate::context::{ContextPipeline, ContextStage};
    use crate::executor::{Executor, StockExecutor};
    let issuer = AuthorityIssuer::new("summary-child", [42; 32], seed.child.clone());
    let read_grants = seed.reference_capabilities(seed.child_agent)?;
    let grants = Capabilities::new(
        read_grants.iter().map(str::to_owned).chain([
            "conversation:bind".to_owned(),
            "model:generate".to_owned(),
            seed.child_private_volume
                .capability(VolumeOperation::Write)?,
            seed.child_private_volume
                .capability(VolumeOperation::Read)?,
        ]),
    );
    let scope = issuer.issue("child", &grants, Some(seed.child_agent), None, &mut None).expect("scope proof");
    let reader = Arc::new(FilesystemContentVerifier::new(storage.host.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &scope, 262_144, None)?);
    let mut child = StreamAggregate::open(
        &storage.stream,
        seed.child.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    )
    .await?;
    let parent_history = parent.history_reader()?;
    let child_history = child.history_reader()?;
    let history_limits = HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: 262_144,
    };
    assert!(matches!(
        child_history
            .summary_fork_stage(
                &parent_history,
                seed,
                &scope,
                reader.clone(),
                limits,
                history_limits
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    child
        .bind_published_child(parent, seed, scope.clone(), #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    let capture = seed
        .summary
        .as_ref()
        .ok_or_else(|| Error::Invalid("no Summary capture".into()))?;
    let denied = issuer.issue("denied", &scope.capabilities().without(&Capabilities::new([
            capture.context.read_capability()?,
            seed.child_private_volume
                .capability(VolumeOperation::Read)?,
        ])), Some(seed.child_agent), None, &mut None).expect("scope proof");
    assert!(matches!(
        child_history
            .summary_fork_stage(
                &parent_history,
                seed,
                &denied,
                reader.clone(),
                limits,
                history_limits
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        child_history
            .summary_fork_stage(
                &parent_history,
                seed,
                &scope,
                reader.clone(),
                Limits {
                    file_bytes: 1,
                    ..limits
                },
                history_limits,
            )
            .await,
        Err(Error::Invalid(_))
    ));
    let mut changed = seed.clone();
    changed.parent_revision += 1;
    assert!(
        child_history
            .summary_fork_stage(
                &parent_history,
                &changed,
                &scope,
                reader.clone(),
                limits,
                history_limits
            )
            .await
            .is_err()
    );
    let other_stream = Arc::new(MemoryStream::default());
    let mut unrelated = StreamAggregate::open(
        &other_stream,
        seed.child.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    )
    .await?;
    unrelated
        .execute(Command {
            operation_id: OperationId::new(),
            idempotency_key: IdempotencyKey::new("independent-child-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::BindConversation {
                agent: seed.child_agent,
            },
        }, #[cfg(feature = "filesystem")] None, &mut None)
        .await?;
    assert!(matches!(
        unrelated
            .history_reader()?
            .summary_fork_stage(
                &parent_history,
                seed,
                &scope,
                reader.clone(),
                limits,
                history_limits
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let stage = Arc::new(
        assert_cold_summary_reads(
            storage,
            parent,
            seed,
            &issuer,
            &scope,
            reader.clone(),
            limits,
        )
        .await?,
    );
    let model = Arc::new(SummaryModel::default());
    let executor = StockExecutor::new(
        Model::new("synthetic", "summary-child", "1", Value::Null)?,
        model.clone(),
        ContextPipeline::new([stage as Arc<dyn ContextStage>]),
        ToolRegistry::new(),
    )
    .with_limits(limits);
    let journal = FilesystemExecutionJournal::new(
                storage.stream.clone(),
                storage.host.clone(),
                &seed.child_private_volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &crate::core::SchemaRegistry::new(),
                &scope,
                262_144,
                None,
            )?
    .with_input_verifier(reader);
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: crate::model::ModelContent::Text("fresh child input".into()),
        selected_context: None,
        max_steps: 1,
    };
    let output = executor.execute(input.clone(), &journal).await?;
    assert_eq!(executor.execute(input.clone(), &journal).await?, output);
    let requests = model
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(
        request.request().messages.get(..captured.messages.len()),
        Some(captured.messages.as_slice())
    );
    assert_eq!(
        request
            .request()
            .messages
            .last()
            .map(|message| &message.content),
        Some(&input.input)
    );
    Ok(())
}

struct ColdSummaryStream {
    client: Arc<MemoryStream>,
    reads: std::sync::atomic::AtomicUsize,
    maximum: std::sync::atomic::AtomicU32,
    fail_read: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl acyclic_stream::StreamProvider for ColdSummaryStream {
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
        .reducer()
        .conversation()
        .and_then(|state| state.messages().last())
        .ok_or_else(|| Error::Invalid("missing parent message".into()))?
        .content
        .clone();
    let mut writer = StreamAggregate::open(
        &storage.stream,
        storage.conversation.clone(),
        storage.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    )
    .await?
    .with_content_verifier(storage.content_verifier.clone());
    for index in 0..count {
        let sequence = writer
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("missing conversation".into()))?
            .messages()
            .len() as u64
            + 1;
        writer
            .execute(Command {
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
                        attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            }, #[cfg(feature = "filesystem")] None, &mut None)
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
    let child_history = HistoryReader::new(&storage.stream, &seed.child, issuer.verifier(&mut None).expect("verifier copy"))?;
    let binding = child_history
        .read_page(
            &child_history.pin(0, None).await?,
            HistoryReadLimits {
                maximum_events: 1,
                maximum_bytes: 262_144,
            },
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
        let cold_parent = HistoryReader::new(&client, &seed.parent, storage.verifier())?;
        let cold_child = HistoryReader::new(&client, &seed.child, issuer.verifier(&mut None).expect("verifier copy"))?;
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
        let cold_parent = HistoryReader::new(&client, &seed.parent, storage.verifier(&mut None).expect("verifier copy"))?;
        let cold_child = HistoryReader::new(&client, &seed.child, issuer.verifier(&mut None).expect("verifier copy"))?;
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

#[tokio::test]
async fn default_compaction_bounds_small_message_continuation() -> Result<()> {
    const TURNS: u16 = 10_000;
    let mut storage = MemoryHarnessStorage::new(AgentId::new(), 262_144).await?;
    let model = Arc::new(SummaryModel::default());
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let instructions = Arc::new(Context {
        messages: [
            "Keep the user's constraints.",
            "Preserve original references.",
        ]
        .into_iter()
        .map(|text| crate::model::ModelMessage {
            role: crate::model::ModelRole::System,
            content: crate::model::ModelContent::Text(text.into()),
        })
        .collect(),
        ..Context::default()
    });
    let build_bundle = |storage: &MemoryHarnessStorage| -> Result<crate::bundle::HarnessBundle> {
        storage
            .builder()
            .model(
                Model::new("synthetic", "small-message-continuation", "1", Value::Null)?,
                model.clone(),
            )
            .grant("model:generate")
            .limits(limits)
            .context(crate::context::ContextPipeline::new([Arc::new(
                crate::context::SourceStage::new(
                    "instructions",
                    "1",
                    instructions.clone(),
                    crate::context::ContextPlacement::Prepend,
                ),
            )
                as Arc<dyn crate::context::ContextStage>]))
            .build()
    };
    let mut bundle = build_bundle(&storage)?;
    let mut original = None;
    for index in 0..TURNS {
        let operation = OperationId::new();
        let input = storage
            .stage(
                operation,
                &format!("turns/{index}/input.txt"),
                b"small input",
                "text/plain",
                "input.txt",
            )
            .await
            .map_err(|error| Error::Storage(format!("retained turn {index} input: {error}")))?;
        let output = storage
            .run_conversation(&bundle, operation, input.clone(), Vec::new(), 1)
            .await
            .map_err(|error| Error::Storage(format!("retained turn {index}: {error}")))?;
        if original.is_none() {
            original = Some((operation, input, output));
        }
        let aggregate = storage.open_conversation(limits).await?;
        let conversation = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("conversation not bound".into()))?;
        assert_eq!(conversation.logical_revision(), u64::from(index + 1) * 2);
        assert!(conversation.messages().len() <= limits.context_messages);
        if index >= 3 {
            assert!(conversation.resident_after_sequence() > 0);
        }
        drop(aggregate);
        if index == TURNS - 3 {
            storage.journal = Arc::new(
                FilesystemExecutionJournal::new(
                storage.stream.clone(),
                storage.host.clone(),
                &storage.volume,
                &storage.verifier(),
                &crate::core::SchemaRegistry::new(),
                &storage.scope,
                storage.maximum_file_bytes,
                None,
            )?
                .with_input_verifier(storage.content_verifier.clone()),
            );
            bundle = build_bundle(&storage)?;
        }
    }
    let requests = model
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len();
    assert!(
        requests > usize::from(TURNS),
        "count pressure must admit summary requests"
    );
    let (operation, input, expected) =
        original.ok_or_else(|| Error::Invalid("missing original turn".into()))?;
    assert_eq!(
        storage
            .run_conversation(&bundle, operation, input, Vec::new(), 1)
            .await?,
        expected
    );
    assert_eq!(
        model
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        requests
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "actual retired parent, full fork publication and cold inherited paging")]
async fn full_fork_after_retirement_requires_exact_archive_witness() -> Result<()> {
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 262_144).await?;
    let limits = Limits { context_messages: 8, ..Limits::default() };
    let model = Arc::new(SummaryModel::default());
    let bundle = storage.builder()
        .model(Model::new("synthetic", "retired-full-fork", "1", Value::Null)?, model)
        .grant("model:generate").limits(limits).build()?;
    for index in 0..6 {
        let operation = OperationId::new();
        let input = storage.stage(operation, &format!("full/{index}.txt"),
            b"small input", "text/plain", "input.txt").await?;
        storage.run_conversation(&bundle, operation, input, Vec::new(), 1).await?;
    }
    let mut parent = storage.open_conversation(limits).await?;
    let conversation = parent.reducer().conversation()
        .ok_or_else(|| Error::Invalid("missing full-fork parent".into()))?;
    assert!(conversation.resident_after_sequence() > 0);
    assert_eq!(conversation.logical_revision(), 12);
    let history = HistoryReader::new(&storage.stream, &storage.conversation, storage.verifier(&mut None).expect("verifier copy"))?;
    assert!(history.fork_history(parent.reducer(), 12,
        HistoryReadLimits { maximum_events: 11, maximum_bytes: 2_097_152 }, &mut None).await.is_err());
    let archived = history.fork_history(parent.reducer(), 12,
        HistoryReadLimits { maximum_events: 12, maximum_bytes: 2_097_152 }, &mut None).await?;
    let provider = storage.volume.provider().clone();
    let project = VolumeRef::new(provider.clone(), "full-parent-project",
        VolumeClass::Project, VolumeOwner::Project("full-project".into()))?;
    let project_head = storage.host.create_volume(&project).await?;
    let scope = storage.issuer.issue("full-parent", &Capabilities::new(storage.scope.capabilities().iter().map(str::to_owned)
            .chain(["fork:publish".to_owned(), project.capability(VolumeOperation::Read)?])), Some(agent), None, &mut None).expect("scope proof");
    let stream_provider = ProviderRef::new("local", "stream", "2")?;
    let preparer = FilesystemForkPreparer::new(storage.host.clone(), parent.reducer().clone(),
        storage.verifier(&mut None).expect("verifier copy"), scope.clone(), project.clone(), stream_provider.clone(),
        storage.content_verifier.clone())?.with_history_context(archived)?;
    let child_agent = AgentId::new();
    let request = ForkRequest {
        authorized_source: None,
        operation_id: OperationId::new(),
        parent: storage.conversation.clone(),
        parent_revision: parent.reducer().revision(),
        child: Authority { kind: AggregateKind::Conversation, id: "retired-full-child".into() },
        child_agent,
        attached_agents: Vec::new(),
        preparation: ForkPreparation {
            child_result_volume: None,
            summary: None,
            child_project_volume: VolumeRef::new(provider.clone(), "full-child-project",
                VolumeClass::Project, VolumeOwner::Project("full-project".into()))?,
            child_private_volume: VolumeRef::new(provider, "full-child-private",
                VolumeClass::AgentPrivate, VolumeOwner::Agent(child_agent))?,
            inherited_through_sequence: 0,
            maximum_inherited_messages: 12,
            maximum_inherited_bytes: 262_144,
            maximum_inherited_references: 64,
        },
        selections: vec![
            ForkSelection { required: true, revision: ResourceRevision::PrivateVolume {
                volume: storage.volume.clone(),
                generation: storage.host.resolve(&crate::filesystem::workspace_ref(
                    storage.volume.provider().clone(), &storage.volume.storage_name()?)?).await?.generation,
                paths: Vec::new(),
            }},
            ForkSelection { required: true, revision: ResourceRevision::History(StreamRef::new(
                stream_provider.clone(), storage.conversation.stream_path(&mut None)?.into_bytes(),
                Some(parent.reducer().revision().to_string()))?) },
            ForkSelection { required: true, revision: ResourceRevision::Project {
                volume: project, generation: project_head.generation,
            }},
        ],
        boundary: None,
    }.with_history_policy(parent.reducer(), ForkHistoryPolicy::Fork, &mut None)?;
    let report = parent.prepare_fork(&preparer, request.clone()).await?;
    assert_eq!(parent.reconcile_fork(&preparer, request).await?, Some(report.clone()));
    let seed = report.into_seed(&mut None)?;
    assert!(seed.summary.is_none());
    assert!(seed.history_witness.is_some());
    let revision = parent.reducer().revision();
    for mutation in 0..4 {
        let mut changed = seed.clone();
        match mutation {
            0 => changed.history_witness = None,
            1 => changed.history_witness = Some([0; 32]),
            2 => { changed.reference_grants.pop(); }
            _ => changed.parent_revision -= 1,
        }
        assert!(matches!(parent.execute(Command {
            operation_id: changed.operation_id,
            idempotency_key: IdempotencyKey::new(format!("full-witness-mutation-{mutation}"))?,
            expected_revision: revision,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::PublishFork { seed: Box::new(changed) },
        }, #[cfg(feature = "filesystem")] None, &mut None).await, Err(Error::Invalid(_) | Error::Unauthorized(_))));
        assert_eq!(parent.reducer().revision(), revision);
    }
    let fork_verifier = Arc::new(CompositeForkVerifier::new(&[
        Arc::new(FilesystemForkVerifier::new(storage.host.clone(), 262_144)?),
        Arc::new(StreamHistoryForkVerifier::new(&stream_provider, &mut None)?),
    ], &mut None)?);
    parent = parent.with_fork_verifier(fork_verifier.clone());
    let command = Command {
        operation_id: seed.operation_id,
        idempotency_key: IdempotencyKey::new("publish-retired-full-fork")?,
        expected_revision: revision,
        scope,
        causal_parent: None,
        action: Action::PublishFork { seed: Box::new(seed.clone()) },
    };
    parent.execute(command.clone()).await?;
    let mut reopened = storage.open_conversation(limits).await?
        .with_fork_verifier(fork_verifier);
    assert!(matches!(reopened.execute(command, #[cfg(feature = "filesystem")] None, &mut None).await?, crate::core::ApplyResult::Replayed { .. }));
    let mut count = 0;
    while count < 12 {
        let page = history.inherited_conversation_page(&seed, child_agent, count,
            HistoryReadLimits { maximum_events: 3, maximum_bytes: 262_144 }).await?;
        assert!(!page.is_empty());
        for message in page {
            count += 1;
            assert_eq!(message.sequence, count);
        }
    }
    assert_eq!(count, 12);
    Ok(())
}
