//! Canonical resource controls use real Stream/Filesystem allocation and IO.
//! The custom sandbox creator deliberately permits corrupt allocation provenance
//! at its storage boundary so the coordinator, not a canned verifier response,
//! must reject it against the retained preparation admission.

use super::*;
use crate::conversation::VolumeOperation;
use crate::filesystem::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemHost,
    FilesystemResourceCreationProvider, FilesystemSchedulerPayloadStore, WorkspaceMutation,
    workspace_ref,
};
use crate::resource_preparation::{
    CreatedResourceOperation, CreatedResourceReceipt, ResourceCreationOrigin,
    ResourceCreationProofRef, ResourceCreationProvider, ResourcePreparationRequest,
    TaskResourcePreparation, created_resource_capability,
};
use crate::resources::{GenerationRef, ResourceKind, ResourceRef, WorkspaceRef};
use crate::runtime::{AgentHarness, Bindings};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};

type MemoryFilesystem = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SandboxAllocation {
    origin: ResourceCreationOrigin,
    request: ResourcePreparationRequest,
    resource: ResourceRef,
    evidence: ResourceRef,
    workspace: WorkspaceRef,
    sandbox_generation: GenerationRef,
}

/// A genuine custom creator: an isolated workspace and a separate, immutable
/// allocation record. Its trusted registry pins evidence generations, never a
/// mutable workspace head. The ordinary task lease gates physical allocation.
struct SandboxCreator {
    provider: ProviderRef,
    filesystem_provider: ProviderRef,
    filesystem: Arc<MemoryFilesystem>,
    allocations: Mutex<BTreeMap<Vec<u8>, GenerationRef>>,
}

impl SandboxCreator {
    fn proof(&self, preparation: TaskId) -> Result<ResourceCreationProofRef> {
        let evidence_volume = VolumeRef::new(
            self.filesystem_provider.clone(), "sandbox-allocation", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([81; 16])),
        )?;
        Ok(ResourceCreationProofRef {
            provider: self.provider.clone(),
            preparation,
            resource: ResourceRef::new(
                ResourceKind::Sandbox,
                self.provider.clone(),
                b"isolated-sandbox".to_vec(),
                None,
            )?,
            evidence: workspace_ref(self.filesystem_provider.clone(), &evidence_volume.storage_name()?)?
                .as_resource()
                .clone(),
        })
    }

    async fn create(
        &self,
        preparation: &TaskResourcePreparation,
        corrupt_origin: impl FnOnce(&mut ResourceCreationOrigin),
    ) -> Result<()> {
        preparation.verify_creation().await?;
        let request = preparation.request();
        request.validate()?;
        let proof = self.proof(preparation.origin().preparation_task)?;
        if request.provider != self.provider
            || request.creation_grant != "resource:create:custom"
            || request.resources != [proof.resource.clone()]
            || request.child.resource_proofs != [proof.clone()]
            || request.maximum_work < 4
        {
            return Err(Error::Unauthorized("sandbox creation request changed".into()));
        }
        let sandbox = VolumeRef::new(
            self.filesystem_provider.clone(),
            "isolated-sandbox",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([81; 16])),
        )?;
        let workspace = self.filesystem.create_volume(&sandbox).await?.workspace;
        let payload = crate::contract::canonical_json_bytes(&request.payload)?;
        let manifest_bytes = payload.len() as u64;
        let sandbox_generation = self.filesystem.apply(
            &workspace,
            None,
            &[WorkspaceMutation::PutFile { path: "/manifest.json".into(), bytes: payload }],
            &IdempotencyKey::new("sandbox-manifest")?,
        ).await?;
        preparation.verify_creation().await?;
        let evidence_volume = VolumeRef::new(
            self.filesystem_provider.clone(),
            "sandbox-allocation",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([81; 16])),
        )?;
        // The evidence identity is a provider-owned workspace, distinct from
        // any child content volume. Only this creator retains its pinned head.
        let evidence = self.filesystem.create_volume(&evidence_volume).await?.workspace;
        let expected_evidence = workspace_ref(
            self.filesystem_provider.clone(), &evidence_volume.storage_name()?,
        )?;
        let mut origin = preparation.origin().clone();
        corrupt_origin(&mut origin);
        let allocation = SandboxAllocation {
            origin,
            request: request.clone(),
            resource: proof.resource.clone(),
            evidence: proof.evidence.clone(),
            workspace,
            sandbox_generation,
        };
        // Reference the actual physical evidence workspace in the registry;
        // its logical opaque proof identity is separately authenticated below.
        assert_eq!(evidence, expected_evidence);
        let bytes = crate::contract::canonical_json_bytes(&allocation)?;
        crate::contract::validate_json_byte_bound(&allocation, request.maximum_bytes)?;
        if manifest_bytes.saturating_add(bytes.len() as u64) > request.maximum_bytes {
            return Err(Error::Invalid("sandbox allocation exceeds its aggregate IO bound".into()));
        }
        let generation = self.filesystem.apply(
            &evidence,
            None,
            &[WorkspaceMutation::PutFile { path: "/allocation.json".into(), bytes }],
            &IdempotencyKey::new("sandbox-allocation")?,
        ).await?;
        preparation.verify_creation().await?;
        self.allocations.lock().await.insert(
            crate::contract::canonical_json_bytes(&proof.resource)?, generation,
        );
        Ok(())
    }
}

impl ResourceCreationProvider for SandboxCreator {
    fn provider(&self) -> &ProviderRef { &self.provider }

    fn verify<'a>(
        &'a self,
        request: &'a ResourcePreparationRequest,
        proof: &'a ResourceCreationProofRef,
    ) -> BoxProviderFuture<'a, Result<CreatedResourceReceipt>> {
        Box::pin(async move {
            request.validate()?;
            if request.provider != self.provider || proof != &self.proof(proof.preparation)? {
                return Err(Error::Unauthorized("sandbox proof identity changed".into()));
            }
            let generation = self.allocations.lock().await
                .get(&crate::contract::canonical_json_bytes(&proof.resource)?)
                .cloned()
                .ok_or_else(|| Error::Unauthorized("sandbox allocation is absent".into()))?;
            let volume = VolumeRef::new(
                self.filesystem_provider.clone(), "sandbox-allocation", VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([81; 16])),
            )?;
            let evidence = workspace_ref(self.filesystem_provider.clone(), &volume.storage_name()?)?;
            let bytes = self.filesystem.read(
                &evidence, Some(&generation), "/allocation.json", request.maximum_bytes,
            ).await?;
            let allocation: SandboxAllocation = crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(error.to_string()))?;
            if crate::contract::canonical_json_bytes(&allocation)? != bytes.as_ref()
                || &allocation.request != request
                || allocation.resource != proof.resource
                || allocation.evidence != proof.evidence
                || allocation.origin.preparation_task != proof.preparation
            {
                return Err(Error::Unauthorized("sandbox allocation differs from request".into()));
            }
            let manifest = self.filesystem.read(
                &allocation.workspace, Some(&allocation.sandbox_generation),
                "/manifest.json", request.maximum_bytes,
            ).await?;
            if manifest.as_ref() != crate::contract::canonical_json_bytes(&request.payload)? {
                return Err(Error::Unauthorized("sandbox allocation payload changed".into()));
            }
            Ok(CreatedResourceReceipt {
                origin: allocation.origin,
                resource: allocation.resource,
                evidence: allocation.evidence,
                allocation_generation: generation,
                operations: vec![CreatedResourceOperation::Named("execute".into())],
                content: Vec::new(),
            })
        })
    }
}

struct CreationFixture {
    host: Arc<CoordinatorTaskHost<MemoryStream>>,
    harness: Arc<AgentHarness>,
    filesystem: Arc<MemoryFilesystem>,
    filesystem_provider: ProviderRef,
    creator: Arc<SandboxCreator>,
    root_scope: RuntimeScope,
    task: crate::registry::ComponentIdentity,
    machine: MachineIdentity,
    schema: Value,
    root: TaskId,
    root_lease: crate::distributed::WorkLease,
    worker: crate::distributed::Worker,
}

impl CreationFixture {
    async fn new() -> Result<Self> {
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let filesystem_provider = ProviderRef::new("created-controls", "filesystem", "2")?;
        let filesystem = Arc::new(FilesystemHost::new(Fs::memory(), filesystem_provider.clone())?);
        let payload_volume = VolumeRef::new(
            filesystem_provider.clone(), "scheduler", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
        )?;
        filesystem.create_volume(&payload_volume).await?;
        let authority = Authority { kind: AggregateKind::Task, id: "created-owner".into() };
        let issuer = AuthorityIssuer::new("created-owner", [80; 32], authority.clone());
        let grants = Capabilities::new([
            "operation:declare".into(), "operation:observe".into(), "operation:cancel".into(),
            "task:spawn:test.created@1".into(), "resource:create:custom".into(),
            "fork:publish".into(), payload_volume.capability(VolumeOperation::Read)?,
            payload_volume.capability(VolumeOperation::Write)?,
        ]);
        let signed = issuer.root("owner", grants.clone());
        let root_scope = RuntimeScope::new(grants, Limits::default())?
            .with_run_limits(crate::runtime::TaskRunLimits {
                concurrency: Some(16), max_steps: Some(4), deadline_epoch_ms: None,
            })?;
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            filesystem.clone(), payload_volume, &issuer.verifier(), &signed,
            root_scope.limits().file_bytes,
        )?);
        let reader = Arc::new(FilesystemContentVerifier::new(
            filesystem.clone(), issuer.verifier(), signed.clone(), root_scope.limits().file_bytes,
        )?);
        let schema = serde_json::json!({"type":"object"});
        let implementation = Arc::new(BatchMachine {
            identity: MachineIdentity { name: "test.created".into(), version: "1".into(), digest: [81; 32] },
            schema: schema.clone(),
        });
        let definition = TaskDefinition::<Value, Value>::resumable(
            implementation.clone(), schema.clone(), schema.clone(),
        )?;
        let task = definition.identity().clone();
        let machine = implementation.identity.clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let mut machines = MachineRegistry::default();
        machines.register(implementation)?;
        let creator = Arc::new(SandboxCreator {
            provider: ProviderRef::new("created-controls", "sandbox", "1")?,
            filesystem_provider: filesystem_provider.clone(), filesystem: filesystem.clone(),
            allocations: Mutex::new(BTreeMap::new()),
        });
        let fs_creator = Arc::new(FilesystemResourceCreationProvider::new(filesystem.clone(), issuer.clone())?);
        let host = Arc::new(CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone()).await?, stream,
            payloads, reader, authority, signed, issuer.verifier(), root_scope.clone(),
            tasks.clone(), machines, Arc::new(SystemUnixMillisClock),
        )?.with_session_limits(crate::scheduler::SessionLimits {
            active_tasks: 16, total_tasks: 32, depth: 4, model_steps: 4,
        })?.with_resource_creation_provider(creator.clone())?
            .with_resource_creation_provider(fs_creator)?);
        let mut bindings = Bindings::local();
        bindings.tasks = tasks;
        bindings.scope = root_scope.clone();
        bindings.durable_host = Some(host.clone());
        let harness = bindings.build()?;
        let root_op = OperationId::from_bytes([82; 16]);
        let root = TaskId::from_bytes(root_op.into_bytes());
        let admission = TaskAdmissionRecord {
            resource_proofs: Vec::new(), operation_id: root_op, task: task.clone(),
            machine: machine.clone(), input: serde_json::json!({}), input_schema: schema.clone(),
            output_schema: schema.clone(), parent: None, grants: root_scope.grants().clone(),
            limits: root_scope.limits(), run_limits: root_scope.run_limits(), policy: None,
            extensions: None, execution: None,
        };
        assert!(matches!(host.admit(admission).await?, Admission::Accepted(id) if id == root));
        let worker = crate::distributed::Worker {
            id: "created-worker".into(), available: Default::default(), labels: BTreeMap::new(),
        };
        let crate::distributed::WorkPull::Claimed(root_lease) = host.pull_work(&worker).await? else {
            return Err(Error::NotFound("created root lease".into()));
        };
        host.start_task(&root_lease).await?;
        Ok(Self { host, harness, filesystem, filesystem_provider, creator,
            root_scope, task, machine, schema, root, root_lease, worker })
    }

    fn admission(&self, byte: u8, input: Value) -> TaskAdmissionRecord {
        TaskAdmissionRecord {
            resource_proofs: Vec::new(), operation_id: OperationId::from_bytes([byte; 16]),
            task: self.task.clone(), machine: self.machine.clone(), input,
            input_schema: self.schema.clone(), output_schema: self.schema.clone(), parent: Some(self.root),
            grants: self.root_scope.grants().clone(), limits: self.root_scope.limits(),
            run_limits: self.root_scope.run_limits(), policy: None, extensions: None, execution: None,
        }
    }

    fn sandbox_request(&self) -> Result<ResourcePreparationRequest> {
        let preparation = TaskId::from_bytes([83; 16]);
        let proof = self.creator.proof(preparation)?;
        let mut child = self.admission(84, serde_json::json!({"action":"execute"}));
        child.grants = Capabilities::new([
            created_resource_capability(&proof.resource, "execute")?,
            "task:spawn:test.created@1".into(),
        ]);
        child = child.with_resource_proofs(vec![proof.clone()])?;
        Ok(ResourcePreparationRequest {
            provider: self.creator.provider.clone(), child, resources: vec![proof.resource],
            creation_grant: "resource:create:custom".into(), maximum_resources: 1,
            maximum_work: 4, maximum_bytes: 65_536, payload: serde_json::json!({"image":"isolated"}),
        })
    }

    async fn prepare(&self, request: &ResourcePreparationRequest) -> Result<(TaskResourcePreparation, crate::distributed::WorkLease)> {
        let prep = self.admission(83, serde_json::to_value(request).map_err(|e| Error::Invalid(e.to_string()))?);
        assert!(matches!(self.host.admit_owned(prep, (&self.root_lease.reservation).into()).await?,
            Admission::Accepted(id) if id == TaskId::from_bytes([83; 16])));
        let crate::distributed::WorkPull::Claimed(lease) = self.host.pull_work(&self.worker).await? else {
            return Err(Error::NotFound("created preparation lease".into()));
        };
        self.host.start_task(&lease).await?;
        let owner = Arc::new(self.host.journal_owner(
            TaskId::from_bytes(lease.operation.operation_id.into_bytes()),
            (&lease.reservation).into(),
        ).await?);
        Ok((owner.resource_preparation(request.clone()).await?, lease))
    }

    async fn admit_child(&self, child: TaskAdmissionRecord) -> Result<Admission<TaskId>> {
        let admitted = self.host.admit_owned(child, (&self.root_lease.reservation).into()).await?;
        if let Admission::Accepted(task) = &admitted {
            self.start_next(*task).await?;
        }
        Ok(admitted)
    }

    async fn start_next(&self, expected: TaskId) -> Result<()> {
        let crate::distributed::WorkPull::Claimed(lease) = self.host.pull_work(&self.worker).await? else {
            return Err(Error::NotFound("created child publication lease".into()));
        };
        assert_eq!(lease.operation.operation_id.into_bytes(), expected.into_bytes());
        self.host.start_task(&lease).await
    }
}

#[tokio::test]
async fn created_custom_operation_requires_canonical_admission_not_a_caller_receipt() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let label = created_resource_capability(&request.resources[0], "execute")?;
    assert!(!fixture.root_scope.grants().contains(&label));
    let (preparation, _) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    let raw = fixture.creator.verify(&request, &request.child.resource_proofs[0]).await?;
    assert_eq!(raw.origin, *preparation.origin());
    // Caller-owned verified records cannot grant a public RuntimeScope authority.
    let unsealed = RuntimeScope::new(Capabilities::new([label.clone()]), request.child.limits)?;
    assert!(fixture.root_scope.narrow(unsealed.grants().clone(), unsealed.limits()).is_err());
    assert!(!fixture.harness.durable_context(fixture.root, OperationId::from_bytes(fixture.root.into_bytes()))
        .await?.scope().grants().contains(&label));
    let mut unrelated = request.child.clone();
    unrelated.operation_id = OperationId::from_bytes([85; 16]);
    assert!(fixture.admit_child(unrelated).await.is_err());
    // A proof without the parent's actual current lease is not admission.
    assert!(fixture.host.admit(request.child.clone()).await.is_err());
    assert!(matches!(fixture.admit_child(request.child.clone()).await?,
        Admission::Accepted(id) if id == TaskId::from_bytes([84; 16])));
    let context = fixture.harness.durable_context(TaskId::from_bytes([84; 16]), request.child.operation_id).await?;
    assert!(context.scope().grants().contains(&label));
    assert!(!context.scope().grants().contains("resource:create:custom"));
    assert!(!fixture.root_scope.grants().contains(&label));
    let narrowed = context.scoped(Capabilities::default(), context.scope().limits())?;
    assert!(!narrowed.scope().grants().contains(&label));
    assert!(narrowed.scoped(Capabilities::new([label.clone()]), narrowed.scope().limits()).is_err());
    let foreign = ResourceRef::new(ResourceKind::Sandbox, fixture.creator.provider.clone(), b"foreign".to_vec(), None)?;
    assert!(context.scoped(Capabilities::new([created_resource_capability(&foreign, "execute")?]), context.scope().limits()).is_err());
    Ok(())
}

#[tokio::test]
async fn corrupt_genuine_allocation_provenance_never_derives_scope() -> Result<()> {
    for corruption in 0..6 {
        let fixture = CreationFixture::new().await?;
        let request = fixture.sandbox_request()?;
        let (preparation, _) = fixture.prepare(&request).await?;
        fixture.creator.create(&preparation, |origin| match corruption {
            0 => origin.intended_child = OperationId::from_bytes([85; 16]),
            1 => origin.admission_digest[0] ^= 1,
            2 => origin.request_digest[0] ^= 1,
            3 => origin.owner.id = "foreign-owner".into(),
            4 => origin.parent_task = TaskId::from_bytes([85; 16]),
            _ => origin.creation_fence.reservation_id = "foreign-creation-lease".into(),
        }).await?;
        // This is authenticated real immutable storage, not fabricated success
        // returned by a mock. The canonical owner must reject its provenance.
        let receipt = fixture.creator.verify(&request, &request.child.resource_proofs[0]).await?;
        assert_ne!(receipt.origin, *preparation.origin());
        assert!(fixture.admit_child(request.child.clone()).await.is_err());
        assert!(fixture.harness.durable_context(TaskId::from_bytes([84; 16]), request.child.operation_id).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn changed_child_request_evidence_and_provider_revision_are_rejected() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let (preparation, _) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    let mut changed_request = request.clone();
    changed_request.payload = serde_json::json!({"image":"foreign"});
    assert!(fixture.creator.verify(&changed_request, &request.child.resource_proofs[0]).await.is_err());
    for change in 0..7 {
        let mut child = request.child.clone();
        match change {
            0 => child.operation_id = OperationId::from_bytes([85; 16]),
            1 => child.input = serde_json::json!({"action":"foreign"}),
            2 => child.grants = fixture.root_scope.grants().clone(),
            3 => child.resource_proofs[0].evidence = workspace_ref(fixture.filesystem_provider.clone(), "foreign-evidence")?.as_resource().clone(),
            4 => child.resource_proofs[0].provider = ProviderRef::new("created-controls", "sandbox", "2")?,
            5 => child.resource_proofs[0].preparation = TaskId::from_bytes([85; 16]),
            _ => child.parent = None,
        }
        assert!(fixture.admit_child(child).await.is_err());
    }
    let mut self_authorizing = fixture.admission(85, serde_json::json!({}));
    self_authorizing.parent = Some(preparation.origin().preparation_task);
    self_authorizing.grants = request.child.grants.clone();
    self_authorizing.resource_proofs = request.child.resource_proofs.clone();
    let prep_lease = fixture.host.recover_work(preparation.origin().preparation_task).await?;
    let crate::distributed::WorkPull::Claimed(prep_lease) = prep_lease else {
        return Err(Error::NotFound("preparation lease for self-authorization control".into()));
    };
    assert!(fixture.host.admit_owned(self_authorizing, (&prep_lease.reservation).into()).await.is_err());
    assert!(matches!(fixture.admit_child(request.child.clone()).await?, Admission::Accepted(_)));
    Ok(())
}

#[tokio::test]
async fn original_creation_fence_survives_current_preparation_and_parent_recovery() -> Result<()> {
    let mut fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let (preparation, original_prep) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    fixture.host.coordinator.lock().await.release_lease(
        &original_prep, IdempotencyKey::new("release-created-preparation")?,
    ).await?;
    let crate::distributed::WorkPull::Claimed(recovered_prep) = fixture.host.pull_work(&fixture.worker).await? else {
        return Err(Error::NotFound("recovered preparation lease".into()));
    };
    fixture.host.start_task(&recovered_prep).await?;
    assert_ne!(recovered_prep.reservation, original_prep.reservation);
    assert!(preparation.verify_creation().await.is_err());
    fixture.host.coordinator.lock().await.release_lease(
        &fixture.root_lease, IdempotencyKey::new("release-created-parent")?,
    ).await?;
    let old_parent_fence = (&fixture.root_lease.reservation).into();
    let crate::distributed::WorkPull::Claimed(recovered_parent) = fixture.host.pull_work(&fixture.worker).await? else {
        return Err(Error::NotFound("recovered parent lease".into()));
    };
    fixture.host.start_task(&recovered_parent).await?;
    assert_ne!(recovered_parent.reservation, fixture.root_lease.reservation);
    fixture.root_lease = recovered_parent;
    let receipt = fixture.creator.verify(&request, &request.child.resource_proofs[0]).await?;
    assert_eq!(receipt.origin.creation_fence, (&original_prep.reservation).into());
    assert!(fixture.host.admit_owned(request.child.clone(), old_parent_fence).await.is_err());
    assert!(matches!(fixture.admit_child(request.child.clone()).await?, Admission::Accepted(_)));
    let context = fixture.harness.durable_context(TaskId::from_bytes([84; 16]), request.child.operation_id).await?;
    assert!(context.scope().grants().contains(&created_resource_capability(&request.resources[0], "execute")?));
    Ok(())
}

#[tokio::test]
async fn cancelled_parent_cannot_consume_existing_created_allocation() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let (preparation, _) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    fixture.host.cancel(fixture.root).await?;
    assert!(preparation.verify_creation().await.is_err());
    // Provider evidence survives cancellation; it is not itself authorization.
    fixture.creator.verify(&request, &request.child.resource_proofs[0]).await?;
    assert!(fixture.admit_child(request.child.clone()).await.is_err());
    Ok(())
}

struct PreparedFilesystemChild {
    request: ResourcePreparationRequest,
    private: VolumeRef,
    project: VolumeRef,
    seed: crate::fork::ForkSeed,
}

impl CreationFixture {
    async fn create_filesystem_child(&self) -> Result<PreparedFilesystemChild> {
        use crate::core::{Action, Command, SchemaRegistry};
        use crate::fork::{ForkPreparation, ForkPreparer, ForkRequest, ForkSelection, ResourceRevision};
        use crate::resources::StreamRef;
        use crate::store::StreamAggregate;

        let parent = Authority { kind: AggregateKind::Conversation, id: "created-fork-parent".into() };
        let parent_agent = AgentId::from_bytes([90; 16]);
        let child_agent = AgentId::from_bytes([91; 16]);
        let fork_issuer = AuthorityIssuer::new("created-fork", [90; 32], parent.clone());
        let parent_project = VolumeRef::new(
            self.filesystem_provider.clone(), "parent-project", VolumeClass::Project,
            VolumeOwner::Project("created-project".into()),
        )?;
        let project_head = self.filesystem.create_volume(&parent_project).await?;
        let parent_private = VolumeRef::new(
            self.filesystem_provider.clone(), "parent-private", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(parent_agent),
        )?;
        let private_head = self.filesystem.create_volume(&parent_private).await?;
        let fork_scope = fork_issuer.root_for_agent(parent_agent, "fork-parent", Capabilities::new([
            "conversation:bind".into(), "fork:publish".into(),
            parent_project.capability(VolumeOperation::Read)?,
            parent_private.capability(VolumeOperation::Read)?,
        ]));
        let resolver = Arc::new(FilesystemContentVerifier::new(
            self.filesystem.clone(), fork_issuer.verifier(), fork_scope.clone(), 65_536,
        )?);
        let fork_stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut aggregate = StreamAggregate::open(
            &fork_stream, parent.clone(), fork_issuer.verifier(), SchemaRegistry::new(),
        ).await?;
        aggregate.execute(Command {
            operation_id: OperationId::from_bytes([90; 16]),
            idempotency_key: IdempotencyKey::new("created-fork-bind")?,
            expected_revision: 0, scope: fork_scope.clone(), causal_parent: None,
            action: Action::BindConversation { agent: parent_agent },
        }).await?;
        let project = VolumeRef::new(
            self.filesystem_provider.clone(), "child-project", VolumeClass::Project,
            VolumeOwner::Project("created-project".into()),
        )?;
        let private = VolumeRef::new(
            self.filesystem_provider.clone(), "child-private", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(child_agent),
        )?;
        let stream_provider = ProviderRef::new("created-controls", "stream", "2")?;
        let fork = ForkRequest {
            operation_id: OperationId::from_bytes([92; 16]), parent: parent.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: Authority { kind: AggregateKind::Conversation, id: "created-fork-child".into() },
            child_agent, attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: project.clone(), child_private_volume: private.clone(),
                summary: None, inherited_through_sequence: 0, maximum_inherited_messages: 1,
                maximum_inherited_bytes: 65_536, maximum_inherited_references: 1,
            },
            selections: vec![
                ForkSelection { required: true, revision: ResourceRevision::History(StreamRef::new(
                    stream_provider.clone(), parent.stream_path()?.into_bytes(),
                    Some(aggregate.reducer().revision().to_string()),
                )?) },
                ForkSelection { required: true, revision: ResourceRevision::Project {
                    volume: parent_project, generation: project_head.generation,
                } },
                ForkSelection { required: true, revision: ResourceRevision::PrivateVolume {
                    volume: parent_private, generation: private_head.generation, paths: Vec::new(),
                } },
            ], boundary: None,
        };
        let preparer = FilesystemForkPreparer::new(
            self.filesystem.clone(), aggregate.reducer().clone(), fork_issuer.verifier(),
            fork_scope, fork.selections.iter().find_map(|selection| match &selection.revision {
                ResourceRevision::Project { volume, .. } => Some(volume.clone()), _ => None,
            }).ok_or_else(|| Error::Invalid("fork fixture project missing".into()))?,
            stream_provider, resolver,
        )?;
        let mut child = self.admission(84, serde_json::json!({"action":"filesystem-io"}));
        // These are requests for exact created volumes, not root grants.
        child.grants = Capabilities::new([
            "task:spawn:test.created@1".into(),
            private.capability(VolumeOperation::Read)?, private.capability(VolumeOperation::Write)?,
            project.capability(VolumeOperation::Read)?, project.capability(VolumeOperation::Write)?,
        ]);
        child = child.with_resource_proofs(preparer.creation_proofs(&fork, TaskId::from_bytes([83; 16]))?)?;
        let request = ResourcePreparationRequest {
            provider: self.filesystem_provider.clone(), child,
            resources: preparer.creation_resources(&fork)?, creation_grant: "fork:publish".into(),
            maximum_resources: 2, maximum_work: 512, maximum_bytes: 16 * 1_024 * 1_024,
            payload: serde_json::to_value(&fork).map_err(|e| Error::Invalid(e.to_string()))?,
        };
        let (preparation, _) = self.prepare(&request).await?;
        let report = preparer.with_task_preparation(Arc::new(preparation))?.prepare(fork).await?;
        let seed = report.into_seed()?;
        Ok(PreparedFilesystemChild { request, private, project, seed })
    }
}

#[tokio::test]
async fn ordinary_task_io_uses_verified_created_volume_without_root_grant_widening() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let read = created.private.capability(VolumeOperation::Read)?;
    let write = created.private.capability(VolumeOperation::Write)?;
    assert!(!fixture.root_scope.grants().contains(&read));
    assert!(!fixture.root_scope.grants().contains(&write));
    assert!(matches!(fixture.admit_child(created.request.child.clone()).await?, Admission::Accepted(_)));
    let task = TaskId::from_bytes([84; 16]);
    let context = fixture.harness.durable_context(task, created.request.child.operation_id).await?;
    let file = context.stage_file(
        OperationId::from_bytes([93; 16]), "ordinary/result.txt", b"created private bytes",
        "text/plain", "result.txt",
    ).await?;
    assert_eq!(file.volume(), &created.private);
    assert_ne!(file.volume(), &created.project);
    assert_eq!(context.read_file(&file).await?, b"created private bytes");
    // Publication changes the mutable head; authentication retains the original
    // immutable allocation generation, not the latest child content generation.
    let reopened = fixture.harness.durable_context(task, created.request.child.operation_id).await?;
    assert_eq!(reopened.read_file(&file).await?, b"created private bytes");
    let read_only = reopened.scoped(Capabilities::new([read.clone()]), reopened.scope().limits())?;
    assert_eq!(read_only.read_file(&file).await?, b"created private bytes");
    assert!(read_only.stage_file(
        OperationId::from_bytes([94; 16]), "ordinary/denied.txt", b"denied", "text/plain", "denied.txt",
    ).await.is_err());
    let no_access = reopened.scoped(Capabilities::default(), reopened.scope().limits())?;
    assert!(no_access.read_file(&file).await.is_err());
    assert!(no_access.scoped(Capabilities::new([write]), no_access.scope().limits()).is_err());
    let root = fixture.harness.durable_context(
        fixture.root, OperationId::from_bytes(fixture.root.into_bytes()),
    ).await?;
    assert!(root.read_file(&file).await.is_err());
    let preparation = fixture.harness.durable_context(
        TaskId::from_bytes([83; 16]), OperationId::from_bytes([83; 16]),
    ).await?;
    assert!(preparation.read_file(&file).await.is_err());
    // Descendants inherit an attenuated exact proof from the immediate parent;
    // they do not become the original receipt's intended child or get writes.
    let mut descendant = created.request.child.clone();
    descendant.operation_id = OperationId::from_bytes([97; 16]);
    descendant.parent = Some(task);
    descendant.grants = Capabilities::new([read.clone()]);
    descendant.limits.file_bytes = 1_024;
    let crate::distributed::WorkPull::Claimed(parent_lease) = fixture.host.recover_work(task).await? else {
        return Err(Error::NotFound("created child parent lease".into()));
    };
    assert!(matches!(fixture.host.admit_owned(
        descendant.clone(), (&parent_lease.reservation).into(),
    ).await?, Admission::Accepted(_)));
    fixture.start_next(TaskId::from_bytes([97; 16])).await?;
    let inherited = fixture.harness.durable_context(
        TaskId::from_bytes([97; 16]), descendant.operation_id,
    ).await?;
    assert_eq!(inherited.read_file(&file).await?, b"created private bytes");
    assert!(!inherited.scope().grants().contains(&created.private.capability(VolumeOperation::Write)?));
    assert!(inherited.stage_file(
        OperationId::from_bytes([98; 16]), "inherited-denied.txt", b"denied",
        "text/plain", "inherited-denied.txt",
    ).await.is_err());
    assert!(!fixture.root_scope.grants().contains(&read));
    Ok(())
}

#[tokio::test]
async fn created_content_reauthenticates_allocation_provenance_on_every_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    assert!(matches!(fixture.admit_child(created.request.child.clone()).await?, Admission::Accepted(_)));
    let context = fixture.harness.durable_context(
        TaskId::from_bytes([84; 16]), created.request.child.operation_id,
    ).await?;
    let file = context.stage_file(
        OperationId::from_bytes([95; 16]), "before-corruption.txt", b"authenticated bytes",
        "text/plain", "before-corruption.txt",
    ).await?;
    assert_eq!(context.read_file(&file).await?, b"authenticated bytes");
    // Corrupt the provider's real retained claim after obtaining the context.
    // Existing handles must not keep working from a cached verification.
    let claim = workspace_ref(
        fixture.filesystem_provider.clone(),
        &format!("fork-allocation-{}", created.private.storage_name()?),
    )?;
    let bytes = fixture.filesystem.read(&claim, None, "/claim.json", 65_536).await?;
    let mut allocation: Value = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
    allocation["task_origin"]["request_digest"] = serde_json::json!(vec![0_u8; 32]);
    fixture.filesystem.apply(
        &claim, None,
        &[WorkspaceMutation::PutFile {
            path: "/claim.json".into(), bytes: crate::contract::canonical_json_bytes(&allocation)?,
        }],
        &IdempotencyKey::new("corrupt-created-content-claim")?,
    ).await?;
    assert!(context.read_file(&file).await.is_err());
    assert!(context.stage_file(
        OperationId::from_bytes([96; 16]), "after-corruption.txt", b"must not publish",
        "text/plain", "after-corruption.txt",
    ).await.is_err());
    assert!(fixture.harness.durable_context(
        TaskId::from_bytes([84; 16]), created.request.child.operation_id,
    ).await.is_err());
    // A copied seed/receipt never repairs or bypasses protected evidence.
    created.seed.validate()?;
    Ok(())
}
