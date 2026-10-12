//! Canonical resource controls use real Stream/Filesystem allocation and IO.

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

pub(crate) type MemoryFilesystem = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

#[cfg(not(target_arch = "wasm32"))]
fn metadata_control_budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        authority_records_read: 512,
        authority_records_appended: 128,
        authority_bytes_read: 1_048_576,
        authority_bytes_written: 1_048_576,
        object_probes: 512,
        backend_read_operations: 512,
        backend_write_operations: 128,
        durability_operations: 128,
        page_reads: 512,
        page_writes: 128,
        object_bytes_read: 1_048_576,
        object_bytes_written: 1_048_576,
        bytes_hashed: 4_194_304,
        bytes_copied: 4_194_304,
        bytes_encoded: 4_194_304,
        source_bytes_read: 1_048_576,
        source_path_components: 128,
        source_entries_visited: 512,
        output_bytes: 1_048_576,
        items_examined: 1024,
        items_returned: 512,
        allocation_operations: 4096,
        peak_allocation_bytes: 4_194_304,
        materializations: 128,
    }
}

struct CreatedControlMachine {
    identity: MachineIdentity,
    schema: Value,
}

impl ResumableMachine for CreatedControlMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> { Ok(input.clone()) }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if state.get("emitted") == Some(&Value::Bool(true)) {
            return Ok(MachineTransition {
                state: state.clone(), commands: Vec::new(),
                status: MachineStatus::Completed { value: serde_json::json!({"ok":true}) },
            });
        }
        if let Some(command) = input.get("created_command") {
            let command = serde_json::from_value(command.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            return Ok(MachineTransition {
                state: serde_json::json!({"emitted":true}), commands: vec![command],
                status: MachineStatus::Suspended,
            });
        }
        Ok(MachineTransition {
            state: state.clone(), commands: Vec::new(), status: MachineStatus::Suspended,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_stock_startup_retains_original_configuration_and_current_context() -> Result<()> {
    let tool = crate::tool::files::read_file()?;
    let grant = format!("tool:{}", tool.definition.name);
    let name = tool.definition.name.clone();
    let revision = tool.definition.revision.clone();
    let mut tools = crate::tool::ToolRegistry::default();
    tools.register(tool)?;
    let fixture = NativeCustomerFixture::new_with_grants(&[&grant], tools).await?;
    let volume = VolumeRef::new(fixture.creation.filesystem_provider.clone(), "scheduler",
        VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::from_bytes([80; 16])))?;
    let resumable = crate::durable_tool::ResumableToolRegistry::default();
    let original = &fixture.creation.host;
    let limits = original.session_limits.expect("the original fixture configures session limits");
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(metadata_control_budget(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let runtime = NativeCustomerRuntime::open_with_creation_providers_with_native_work(
        fixture.creation.stream.clone(), fixture.creation.filesystem.clone(),
        &volume, &original.verifier, &original.owner_scope, &fixture.creation.root_scope,
        &original.tasks, &original.machines, fixture.runtime.tool_registry(), &resumable,
        limits, 16, fixture.creation.root_scope.limits().file_bytes,
        original.clock.clone(), original.policy.clone(), &original.resource_creation, &work,
    ).await?;
    assert!(runtime.tool_registry().get_version_with_native_work(&name, &revision, &work)?.is_some());
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let fence = NativeCustomerRuntime::original_lease_fence_with_io(&fixture.creation.root_lease, &io)?;
    let context = runtime.task_context_with_native_work(fixture.creation.root, &fence, &work).await?;
    assert!(context.context().original_task_binding_matches(fixture.creation.root, &fence));
    assert!(work.receipt().backend_read_operations > 0);
    assert!(work.live_allocation_bytes() > 0);
    drop(context);
    drop(fence);
    drop(io);
    drop(runtime);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_stock_startup_denies_preallocation_and_precancel_before_sdk_io() -> Result<()> {
    let fixture = NativeCustomerFixture::new_with_grants(&[], crate::tool::ToolRegistry::default()).await?;
    let volume = VolumeRef::new(fixture.creation.filesystem_provider.clone(), "scheduler",
        VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::from_bytes([80; 16])))?;
    let resumable = crate::durable_tool::ResumableToolRegistry::default();
    let original = &fixture.creation.host;
    let limits = original.session_limits.expect("the original fixture configures session limits");
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = metadata_control_budget();
        if !cancelled { budget.allocation_operations = 0; }
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        if cancelled { cancellation.cancel(); }
        let result = NativeCustomerRuntime::open_with_creation_providers_with_native_work(
            fixture.creation.stream.clone(), fixture.creation.filesystem.clone(),
            &volume, &original.verifier, &original.owner_scope, &fixture.creation.root_scope,
            &original.tasks, &original.machines, fixture.runtime.tool_registry(), &resumable,
            limits, 16, fixture.creation.root_scope.limits().file_bytes,
            original.clock.clone(), original.policy.clone(), &original.resource_creation, &work,
        ).await;
        assert!(matches!(&result, Err(error) if error.is_native_failure()));
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        drop(result);
        assert_eq!(work.live_allocation_bytes(), 0);
    }
    Ok(())
}

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

/// A genuine custom creator: an isolated workspace and a separate, immutable allocation record.
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
                references: Vec::new(),
            })
        })
    }
}

pub(crate) struct OriginalRootFilesystemBirth {
    pub(crate) volume: VolumeRef,
    pub(crate) operation: acyclic_fs::OperationId,
    pub(crate) config: acyclic_fs::VolumeConfig,
    pub(crate) initial_generation: acyclic_fs::GenerationId,
    pub(crate) selected_generation: acyclic_fs::GenerationId,
}

pub(crate) struct CreationFixture {
    pub(crate) host: Arc<CoordinatorTaskHost<MemoryStream>>,
    pub(crate) harness: Arc<AgentHarness>,
    pub(crate) filesystem: Arc<MemoryFilesystem>,
    pub(crate) stream: StreamClient<MemoryStream>,
    pub(crate) filesystem_provider: ProviderRef,
    pub(crate) filesystem_creator: Arc<FilesystemResourceCreationProvider<MemoryAuthorityBackend, MemoryObjectBackend>>,
    creator: Arc<SandboxCreator>,
    pub(crate) root_scope: RuntimeScope,
    task: crate::registry::ComponentIdentity,
    machine: MachineIdentity,
    schema: Value,
    pub(crate) root: TaskId,
    pub(crate) root_lease: crate::distributed::WorkLease,
    pub(crate) worker: crate::distributed::Worker,
    pub(crate) original_root_births: Option<[OriginalRootFilesystemBirth; 2]>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type NativeCustomerRuntime = crate::filesystem::FilesystemTaskRuntime<
    MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend,
>;

/// One configured real customer, shared by native transport and stock controls.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCustomerFixture {
    pub(crate) creation: CreationFixture,
    pub(crate) runtime: NativeCustomerRuntime,
}

/// Actual issued context and original recovered lease precede their custody.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCustomerContext {
    pub(crate) context: Arc<crate::NativeTaskContext>,
    pub(crate) lease: crate::distributed::WorkLease,
    pub(crate) fence: crate::scheduler::LeaseFence,
    pub(crate) custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCustomerFixture {
    /// Setup is ordinary configured construction, before native observation.
    pub(crate) async fn new_with_grants(
        grants: &[&str], tools: crate::tool::ToolRegistry,
    ) -> Result<Self> {
        let creation = CreationFixture::new_with_grants(grants).await?;
        let volume = VolumeRef::new(
            creation.filesystem_provider.clone(), "scheduler",
            VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
        )?;
        let runtime = NativeCustomerRuntime::from_host(
            creation.stream.clone(), Arc::clone(&creation.harness),
            Arc::clone(&creation.host), Arc::clone(&creation.filesystem), volume,
            creation.host.verifier.clone(), creation.host.owner_scope.clone(),
            tools, creation.root_scope.limits().file_bytes, None,
        )?;
        Ok(Self { creation, runtime })
    }

    pub(crate) async fn context_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeCustomerContext> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut recovered = self.creation.host.recover_work_with_native_work(
            self.creation.root, work,
        ).await?;
        if !recovered.1.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native customer recovery differs from its original work",
            });
        }
        if matches!(&recovered.0, crate::distributed::WorkPull::Unresolved { .. }) {
            let crate::distributed::WorkPull::Unresolved { error, .. } = recovered.0 else {
                unreachable!("the original recovery was unresolved")
            };
            return Err(error);
        }
        let lease = match &recovered.0 {
            crate::distributed::WorkPull::Claimed(lease) => lease,
            _ => return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "native customer requires its original admitted lease",
            }),
        };
        if lease.reservation != self.creation.root_lease.reservation
            || lease.operation.operation_id != self.creation.root_lease.operation.operation_id
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "native customer recovery changed its original lease",
            });
        }
        let fence = NativeCustomerRuntime::original_lease_fence_with_io(lease, &io)?;
        let context = self.runtime.task_context_with_native_work(
            self.creation.root, &fence, work,
        ).await?;
        let mut fence_custody = io.take_native_custody(io.owned_bytes()?)?;
        recovered.1.absorb(&mut fence_custody)?;
        let crate::distributed::WorkPull::Claimed(lease) = recovered.0 else {
            unreachable!("the borrowed original recovery was claimed")
        };
        Ok(NativeCustomerContext { context, lease, fence, custody: recovered.1 })
    }
}

impl CreationFixture {
    pub(crate) async fn new() -> Result<Self> {
        Self::new_with_grants(&[]).await
    }

    pub(crate) async fn new_with_grants(extra: &[&str]) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        { Self::new_with_creation(extra, None).await }
        #[cfg(target_arch = "wasm32")]
        { Self::new_with_creation(extra).await }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn new_with_original_root_births(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        Self::new_with_creation(&[], Some(work)).await
    }

    async fn new_with_creation(
        extra: &[&str],
        #[cfg(not(target_arch = "wasm32"))] bootstrap: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> Result<Self> {
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let filesystem_provider = ProviderRef::new("created-controls", "filesystem", "2")?;
        let fs = Fs::memory();
        let filesystem = Arc::new(FilesystemHost::new(fs.clone(), filesystem_provider.clone())?);
        let payload_volume = VolumeRef::new(
            filesystem_provider.clone(), "scheduler", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let original_root_births = if let Some(work) = bootstrap {
            let project = VolumeRef::new(filesystem_provider.clone(), "parent-project", VolumeClass::Project,
                VolumeOwner::Project("created-project".into()))?;
            let config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable);
            let mut births = Vec::new();
            for (volume, operation, contents) in [
                (project, acyclic_fs::OperationId::from_bytes([177; 16]), b"original project import".as_slice()),
                (payload_volume.clone(), acyclic_fs::OperationId::from_bytes([178; 16]), b"original root private".as_slice()),
            ] {
                let name_custody;
                let (name, custody) = volume.storage_name_with_native_work(work)?;
                name_custody = custody;
                let initial = fs.create_workspace_with_operation_with_native_work(
                    &name, config, operation, work, work.cancellation(),
                ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
                let initial_generation = initial.id();
                let workspace = workspace_ref(filesystem_provider.clone(), &name)?;
                let initial_ref = GenerationRef::new(filesystem_provider.clone(),
                    initial_generation.digest().into_bytes().to_vec(), None)?;
                let selected = filesystem.apply(&workspace, Some(&initial_ref), &[WorkspaceMutation::PutFile {
                    path: "/root.txt".into(), bytes: contents.to_vec().into(),
                }], &IdempotencyKey::new(format!("original-root-import-{}", operation))?).await?;
                let selected_generation = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
                    selected.as_resource().key().try_into().map_err(|_| Error::Invalid("fixture generation size".into()))?,
                ));
                assert_ne!(initial_generation, selected_generation);
                births.push(OriginalRootFilesystemBirth {
                    volume, operation, config, initial_generation, selected_generation,
                });
                drop(initial);
                drop(name);
                drop(name_custody);
            }
            Some(births.try_into().map_err(|_| Error::Invalid("fixture Root birth count".into()))?)
        } else {
            filesystem.create_volume(&payload_volume).await?;
            None
        };
        #[cfg(target_arch = "wasm32")]
        let original_root_births = {
            filesystem.create_volume(&payload_volume).await?;
            None
        };
        let authority = Authority { kind: AggregateKind::Task, id: "created-owner".into() };
        let issuer = AuthorityIssuer::new("created-owner", [80; 32], authority.clone());
        let grants = Capabilities::new([
            "operation:declare".into(), "operation:observe".into(), "operation:cancel".into(),
            "task:spawn:test.created@1".into(), "resource:create:custom".into(),
            "fork:publish".into(), payload_volume.capability(VolumeOperation::Read)?,
            "conversation:bind".into(),
            payload_volume.capability(VolumeOperation::Write)?,
            VolumeRef::new(filesystem_provider.clone(), "parent-project", VolumeClass::Project,
                VolumeOwner::Project("created-project".into()))?.capability(VolumeOperation::Read)?,
            VolumeRef::new(filesystem_provider.clone(), "parent-private", VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([90;16])))?.capability(VolumeOperation::Read)?,
        ].into_iter().chain(extra.iter().map(|grant| (*grant).to_owned())));
        let signed = issuer.root_for_agent(AgentId::from_bytes([80; 16]), "owner", grants.clone());
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
        let implementation = Arc::new(CreatedControlMachine {
            identity: MachineIdentity {
                name: "test.created".into(), version: "1".into(),
                digest: *blake3::hash(include_str!("created_resource_access.rs").as_bytes()).as_bytes(),
            },
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
        #[cfg(not(target_arch = "wasm32"))]
        let fs_creator = if let Some(work) = bootstrap {
            FilesystemResourceCreationProvider::new_with_native_work(filesystem.clone(), &issuer, work)?
                .into_native_shared(work)?
        } else {
            Arc::new(FilesystemResourceCreationProvider::new(filesystem.clone(), issuer.clone())?)
        };
        #[cfg(target_arch = "wasm32")]
        let fs_creator = Arc::new(FilesystemResourceCreationProvider::new(filesystem.clone(), issuer.clone())?);
        let host = Arc::new(CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone()).await?, stream,
            payloads, reader, authority, signed, issuer.verifier(), root_scope.clone(),
            tasks.clone(), machines, Arc::new(SystemUnixMillisClock),
        )?.with_session_limits(crate::scheduler::SessionLimits {
            active_tasks: 16, total_tasks: 32, depth: 4, model_steps: 4,
        })?.with_resource_creation_provider(creator.clone())?
            .with_resource_creation_provider(fs_creator.clone())?);
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
        let stream = host.stream.clone();
        Ok(Self { host, harness, filesystem, stream, filesystem_provider, creator,
            root_scope, task, machine, schema, root, root_lease, worker, original_root_births,
            filesystem_creator: fs_creator })
    }

    pub(crate) fn admission(&self, byte: u8, input: Value) -> TaskAdmissionRecord {
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
        Ok(ResourcePreparationRequest { command: None, source_checkpoint:None, provider: self.creator.provider.clone(), child, resources: vec![proof.resource],
        input_files: Vec::new(), read_authorization: None,
        creation_grant: "resource:create:custom".into(), maximum_resources: 1,
        maximum_work: 4, maximum_bytes: 65_536, payload: serde_json::json!({"image":"isolated"}), })
    }

    async fn prepare(&self, request: &ResourcePreparationRequest)
        -> Result<(TaskResourcePreparation, crate::distributed::WorkLease, crate::filesystem::InvocationCancellation)> {
        let invocation = crate::filesystem::InvocationCancellation(acyclic_fs::CancellationToken::new());
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
        ).await?.with_invocation_cancellation(&invocation.0));
        Ok((owner.resource_preparation(request.clone()).await?, lease, invocation))
    }

    pub(crate) async fn admit_child(&self, child: TaskAdmissionRecord) -> Result<Admission<TaskId>> {
        let preparation = TaskId::from_bytes([83; 16]);
        let current = self.host.observe_task(preparation).await?;
        if current.phase == crate::scheduler::OperationPhase::Running {
            let reservation = current.reservation.ok_or_else(|| Error::NotFound("preparation completion lease".into()))?;
            self.host.settle_task(preparation, (&reservation).into(),
                Outcome::Succeeded(serde_json::json!({"prepared":true}))).await?;
        }
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
    let (preparation, _, _invocation) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    let raw = fixture.creator.verify(&request, &request.child.resource_proofs[0]).await?;
    assert_eq!(raw.origin, *preparation.origin());
    let unsealed = RuntimeScope::new(Capabilities::new([label.clone()]), request.child.limits)?;
    assert!(fixture.root_scope.narrow(unsealed.grants().clone(), unsealed.limits()).is_err());
    assert!(!fixture.harness.durable_context(fixture.root, OperationId::from_bytes(fixture.root.into_bytes()))
        .await?.scope().grants().contains(&label));
    let mut unrelated = request.child.clone();
    unrelated.operation_id = OperationId::from_bytes([85; 16]);
    assert!(fixture.admit_child(unrelated).await.is_err());
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
        let (preparation, _, _invocation) = fixture.prepare(&request).await?;
        fixture.creator.create(&preparation, |origin| match corruption {
            0 => origin.intended_child = OperationId::from_bytes([85; 16]),
            1 => origin.admission_digest[0] ^= 1,
            2 => origin.request_digest[0] ^= 1,
            3 => origin.owner.id = "foreign-owner".into(),
            4 => origin.parent_task = TaskId::from_bytes([85; 16]),
            _ => origin.creation_fence.reservation_id = "foreign-creation-lease".into(),
        }).await?;
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
    let (preparation, _, _invocation) = fixture.prepare(&request).await?;
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
    let (preparation, original_prep, _invocation) = fixture.prepare(&request).await?;
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
    let (preparation, _, _invocation) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    assert!(!preparation.cancellation().expect("caller invocation is sealed").is_cancelled());
    fixture.host.cancel(fixture.root).await?;
    assert!(preparation.verify_creation().await.is_err());
    assert!(preparation.cancellation().expect("same caller invocation").is_cancelled());
    fixture.creator.verify(&request, &request.child.resource_proofs[0]).await?;
    assert!(fixture.admit_child(request.child.clone()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn sealed_preparation_borrows_the_actual_callers_drop_guard_token() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let (preparation, _, invocation) = fixture.prepare(&request).await?;
    assert!(!preparation.cancellation().expect("actual owner invocation").is_cancelled());
    drop(invocation);
    assert!(preparation.cancellation().expect("same owner invocation").is_cancelled());
    Ok(())
}

pub(crate) struct PreparedFilesystemChild {
    pub(crate) request: ResourcePreparationRequest,
    pub(crate) private: VolumeRef,
    pub(crate) project: VolumeRef,
    pub(crate) seed: crate::fork::ForkSeed,
    pub(crate) report: crate::fork::ForkReport,
    pub(crate) parent: crate::core::Reducer,
    pub(crate) parent_scope: crate::core::Scope,
    pub(crate) parent_verifier: crate::core::AuthorityVerifier,
}

impl CreationFixture {
    pub(crate) async fn create_filesystem_child(&self) -> Result<PreparedFilesystemChild> {
        self.create_filesystem_child_with_grants(&[]).await
    }

    pub(crate) async fn create_filesystem_child_with_grants(&self, additional: &[&str]) -> Result<PreparedFilesystemChild> {
        self.create_filesystem_child_with_options(additional, false).await
    }

    async fn create_filesystem_child_with_options(&self, additional: &[&str], with_result: bool) -> Result<PreparedFilesystemChild> {
        use crate::core::{Action, Command, SchemaRegistry};
        use crate::fork::{ForkPreparation, ForkPreparer, ForkRequest, ForkSelection, ResourceRevision};
        use crate::resources::StreamRef;
        use crate::store::StreamAggregate;

        let parent = Authority { kind: AggregateKind::Conversation, id: "created-fork-parent".into() };
        let parent_agent = AgentId::from_bytes([80; 16]);
        let child_agent = AgentId::from_bytes([91; 16]);
        let fork_issuer = AuthorityIssuer::new("created-owner", [80; 32], parent.clone());
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
        let fork_stream = self.host.stream.clone();
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
        let result = with_result.then(|| VolumeRef::new(
            self.filesystem_provider.clone(), "child-result", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(child_agent),
        )).transpose()?;
        let stream_provider = ProviderRef::new("created-controls", "stream", "2")?;
        let fork = ForkRequest {
            authorized_source: None,
            operation_id: OperationId::from_bytes([92; 16]), parent: parent.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: Authority { kind: AggregateKind::Conversation, id: "created-fork-child".into() },
            child_agent, attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_result_volume: None,
                child_project_volume: project.clone(), child_private_volume: private.clone(),
                child_result_volume: result.clone(),
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
        let history = crate::store::HistoryReader::new(
            &fork_stream, &parent, fork_issuer.verifier(),
        )?.fork_history(
            aggregate.reducer(), 0,
            crate::store::HistoryReadLimits { maximum_events:2, maximum_bytes:65_536 },
        ).await?;
        let preparer = FilesystemForkPreparer::new(
            self.filesystem.clone(), aggregate.reducer().clone(), fork_issuer.verifier(),
            fork_scope.clone(), fork.selections.iter().find_map(|selection| match &selection.revision {
                ResourceRevision::Project { volume, .. } => Some(volume.clone()), _ => None,
            }).ok_or_else(|| Error::Invalid("fork fixture project missing".into()))?,
            stream_provider, resolver,
        )?.with_history_context(history)?;
        let mut child = self.admission(84, serde_json::json!({"action":"filesystem-io"}));
        child.grants = Capabilities::new([
            "task:spawn:test.created@1".into(),
            private.capability(VolumeOperation::Read)?, private.capability(VolumeOperation::Write)?,
            project.capability(VolumeOperation::Read)?, project.capability(VolumeOperation::Write)?,
        ]);
        child.grants = Capabilities::new(child.grants.iter().chain(additional.iter().copied()));
        if let Some(result) = &result {
            child.grants = Capabilities::new(child.grants.iter().chain([
                result.capability(VolumeOperation::Read)?, result.capability(VolumeOperation::Write)?,
            ].iter().map(String::as_str)));
        }
        child = child.with_resource_proofs(preparer.creation_proofs(&fork, TaskId::from_bytes([83; 16]), None)?)?;
        let request = ResourcePreparationRequest {
            command: None, source_checkpoint: None,
            provider: self.filesystem_provider.clone(), child,
            resources: preparer.creation_resources(&fork, None)?, creation_grant: "fork:publish".into(),
            maximum_resources: if with_result {3} else {2}, maximum_work: 16_384, maximum_bytes: 16 * 1_024 * 1_024,
            payload: serde_json::to_value(&fork).map_err(|e| Error::Invalid(e.to_string()))?,
            input_files: Vec::new(), read_authorization: fork.authorized_source.clone(),
        };
        let (preparation, _, _invocation) = self.prepare(&request).await?;
        let report = preparer.prepare_for_task(fork, &preparation).await?;
        let seed = report.clone().into_seed()?;
        Ok(PreparedFilesystemChild { request, private, project, seed, report,
            parent: aggregate.reducer().clone(), parent_scope: fork_scope, parent_verifier: fork_issuer.verifier() })
    }
}

mod recursive_lifecycle;
#[cfg(not(target_arch = "wasm32"))]
mod creator_native;

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
    assert_eq!(context.scope().current_created_private_volume()?, Some(&created.private));
    assert_eq!(context.scope().current_created_agent()?, Some(created.seed.child_agent));
    assert_eq!(context.scope().current_created_authority()?, Some(&created.seed.child));
    let file = context.stage_file(
        OperationId::from_bytes([93; 16]), "ordinary/result.txt", b"created private bytes",
        "text/plain", "result.txt",
    ).await?;
    assert_eq!(file.volume(), &created.private);
    assert_ne!(file.volume(), &created.project);
    assert_eq!(context.read_file(&file).await?.as_ref(), b"created private bytes".as_ref());
    let artifact = context.stage_artifact(
        OperationId::from_bytes([100; 16]), "ordinary/artifact.txt", b"created artifact bytes",
        "text/plain", "artifact.txt",
    ).await?;
    assert_eq!(artifact.volume(), &created.private);
    assert_eq!(context.read_artifact(&artifact).await?.as_ref(), b"created artifact bytes".as_ref());
    let reopened = fixture.harness.durable_context(task, created.request.child.operation_id).await?;
    assert_eq!(reopened.read_file(&file).await?.as_ref(), b"created private bytes".as_ref());
    let read_only = reopened.scoped(Capabilities::new([read.clone()]), reopened.scope().limits())?;
    assert_eq!(read_only.read_file(&file).await?.as_ref(), b"created private bytes".as_ref());
    assert_eq!(read_only.scope().current_created_agent()?, Some(created.seed.child_agent));
    assert_eq!(read_only.scope().current_created_authority()?, Some(&created.seed.child));
    assert!(read_only.stage_file(
        OperationId::from_bytes([94; 16]), "ordinary/denied.txt", b"denied", "text/plain", "denied.txt",
    ).await.is_err());
    assert!(read_only.stage_artifact(
        OperationId::from_bytes([101; 16]), "ordinary/denied-artifact.txt", b"denied",
        "text/plain", "denied-artifact.txt",
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
    assert_eq!(inherited.read_file(&file).await?.as_ref(), b"created private bytes".as_ref());
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
    assert_eq!(context.read_file(&file).await?.as_ref(), b"authenticated bytes".as_ref());
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
    created.seed.validate()?;
    Ok(())
}

struct CreatedIoCommands {
    output: Mutex<Option<FileRef>>,
}

impl crate::filesystem::TaskCommandHost for CreatedIoCommands {
    fn execute<'a>(
        &'a self,
        context: crate::runtime::TaskContext,
        _: crate::scheduler::LeaseFence,
        command: crate::workflow::WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<crate::filesystem::TaskCommandProgress>> {
        Box::pin(async move {
            if command.kind != "test.created.content.v1" {
                return Err(Error::Unsupported("unknown created-content command".into()));
            }
            let text = payload.get("text").and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("created-content command lacks text".into()))?;
            let file = context.stage_file_once(
                command.operation_id, "ordinary/worker.txt", text.as_bytes(), "text/plain", "worker.txt",
            ).await?;
            *self.output.lock().await = Some(file);
            Ok(crate::filesystem::TaskCommandProgress::Ready(serde_json::json!({"written":true})))
        })
    }
}

#[tokio::test]
async fn created_scope_executes_existing_workflow_and_journal_input_boundaries() -> Result<()> {
    use crate::executor::ExecutionJournal as _;
    use crate::filesystem::{
        FilesystemExecutionJournal, FilesystemTaskRuntime, TaskWorkerAttempt, TaskWorkerOutcome,
    };
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    assert!(matches!(fixture.admit_child(created.request.child.clone()).await?, Admission::Accepted(_)));
    let task = TaskId::from_bytes([84; 16]);
    let observation = fixture.host.observe_task(task).await?;
    let reservation = observation.reservation.ok_or_else(|| Error::NotFound("created task lease".into()))?;
    let lease = crate::distributed::WorkLease {
        operation: observation.spec, reservation, checkpoint: observation.checkpoint,
        operation_revision: observation.revision,
    };
    let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let original_owner = fixture.host.journal_owner(task,fence.clone()).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let native_work = acyclic_fs::NativeMaterializationWork::new(
        metadata_control_budget(),&cancellation,
    ).map_err(|failure| Error::NativeWork {cause:failure.error,custody:None})?;
    let pending = original_owner.outcome_with_native_work(&native_work).await?;
    assert!(pending.0.is_none());
    assert_eq!(pending.1.bytes(),0);
    drop(pending);
    let context = fixture.harness.durable_context(task, created.request.child.operation_id).await?;
    let live_before = native_work.live_allocation_bytes();
    let staged = context.stage_file_once_with_native_work(
        OperationId::from_bytes([117;16]),"native/control.txt",b"owned",
        "text/plain","control.txt",&native_work,
    ).await?;
    assert_eq!(staged.0.volume(),&created.private);
    assert!(staged.1.bytes() > 0);
    assert_eq!(native_work.live_allocation_bytes(),live_before+staged.1.bytes());
    let read = context.read_file_with_native_work(&staged.0,&native_work).await?;
    assert_eq!(read.0,b"owned");
    assert_eq!(native_work.live_allocation_bytes(),live_before+staged.1.bytes()+read.1.bytes());
    drop(read);
    assert_eq!(native_work.live_allocation_bytes(),live_before+staged.1.bytes());
    drop(staged);
    assert_eq!(native_work.live_allocation_bytes(),live_before);
    let command_payload = context.stage_file_once(
        OperationId::from_bytes([102; 16]), "ordinary/command.json",
        &crate::contract::canonical_json_bytes(&serde_json::json!({"text":"real created-volume command"}))?,
        "application/json", "command.json",
    ).await?;
    let volume = VolumeRef::new(
        fixture.filesystem_provider.clone(), "scheduler", VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
    )?;
    let journal = FilesystemExecutionJournal::for_task(
        fixture.host.journal_owner(task, fence.clone()).await?, OperationId::from_bytes([103; 16]),
        OperationId::from_bytes([103; 16]),
        fixture.filesystem.clone(), volume.clone(), fixture.host.verifier.clone(),
        fixture.host.owner_scope.clone(), 65_536,
    )?;
    journal.verify_input_file(&command_payload).await?;
    let runtime = FilesystemTaskRuntime::open_with_creation_providers(
        fixture.host.stream.clone(), fixture.filesystem.clone(), volume,
        fixture.host.verifier.clone(), fixture.host.owner_scope.clone(), fixture.root_scope.clone(),
        fixture.host.tasks.clone(), fixture.host.machines.clone(), crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits { active_tasks:16, total_tasks:32, depth:4, model_steps:4 },
        16, 65_536, Arc::new(SystemUnixMillisClock), None, fixture.host.resource_creation.clone(),
        crate::durable_tool::ResumableToolRegistry::default(),
    ).await?;
    let mut session = runtime.open_task(task, fence).await?;
    session.step(
        OperationId::from_bytes([104; 16]), IdempotencyKey::new("created-workflow-input")?,
        serde_json::json!({"created_command":crate::workflow::WorkflowCommand {
            operation_id:OperationId::from_bytes([105; 16]), kind:"test.created.content.v1".into(),
            payload:command_payload,
        }}),
    ).await?;
    let commands = CreatedIoCommands { output:Mutex::new(None) };
    assert!(matches!(
        runtime.resume_task(lease, &commands, 2).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task:completed }) if completed == task
    ));
    let output = commands.output.lock().await.clone()
        .ok_or_else(|| Error::NotFound("created workflow content result".into()))?;
    assert_eq!(output.volume(), &created.private);
    assert_eq!(context.read_file(&output).await?.as_ref(), b"real created-volume command".as_ref());
    assert!(!fixture.root_scope.grants().contains(&created.private.capability(VolumeOperation::Write)?));
    assert!(matches!(runtime.task_host().outcome(task).await?, Some(Outcome::Succeeded(_))));
    let terminal = original_owner.outcome_with_native_work(&native_work).await?;
    assert!(matches!(&terminal.0,Some(Outcome::Succeeded(_))));
    assert!(terminal.1.bytes() > 0);
    drop(terminal);
    assert!(native_work.receipt().backend_read_operations > 0);
    assert!(original_owner.verify_with_native_work(true,&native_work).await.is_err());
    cancellation.cancel();
    let before = native_work.receipt();
    assert!(matches!(original_owner.outcome_with_native_work(&native_work).await,
        Err(Error::NativeWork {cause:acyclic_fs::NativeTreeMaterializationError::Cancelled,..})));
    assert_eq!(native_work.receipt().backend_read_operations,before.backend_read_operations);
    Ok(())
}

fn creation_control_budget() -> acyclic_fs::WorkBudget {
    let units = 16_384;
    let bytes = 16 * 1_024 * 1_024;
    acyclic_fs::WorkBudget {
        authority_records_read: units, authority_records_appended: units,
        authority_bytes_read: bytes, authority_bytes_written: bytes,
        object_probes: units, backend_read_operations: units, backend_write_operations: units,
        durability_operations: units, page_reads: units, page_writes: units,
        object_bytes_read: bytes, object_bytes_written: bytes, bytes_hashed: bytes,
        bytes_copied: bytes, bytes_encoded: bytes, source_bytes_read: bytes,
        source_path_components: units, source_entries_visited: units, output_bytes: bytes,
        items_examined: units, items_returned: units, allocation_operations: units,
        peak_allocation_bytes: bytes, materializations: units,
    }
}

#[tokio::test]
async fn explicit_unmounted_result_and_original_scratch_proof_use_same_created_scope() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child_with_options(
        &["fork:publish","conversation:bind"],true,
    ).await?;
    assert_eq!(created.request.resources.len(),3);
    assert_eq!(created.request.child.resource_proofs.len(),3);
    let allocation_cancellation = acyclic_fs::CancellationToken::new();
    let allocation_io = crate::filesystem::PreparationIo::from_budget(
        creation_control_budget(),&allocation_cancellation,
    )?;
    fixture.filesystem.verify_fork_allocation_io(
        &created.seed,&created.private,Some(&allocation_io),
    ).await?;
    assert!(allocation_io.receipt().backend_read_operations > 0);
    assert_eq!(allocation_io.owned_bytes()?,0);
    let result = created.seed.child_result_volume.as_ref()
        .ok_or_else(|| Error::NotFound("created result slot".into()))?;
    assert_ne!(result,&created.private);
    assert_ne!(result,&created.project);
    assert!(created.seed.child_result_generation.is_some());
    assert!(!created.seed.resources.iter().any(|resource| match &resource.revision {
        crate::fork::ResourceRevision::PrivateVolume { volume,.. }
        | crate::fork::ResourceRevision::Project { volume,.. } => volume == result,
        _ => false,
    }));
    assert!(!fixture.root_scope.grants().contains(&result.capability(VolumeOperation::Write)?));
    fixture.admit_child(created.request.child.clone()).await?;
    let task = TaskId::from_bytes(created.request.child.operation_id.into_bytes());
    let context = fixture.harness.durable_context(task,created.request.child.operation_id).await?;
    let file = context.stage_file(
        OperationId::from_bytes([110;16]),"scratch/control.txt",b"scratch",
        "text/plain","control.txt",
    ).await?;
    let artifact = context.stage_artifact(
        OperationId::from_bytes([111;16]),"artifacts/control.txt",b"result",
        "text/plain","control.txt",
    ).await?;
    assert_eq!(file.volume(),&created.private);
    assert_eq!(artifact.volume(),result);
    assert_eq!(context.read_file(&artifact).await?.as_ref(), b"result".as_ref());
    assert_eq!(context.scope().current_created_agent()?,Some(created.seed.child_agent));
    let observation = fixture.host.observe_task(task).await?;
    let reservation = observation.reservation
        .ok_or_else(|| Error::NotFound("created child reservation".into()))?;
    let owner = fixture.host.journal_owner(task,(&reservation).into()).await?;
    assert_eq!(owner.authenticated_binding_agent()?,Some(created.seed.child_agent));
    owner.authenticate_fork_parent(&created.seed.child)?;
    assert!(owner.authenticate_fork_parent(&fixture.host.authority).is_err());
    let proof = owner.child_scratch_creation_proof()?;
    assert_eq!(proof.original_task(),task);
    assert_eq!(proof.agent(),created.seed.child_agent);
    assert_eq!(proof.volume(),&created.private);
    let cancellation = acyclic_fs::CancellationToken::new();
    let authorized = proof.authorize_settlement_measured(&owner,creation_control_budget(),&cancellation).await;
    assert!(authorized.result.is_ok());
    assert!(authorized.counters.backend_read_operations > 0);
    let root_owner = fixture.host.journal_owner(
        fixture.root,(&fixture.root_lease.reservation).into(),
    ).await?;
    assert!(root_owner.child_scratch_creation_proof().is_err());
    let hostile = proof.authorize_settlement_measured(&root_owner,creation_control_budget(),&cancellation).await;
    assert!(hostile.result.is_err());
    assert_eq!(hostile.counters.backend_read_operations,0);
    let denied = proof.authorize_settlement_measured(&owner,acyclic_fs::WorkBudget::default(),&cancellation).await;
    assert!(denied.result.is_err());
    assert_eq!(denied.counters.backend_read_operations,0);
    #[cfg(not(target_arch = "wasm32"))]
    {
        let native_cancellation = acyclic_fs::CancellationToken::new();
        let native_work = acyclic_fs::NativeMaterializationWork::new(
            creation_control_budget(), &native_cancellation,
        ).map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
        proof.authorize_settlement_with_native_work(&owner, &native_work).await?;
        assert!(native_work.receipt().backend_read_operations > 0);
        let before = native_work.receipt().backend_read_operations;
        assert!(matches!(
            proof.authorize_settlement_with_native_work(&root_owner, &native_work).await,
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })
        ));
        assert_eq!(native_work.receipt().backend_read_operations, before);
        native_cancellation.cancel();
        assert!(matches!(
            proof.authorize_settlement_with_native_work(&owner, &native_work).await,
            Err(Error::NativeWork { .. })
        ));
        assert_eq!(native_work.receipt().backend_read_operations, before);

        let fresh_cancellation = acyclic_fs::CancellationToken::new();
        let mut minimal = creation_control_budget();
        minimal.peak_allocation_bytes = acyclic_fs::NativeMaterializationWork::accounting_state_bytes();
        let denied_work = acyclic_fs::NativeMaterializationWork::new(
            minimal, &fresh_cancellation,
        ).map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
        assert!(matches!(
            proof.authorize_settlement_with_native_work(&owner, &denied_work).await,
            Err(Error::NativeWork { .. })
        ));
        assert_eq!(denied_work.receipt().backend_read_operations, 0);
    }
    cancellation.cancel();
    let cancelled = proof.authorize_settlement_measured(&owner,creation_control_budget(),&cancellation).await;
    assert!(cancelled.result.is_err());
    assert_eq!(cancelled.counters.backend_read_operations,0);

    let claim = workspace_ref(fixture.filesystem_provider.clone(),
        &format!("fork-allocation-{}",result.storage_name()?))?;
    let bytes = fixture.filesystem.read(&claim,None,"/claim.json",65_536).await?;
    let mut record: Value = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
    record["role"] = serde_json::json!("scratch");
    fixture.filesystem.apply(&claim,None,&[WorkspaceMutation::PutFile {
        path:"/claim.json".into(),bytes:crate::contract::canonical_json_bytes(&record)?,
    }],&IdempotencyKey::new("corrupt-result-role")?).await?;
    assert!(context.stage_artifact(
        OperationId::from_bytes([112;16]),"artifacts/after-corruption.txt",b"must not publish",
        "text/plain","after-corruption.txt",
    ).await.is_err());
    let rejected_io = crate::filesystem::PreparationIo::from_budget(
        creation_control_budget(),&allocation_cancellation,
    )?;
    assert!(fixture.filesystem.verify_fork_allocation_io(
        &created.seed,result,Some(&rejected_io),
    ).await.is_err());
    assert!(rejected_io.receipt().backend_read_operations > 0);
    assert_eq!(rejected_io.owned_bytes()?,0);
    Ok(())
}

#[tokio::test]
async fn uncertain_root_choice_is_denial_only_and_rejects_identity_substitution() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let owner = fixture.host.journal_owner(
        fixture.root,(&fixture.root_lease.reservation).into(),
    ).await?;
    let operation = OperationId::from_bytes([113;16]);
    let intent = [114;32];
    let audience = [115;32];
    owner.pin_root_binding_choice(operation,intent,audience)?;
    owner.pin_root_binding_choice(operation,intent,audience)?;
    for offset in 0..32 {
        let mut changed_intent = intent;
        changed_intent[offset] ^= 1;
        assert!(matches!(owner.pin_root_binding_choice(operation,changed_intent,audience),
            Err(Error::Conflict(_))));
        let mut changed_audience = audience;
        changed_audience[offset] ^= 1;
        assert!(matches!(owner.pin_root_binding_choice(operation,intent,changed_audience),
            Err(Error::Conflict(_))));
    }
    assert!(matches!(owner.pin_root_binding_choice(
        OperationId::from_bytes([116;16]),intent,audience),Err(Error::Conflict(_))));
    assert!(owner.root_binding.as_ref().and_then(|state| state.proof.get()).is_none());
    assert!(owner.authenticate_fork_parent(&fixture.host.authority).is_err());
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            creation_control_budget(), &cancellation,
        ).map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
        assert!(matches!(
            owner.authenticated_root_binding_with_native_work(&work).await,
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })
        ));
        assert_eq!(work.receipt().backend_read_operations, 0);
    }
    Ok(())
}

mod mailbox_parcels;
mod source_authorization;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_cancel_retains_original_retry_and_never_claims_terminal_cleanup() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    fixture.host.cancel_with_native_work(fixture.root, &work).await?;
    fixture.host.cancel_with_native_work(fixture.root, &work).await?;
    let state = fixture.host.observe_task(fixture.root).await?;
    assert!(state.cancellation_requested);
    assert!(state.outcome.is_none());
    assert!(work.receipt().backend_read_operations > 0);
    let before = work.receipt().backend_read_operations;
    cancellation.cancel();
    assert!(matches!(
        fixture.host.cancel_with_native_work(fixture.root, &work).await,
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(work.receipt().backend_read_operations, before);

    let fresh_cancellation = acyclic_fs::CancellationToken::new();
    let mut minimal = creation_control_budget();
    minimal.peak_allocation_bytes = acyclic_fs::NativeMaterializationWork::accounting_state_bytes();
    let denied = acyclic_fs::NativeMaterializationWork::new(
        minimal, &fresh_cancellation,
    ).map_err(|(cause, _)| Error::NativeWork { cause, custody: None })?;
    assert!(matches!(
        fixture.host.cancel_with_native_work(fixture.root, &denied).await,
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(denied.receipt().backend_read_operations, 0);
    Ok(())
}

#[tokio::test]
async fn canonical_root_preparation_witness_has_no_fabricated_scratch() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })?;
    let witness = fixture.host.task_admission_readonly_witness_with_native_work(
        fixture.root, &work,
    ).await?;
    assert_eq!(witness.task(), fixture.root);
    assert_eq!(witness.admission_operation(), fixture.root_lease.operation.operation_id);
    assert!(witness.scratch_proof().is_none());
    assert!(witness._native_custody.bytes() > 0);
    witness.verify_with_native_work(&work).await?;
    let foreign = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })?;
    let before = foreign.receipt();
    assert!(matches!(witness.verify_with_native_work(&foreign).await,
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(foreign.receipt(), before);
    cancellation.cancel();
    let before = work.receipt();
    assert!(matches!(witness.verify_with_native_work(&work).await,
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
    drop(witness);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

fn native_registered_root_input(
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    use crate::store::history::HistoryIo;
    #[derive(Serialize)]
    struct Input<'a> { payload: &'a str }
    let mut io = crate::store::history::native_work_io(work)?;
    let value = crate::contract::json_value_bounded(
        &Input { payload: "original registered root" },
        crate::contract::JsonBounds {
            maximum_bytes: 4096, maximum_items: 64, maximum_depth: 8,
        },
        Some(crate::wire::ErrorCode::Invalid),
        &mut |charge| io.codec_charge(charge),
    )?;
    Ok((value, io.into_custody()))
}

#[tokio::test]
async fn registered_native_root_keeps_configured_scope_and_same_ack_identity() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let machine = Arc::new(CreatedControlMachine {
        identity: fixture.machine.clone(), schema: fixture.schema.clone(),
    });
    let definition = TaskDefinition::<Value, Value>::resumable(
        machine, fixture.schema.clone(), fixture.schema.clone(),
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })?;
    let operation = OperationId::from_bytes([220; 16]);
    let expected = TaskId::from_bytes(operation.into_bytes());
    for _ in 0..2 {
        let record = fixture.harness.registered_root_admission_with_native_work(
            operation, &definition, native_registered_root_input(&work)?, &work,
        )?;
        assert!(record.0.parent.is_none());
        assert_eq!(record.0.task, fixture.task);
        assert_eq!(record.0.grants, *fixture.root_scope.grants());
        assert_eq!(record.0.run_limits, fixture.root_scope.run_limits());
        let (admitted, custody) = fixture.host.admit_with_native_work(record, &work)?.await?;
        assert!(matches!(admitted, Admission::Accepted(task) if task == expected));
        assert!(custody.matches_work(&work));
        assert_eq!(custody.bytes(), std::mem::size_of::<Admission<TaskId>>() as u64);
        drop(admitted);
        drop(custody);
    }
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn registered_native_root_rejects_foreign_input_and_child_without_fence_before_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let definition = TaskDefinition::<Value, Value>::resumable(
        Arc::new(CreatedControlMachine { identity: fixture.machine.clone(), schema: fixture.schema.clone() }),
        fixture.schema.clone(), fixture.schema.clone(),
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })?;
    let foreign = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })?;
    let before = foreign.receipt();
    assert!(matches!(fixture.harness.registered_root_admission_with_native_work(
        OperationId::from_bytes([221; 16]), &definition, native_registered_root_input(&work)?, &foreign,
    ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(foreign.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(foreign.receipt().backend_write_operations, before.backend_write_operations);
    let mut record = fixture.harness.registered_root_admission_with_native_work(
        OperationId::from_bytes([221; 16]), &definition, native_registered_root_input(&work)?, &work,
    )?;
    record.0.parent = Some(fixture.root);
    let before = work.receipt();
    assert!(matches!(fixture.host.admit_with_native_work(record, &work),
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(work.receipt().backend_write_operations, before.backend_write_operations);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn registered_native_root_cancellation_precedes_metadata_copy_and_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let definition = TaskDefinition::<Value, Value>::resumable(
        Arc::new(CreatedControlMachine {
            identity: fixture.machine.clone(), schema: fixture.schema.clone(),
        }),
        fixture.schema.clone(), fixture.schema.clone(),
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })?;
    let input = native_registered_root_input(&work)?;
    cancellation.cancel();
    let before = work.receipt();
    assert!(matches!(fixture.harness.registered_root_admission_with_native_work(
        OperationId::from_bytes([222; 16]), &definition, input, &work,
    ), Err(Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
    })));
    assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(work.receipt().backend_write_operations, before.backend_write_operations);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn authenticated_root_scope_uses_original_quota_without_created_resource_fallback() -> Result<()> {
    use crate::store::history::HistoryIo as _;
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })?;
    let (admission, admission_custody) = fixture.host.admission_with_native_work(
        fixture.root_lease.operation.operation_id, &work,
    ).await?;
    assert!(admission.resource_proofs.is_empty());
    let mut io = crate::store::history::native_work_io(&work)?;
    let (scope, held) = fixture.root_scope.with_created_access_with_work(
        &admission, None, &mut Some(&mut io),
    )?;
    assert_eq!(scope.grants(), &admission.grants);
    assert_eq!(scope.run_limits(), admission.run_limits);
    assert!(scope.resource_proofs().is_empty());
    assert!(held >= std::mem::size_of::<crate::runtime::RuntimeScope>() as u64);
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(fixture.root_scope.with_created_access_with_work(
        &admission, None, &mut Some(&mut io),
    ), Err(Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
    })));
    assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(work.receipt().backend_write_operations, before.backend_write_operations);
    drop(scope);
    io.release_owned(held)?;
    drop(admission);
    drop(admission_custody);
    drop(io);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn original_read_source_cannot_become_native_directory_execution_owner() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })?;
    let (context, custody) = fixture.harness.durable_context_with_native_work(
        fixture.root, fixture.root_lease.operation.operation_id, &work,
    ).await?;
    let before = work.receipt();
    assert!(matches!(context.verify_original_execution_owner_with_native_work(&work),
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(work.receipt(), before);
    drop(context);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_admission_exhaustion_precedes_source_io_and_preserves_old_ack() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let operation = OperationId::from_bytes(fixture.root.into_bytes());
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut budget = creation_control_budget();
    budget.allocation_operations = 1;
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .expect("only the original ledger allocation");
    let mut io = crate::store::history::native_work_io(&work)?;
    let before = work.receipt();
    assert!(matches!(fixture.host.admission_with_work(operation, &mut Some(&mut io)).await,
        Err(Error::NativeWork { .. })));
    assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
    let retained = fixture.host.admission_with_work(operation, &mut None).await?;
    assert_eq!(retained.operation_id, operation);
    assert_eq!(retained.parent, None);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_admission_json_metadata_denial_precedes_reader_and_hash() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let operation = OperationId::from_bytes(fixture.root.into_bytes());
    let declaration = fixture.host.retained_declaration_with_work(operation, &mut None).await?
        .expect("original acknowledged declaration");
    let original = declaration.state;
    let descriptor = crate::conversation::FileDescriptor::new(
        *original.descriptor().sha256(), original.descriptor().byte_length(), "text/plain",
    )?;
    let reference = FileRef::new(
        original.volume().clone(), original.path(), original.version(), descriptor, original.display_name(),
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        creation_control_budget(), &cancellation,
    ).expect("original native metadata ledger");
    let mut io = crate::store::history::native_work_io(&work)?;
    assert!(matches!(fixture.host.read_json_with_work(&reference, &mut Some(&mut io)).await,
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(fixture.host.admission_with_work(operation, &mut None).await?.operation_id, operation);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_fixture_scope(
    fixture: &CreationFixture, operation: OperationId,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(TaskAdmissionRecord, RuntimeScope, acyclic_fs::NativeAllocation)> {
    let mut io = crate::store::history::native_work_io(work)?;
    let admission = fixture.host.admission_with_work(operation, &mut Some(&mut io)).await?;
    let (scope, scope_held) = fixture.host.authenticated_scope_with_work(&admission, &mut Some(&mut io)).await?;
    let record_held = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
        .checked_add(admission.logical_owned_bytes()?).expect("exact retained fixture record");
    let custody = io.take_native_custody(record_held.checked_add(scope_held)
        .expect("record and scope retain separate exact allowances"))?;
    assert!(custody.matches_work(work));
    Ok((admission, scope, custody))
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_original_root_child_grandchild_keep_exact_created_source() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    fixture.admit_child(created.request.child.clone()).await?;
    let child = TaskId::from_bytes(created.request.child.operation_id.into_bytes());
    let read = created.private.capability(VolumeOperation::Read)?;
    let write = created.private.capability(VolumeOperation::Write)?;
    let mut descendant = created.request.child.clone();
    descendant.operation_id = OperationId::from_bytes([97; 16]);
    descendant.parent = Some(child);
    descendant.grants = Capabilities::new([read.clone()]);
    descendant.limits.file_bytes = 1_024;
    let crate::distributed::WorkPull::Claimed(lease) = fixture.host.recover_work(child).await? else {
        return Err(Error::NotFound("native created parent lease".into()));
    };
    fixture.host.admit_owned(descendant.clone(), (&lease.reservation).into()).await?;
    fixture.start_next(TaskId::from_bytes([97; 16])).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(creation_control_budget(), &cancellation).unwrap();
    let live = work.live_allocation_bytes();
    let root = native_fixture_scope(&fixture, OperationId::from_bytes(fixture.root.into_bytes()), &work).await?;
    assert!(root.0.parent.is_none());
    assert!(root.0.resource_proofs.is_empty());
    assert!(root.1.current_created_agent_with_native_work(&work)?.is_none());
    assert!(!root.1.grants().contains(&read));
    assert!(root.2.matches_work(&work));
    let current = native_fixture_scope(&fixture, created.request.child.operation_id, &work).await?;
    assert!(current.0 == created.request.child);
    assert_eq!(current.1.current_created_agent_with_native_work(&work)?, Some(created.seed.child_agent));
    assert_eq!(current.1.current_created_authority_with_native_work(&work)?, Some(&created.seed.child));
    assert_eq!(current.1.current_created_private_volume_with_native_work(&work)?, Some(&created.private));
    assert!(current.1.grants().contains(&write));
    let inherited = native_fixture_scope(&fixture, descendant.operation_id, &work).await?;
    assert!(inherited.0 == descendant);
    assert_eq!(inherited.1.current_created_agent_with_native_work(&work)?, Some(created.seed.child_agent));
    assert_eq!(inherited.1.current_created_authority_with_native_work(&work)?, Some(&created.seed.child));
    assert!(inherited.1.grants().contains(&read));
    assert!(!inherited.1.grants().contains(&write));
    let readonly = current.1.narrow_with_native_work(
        Capabilities::new_with_native_work([read.as_str()], &work)?, current.1.limits(), &work,
    )?;
    assert!(!readonly.0.grants().contains(&write));
    assert!(readonly.0.narrow_with_native_work(
        Capabilities::new_with_native_work([write.as_str()], &work)?, readonly.0.limits(), &work,
    ).is_err());
    let mut io = crate::store::history::native_work_io(&work)?;
    fixture.host.validate_parent_scope_with_work(Some(child), &readonly.0, &mut Some(&mut io)).await?;
    drop(io);
    drop(readonly);
    drop(inherited);
    drop(current);
    drop(root);
    assert_eq!(work.live_allocation_bytes(), live);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(!fixture.root_scope.grants().contains(&write));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_original_proof_cycle_and_foreign_revision_never_seal_access() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    fixture.admit_child(created.request.child.clone()).await?;
    for change in 0..4 {
        let mut admission = created.request.child.clone();
        match change {
            0 => admission.resource_proofs[0].provider = ProviderRef::new("created-controls", "filesystem", "foreign")?,
            1 => admission.resource_proofs[0].evidence = workspace_ref(fixture.filesystem_provider.clone(), "foreign-evidence")?.as_resource().clone(),
            2 => admission.resource_proofs[0].preparation = TaskId::from_bytes([85; 16]),
            _ => admission.parent = None,
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(creation_control_budget(), &cancellation).unwrap();
        let input_custody = work.retain_owned(std::mem::size_of::<TaskAdmissionRecord>() as u64 + admission.logical_owned_bytes()?)?;
        let mut io = crate::store::history::native_work_io(&work)?;
        let result = fixture.host.authenticated_scope_with_work(&admission, &mut Some(&mut io)).await;
        assert!(result.is_err());
        assert!(result.as_ref().err().unwrap().is_native_failure());
        drop(result);
        drop(io);
        drop(admission);
        drop(input_custody);
        assert!(fixture.host.admission_with_work(OperationId::from_bytes(fixture.root.into_bytes()), &mut None).await?.parent.is_none());
    }
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(creation_control_budget(), &cancellation).unwrap();
    let mut io = crate::store::history::native_work_io(&work)?;
    let admission = fixture.host.read_retained_admission_counted(created.request.child.operation_id, &mut Some(&mut io)).await.map(|(record, _)| record)?;
    let mut visited = Vec::new();
    CoordinatorTaskHost::<MemoryStream>::reserve_proof_slots(&mut visited, &mut Some(&mut io))?;
    visited.push(admission.operation_id);
    let prefix = work.receipt();
    let error = fixture.host.verify_created_access_with_work(&admission, &mut visited, &mut Some(&mut io)).await;
    assert!(matches!(error, Err(Error::NativeBoundary { .. })));
    assert_eq!(work.receipt().backend_read_operations, prefix.backend_read_operations);
    drop(error);
    drop(visited);
    drop(admission);
    drop(io);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_stock_source_provenance_and_cancelled_preparation_are_not_authority() -> Result<()> {
    for change in 0..5 {
        let fixture = CreationFixture::new().await?;
        let created = fixture.create_filesystem_child().await?;
        fixture.admit_child(created.request.child.clone()).await?;
        let claim = workspace_ref(fixture.filesystem_provider.clone(),
            &format!("fork-allocation-{}", created.private.storage_name()?))?;
        let bytes = fixture.filesystem.read(&claim, None, "/claim.json", 65_536).await?;
        let mut source: Value = crate::contract::json_from_slice(&bytes)?;
        match change {
            0 => source["task_origin"]["owner"]["id"] = serde_json::json!("foreign-owner"),
            1 => source["task_origin"]["preparation_task"] = serde_json::to_value(TaskId::from_bytes([85; 16])).unwrap(),
            2 => source["task_origin"]["intended_child"] = serde_json::to_value(TaskId::from_bytes([85; 16])).unwrap(),
            3 => source["task_origin"]["request_digest"] = serde_json::to_value([0u8; 32]).unwrap(),
            _ => source["task_origin"]["admission_digest"] = serde_json::to_value([0u8; 32]).unwrap(),
        }
        fixture.filesystem.apply(&claim, None, &[WorkspaceMutation::PutFile {
            path: "/claim.json".into(), bytes: crate::contract::canonical_json_bytes(&source)?,
        }], &IdempotencyKey::new("native-corrupt-source-provenance")?).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(creation_control_budget(), &cancellation).unwrap();
        let live = work.live_allocation_bytes();
        let error = native_fixture_scope(&fixture, created.request.child.operation_id, &work).await;
        assert!(error.is_err());
        assert!(error.as_ref().err().unwrap().is_native_failure());
        assert!(work.receipt().backend_read_operations > 0);
        drop(error);
        assert_eq!(work.live_allocation_bytes(), live);
        assert!(fixture.host.admission_with_work(OperationId::from_bytes(fixture.root.into_bytes()), &mut None).await?.parent.is_none());
    }
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    fixture.host.cancel(TaskId::from_bytes([83; 16])).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(creation_control_budget(), &cancellation).unwrap();
    let mut io = crate::store::history::native_work_io(&work)?;
    let error = fixture.host.authenticated_scope_with_work(&created.request.child, &mut Some(&mut io)).await;
    assert!(matches!(error, Err(Error::NativeBoundary { code: ErrorCode::Unauthorized, .. })));
    drop(error);
    drop(io);
    assert!(fixture.host.admission_with_work(OperationId::from_bytes(fixture.root.into_bytes()), &mut None).await?.parent.is_none());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_declared_original_scope_mac_denial_precedes_admission_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let operation = OperationId::from_bytes(fixture.root.into_bytes());
    let mut declaration = fixture.host.retained_declaration_with_work(operation, &mut None).await?
        .expect("genuine previously acknowledged root");
    let foreign = crate::core::AuthorityIssuer::new(
        "created-controls-foreign", [93; 32], fixture.host.owner.clone(),
    );
    declaration.owner_scope = foreign.root("foreign-declaration",
        Capabilities::new(["operation:declare", "operation:observe"]));
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(creation_control_budget(), &cancellation)
        .expect("original finite scope authentication ledger");
    let mut io = crate::store::history::native_work_io(&work)?;
    let before = work.receipt();
    let denial = fixture.host.read_declared_admission_counted(
        &declaration, &mut Some(&mut io),
    ).await.map(|(record, _)| record);
    assert!(denial.is_err());
    assert!(denial.as_ref().err().expect("foreign original Scope MAC").is_native_failure());
    assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
    drop(denial);
    drop(io);
    let (root, custody) = fixture.host.admission_with_native_work(operation, &work).await?;
    assert_eq!(custody.bytes(), std::mem::size_of::<TaskAdmissionRecord>() as u64
        + root.logical_owned_bytes()?);
    assert!(custody.matches_work(&work));
    assert_eq!(root.parent, None);
    assert!(root.resource_proofs.is_empty());
    Ok(())
}
