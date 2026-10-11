use super::*;
use crate::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use crate::resources::GenerationRef;
use crate::AgentId;

/// Explicit original ROOT content role, independent of child allocation roles.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RootBootstrapContentRole {
    /// The original project source.
    Project,
    /// The original bound agent's private scratch source.
    Scratch,
}

/// Exact protected original ROOT allocation and selected generation to verify.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootBootstrapContentRequest {
    /// Original admitted ROOT operation, not the RootBind operation.
    pub root_operation: OperationId,
    /// Original canonical task identity.
    pub graph_root: TaskId,
    /// Explicit content role.
    pub role: RootBootstrapContentRole,
    /// Exact allocated volume.
    pub volume: VolumeRef,
    /// Existing protected allocation record reference.
    pub evidence: ResourceRef,
    /// Actual immutable selected generation.
    pub generation: GenerationRef,
    /// Exact protected native snapshot descriptor, if present.
    pub native_snapshot: Option<Value>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct FundedRootPayload<T> {
    data: T,
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl RootBootstrapContentRequest {
    /// Dynamic payload capacities only; the request's inline slot is separate.
    /// Native callers inspect the resident snapshot before this layout audit.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = created_access_add(self.volume.logical_owned_bytes()?,
            self.evidence.logical_owned_bytes()?)?;
        bytes = created_access_add(bytes, self.generation.logical_owned_bytes()?)?;
        if let Some(snapshot) = &self.native_snapshot {
            let owned = crate::tool::value_owned_bytes(snapshot)?;
            bytes = created_access_add(bytes, owned - std::mem::size_of::<Value>() as u64)?;
        }
        Ok(bytes)
    }

    /// Copies concrete reference fields and the resident snapshot on the
    /// original native ledger, admitting each real copy before allocation.
    pub fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        inspect_snapshot(self, work)?;
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut io: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
        io.as_deref_mut().expect("native observer").retain_owned(std::mem::size_of::<Self>() as u64)?;
        let snapshot = match &self.native_snapshot {
            Some(value) => {
                let (data, custody) = crate::tool::clone_value_with_native_work(value, work)?;
                Some(FundedRootPayload { data, custody })
            }
            None => None,
        };
        let volume = self.volume.clone_with_work(&mut io)?;
        let evidence = copy_reference(&self.evidence, &mut io)?;
        let generation = GenerationRef::from_resource_with_work(
            copy_reference(self.generation.as_resource(), &mut io)?, &mut io,
        )?;
        let (native_snapshot, snapshot_custody) = match snapshot {
            Some(FundedRootPayload { data, custody }) => (Some(data), Some(custody)),
            None => (None, None),
        };
        let copied = Self {
            root_operation: self.root_operation, graph_root: self.graph_root, role: self.role,
            volume, evidence, generation, native_snapshot,
        };
        if let Some(custody) = snapshot_custody {
            if let Err(error) = io.as_deref_mut().expect("native observer").absorb_native_custody(custody) {
                drop(copied);
                return Err(error);
            }
        }
        // Value's inline slot already lives inside this admitted request slot.
        if copied.native_snapshot.is_some() {
            io.as_deref_mut().expect("native observer").release_owned(std::mem::size_of::<Value>() as u64)?;
        }
        drop(io);
        Ok((copied, observer.into_custody()))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn copy_reference(
    source: &ResourceRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<ResourceRef> {
    source.validate_with_work(work)?;
    let io = work.as_deref_mut().expect("native observer");
    let namespace = io.clone_text(source.provider().namespace())?;
    let family = io.clone_text(source.provider().family())?;
    let revision = io.clone_text(source.provider().version())?;
    let provider = ProviderRef::new_with_work(namespace, family, revision, work)?;
    let mut key = Vec::new();
    let io = work.as_deref_mut().expect("native observer");
    io.admit_copy(source.key().len() as u64)?;
    io.reserve_bytes(&mut key, source.key().len())?;
    io.copy_bytes(source.key().len() as u64)?;
    key.extend_from_slice(source.key());
    let version = match source.version() {
        Some(version) => Some(io.clone_text(version)?),
        None => None,
    };
    ResourceRef::new_with_work(source.kind(), provider, key, version, work)
}

/// Provider-authenticated ROOT content; only the private consumer grants access.
pub struct RootBootstrapContentReceipt {
    /// Exact authenticated request, never a replacement generation.
    pub request: RootBootstrapContentRequest,
    /// Audience authenticated by the original canonical RootBind.
    pub audience: Authority,
    /// Agent authenticated by the original canonical RootBind.
    pub agent: AgentId,
    /// Existing authenticated content machinery.
    pub binding: crate::runtime::ContentBindings,
}

/// Borrowed authentication context; callers cannot construct one from labels.
#[cfg(feature = "filesystem")]
#[derive(Clone, Copy)]
pub struct RootBootstrapContentContext<'a> {
    original_root: &'a TaskAdmissionRecord,
    owner: &'a Authority,
    audience: &'a Authority,
    agent: AgentId,
    admission_digest: [u8; 32],
}

#[cfg(feature = "filesystem")]
impl<'a> RootBootstrapContentContext<'a> {
    /// Authentic original ROOT admission, never the current result task.
    pub const fn original_root(&self) -> &'a TaskAdmissionRecord { self.original_root }
    /// Trusted coordinator owner, distinct from the RootBind audience.
    pub const fn owner(&self) -> &'a Authority { self.owner }
    /// Canonical RootBind audience.
    pub const fn audience(&self) -> &'a Authority { self.audience }
    /// Canonical RootBind agent.
    pub const fn agent(&self) -> AgentId { self.agent }
    /// Canonical original admission digest used by the SDK content seal.
    /// This is distinct from the protected workflow/RootBind admission digest.
    pub const fn original_admission_digest(&self) -> [u8; 32] { self.admission_digest }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn from_verified_root_with_native_work<P: acyclic_stream::StreamProvider>(
        owner: &'a crate::durable_host::TaskJournalOwner<P>,
        original_root: &'a TaskAdmissionRecord,
        binding: &'a crate::store::VerifiedRootBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        owner.verify_root_bootstrap_context_with_native_work(original_root, binding, work).await?;
        Self::from_authenticated_source_with_native_work(
            original_root, owner.root_bootstrap_owner(), binding, work,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn from_verified_original_root_with_native_work<P: acyclic_stream::StreamProvider>(
        host: &'a crate::durable_host::CoordinatorTaskHost<P>,
        original: &'a crate::distributed::OriginalReservationProof,
        original_root: &'a TaskAdmissionRecord,
        binding: &'a crate::store::VerifiedRootBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        host.verify_original_root_bootstrap_source_with_native_work(
            original, original_root, binding, work,
        )?;
        Self::from_authenticated_source_with_native_work(
            original_root, &original.declaration().owner, binding, work,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn from_authenticated_source_with_native_work(
        original_root: &'a TaskAdmissionRecord,
        owner: &'a Authority,
        binding: &'a crate::store::VerifiedRootBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut io: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
        created_access_step(&mut io)?;
        if original_root.parent.is_some()
            || original_root.task_id() != binding.original_task()
            || crate::runtime::task_id_for_admission(original_root.operation_id) != binding.original_task()
        {
            return Err(root_denial("ROOT bootstrap context differs from original admission"));
        }
        let admission_digest = original_root.canonical_digest_with_work(&mut io)?;
        Ok(Self {
            original_root, owner,
            audience: binding.audience(), agent: binding.agent(), admission_digest,
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn root_denial(reason: &'static str) -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn inspect_snapshot(
    request: &RootBootstrapContentRequest,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    io.check_cancellation()?;
    if let Some(snapshot) = &request.native_snapshot {
        let remaining = io.remaining()?;
        crate::contract::json_owned_value_shape_bounded(
            snapshot,
            crate::contract::JsonBounds {
                maximum_bytes: remaining.bytes_encoded,
                maximum_items: remaining.items_examined,
                maximum_depth: 256,
            },
            crate::contract::JsonValueSchema::None,
            &mut |charge| io.codec_charge(charge, false),
        )?;
    }
    Ok(())
}

/// Mandatory original READ preflight, performed before calling a provider.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn preflight_root_bootstrap_with_native_work(
    context: RootBootstrapContentContext<'_>,
    request: &RootBootstrapContentRequest,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    inspect_snapshot(request, work)?;
    let mut observer = crate::store::history::native_work_io(work)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let mut io: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
    created_access_step(&mut io)?;
    request.volume.validate_with_work(&mut io)?;
    request.evidence.validate_with_work(&mut io)?;
    request.generation.validate_with_work(&mut io)?;
    if context.original_root.parent.is_some()
        || request.root_operation != context.original_root.operation_id
        || request.graph_root != context.original_root.task_id()
        || request.generation.as_resource().provider() != request.volume.provider()
        || request.evidence.provider() != request.volume.provider()
        || !match request.role {
            RootBootstrapContentRole::Project => request.volume.class() == VolumeClass::Project,
            RootBootstrapContentRole::Scratch => request.volume.class() == VolumeClass::AgentPrivate
                && request.volume.owner() == &VolumeOwner::Agent(context.agent),
        }
    {
        return Err(root_denial("ROOT bootstrap request differs from original binding"));
    }
    let (read, custody) = request.volume.capability_with_native_work(VolumeOperation::Read, work)?;
    let allowed = context.original_root.grants.contains(&read);
    drop(read);
    drop(custody);
    if !allowed {
        return Err(root_denial("ROOT bootstrap original admission lacks volume READ"));
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn verify_receipt_identity(
    context: RootBootstrapContentContext<'_>,
    request: &RootBootstrapContentRequest,
    actual: &RootBootstrapContentRequest,
    audience: &Authority,
    agent: AgentId,
    writer: Option<&VolumeRef>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    preflight_root_bootstrap_with_native_work(context, request, work)?;
    preflight_root_bootstrap_with_native_work(context, actual, work)?;
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: audience.id.len() as u64 }, false)?;
    if actual != request || audience != context.audience || agent != context.agent
        || writer.is_some_and(|volume| volume != &request.volume)
    {
        return Err(root_denial("ROOT bootstrap receipt changed request, audience or agent"));
    }
    if writer.is_some() {
        let (data, custody) = request.volume.capability_with_native_work(VolumeOperation::Write, work)?;
        let label = FundedRootPayload { data, custody };
        if !context.original_root.grants.contains(&label.data) {
            return Err(root_denial("ROOT bootstrap writer exceeds original admission WRITE"));
        }
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct ReceivedRootBootstrap {
    // Source DATA always drops before the allocation which funds it.
    receipt: RootBootstrapContentReceipt,
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl CreatedResourceAccess {
    pub(crate) fn consume_root_bootstrap_with_native_work(
        context: RootBootstrapContentContext<'_>,
        request: &RootBootstrapContentRequest,
        receipt: RootBootstrapContentReceipt,
        custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let received = ReceivedRootBootstrap { receipt, custody };
        work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if !received.custody.work().shares_allocation_context(work) {
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
            });
        }
        verify_receipt_identity(
            context, request, &received.receipt.request,
            &received.receipt.audience, received.receipt.agent,
            received.receipt.binding.writer.as_ref().map(|writer| writer.volume()), work,
        )?;
        let source_owned = created_access_add(
            created_access_add(std::mem::size_of::<RootBootstrapContentReceipt>() as u64,
                received.receipt.request.logical_owned_bytes()?)?,
            received.receipt.audience.id.capacity() as u64,
        )?;
        if received.custody.bytes() < source_owned {
            return Err(root_denial("ROOT bootstrap receipt lacks actual payload custody"));
        }
        // This observer is created before all copied payloads, so unwinding
        // drops those payloads before their original-ledger allocation guards.
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut io: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
        let (data, custody) = request.volume.capability_with_native_work(VolumeOperation::Read, work)?;
        let read = FundedRootPayload { data, custody };
        let write = if received.receipt.binding.writer.is_some() {
            let (data, custody) = request.volume.capability_with_native_work(VolumeOperation::Write, work)?;
            Some(FundedRootPayload { data, custody })
        } else { None };
        let labels = [Some(read.data.as_str()), write.as_ref().map(|label| label.data.as_str())];
        let (data, custody) = crate::Capabilities::new_with_native_work(
            labels.into_iter().flatten(), work,
        )?;
        let funded_grants = FundedRootPayload { data, custody };
        drop((read, write));
        let mut content = Vec::new();
        created_access_reserve(&mut content, 1, &mut io)?;
        let volume = request.volume.clone_with_work(&mut io)?;
        let authority = io.as_deref_mut().expect("native observer").clone_authority(context.audience)?;
        io.as_deref_mut().expect("native observer").retain_owned(std::mem::size_of::<Self>() as u64)?;
        let default_writer = request.role == RootBootstrapContentRole::Scratch
            && received.receipt.binding.writer.is_some();
        // ContentBindings owns opaque shared provider handles, not SDK buffers.
        content.push(CreatedContentBinding {
            volume, authority: Some(authority), binding: received.receipt.binding.clone(),
            default_writer,
            artifact_writer: false, scratch_creation: None,
        });
        let mut access = Self {
            operation: context.original_root.operation_id,
            admission_digest: context.admission_digest,
            grants: funded_grants.data, content, references: Vec::new(), proofs: Vec::new(),
            publication_lease: None,
            current_default_origin: default_writer.then_some(request.root_operation),
            native_custody: None,
        };
        drop(received);
        // Source custody has physically retired; only actual final copied
        // payload custody survives, with no provider-claim-to-query substitution.
        if let Err(error) = io.as_deref_mut().expect("native observer").absorb_native_custody(funded_grants.custody) {
            drop(access);
            return Err(error);
        }
        let owned = access.logical_owned_bytes()?;
        access.native_custody = Some(io.as_deref_mut().expect("native observer").take_native_custody(owned)?);
        Ok(access)
    }

    /// Combines only sealed original ROOT roles, never child or result access.
    pub(crate) fn merge_root_bootstrap_with_native_work(
        self, next: Self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if self.operation != next.operation || self.admission_digest != next.admission_digest
            || !self.proofs.is_empty() || !next.proofs.is_empty()
            || !self.references.is_empty() || !next.references.is_empty()
            || self.publication_lease.is_some() || next.publication_lease.is_some()
            || self.content.len() != 1 || next.content.len() != 1
            || self.current_default_origin.is_some() && next.current_default_origin.is_some()
            || self.content[0].volume == next.content[0].volume
            || self.content[0].volume.class() == next.content[0].volume.class()
            || self.native_custody.as_ref().is_none_or(|guard| !guard.work().shares_allocation_context(work))
            || next.native_custody.as_ref().is_none_or(|guard| !guard.work().shares_allocation_context(work))
        {
            return Err(root_denial("ROOT bootstrap seals cannot be combined"));
        }
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut io: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
        let (data, custody) = crate::Capabilities::new_with_native_work(
            self.grants.iter().chain(next.grants.iter()), work,
        )?;
        let funded_grants = FundedRootPayload { data, custody };
        let mut content = Vec::new();
        created_access_reserve(&mut content, 2, &mut io)?;
        for source in self.content.iter().chain(next.content.iter()) {
            created_access_step(&mut io)?;
            let volume = source.volume.clone_with_work(&mut io)?;
            let authority = match &source.authority {
                Some(authority) => Some(io.as_deref_mut().expect("native observer").clone_authority(authority)?),
                None => return Err(root_denial("ROOT bootstrap seal lost its audience")),
            };
            content.push(CreatedContentBinding {
                volume, authority, binding: source.binding.clone(),
                default_writer: source.default_writer, artifact_writer: false, scratch_creation: None,
            });
        }
        io.as_deref_mut().expect("native observer").retain_owned(std::mem::size_of::<Self>() as u64)?;
        let mut access = Self {
            operation: self.operation, admission_digest: self.admission_digest,
            grants: funded_grants.data, content, references: Vec::new(), proofs: Vec::new(),
            publication_lease: None,
            current_default_origin: self.current_default_origin.or(next.current_default_origin),
            native_custody: None,
        };
        drop((self, next));
        if let Err(error) = io.as_deref_mut().expect("native observer").absorb_native_custody(funded_grants.custody) {
            drop(access);
            return Err(error);
        }
        let owned = access.logical_owned_bytes()?;
        access.native_custody = Some(io.as_deref_mut().expect("native observer").take_native_custody(owned)?);
        Ok(access)
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use crate::core::AggregateKind;

    fn work(cancellation: &CancellationToken, allocations: u64) -> Result<NativeMaterializationWork> {
        NativeMaterializationWork::new(WorkBudget {
            bytes_encoded: 2_000_000, bytes_copied: 2_000_000, bytes_hashed: 2_000_000,
            items_examined: 100_000, allocation_operations: allocations,
            peak_allocation_bytes: 2_000_000, ..WorkBudget::default()
        }, cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })
    }

    fn request(role: RootBootstrapContentRole, agent: AgentId) -> Result<RootBootstrapContentRequest> {
        let provider = ProviderRef::new("root-control", "filesystem", "1")?;
        let volume = VolumeRef::new(provider.clone(), "original-volume",
            match role {
                RootBootstrapContentRole::Project => VolumeClass::Project,
                RootBootstrapContentRole::Scratch => VolumeClass::AgentPrivate,
            },
            match role {
                RootBootstrapContentRole::Project => VolumeOwner::Project("original-project".into()),
                RootBootstrapContentRole::Scratch => VolumeOwner::Agent(agent),
            })?;
        let root_operation = OperationId::from_bytes([121; 16]);
        Ok(RootBootstrapContentRequest {
            root_operation, graph_root: crate::runtime::task_id_for_admission(root_operation),
            role, volume,
            evidence: ResourceRef::new(crate::resources::ResourceKind::Workspace, provider.clone(),
                b"protected-original-allocation".to_vec(), None)?,
            generation: GenerationRef::new(provider, b"actual-selected-generation".to_vec(), None)?,
            native_snapshot: Some(serde_json::json!({"actual":"descriptor","selected":121})),
        })
    }

    fn admission(request: &RootBootstrapContentRequest, read: bool, write: bool) -> Result<TaskAdmissionRecord> {
        let mut labels = Vec::new();
        if read { labels.push(request.volume.capability(VolumeOperation::Read)?); }
        if write { labels.push(request.volume.capability(VolumeOperation::Write)?); }
        Ok(TaskAdmissionRecord {
            operation_id: request.root_operation,
            task: crate::registry::ComponentIdentity {
                name: "test.root-seal".into(), version: "1".into(), digest: [122; 32],
            },
            machine: crate::workflow::MachineIdentity {
                name: "test.root-seal".into(), version: "1".into(), digest: [123; 32],
            },
            input: serde_json::json!({"retained":"original"}),
            input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({"type":"object"}),
            parent: None, resource_proofs: Vec::new(), grants: crate::Capabilities::new(labels),
            limits: crate::Limits::default(), run_limits: crate::runtime::TaskRunLimits::default(),
            policy: None, extensions: None, execution: None,
        })
    }

    // These controls exercise the private post-authentication consumer seam.
    // They do not mint a public context or mock a provider/FS authentication.
    fn context<'a>(admission: &'a TaskAdmissionRecord, owner: &'a Authority, audience: &'a Authority,
        agent: AgentId) -> RootBootstrapContentContext<'a>
    {
        RootBootstrapContentContext {
            original_root: admission, owner, audience, agent,
            admission_digest: crate::contract::canonical_json_digest(&admission.canonical_payload())
                .expect("canonical fixture admission"),
        }
    }

    fn unauthorized(result: Result<()>) {
        assert!(matches!(result, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
    }

    #[test]
    fn original_read_is_required_before_io_and_write_does_not_substitute() -> Result<()> {
        let agent = AgentId::from_bytes([125; 16]);
        let request = request(RootBootstrapContentRole::Project, agent)?;
        let owner = Authority { kind: AggregateKind::Task, id: "owner".into() };
        let audience = Authority { kind: AggregateKind::Conversation, id: "audience".into() };
        for (read, write) in [(true, false), (true, true), (false, false), (false, true)] {
            let admitted = admission(&request, read, write)?;
            let work = work(&CancellationToken::new(), 100_000)?;
            let result = preflight_root_bootstrap_with_native_work(
                context(&admitted, &owner, &audience, agent), &request, &work,
            );
            if read { result?; } else { unauthorized(result); }
            assert_eq!(work.receipt().authority_records_read, 0);
            assert_eq!(work.receipt().backend_read_operations, 0);
            assert_eq!(work.receipt().backend_write_operations, 0);
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        Ok(())
    }

    #[test]
    fn child_task_operation_and_private_owner_cannot_become_original_root() -> Result<()> {
        let agent = AgentId::from_bytes([125; 16]);
        let owner = Authority { kind: AggregateKind::Task, id: "owner".into() };
        let audience = Authority { kind: AggregateKind::Conversation, id: "audience".into() };
        let original = request(RootBootstrapContentRole::Scratch, agent)?;
        let admitted = admission(&original, true, false)?;
        let work = work(&CancellationToken::new(), 100_000)?;
        preflight_root_bootstrap_with_native_work(context(&admitted, &owner, &audience, agent), &original, &work)?;
        let mut child = admitted.clone();
        child.parent = Some(TaskId::from_bytes([126; 16]));
        unauthorized(preflight_root_bootstrap_with_native_work(context(&child, &owner, &audience, agent), &original, &work));
        let mut changed = original.clone();
        changed.graph_root = TaskId::from_bytes([127; 16]);
        unauthorized(preflight_root_bootstrap_with_native_work(context(&admitted, &owner, &audience, agent), &changed, &work));
        changed = original.clone();
        changed.root_operation = OperationId::from_bytes([128; 16]);
        unauthorized(preflight_root_bootstrap_with_native_work(context(&admitted, &owner, &audience, agent), &changed, &work));
        unauthorized(preflight_root_bootstrap_with_native_work(
            context(&admitted, &owner, &audience, AgentId::from_bytes([129; 16])), &original, &work,
        ));
        changed = original.clone();
        changed.role = RootBootstrapContentRole::Project;
        unauthorized(preflight_root_bootstrap_with_native_work(context(&admitted, &owner, &audience, agent), &changed, &work));
        Ok(())
    }

    #[test]
    fn receipt_identity_audience_agent_and_write_are_exact() -> Result<()> {
        let agent = AgentId::from_bytes([125; 16]);
        let original = request(RootBootstrapContentRole::Project, agent)?;
        let owner = Authority { kind: AggregateKind::Task, id: "owner".into() };
        let audience = Authority { kind: AggregateKind::Conversation, id: "audience".into() };
        let admitted = admission(&original, true, false)?;
        let context = context(&admitted, &owner, &audience, agent);
        let work = work(&CancellationToken::new(), 100_000)?;
        verify_receipt_identity(context, &original, &original, &audience, agent, None, &work)?;
        unauthorized(verify_receipt_identity(context, &original, &original, &owner, agent, None, &work));
        unauthorized(verify_receipt_identity(context, &original, &original, &audience,
            AgentId::from_bytes([130; 16]), None, &work));
        unauthorized(verify_receipt_identity(context, &original, &original, &audience,
            agent, Some(&original.volume), &work));
        let admitted_rw = admission(&original, true, true)?;
        verify_receipt_identity(super::tests::context(&admitted_rw, &owner, &audience, agent),
            &original, &original, &audience, agent, Some(&original.volume), &work)?;
        for kind in 0..3 {
            let mut changed = original.clone();
            match kind {
                0 => changed.native_snapshot = Some(serde_json::json!({"actual":"different"})),
                1 => changed.generation = GenerationRef::new(original.volume.provider().clone(), b"other".to_vec(), None)?,
                _ => changed.evidence = ResourceRef::new(crate::resources::ResourceKind::Workspace,
                    original.volume.provider().clone(), b"other-evidence".to_vec(), None)?,
            }
            unauthorized(verify_receipt_identity(context, &original, &changed, &audience, agent, None, &work));
        }
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn native_copy_cancellation_allocation_denial_and_deep_value_stop_before_io() -> Result<()> {
        let agent = AgentId::from_bytes([125; 16]);
        let mut request = request(RootBootstrapContentRole::Project, agent)?;
        let cancellation = CancellationToken::new();
        let work = work(&cancellation, 100_000)?;
        let (copied, custody) = request.clone_with_native_work(&work)?;
        assert!(copied == request);
        assert_eq!(custody.bytes(), std::mem::size_of::<RootBootstrapContentRequest>() as u64
            + copied.logical_owned_bytes()?);
        assert!(work.receipt().bytes_copied > 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        drop(copied);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), 0);
        cancellation.cancel();
        assert!(matches!(request.clone_with_native_work(&work), Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        })));
        let denied = super::tests::work(&CancellationToken::new(), 0)?;
        assert!(request.clone_with_native_work(&denied).is_err());
        assert_eq!(denied.receipt().bytes_copied, 0);
        assert_eq!(denied.live_allocation_bytes(), 0);
        let mut value = Value::Null;
        for _ in 0..300 { value = Value::Array(vec![value]); }
        request.native_snapshot = Some(value);
        let depth_work = super::tests::work(&CancellationToken::new(), 100_000)?;
        assert!(request.clone_with_native_work(&depth_work).is_err());
        assert_eq!(depth_work.receipt().bytes_copied, 0);
        assert_eq!(depth_work.live_allocation_bytes(), 0);
        Ok(())
    }
}
