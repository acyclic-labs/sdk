use super::*;
use crate::runtime::TaskStateProvider;
use crate::conversation::VolumeRef;
#[cfg(feature = "filesystem")]
use crate::conversation::{VolumeClass, VolumeOperation, VolumeOwner};
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
impl RootBootstrapContentRequest {
    /// Dynamic payload capacities only; the request's inline slot is separate.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = created_access_add(self.volume.logical_owned_bytes()?,
            self.evidence.logical_owned_bytes()?)?;
        bytes = created_access_add(bytes, self.generation.logical_owned_bytes()?)?;
        if let Some(snapshot) = &self.native_snapshot {
            bytes = created_access_add(bytes, crate::contract::json_value_logical_owned_bytes(snapshot)?)?;
        }
        Ok(bytes)
    }

    /// Copies the request on its original native ledger.
    pub fn copy(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        inspect_snapshot(self, work)?;
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut observed = Some(&mut observer as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, std::mem::size_of::<Self>() as u64)?;
        let native_snapshot = self.native_snapshot.as_ref()
            .map(|value| crate::core::copy_value(value, &mut quota)).transpose()?;
        let volume = self.volume.copy(quota.work())?;
        quota.adopt(volume.logical_owned_bytes()?)?;
        let evidence = copy_reference(&self.evidence, &mut quota)?;
        let generation = GenerationRef::from_resource_with_work(
            copy_reference(self.generation.as_resource(), &mut quota)?, quota.work(),
        )?;
        let copied = Self {
            root_operation: self.root_operation, graph_root: self.graph_root, role: self.role,
            volume, evidence, generation, native_snapshot,
        };
        quota.transfer();
        drop(quota);
        Ok((copied, observer.into_custody()))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn copy_reference(
    source: &ResourceRef, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ResourceRef> {
    source.validate_with_work(quota.work())?;
    let provider = crate::core::copy_provider(source.provider(), quota)?;
    let key = crate::core::copy_copy_vector(source.key(), quota)?;
    let version = source.version().map(|value| crate::core::copy_text(value, quota)).transpose()?;
    ResourceRef::new_with_work(source.kind(), provider, key, version, quota.work())
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
        original: &'a (crate::scheduler::OperationSpec, crate::scheduler::LeaseFence),
        original_root: &'a TaskAdmissionRecord,
        binding: &'a crate::store::VerifiedRootBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        host.verify_original_root_bootstrap_source_with_native_work(
            original, original_root, binding, work,
        )?;
        Self::from_authenticated_source_with_native_work(
            original_root, original.0.owner.authority(), binding, work,
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
    request.volume.validate(&mut io)?;
    request.evidence.validate_with_work(&mut io)?;
    request.generation.validate_with_work(&mut io)?;
    if request.root_operation != context.original_root.operation_id
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
    let read = request.volume.capability(VolumeOperation::Read, &mut io)?;
    let allowed = context.original_root.grants.contains(&read);
    let bytes = read.capacity() as u64;
    drop(read);
    created_access_release(&mut io, bytes)?;
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
    inspect_snapshot(actual, work)?;
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: audience.id.len() as u64 }, false)?;
    if actual != request || audience != context.audience || agent != context.agent
        || writer.is_some_and(|volume| volume != &request.volume)
    {
        return Err(root_denial("ROOT bootstrap receipt changed request, audience or agent"));
    }
    if writer.is_some() {
        let label = crate::executor::with_history_io(Some(&io), |work|
            request.volume.capability(VolumeOperation::Write, work))?;
        if !context.original_root.grants.contains(&label) {
            return Err(root_denial("ROOT bootstrap writer exceeds original admission WRITE"));
        }
    }
    Ok(())
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
        let received = (receipt, custody);
        work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if !received.1.work().shares_allocation_context(work) {
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
            });
        }
        verify_receipt_identity(
            context, request, &received.0.request,
            &received.0.audience, received.0.agent,
            received.0.binding.writer.as_ref().map(|writer| writer.volume()), work,
        )?;
        let source_owned = created_access_add(
            created_access_add(std::mem::size_of::<RootBootstrapContentReceipt>() as u64,
                received.0.request.logical_owned_bytes()?)?,
            received.0.audience.id.capacity() as u64,
        )?;
        if received.1.bytes() < source_owned {
            return Err(root_denial("ROOT bootstrap receipt lacks actual payload custody"));
        }
        // Declare the observer before the payloads so guards outlive them.
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut io: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
        let read = request.volume.capability(VolumeOperation::Read, &mut io)?;
        let write = if received.0.binding.writer.is_some() {
            Some(request.volume.capability(VolumeOperation::Write, &mut io)?)
        } else { None };
        let labels = [Some(read.as_str()), write.as_deref()];
        let (data, custody) = crate::Capabilities::new_with_native_work(
            labels.into_iter().flatten(), work,
        )?;
        let funded_grants = (data, custody);
        let label_bytes = read.capacity() as u64 + write.as_ref().map_or(0, |label| label.capacity() as u64);
        drop((read, write));
        created_access_release(&mut io, label_bytes)?;
        let mut content = Vec::new();
        created_access_reserve(&mut content, 1, &mut io)?;
        let volume = request.volume.copy(&mut io)?;
        let authority = io.as_deref_mut().expect("native observer").clone_authority(context.audience)?;
        io.as_deref_mut().expect("native observer").retain_owned(std::mem::size_of::<Self>() as u64)?;
        let default_writer = request.role == RootBootstrapContentRole::Scratch
            && received.0.binding.writer.is_some();
        // ContentBindings owns opaque shared provider handles, not SDK buffers.
        content.push(CreatedContentBinding {
            volume, authority: Some(authority), binding: received.0.binding.clone(),
            default_writer,
            artifact_writer: false, scratch_creation: None,
        });
        let mut access = Self {
            operation: context.original_root.operation_id,
            admission_digest: context.admission_digest,
            grants: funded_grants.0, content, references: Vec::new(), proofs: Vec::new(),
            publication_lease: None,
            native_custody: None,
        };
        drop(received);
        // Retire source custody before transferring the copied payloads.
        if let Err(error) = io.as_deref_mut().expect("native observer").absorb_native_custody(funded_grants.1) {
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
        let funded_grants = (data, custody);
        let mut content = Vec::new();
        created_access_reserve(&mut content, 2, &mut io)?;
        for source in self.content.iter().chain(next.content.iter()) {
            created_access_step(&mut io)?;
            let volume = source.volume.copy(&mut io)?;
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
            grants: funded_grants.0, content, references: Vec::new(), proofs: Vec::new(),
            publication_lease: None,
            native_custody: None,
        };
        drop((self, next));
        if let Err(error) = io.as_deref_mut().expect("native observer").absorb_native_custody(funded_grants.1) {
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
        let volume = VolumeRef::new(provider.clone(), "original-volume".into(),
            match role {
                RootBootstrapContentRole::Project => VolumeClass::Project,
                RootBootstrapContentRole::Scratch => VolumeClass::AgentPrivate,
            },
            match role {
                RootBootstrapContentRole::Project => VolumeOwner::Project("original-project".into()),
                RootBootstrapContentRole::Scratch => VolumeOwner::Agent(agent),
            }, &mut None)?;
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
        if read { labels.push(request.volume.capability(VolumeOperation::Read, &mut None)?); }
        if write { labels.push(request.volume.capability(VolumeOperation::Write, &mut None)?); }
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
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        Ok(())
    }

    #[test]
    fn request_identity_and_private_owner_match_original_root() -> Result<()> {
        let agent = AgentId::from_bytes([125; 16]);
        let owner = Authority { kind: AggregateKind::Task, id: "owner".into() };
        let audience = Authority { kind: AggregateKind::Conversation, id: "audience".into() };
        let original = request(RootBootstrapContentRole::Scratch, agent)?;
        let admitted = admission(&original, true, false)?;
        let work = work(&CancellationToken::new(), 100_000)?;
        preflight_root_bootstrap_with_native_work(context(&admitted, &owner, &audience, agent), &original, &work)?;
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
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn native_copy_cancellation_allocation_denial_and_deep_value_stop_before_io() -> Result<()> {
        let agent = AgentId::from_bytes([125; 16]);
        let mut request = request(RootBootstrapContentRole::Project, agent)?;
        let cancellation = CancellationToken::new();
        let work = work(&cancellation, 100_000)?;
        let (copied, custody) = request.copy(&work)?;
        assert!(copied == request);
        assert!(work.receipt().bytes_copied > 0);
        drop(copied);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), 0);
        cancellation.cancel();
        assert!(matches!(request.copy(&work), Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        })));
        let denied = super::tests::work(&CancellationToken::new(), 0)?;
        assert!(request.copy(&denied).is_err());
        assert_eq!(denied.receipt().bytes_copied, 0);
        assert_eq!(denied.live_allocation_bytes(), 0);
        let mut value = Value::Null;
        for _ in 0..300 { value = Value::Array(vec![value]); }
        request.native_snapshot = Some(value);
        let depth_work = super::tests::work(&CancellationToken::new(), 100_000)?;
        assert!(request.copy(&depth_work).is_err());
        assert_eq!(depth_work.receipt().bytes_copied, 0);
        assert_eq!(depth_work.live_allocation_bytes(), 0);
        Ok(())
    }
}
