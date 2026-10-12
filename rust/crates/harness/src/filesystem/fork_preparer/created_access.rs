//! Task-bound access over the existing protected allocation and seed journals.

use super::*;
use crate::{
    Capabilities, TaskId,
    conversation::ContentFuture,
    conversation::{Attachment, ContentPublisher, FileRef, Limits, PrivateDirectoryPage, VolumeClass, VolumeOwner},
    core::AuthorityIssuer,
    resource_preparation::{CreatedContentBinding, CreatedReferenceBinding, CreatedResourceOperation,
        CreatedResourceReceipt, ResourceCreationProofRef, ResourceCreationProvider, ResourcePreparationRequest},
    resources::GenerationRef,
    runtime::ContentBindings,
};
use super::super::{FilesystemContentPublisher, FilesystemContentVerifier};
use acyclic_fs::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};

/// Trusted creation verifier configured with host-managed signing authority.
pub struct FilesystemResourceCreationProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    issuer: AuthorityIssuer,
    #[cfg(not(target_arch = "wasm32"))]
    root_project: std::sync::OnceLock<Arc<FilesystemRootBootstrapAllocation<A, O>>>,
    #[cfg(not(target_arch = "wasm32"))]
    root_scratch: std::sync::OnceLock<Arc<FilesystemRootBootstrapAllocation<A, O>>>,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemResourceCreationProvider<A, O> {
    /// Binds the Filesystem host and the preparation coordinator's issuer.
    pub fn new(host: Arc<FilesystemHost<A, O>>, issuer: AuthorityIssuer) -> Result<Self> {
        host.provider.validate()?;
        Ok(Self {
            host, issuer,
            #[cfg(not(target_arch = "wasm32"))]
            root_project: std::sync::OnceLock::new(),
            #[cfg(not(target_arch = "wasm32"))]
            root_scratch: std::sync::OnceLock::new(),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        })
    }

    /// Copies the configured signing identity through the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        host: Arc<FilesystemHost<A, O>>, issuer: &AuthorityIssuer,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        host.provider.validate_with_native_work(work)?;
        let mut custody = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(Error::from)?;
        let (issuer, mut issuer_custody) = issuer.clone_with_native_work(work)?;
        custody.absorb(&mut issuer_custody)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(issuer_custody) })?;
        Ok(Self {
            host, issuer, native_custody: Some(custody),
            root_project: std::sync::OnceLock::new(), root_scratch: std::sync::OnceLock::new(),
        })
    }

    /// Shares the already paid logical creator Self with one SDK Arc attempt.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self.native_custody.as_ref().is_some_and(|guard| guard.matches_work(work)) {
            return Err(native_error(crate::wire::ErrorCode::Invalid, "resource creator belongs to another original native resource"));
        }
        work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
            ..acyclic_fs::WorkCounters::default() })
            .map_err(Error::from)?;
        Ok(Arc::new(self))
    }

    /// Pins only this configured creator's already protected original Root identities.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bind_root_bootstrap_allocation_with_native_work(
        &self, proof: Arc<FilesystemRootBootstrapAllocation<A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        let verifier_custody;
        let (verifier, custody) = self.issuer.verifier_with_native_work(work)?;
        verifier_custody = custody;
        let configured = proof.creator == verifier;
        drop(verifier);
        drop(verifier_custody);
        if !Arc::ptr_eq(&proof.host, &self.host) || proof.owner != *self.issuer.audience()
            || !configured || !proof.matches_native_work(work)
        {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root allocation is not this configured creator's protected original"));
        }
        let slot = match proof.role {
            crate::resource_preparation::RootBootstrapContentRole::Project => &self.root_project,
            crate::resource_preparation::RootBootstrapContentRole::Scratch => &self.root_scratch,
        };
        let other = if proof.role == crate::resource_preparation::RootBootstrapContentRole::Project {
            self.root_scratch.get()
        } else { self.root_project.get() };
        if other.is_some_and(|other| other.root != proof.root || other.owner != proof.owner
            || other.agent != proof.agent || other.admission_digest != proof.admission_digest)
        {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "configured Root allocations belong to different original admissions"));
        }
        if let Err(proof) = slot.set(proof) {
            if !slot.get().is_some_and(|prior| Arc::ptr_eq(prior, &proof)) {
                return Err(native_error(crate::wire::ErrorCode::Conflict, "configured original Root allocation changed"));
            }
        }
        Ok(())
    }

    /// Borrows the two configured allocations without cloning proof payloads.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bound_root_bootstrap_allocations(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<[&Arc<FilesystemRootBootstrapAllocation<A, O>>; 2]> {
        work.remaining()?;
        let project = self.root_project.get().ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "original Root Project allocation is not bound"))?;
        let scratch = self.root_scratch.get().ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "original Root Scratch allocation is not bound"))?;
        if !project.matches_native_work(work) || !scratch.matches_native_work(work) {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "original Root allocations belong to another materialization ledger"));
        }
        Ok([project, scratch])
    }
}

/// Original Root allocation association issued only by the configured creator after canonical Root binding and protected Filesystem birth authentication.
pub struct FilesystemRootBootstrapAllocation<A, O> {
    pub(crate) host: Arc<FilesystemHost<A, O>>,
    pub(crate) owner: Authority,
    pub(crate) audience: Authority,
    creator: crate::core::AuthorityVerifier,
    pub(crate) root: TaskId,
    pub(crate) admission_digest: [u8; 32],
    pub(crate) agent: crate::AgentId,
    pub(crate) role: crate::resource_preparation::RootBootstrapContentRole,
    pub(crate) request: crate::resource_preparation::RootBootstrapContentRequest,
    pub(crate) operation: acyclic_fs::OperationId,
    pub(crate) config: acyclic_fs::VolumeConfig,
    pub(crate) initial_generation: acyclic_fs::GenerationId,
    pub(crate) selected_generation: acyclic_fs::GenerationId,
    pub(crate) native_snapshot: Option<acyclic_fs::LazySnapshotRef>,
    pub(crate) native_source: Option<super::super::root_source::FsRootNativeSource<A, O>>,
    #[cfg(not(target_arch = "wasm32"))]
    publication_lease: Option<Arc<dyn crate::resource_preparation::CreatedTaskLease>>,
    pub(crate) writable: bool,
    record_digest: [u8; 32],
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) witness: crate::durable_host::RootBootstrapBindingWitness,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: acyclic_fs::NativeAllocation,
}

#[derive(Serialize)]
struct RootBootstrapRecord<'a> {
    owner: &'a Authority,
    audience: &'a Authority,
    root: TaskId,
    admission_digest: [u8; 32],
    binding_admission_digest: [u8; 32],
    role: crate::resource_preparation::RootBootstrapContentRole,
    agent: crate::AgentId,
    bind_operation: OperationId,
    bind_intent: [u8; 32],
    creation_fence: &'a crate::scheduler::LeaseFence,
    volume: &'a VolumeRef,
    creation_operation: acyclic_fs::OperationId,
    config: acyclic_fs::VolumeConfig,
    initial_generation: acyclic_fs::GenerationId,
    selected_generation: acyclic_fs::GenerationId,
    native_snapshot: Option<acyclic_fs::LazySnapshotRef>,
    writable: bool,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemResourceCreationProvider<A, O>
{
    /// Associates successful pre-admission FS allocation with the actual original Root owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn protect_root_bootstrap_with_native_work<'a, P: StreamProvider>(
        &'a self, owner: &'a crate::durable_host::TaskJournalOwner<P>,
        volume: &'a VolumeRef, role: crate::resource_preparation::RootBootstrapContentRole,
        operation: acyclic_fs::OperationId,
        config: acyclic_fs::VolumeConfig, initial_generation: acyclic_fs::GenerationId,
        selected_generation: acyclic_fs::GenerationId,
        native_import: Option<(&'a super::super::root_source::FsRootNativeSource<A, O>, acyclic_fs::LazySnapshotRef)>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, FilesystemRootBootstrapAllocation<A, O>>> {
        volume.validate_with_native_work(work)?;
        let agent = owner.authenticated_binding_agent_with_native_work(work)?;
        if owner.root_bootstrap_owner() != self.issuer.audience() || volume.provider() != &self.host.provider
            || !match role {
                crate::resource_preparation::RootBootstrapContentRole::Project =>
                    volume.class() == VolumeClass::Project,
                crate::resource_preparation::RootBootstrapContentRole::Scratch =>
                    volume.class() == VolumeClass::AgentPrivate
                        && matches!(volume.owner(), VolumeOwner::Agent(owner) if Some(*owner) == agent),
            }
        {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root bootstrap differs from its configured original creator or namespace"));
        }
        if native_import.is_some_and(|(source, _)| !source.matches_native_work(work)) {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root lazy source belongs to another original native ledger"));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let creator_custody;
            let (creator, custody) = self.issuer.verifier_with_native_work(work)?;
            creator_custody = custody;
            owner.authenticate_root_bootstrap_creator_with_native_work(&creator, work).await?;
            drop(creator);
            drop(creator_custody);
            let witness = owner.root_bootstrap_binding_witness_with_native_work(work).await?;
            let root = witness.binding();
            let original_custody;
            let (original_root, custody) = owner.original_root_admission_with_native_work(work).await?;
            original_custody = custody;
            let context = crate::resource_preparation::RootBootstrapContentContext::from_verified_root_with_native_work(
                owner, &original_root, root, work,
            ).await?;
            volume.validate_with_native_work(work)?;
            if owner.root_bootstrap_owner() != self.issuer.audience() || volume.provider() != &self.host.provider
                || !match role {
                    crate::resource_preparation::RootBootstrapContentRole::Project =>
                        volume.class() == crate::conversation::VolumeClass::Project,
                    crate::resource_preparation::RootBootstrapContentRole::Scratch =>
                        volume.class() == crate::conversation::VolumeClass::AgentPrivate
                            && volume.owner() == &crate::conversation::VolumeOwner::Agent(root.agent()),
                }
            {
                return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root bootstrap differs from its configured creator, actor or namespace"));
            }
            owner.require_volume_grant_with_native_work(volume, VolumeOperation::Read, work)?;
            let io = PreparationIo::from_native_work(work)?;
            let write = io.capability(volume, "volume:write:")?;
            let writable = context.original_root().grants.contains(&write);
            let write_bytes = write.capacity() as u64;
            drop(write);
            io.release(write_bytes)?;
            if writable { owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?; }
            let publication_lease = if writable {
                Some(owner.root_bootstrap_publication_lease_with_native_work(work).await?)
            } else { None };
            let name_hold;
            let name = bounded_io::volume_storage_name(volume, Some(&io))?;
            name_hold = io.guard_owned(name.capacity() as u64)?;
            let birth = self.host.filesystem.workspace_for_creation_operation_with_native_work(
                &name, config, operation, work, &io.cancellation,
            ).await.map_err(native_workspace_error)?
                .ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "Root bootstrap has no original protected Filesystem birth"))?;
            if birth.id() != initial_generation {
                return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root bootstrap initial generation differs from its actual birth"));
            }
            let selected = birth.workspace().generation_with_native_work(
                selected_generation, work, &io.cancellation,
            ).await.map_err(native_workspace_error)?;
            let native_snapshot = if let Some((lazy, frozen)) = native_import {
                if lazy.workspace_id() != birth.workspace_id() {
                    return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root bootstrap import names another original workspace"));
                }
                if frozen.authored_generation != selected.id() || frozen.workspace_id != birth.workspace_id() {
                    return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root bootstrap selected generation differs from its original import"));
                }
                lazy.validate_snapshot_with_native_work(frozen, work)?.await?;
                Some(frozen)
            } else { None };
            drop(selected);
            drop(birth);
            drop(name);
            drop(name_hold);
            let mut record = io.encode_json(&RootBootstrapRecord {
                owner: owner.root_bootstrap_owner(), audience: root.audience(), root: root.original_task(),
                admission_digest: context.original_admission_digest(), binding_admission_digest: root.admission_digest(),
                role, agent: root.agent(),
                bind_operation: root.bind_operation(), bind_intent: root.intent_digest(),
                creation_fence: root.fence(), volume, creation_operation: operation, config,
                initial_generation, selected_generation,
                native_snapshot, writable,
            }, MAX_REQUEST_BYTES, true)?;
            let journal_hold;
            let journal = allocation_ref(&self.host.provider, volume, Some(&io))?;
            journal_hold = io.guard_owned(journal.logical_owned_bytes()?)?;
            let name = std::str::from_utf8(journal.as_resource().key()).map_err(|_| native_error(crate::wire::ErrorCode::Invalid, "Root allocation journal name is not UTF-8"))?;
            let mut key_bytes = [0_u8; 32];
            hex::encode_to_slice(root.original_task().into_bytes(), &mut key_bytes).map_err(|_| native_error(crate::wire::ErrorCode::Invalid, "Root allocation identity encoding failed"))?;
            let suffix = std::str::from_utf8(&key_bytes).map_err(|_| native_error(crate::wire::ErrorCode::Invalid, "Root allocation identity is not ASCII"))?;
            let create = IdempotencyKey::new(Self::owned_label("root-bootstrap-journal:", suffix, &io)?)?;
            bounded_io::create(&self.host, name, &create, Some(&io)).await?;
            match bounded_io::read(&self.host, &journal, "/claim.json", MAX_REQUEST_BYTES, Some(&io)).await {
                Ok(_) => return Err(native_error(crate::wire::ErrorCode::Unauthorized, "child allocation cannot be claimed as original Root bootstrap")),
                Err(Error::NotFound(_)) => {},
                Err(error) => return Err(error),
            }
            let retained = match bounded_io::read(&self.host, &journal, "/root-bootstrap.json",
                MAX_REQUEST_BYTES, Some(&io)).await {
                Ok(bytes) => Some(bytes),
                Err(Error::NotFound(_)) => None,
                Err(error) => return Err(error),
            };
            if let Some(bytes) = retained {
                if bytes != record {
                    return Err(native_error(crate::wire::ErrorCode::Conflict, "original Root bootstrap association changed"));
                }
            } else {
                let observed = bounded_io::resolve(&self.host, &journal, Some(&io)).await?;
                let key = IdempotencyKey::new(Self::owned_label("root-bootstrap-claim:", suffix, &io)?)?;
                let path = Self::owned_label("", "/root-bootstrap.json", &io)?;
                let mutations = [WorkspaceMutation::PutFile { path, bytes: record }];
                let outcome = bounded_io::apply(&self.host, &journal, Some(&observed.generation),
                    &mutations, &key, Some(&io)).await;
                let [WorkspaceMutation::PutFile { bytes: recovered, .. }] = mutations else {
                    unreachable!("the one Root association mutation is a file write")
                };
                record = recovered;
                match outcome {
                    Ok(_) | Err(Error::Conflict(_)) => {},
                    Err(error) => return Err(error),
                }
                let retained = bounded_io::read(&self.host, &journal, "/root-bootstrap.json",
                    MAX_REQUEST_BYTES, Some(&io)).await?;
                if retained != record {
                    return Err(native_error(crate::wire::ErrorCode::Conflict, "protected Root bootstrap association was not published exactly"));
                }
            }
            drop(journal);
            drop(journal_hold);
            let admission_digest = context.original_admission_digest();
            let agent = root.agent();
            let root = root.original_task();
            let native_source = native_import.map(|(source, _)| source.clone());
            io.claim(std::mem::size_of::<FilesystemRootBootstrapAllocation<A, O>>() as u64)?;
            let (owner, audience) = {
                let mut observer = io.history_io();
                (crate::store::history::HistoryIo::clone_authority(&mut observer, owner.root_bootstrap_owner())?,
                    crate::store::history::HistoryIo::clone_authority(&mut observer, witness.binding().audience())?)
            };
            let creator = {
                let mut observer = io.history_io();
                self.issuer.verifier_with_work(&mut Some(&mut observer))?
            };
            let work = io.original_native_work().ok_or(native_error(crate::wire::ErrorCode::Invalid, "Root bootstrap requires its original native ledger"))?;
            let (volume, volume_custody) = volume.clone_with_native_work(work)?;
            io.absorb_native_custody(volume_custody)?;
            let (generation, generation_custody) = GenerationRef::new_with_native_work(
                &self.host.provider, &selected_generation.digest().into_bytes(), None, work,
            )?;
            io.absorb_native_custody(generation_custody)?;
            let evidence = allocation_ref(&self.host.provider, &volume, Some(&io))?.into_resource();
            let snapshot = if let Some(snapshot) = native_snapshot {
                let remaining = io.remaining()?;
                let value = crate::contract::json_value_bounded_native_trusted(&snapshot, crate::contract::JsonBounds {
                    maximum_bytes: remaining.bytes_encoded, maximum_items: remaining.items_examined, maximum_depth: 32,
                }, crate::wire::ErrorCode::Invalid, &mut |charge| io.codec_charge(charge, true))?;
                io.release(std::mem::size_of::<serde_json::Value>() as u64)?;
                Some(value)
            } else { None };
            let request = crate::resource_preparation::RootBootstrapContentRequest {
                root_operation: OperationId::from_bytes(root.into_bytes()), graph_root: root, role,
                volume, evidence, generation, native_snapshot: snapshot,
            };
            let hashing = WorkCounters { bytes_hashed: record.len() as u64, ..WorkCounters::default() };
            io.preflight(hashing)?;
            let record_digest = *blake3::hash(&record).as_bytes();
            io.charge(hashing)?;
            drop(record);
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            drop(original_root);
            drop(original_custody);
            Ok(FilesystemRootBootstrapAllocation {
                host: self.host.clone(), owner, audience, creator, root, admission_digest, agent, role, request,
                operation, config, initial_generation, selected_generation, native_snapshot, native_source, writable,
                publication_lease,
                record_digest, witness, _custody: custody,
            })
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemRootBootstrapAllocation<A, O> {
    pub(crate) async fn authenticate_record(&self, io: &PreparationIo) -> Result<()> {
        let journal = allocation_ref(&self.host.provider, &self.request.volume, Some(io))?;
        let journal_hold = io.guard_owned(journal.logical_owned_bytes()?)?;
        let bytes = bounded_io::read(&self.host, &journal, "/root-bootstrap.json", MAX_REQUEST_BYTES, Some(io)).await?;
        let bytes_hold = io.guard_bytes(&bytes)?;
        let hashing = WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::default() };
        io.preflight(hashing)?;
        let exact = *blake3::hash(&bytes).as_bytes() == self.record_digest;
        io.charge(hashing)?;
        drop(bytes);
        drop(bytes_hold);
        drop(journal);
        drop(journal_hold);
        if !exact {
            return Err(bounded_io::unauthorized(Some(io), "protected original Root bootstrap association changed"));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn reauthenticate_with_io(&self, io: &PreparationIo) -> Result<()> {
        self.authenticate_record(io).await?;
        self.authenticate_birth_with_io(io).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn authenticate_birth_with_io(&self, io: &PreparationIo) -> Result<()> {
        let name_hold;
        let name = bounded_io::volume_storage_name(&self.request.volume, Some(io))?;
        name_hold = io.guard_owned(name.capacity() as u64)?;
        let birth = if let Some(work) = io.original_native_work() {
            self.host.filesystem.workspace_for_creation_operation_with_native_work(
                &name, self.config, self.operation, work, &io.cancellation,
            ).await.map_err(native_workspace_error)?
        } else {
            io.receive(self.host.filesystem.workspace_for_creation_operation_measured(
                &name, self.config, self.operation, io.remaining()?, &io.cancellation,
            ).await)?
        };
        if !birth.as_ref().is_some_and(|birth| birth.id() == self.initial_generation) {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root content original protected Filesystem birth changed"));
        }
        drop(birth);
        drop(name);
        drop(name_hold);
        Ok(())
    }

}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemRootBootstrapAllocation<A, O>
{
    pub(in crate::filesystem) fn source_generation_with_io<'a>(
        &'a self, io: Option<&PreparationIo>,
    ) -> Result<Option<&'a GenerationRef>> {
        let Some(source) = self.native_source.as_ref() else { return Ok(None); };
        let snapshot = self.native_snapshot.ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "Root lazy content has no authenticated original frozen snapshot"))?;
        if snapshot.workspace_id != source.workspace_id()
            || snapshot.authored_generation != self.selected_generation
            || self.request.generation.as_resource().key() != snapshot.authored_generation.digest().into_bytes().as_slice()
        {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root lazy publication basis differs from its protected allocation"));
        }
        if let Some(io) = io {
            io.check_cancellation()?;
            if io.original_native_work().is_some_and(|work|
                !self.matches_native_work(work) || !source.matches_native_work(work))
            {
                return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root lazy publication basis belongs to another original ledger"));
            }
        }
        Ok(Some(&self.request.generation))
    }
    pub(in crate::filesystem) fn source_generation_for_file_with_io<'a>(
        &'a self, file: &FileRef, io: Option<&PreparationIo>,
    ) -> Result<Option<&'a GenerationRef>> {
        let Some(generation) = self.source_generation_with_io(io)? else { return Ok(None); };
        if file.volume() != &self.request.volume {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root lazy source reference names another original volume"));
        }
        let snapshot = self.native_snapshot.expect("authenticated original lazy snapshot");
        if let Some(io) = io {
            io.charge(WorkCounters { bytes_encoded: 64, bytes_copied: 64,
                ..WorkCounters::default() })?;
        }
        let revision = blake3::Hash::from_bytes(snapshot.id.into_bytes()).to_hex();
        Ok((file.version() == revision.as_str()).then_some(generation))
    }

    pub(in crate::filesystem) async fn publish_lazy_candidate_with_io(
        &self, mutations: &[super::super::WorkspaceMutation], key: &IdempotencyKey,
        permit: acyclic_fs::PublicationPermit, io: Option<&PreparationIo>,
    ) -> Result<(acyclic_fs::Generation<A, O>,
        std::result::Result<acyclic_fs::LazySnapshotRef, Error>)>
    {
        let ordinary;
        let io = match io {
            Some(io) => io,
            None => {
                ordinary = PreparationIo::for_unmeasured(self._custody.work().cancellation())?;
                &ordinary
            }
        };
        let lease = self.publication_lease.as_ref().filter(|_| self.writable)
            .ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "Root lazy content has no original declared publication authority"))?;
        self.source_generation_with_io(Some(io))?.ok_or(native_error(crate::wire::ErrorCode::Invalid, "Root eager content does not select a lazy publication candidate"))?;
        io.verify_publication_lease(lease.as_ref()).await?;
        self.reauthenticate_with_io(io).await?;
        let source = self.native_source.as_ref().expect("authenticated original lazy source");
        let snapshot = self.native_snapshot.expect("authenticated original lazy snapshot");
        let hashing = WorkCounters { bytes_hashed: key.as_str().len() as u64,
            ..WorkCounters::default() };
        io.preflight(hashing)?;
        let operation = super::super::filesystem_key(key);
        io.charge(hashing)?;
        let future = source.publish_with_io(snapshot, mutations, operation, permit, io, lease)?;
        let frame = match &future {
            crate::store::history::HistoryContentFuture::Ordinary { frame_bytes, .. } => *frame_bytes,
            crate::store::history::HistoryContentFuture::Native(_) => 0,
        };
        let published = future.await;
        let retired = if frame == 0 { Ok(()) } else { io.release(frame) };
        match published {
            Ok(value) => retired.map(|()| value),
            Err(error) => { let _ = retired; Err(error) }
        }
    }
}

impl<A, O> FilesystemRootBootstrapAllocation<A, O> {
    /// Borrows the exact request minted from the protected original allocation.
    pub fn request(&self) -> &crate::resource_preparation::RootBootstrapContentRequest {
        &self.request
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool {
        self._custody.matches_work(work)
    }

    /// Shares the admitted creator token without copying its protected record.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self.matches_native_work(work) {
            return Err(native_error(crate::wire::ErrorCode::Invalid, "Root bootstrap allocation belongs to another original ledger"));
        }
        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        Ok(Arc::new(self))
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct RootCreatedFilesystemAccess<A, O> {
    allocation: Arc<FilesystemRootBootstrapAllocation<A, O>>,
    reader: FilesystemContentVerifier<A, O>,
    writer: Option<FilesystemContentPublisher<A, O>>,
    _custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> RootCreatedFilesystemAccess<A, O> {
    async fn authenticate(&self, volume: &VolumeRef, io: &PreparationIo) -> Result<()> {
        if volume != &self.allocation.request.volume {
            return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root content handle names another original namespace"));
        }
        self.allocation.reauthenticate_with_io(io).await
    }

    async fn authenticate_original(&self, volume: &VolumeRef) -> Result<()> {
        let io = PreparationIo::from_native_work(self.allocation._custody.work())?;
        self.authenticate(volume, &io).await
    }

    fn writer(&self) -> Result<&FilesystemContentPublisher<A, O>> {
        self.writer.as_ref().ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "original Root content has no declared publication authority"))
    }

    fn publication_lease(&self) -> Result<&dyn crate::resource_preparation::CreatedTaskLease> {
        self.allocation.publication_lease.as_deref().ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "original Root content has no current publication lease"))
    }

    fn authorize_path(&self, volume: &VolumeRef, prefix: &str, path: &str,
        io: &PreparationIo) -> Result<()> {
        if volume != &self.allocation.request.volume {
            return Err(io.invalid("Root source path names another namespace"));
        }
        if !path.is_empty() { bounded_io::validate_content_path(path, Some(io))?; }
        if !prefix.is_empty() { bounded_io::validate_content_path(prefix, Some(io))?; }
        if crate::conversation::is_internal_path(path) || crate::conversation::is_internal_path(prefix)
            || (!prefix.is_empty() && path != prefix
                && !path.strip_prefix(prefix).is_some_and(|suffix| suffix.starts_with('/')))
        {
            return Err(io.invalid("Root source path escapes its signed read boundary"));
        }
        let mut metadata = io.history_io();
        self.reader.verifier.verify_with_work(&self.reader.scope, &mut Some(&mut metadata))?;
        let directory = io.contains_capability(self.reader.scope.capabilities(), &(volume, prefix), "directory:read:")?;
        let entire = io.contains_capability(self.reader.scope.capabilities(), volume, "volume:read:")?;
        if self.reader.scope.agent().is_none() || (!directory && !entire) {
            return Err(io.invalid("Root source path has no original signed read authority"));
        }
        Ok(())
    }

    async fn read_snapshot_bytes(&self, snapshot: acyclic_fs::LazySnapshotRef, path: &str,
        maximum: u64, io: &PreparationIo) -> Result<acyclic_stream::OwnedBytes> {
        let source = self.allocation.native_source.as_ref()
            .ok_or_else(|| bounded_io::unauthorized(Some(io), "Root source lacks its retained lazy backing"))?;
        if let Some(work) = io.original_native_work() {
            source.read_with_native_work(snapshot, path, maximum, work)?.await
        } else {
            io.receive_content(source.read_measured(snapshot, path, maximum,
                io.remaining()?, &io.cancellation).await)
        }
    }

    async fn read_snapshot_file(&self, file: &FileRef, maximum: u64,
        io: &PreparationIo) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let Some(snapshot) = self.allocation.native_snapshot else { return Ok(None); };
        io.charge(WorkCounters { bytes_encoded: 64, bytes_copied: 64, ..WorkCounters::default() })?;
        if file.version() != blake3::Hash::from_bytes(snapshot.id.into_bytes()).to_hex().as_str() {
            return Ok(None);
        }
        self.authorize_path(file.volume(), "", file.path(), io)?;
        self.authenticate(file.volume(), io).await?;
        let absolute = bounded_io::format_text(format_args!("/{}", file.path()), Some(io))?;
        let bytes = self.read_snapshot_bytes(snapshot, &absolute,
            maximum.min(self.reader.maximum_bytes), io).await?;
        file.descriptor().verify_with_io(bytes.as_ref(), Some(io))?;
        let absolute_capacity = absolute.capacity() as u64;
        drop(absolute);
        io.release(absolute_capacity)?;
        Ok(Some(bytes))
    }

    async fn read_snapshot_path(&self, volume: &VolumeRef, prefix: &str, path: &str,
        expected: Option<&GenerationRef>, maximum: u64, io: &PreparationIo) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let snapshot = self.allocation.native_snapshot.ok_or_else(|| io.invalid("Root path has no original lazy snapshot"))?;
        if expected.is_some_and(|value| value != &self.allocation.request.generation)
            || maximum == 0 || maximum > self.reader.maximum_bytes
        {
            return Err(io.invalid("Root path differs from its immutable source selection or limit"));
        }
        bounded_io::validate_content_path(path, Some(io))?;
        self.authorize_path(volume, prefix, path, io)?;
        self.authenticate(volume, io).await?;
        let absolute = bounded_io::format_text(format_args!("/{path}"), Some(io))?;
        let bytes = self.read_snapshot_bytes(snapshot, &absolute, maximum, io).await?;
        let absolute_capacity = absolute.capacity() as u64;
        drop(absolute);
        io.release(absolute_capacity)?;
        let header = if io.uses_native_work() { std::mem::size_of::<crate::conversation::FileDescriptor>() as u64 } else { 0 };
        io.claim(header)?;
        let descriptor = crate::conversation::FileDescriptor::from_bytes_with_io(
            &bytes, "application/octet-stream", Some(io),
        )?;
        io.charge(WorkCounters { bytes_encoded: 64, bytes_copied: 64, ..WorkCounters::default() })?;
        let version = blake3::Hash::from_bytes(snapshot.id.into_bytes()).to_hex();
        if io.uses_native_work() { io.claim(std::mem::size_of::<FileRef>() as u64)?; }
        let file = FileRef::new_with_io(volume, path, version.as_str(), &descriptor,
            path.rsplit('/').next().unwrap_or(path), Some(io))?;
        let descriptor_capacity = header + descriptor.logical_owned_bytes();
        drop(descriptor);
        io.release(descriptor_capacity)?;
        Ok((file, bytes))
    }

    async fn list_snapshot_directory(&self, volume: &VolumeRef, prefix: &str, path: &str,
        expected: Option<&GenerationRef>, after: Option<&str>, maximum: u32) -> Result<PrivateDirectoryPage> {
        if maximum == 0 || expected.is_some_and(|value| value != &self.allocation.request.generation) {
            return Err(native_error(crate::wire::ErrorCode::Invalid, "Root directory page differs from its immutable selection or bound"));
        }
        let io = PreparationIo::for_unmeasured(self.allocation._custody.work().cancellation())?;
        self.authorize_path(volume, prefix, path, &io)?;
        self.authenticate(volume, &io).await?;
        let snapshot = self.allocation.native_snapshot.expect("frozen directory route");
        let source = self.allocation.native_source.as_ref().ok_or_else(|| io.invalid("Root directory lacks its retained lazy backing"))?;
        let absolute = bounded_io::format_text(format_args!("/{path}"), Some(&io))?;
        let bound = (maximum as usize).checked_add(1).ok_or_else(|| io.invalid("Root directory page bound overflow"))?;
        let mut entries: Vec<crate::conversation::PrivateDirectoryEntry> = Vec::new();
        let mut cursor = None;
        loop {
            let page = io.receive_content(source.list_measured(snapshot, &absolute, cursor, 128,
                io.remaining()?, &io.cancellation).await)?;
            for entry in page.entries {
                let kind = match entry.kind {
                    acyclic_fs::demand::SourceNodeKind::RegularFile => crate::conversation::PrivateDirectoryEntryKind::File,
                    acyclic_fs::demand::SourceNodeKind::Directory => crate::conversation::PrivateDirectoryEntryKind::Directory,
                    _ => continue,
                };
                let name = entry.name.unicode_text().ok_or_else(|| io.invalid("Root directory name is not representable losslessly"))?;
                if after.is_some_and(|after| name.as_ref() <= after) { continue; }
                let position = match entries.binary_search_by(|entry| entry.name.as_str().cmp(name.as_ref())) {
                    Ok(_) => continue,
                    Err(position) => position,
                };
                if position >= bound { continue; }
                if entries.len() == bound { entries.pop(); }
                entries.insert(position, crate::conversation::PrivateDirectoryEntry { name: name.into_owned(), kind });
            }
            cursor = page.next;
            if cursor.is_none() { break; }
        }
        let has_more = entries.len() > maximum as usize;
        if has_more { entries.pop(); }
        let page = PrivateDirectoryPage { generation: self.allocation.request.generation.clone(), entries, has_more };
        page.validate()?;
        Ok(page)
    }


}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentResidencyVerifier
    for RootCreatedFilesystemAccess<A, O>
{
    fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.read(file).await.map(|_| ()) })
    }
    fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async move {
            let io = PreparationIo::for_unmeasured(self.allocation._custody.work().cancellation())?;
            if let Some(bytes) = self.read_snapshot_file(file, self.reader.maximum_bytes, &io).await? {
                return Ok(bytes);
            }
            self.authenticate(file.volume(), &io).await?;
            self.reader.read(file).await
        })
    }
    fn supports_measured_read(&self, volume: &VolumeRef, budget: WorkBudget,
        cancellation: &CancellationToken) -> MeasuredResult<OperationReceipt<bool>, Error> {
        if volume != &self.allocation.request.volume {
            return Ok(OperationReceipt { value: false, work: WorkCounters::default() });
        }
        self.reader.supports_measured_read(volume, budget, cancellation)
    }
    fn supports_native_read(&self, volume: &VolumeRef, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        if volume != &self.allocation.request.volume { return Ok(false); }
        self.reader.supports_native_read(volume, work)
    }
    fn read_with_native_work<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            if let Some(bytes) = self.read_snapshot_file(file, maximum_bytes, &io).await? {
                return Ok(bytes);
            }
            self.authenticate(file.volume(), &io).await?;
            drop(io);
            self.reader.read_with_native_work(file, maximum_bytes, work)?.await
        })
    }
    fn verify_with_native_work<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            if self.read_snapshot_file(file, maximum_bytes, &io).await?.is_some() {
                return Ok(());
            }
            self.authenticate(file.volume(), &io).await?;
            drop(io);
            self.reader.verify_with_native_work(file, maximum_bytes, work)?.await
        })
    }
    fn read_measured<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<acyclic_stream::OwnedBytes>, Error>> {
        Box::pin(async move {
            let io = PreparationIo::from_budget(budget, cancellation).map_err(OperationFailure::before_work)?;
            let result = async {
                if let Some(bytes) = self.read_snapshot_file(file, maximum_bytes, &io).await? {
                    return Ok(bytes);
                }
                self.authenticate(file.volume(), &io).await?;
                io.receive_content(self.reader.read_measured(file, maximum_bytes, io.remaining()?, cancellation).await)
            }.await;
            content_result(&io, result)
        })
    }
    fn verify_measured<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<()>, Error>> {
        Box::pin(async move {
            self.read_measured(file, maximum_bytes, budget, cancellation).await
                .map(|receipt| OperationReceipt { value: (), work: receipt.work })
        })
    }
    fn read_private_prefix<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str,
        generation: &'a GenerationRef, maximum_bytes: u64) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async move {
            if self.allocation.native_snapshot.is_some() {
                let io = PreparationIo::for_unmeasured(self.allocation._custody.work().cancellation())?;
                return self.read_snapshot_path(volume, prefix, path,
                    Some(generation), maximum_bytes, &io).await.map(|(_, bytes)| bytes);
            }
            self.authenticate_original(volume).await?;
            self.reader.read_private_prefix(volume, prefix, path, generation, maximum_bytes).await
        })
    }
    fn list_private_directory<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str,
        generation: Option<&'a GenerationRef>, after: Option<&'a str>, maximum_entries: u32)
        -> BoxFuture<'a, Result<PrivateDirectoryPage>> {
        Box::pin(async move {
            if self.allocation.native_snapshot.is_some() {
                return self.list_snapshot_directory(volume, prefix, path, generation, after, maximum_entries).await;
            }
            self.authenticate_original(volume).await?;
            ContentResidencyVerifier::list_private_directory(&self.reader, volume, prefix, path,
                generation, after, maximum_entries).await
        })
    }
    fn read_private_path<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str,
        generation: Option<&'a GenerationRef>) -> BoxFuture<'a, Result<(FileRef, acyclic_stream::OwnedBytes)>> {
        Box::pin(async move {
            if self.allocation.native_snapshot.is_some() {
                let io = PreparationIo::for_unmeasured(self.allocation._custody.work().cancellation())?;
                return self.read_snapshot_path(volume, prefix, path,
                    generation, self.reader.maximum_bytes, &io).await;
            }
            self.authenticate_original(volume).await?;
            ContentResidencyVerifier::read_private_path(&self.reader, volume, prefix, path, generation).await
        })
    }
    fn read_private_path_with_native_work<'a>(
        &'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str,
        expected: Option<&'a GenerationRef>, maximum: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_stream::OwnedBytes, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            if self.allocation.native_snapshot.is_some() {
                let io = PreparationIo::from_native_work(work)?;
                let (file, bytes) = self.read_snapshot_path(volume, prefix, path, expected, maximum, &io).await?;
                let retained = std::mem::size_of::<FileRef>() as u64 + file.logical_owned_bytes()?;
                Ok((file, bytes, io.into_native_custody(retained)?))
            } else {
                let io = PreparationIo::from_native_work(work)?;
                self.authenticate(volume, &io).await?;
                drop(io);
                self.reader.read_private_path_with_native_work(volume, prefix, path, expected, maximum, work)?.await
            }
        })
    }
    fn verify_manifest<'a>(&'a self, file: &'a FileRef, count: u32, limits: &'a Limits) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            crate::conversation::verified_attachment_manifest(self, file, count, limits).await?;
            Ok(())
        })
    }
    fn load_manifest<'a>(&'a self, file: &'a FileRef, count: u32) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move {
            let bytes = self.read(file).await?;
            let io = PreparationIo::for_unmeasured(self.allocation._custody.work().cancellation())?;
            let items = crate::conversation::decode_manifest_bounded(file, &bytes, count as usize, &io)?;
            if items.len() != count as usize {
                return Err(io.invalid("Root attachment manifest count is incomplete"));
            }
            Ok(items)
        })
    }
}

macro_rules! created_publisher {
    ($this:ident, $budget:ident, $cancellation:ident, $io:expr) => {
        fn stage_once<'a>(&'a $this, operation: OperationId, path: &'a str, bytes: &'a [u8],
            media_type: &'a str, display_name: &'a str) -> BoxFuture<'a, Result<FileRef>> {
            Box::pin(async move { $this.publish(operation, path, bytes, media_type, display_name, None, None).await })
        }
        fn stage_once_measured<'a>(&'a $this, operation: OperationId, path: &'a str, bytes: &'a [u8],
            media_type: &'a str, display_name: &'a str, $budget: WorkBudget, $cancellation: &'a CancellationToken)
            -> ContentFuture<'a, MeasuredResult<OperationReceipt<FileRef>, Error>> {
            Box::pin(async move {
                let io = $io.map_err(OperationFailure::before_work)?;
                let result = $this.publish(operation, path, bytes, media_type, display_name, None, Some(&io)).await;
                content_result(&io, result)
            })
        }
        #[cfg(not(target_arch = "wasm32"))]
        fn stage_once_with_native_work<'a>(&'a $this, operation: OperationId, path: &'a str, bytes: &'a [u8],
            media_type: &'a str, display_name: &'a str, work: &'a acyclic_fs::NativeMaterializationWork)
            -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
            crate::NativeFutureBundle::new(work, || async move {
                let io = PreparationIo::from_native_work(work)?;
                let file = $this.publish(operation, path, bytes, media_type, display_name, None, Some(&io)).await?;
                let custody = io.into_native_custody(file.logical_owned_bytes()?)?;
                Ok((file, custody))
            })
        }
        fn observe_staged_once<'a>(&'a $this, operation: OperationId, maximum_bytes: u64) -> BoxFuture<'a, Result<Option<FileRef>>> {
            Box::pin(async move { $this.observe(operation, maximum_bytes, None).await })
        }
        fn observe_staged_once_measured<'a>(&'a $this, operation: OperationId, maximum_bytes: u64,
            $budget: WorkBudget, $cancellation: &'a CancellationToken)
            -> ContentFuture<'a, MeasuredResult<OperationReceipt<Option<FileRef>>, Error>> {
            Box::pin(async move {
                let io = $io.map_err(OperationFailure::before_work)?;
                let result = $this.observe(operation, maximum_bytes, Some(&io)).await;
                content_result(&io, result)
            })
        }
        fn stage_at<'a>(&'a $this, operation: OperationId, source: &'a FileRef, bytes: &'a [u8]) -> BoxFuture<'a, Result<FileRef>> {
            Box::pin(async move { $this.publish(operation, source.path(), bytes,
                source.descriptor().media_type(), source.display_name(), Some(source), None).await })
        }
    };
}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> RootCreatedFilesystemAccess<A, O> {
    async fn publish(&self, operation: OperationId, path: &str, bytes: &[u8], media_type: &str,
        display_name: &str, source: Option<&FileRef>, io: Option<&PreparationIo>) -> Result<FileRef> {
        let lease = self.publication_lease()?;
        let volume = source.map_or(&self.allocation.request.volume, FileRef::volume);
        match io {
            Some(io) => { io.verify_publication_lease(lease).await?; self.authenticate(volume, io).await?; }
            None => { lease.verify().await?; self.authenticate_original(volume).await?; }
        }
        if let Some(source) = source {
            let ordinary;
            let source_io = match io {
                Some(io) => io,
                None => { ordinary = PreparationIo::for_unmeasured(self.allocation._custody.work().cancellation())?; &ordinary }
            };
            if self.allocation.source_generation_for_file_with_io(source, Some(source_io))?.is_some() {
                let original = self.read_snapshot_file(source, self.reader.maximum_bytes, source_io).await?
                    .ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "Root replacement does not authenticate its original lazy source"))?;
                drop(original);
            }
            self.writer()?.stage_at_with_io(operation, source, bytes, io, Some(lease), Some(&self.allocation)).await
        } else {
            self.writer()?.stage_once_with_io(operation, path, bytes, media_type, display_name, io, Some(lease), Some(&self.allocation)).await
        }
    }
    async fn observe(&self, operation: OperationId, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<Option<FileRef>> {
        match io {
            Some(io) => self.authenticate(&self.allocation.request.volume, io).await?,
            None => self.authenticate_original(&self.allocation.request.volume).await?,
        }
        self.writer()?.observe_once_with_io(operation, maximum_bytes, io).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentPublisher for RootCreatedFilesystemAccess<A, O> {
    fn volume(&self) -> &VolumeRef { &self.allocation.request.volume }
    fn supports_native_stage(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> { self.writer()?.supports_native_stage(work) }
    fn stage<'a>(&'a self, operation: OperationId, path: &'a str, bytes: &'a [u8], media_type: &'a str,
        display_name: &'a str) -> BoxFuture<'a, Result<FileRef>> { self.stage_once(operation, path, bytes, media_type, display_name) }
    created_publisher!(self, budget, cancellation, PreparationIo::from_budget(budget, cancellation));
    fn stage_at_with_native_work<'a>(&'a self, operation: OperationId, source: &'a FileRef, bytes: &'a [u8],
        work: &'a acyclic_fs::NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = self.publish(operation, source.path(), bytes, source.descriptor().media_type(),
                source.display_name(), Some(source), Some(&io)).await?;
            let custody = io.into_native_custody(file.logical_owned_bytes()?)?;
            Ok((file, custody))
        })
    }
}

fn original_creation_key(
    operation: OperationId, digest: [u8; 32], role: &ForkAllocationRole, io: &PreparationIo,
) -> Result<IdempotencyKey> {
    let suffix = match role {
        ForkAllocationRole::Scratch => "scratch",
        ForkAllocationRole::Project => "project",
        ForkAllocationRole::Result => "result",
    };
    IdempotencyKey::new(bounded_io::format_text(
        format_args!("fork:{operation}:{}:{suffix}", blake3::Hash::from(digest)), Some(io))?)
}

struct AuthenticatedAllocation {
    origin: ResourceCreationOrigin,
    volume: VolumeRef,
    generation: GenerationRef,
    prepared_generation: GenerationRef,
    agent: crate::AgentId,
    authority: Option<Authority>,
    default_writer: bool,
    artifact_writer: bool,
    creation_operation: acyclic_fs::OperationId,
    reference_files: Vec<FileRef>,
}

fn task_origin(claim: &AllocationClaim) -> Result<&ResourceCreationOrigin> {
    claim.task_origin.as_ref().ok_or_else(|| Error::Unauthorized(
        "raw trusted fork allocations cannot grant created-task access".into(),
    ))
}

async fn authenticate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, owner: &Authority,
    request: &ResourcePreparationRequest, proof: &ResourceCreationProofRef,
    io: Option<&PreparationIo>,
) -> Result<AuthenticatedAllocation> {
    let owned;
    let io = match io {
        Some(io) => io,
        None => { owned = PreparationIo::new(request)?; &owned }
    };
    request.validate()?;
    proof.validate()?;
    if request.provider != host.provider || proof.provider != host.provider
        || request.creation_grant != capability::FORK_PUBLISH
        || !request.child.resource_proofs.contains(proof)
    {
        return Err(Error::Unauthorized("created access differs from the admitted provider boundary".into()));
    }
    io.encode(request, request.maximum_bytes)?;
    let record_limit = request.maximum_bytes.min(MAX_REPORT_BYTES);
    let payload_hold;
    let payload = io.encode(&request.payload, record_limit)?;
    payload_hold = io.guard_bytes(&payload)?;
    let fork = io.decode_preparation::<ForkRequest>(&payload)?.into_retained();
    drop(payload);
    drop(payload_hold);
    fork.validate()?;
    let encoded_hold;
    let encoded = io.encode(&fork, record_limit)?;
    encoded_hold = io.guard_bytes(&encoded)?;
    let decoded = io.decode_preparation::<serde_json::Value>(&encoded)?;
    if decoded.value != request.payload {
        return Err(Error::Unauthorized("fork payload is not the exact admitted request".into()));
    }
    drop(decoded);
    drop(encoded);
    drop(encoded_hold);
    let count = creation_volumes(&fork).count();
    let slots = count * std::mem::size_of::<crate::resources::ResourceRef>();
    io.claim(slots as u64)?;
    io.allocation_attempt(slots as u64)?;
    let mut resources = Vec::with_capacity(count);
    for (volume, _) in creation_volumes(&fork) {
        if volume.provider() != &host.provider {
            return Err(Error::Unauthorized("created volume belongs to another provider".into()));
        }
        resources.push(bounded_io::content_workspace(&host.provider, volume, Some(io))?.into_resource());
    }
    if resources != request.resources {
        return Err(Error::Unauthorized("created identities differ from the exact fork allocation".into()));
    }
    let index = resources.iter().position(|resource| resource == &proof.resource)
        .ok_or_else(|| Error::Unauthorized("proof does not name a created fork volume".into()))?;
    let (selected_volume, _) = creation_volumes(&fork).nth(index)
        .ok_or_else(|| Error::Unauthorized("created allocation index is absent".into()))?;
    let evidence = allocation_ref(&host.provider, selected_volume, Some(io))?;
    if evidence.as_resource() != &proof.evidence {
        return Err(Error::Unauthorized("proof does not name this protected allocation journal".into()));
    }
    let journal = preparation_journal(&host.provider, fork.operation_id, Some(io))?;
    if !read_request_claim(host, &journal, &fork, record_limit, Some(io)).await? {
        return Err(Error::Unauthorized("fork preparation request is not retained".into()));
    }
    let report = read_preparation_report(host, &journal, &fork, record_limit, Some(io)).await?
        .ok_or_else(|| Error::Unauthorized("fork allocation has no completed report".into()))?;
    let mut observer = io.history_io();
    let seed = report.into_seed_with_work(&mut Some(&mut observer))?;
    let encoded_hold;
    let encoded = io.encode(&fork, record_limit)?;
    encoded_hold = io.guard_bytes(&encoded)?;
    let canonical_hold;
    let canonical = io.encode_json(request, request.maximum_bytes, true)?;
    canonical_hold = io.guard_bytes(&canonical)?;
    let hashing = WorkCounters { bytes_hashed: (encoded.len() + canonical.len()) as u64,
        ..WorkCounters::default() };
    io.preflight(hashing)?;
    let digest = *blake3::hash(&encoded).as_bytes();
    let request_digest = *blake3::hash(&canonical).as_bytes();
    io.charge(hashing)?;
    drop((encoded, canonical));
    drop((encoded_hold, canonical_hold));
    let mut observer = io.history_io();
    seed.validate_with_work(&mut Some(&mut observer))?;
    let seed_hold;
    let encoded_seed = io.encode(&seed, record_limit)?;
    seed_hold = io.guard_bytes(&encoded_seed)?;
    let hashing = WorkCounters { bytes_hashed: encoded_seed.len() as u64,
        ..WorkCounters::default() };
    io.preflight(hashing)?;
    let binding = SeedBinding { digest: *blake3::hash(&encoded_seed).as_bytes() };
    io.charge(hashing)?;
    drop(encoded_seed);
    drop(seed_hold);
    let mut selected: Option<AuthenticatedAllocation> = None;
    let mut original_origin: Option<ResourceCreationOrigin> = None;
    for (volume_index, (volume, role)) in creation_volumes(&fork).enumerate() {
        let allocation = allocation_ref(&host.provider, volume, Some(io))?;
        let mut claim = read_record::<A, O, AllocationClaim>(host, &allocation,
            "/claim.json", record_limit, Some(io)).await?
            .ok_or_else(|| Error::Unauthorized("created allocation claim is missing".into()))?;
        let retained_binding = read_record::<A, O, SeedBinding>(host, &allocation,
            "/seed.json", record_limit, Some(io)).await?
            .ok_or_else(|| Error::Unauthorized("created allocation seed is missing".into()))?;
        let origin = task_origin(&claim)?;
        let prior_origin = selected.as_ref().map(|known| &known.origin).or(original_origin.as_ref());
        if claim.operation_id != fork.operation_id || claim.preparation_digest != digest
            || claim.parent != fork.parent || claim.child != fork.child || claim.volume != *volume
            || claim.role != role
            || retained_binding != binding || &origin.owner != owner
            || origin.preparation_task != proof.preparation
            || Some(origin.parent_task) != request.child.parent
            || origin.intended_child != request.child.operation_id
            || origin.request_digest != request_digest
            || prior_origin.is_some_and(|prior| prior != origin)
        {
            return Err(Error::Unauthorized("created allocation does not authenticate the original task request".into()));
        }
        let prepared_generation = match role {
            ForkAllocationRole::Scratch => &seed.child_private_generation,
            ForkAllocationRole::Project => seed.resources.iter().find_map(|resource| match &resource.revision {
                ResourceRevision::Project { volume: project, generation } if project == volume => Some(generation),
                _ => None,
            }).ok_or_else(|| Error::Unauthorized("original child project generation is absent".into()))?,
            ForkAllocationRole::Result => seed.child_result_generation.as_ref()
                .ok_or_else(|| Error::Unauthorized("original child result generation is absent".into()))?,
        };
        let workspace = bounded_io::content_workspace(&host.provider, volume, Some(io))?;
        let original_workspace = bounded_io::open(host, &workspace, io).await?;
        let prepared = bounded_io::generation(host, &original_workspace, prepared_generation, io).await?;
        let creation_key = original_creation_key(fork.operation_id, digest, &role, io)?;
        io.charge(WorkCounters { bytes_hashed: creation_key.as_str().len() as u64,
            ..WorkCounters::default() })?;
        let key = super::super::filesystem_key(&creation_key);
        #[cfg(not(target_arch = "wasm32"))]
        let initial = if let Some(original) = io.original_native_work() {
            original_workspace.operation_generation_with_native_work(acyclic_fs::OperationId::from_bytes(key.into_bytes()), original, &io.cancellation)
                .await.map_err(native_workspace_error)?
        } else {
            io.receive(original_workspace.operation_generation_measured(
                key, io.remaining()?, &io.cancellation).await)?
        };
        #[cfg(target_arch = "wasm32")]
        let initial = io.receive(original_workspace.operation_generation_measured(
            key, io.remaining()?, &io.cancellation).await)?;
        let initial = initial.ok_or_else(|| Error::Unauthorized("original fork allocation operation is missing".into()))?;
        let generation = bounded_io::generation_ref(host, &initial, Some(io))?;
        let captured_generation = match role {
            ForkAllocationRole::Result => seed.child_result_generation.as_ref(),
            _ => seed.resources.iter().find_map(|resource| match &resource.revision {
                ResourceRevision::PrivateVolume { volume: captured, generation, .. }
                | ResourceRevision::Project { volume: captured, generation } if captured == volume => Some(generation),
                _ => None,
            }),
        }.ok_or_else(|| Error::Unauthorized("original allocated volume is absent from the seed".into()))?;
        if captured_generation != &generation {
            return Err(Error::Unauthorized("seed capture differs from the original allocation operation".into()));
        }
        if volume_index == index {
            let prepared_generation = bounded_io::generation_ref(host, &prepared, Some(io))?;
            if role == ForkAllocationRole::Scratch {
                let copied = WorkCounters { bytes_copied: fork.child.id.len() as u64,
                    ..WorkCounters::default() };
                io.preflight(copied)?;
                io.claim(fork.child.id.len() as u64)?;
                if !fork.child.id.is_empty() { io.allocation_attempt(fork.child.id.len() as u64)?; }
                io.charge(copied)?;
            }
            let origin = claim.task_origin.take().ok_or_else(|| Error::Unauthorized(
                "original allocated task origin is absent".into()))?;
            selected = Some(AuthenticatedAllocation {
                origin, volume: claim.volume, generation, prepared_generation,
                agent: fork.child_agent, default_writer: role == ForkAllocationRole::Scratch,
                artifact_writer: role == ForkAllocationRole::Result,
                authority: (role == ForkAllocationRole::Scratch).then(|| fork.child.clone()),
                creation_operation: acyclic_fs::OperationId::from_bytes(key.into_bytes()),
                reference_files: Vec::new(),
            });
        }
        if selected.is_none() {
            original_origin = claim.task_origin.take();
        }
    }
    let mut selected = selected.ok_or_else(|| Error::Unauthorized("created allocation identity is absent".into()))?;
    let mut granted = BTreeSet::new();
    for reference in seed.reference_grants {
        require_reference_donor(&selected.origin.source_grants, &reference)?;
        if reference.attachment_manifest.as_ref().is_some_and(|manifest|
            !seed.attachment_manifests.contains(manifest))
        {
            return Err(Error::Unauthorized("inherited member proof is not retained in the original seed".into()));
        }
        if reference.reader == seed.child_agent {
            let capability = reference.capability()?;
            if !request.child.grants.contains(&capability) {
                return Err(Error::Unauthorized("child admission omitted its sealed exact-file read grant".into()));
            }
            if selected.default_writer && granted.insert(capability) {
                selected.reference_files.push(reference.file);
            }
        }
    }
    for file in &request.input_files {
        if !crate::runtime::read_granted(&selected.origin.source_grants, file)?
            || !request.child.grants.contains(&file.read_capability()?)
        {
            return Err(Error::Unauthorized("child input lacks original donor and exact child read authority".into()));
        }
        if selected.default_writer {
            let bytes = host.read_pinned_io(file, request.maximum_bytes, Some(io)).await?;
            let byte_custody = io.guard_bytes(&bytes)?;
            drop(bytes);
            drop(byte_custody);
            if granted.insert(file.read_capability()?) {
                selected.reference_files.push(file.clone());
            }
        }
    }
    Ok(selected)
}

/// Caller work and original creation limits constrain the same actual ledger.
fn content_io(request: &ResourcePreparationRequest, mut budget: WorkBudget,
    cancellation: &CancellationToken) -> Result<PreparationIo> {
    macro_rules! cap {
        ($limit:expr; $($field:ident),+ $(,)?) => { $(budget.$field = budget.$field.min($limit);)+ };
    }
    cap!(request.maximum_work; authority_records_read, authority_records_appended,
        object_probes, backend_read_operations, backend_write_operations, durability_operations,
        page_reads, page_writes, source_path_components, source_entries_visited,
        items_examined, items_returned, allocation_operations, materializations);
    cap!(request.maximum_bytes; authority_bytes_read, authority_bytes_written,
        object_bytes_read, object_bytes_written, bytes_hashed, bytes_copied, bytes_encoded,
        source_bytes_read, output_bytes, peak_allocation_bytes);
    PreparationIo::from_budget(budget, cancellation)
}

fn content_result<T>(io: &PreparationIo, result: Result<T>) -> MeasuredResult<OperationReceipt<T>, Error> {
    match result {
        Ok(value) => Ok(OperationReceipt { value, work: io.receipt() }),
        Err(error) => Err(OperationFailure::new(error, io.receipt())),
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ResourceCreationProvider
    for FilesystemResourceCreationProvider<A, O>
{
    fn provider(&self) -> &ProviderRef { &self.host.provider }

    #[cfg(not(target_arch = "wasm32"))]
    fn root_bootstrap_sources(&self, operation: OperationId)
        -> [Option<(&crate::resource_preparation::RootBootstrapContentRequest,
            &crate::durable_host::RootBootstrapBindingWitness)>; 2]
    {
        fn selected<A, O>(
            slot: &std::sync::OnceLock<Arc<FilesystemRootBootstrapAllocation<A, O>>>, operation: OperationId,
        ) -> Option<(&crate::resource_preparation::RootBootstrapContentRequest,
            &crate::durable_host::RootBootstrapBindingWitness)>
        {
            slot.get().filter(|allocation| allocation.request.root_operation == operation)
                .map(|allocation| (&allocation.request, &allocation.witness))
        }
        [selected(&self.root_project, operation), selected(&self.root_scratch, operation)]
    }
    fn verify<'a>(&'a self, request: &'a ResourcePreparationRequest, proof: &'a ResourceCreationProofRef)
        -> BoxFuture<'a, Result<CreatedResourceReceipt>>
    {
        Box::pin(async move { self.verify_with_io(request, proof, None).await })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self, request: &'a ResourcePreparationRequest, proof: &'a ResourceCreationProofRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (CreatedResourceReceipt, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let receipt = self.verify_with_io(request, proof, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((receipt, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_root_bootstrap_with_native_work<'a>(
        &'a self, context: crate::resource_preparation::RootBootstrapContentContext<'a>,
        request: &'a crate::resource_preparation::RootBootstrapContentRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (crate::resource_preparation::RootBootstrapContentReceipt, acyclic_fs::NativeAllocation)>> {
        crate::resource_preparation::preflight_root_bootstrap_with_native_work(context, request, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            use crate::resource_preparation::{RootBootstrapContentRole, RootBootstrapContentReceipt};
            let allocation = match request.role {
                RootBootstrapContentRole::Project => self.root_project.get(),
                RootBootstrapContentRole::Scratch => self.root_scratch.get(),
            }.ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "Root content has no configured protected original allocation"))?;
            work.remaining()?;
            let generation: Option<[u8; 32]> = request.generation.as_resource().key().try_into().ok();
            let frozen = match &request.native_snapshot {
                Some(value) => Some(acyclic_fs::LazySnapshotRef::deserialize(value)
                    .map_err(|_| native_error(crate::wire::ErrorCode::Unauthorized, "Root content snapshot is not its original fixed descriptor"))?),
                None => None,
            };
            if context.owner() != &allocation.owner || context.audience() != &allocation.audience
                || context.agent() != allocation.agent || context.original_root().parent.is_some()
                || context.original_root().task_id() != allocation.root
                || context.original_admission_digest() != allocation.admission_digest
                || request.root_operation != context.original_root().operation_id
                || request.graph_root != allocation.root || request.volume != allocation.request.volume
                || request.role != allocation.role
                || request.generation.as_resource().provider() != &self.host.provider
                || generation != Some(allocation.selected_generation.digest().into_bytes())
                || frozen != allocation.native_snapshot
                || request.native_snapshot != allocation.request.native_snapshot
            {
                return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root content request differs from its actual protected original association"));
            }
            let io = PreparationIo::from_native_work(work)?;
            let journal_hold;
            let journal = allocation_ref(&self.host.provider, &allocation.request.volume, Some(&io))?;
            journal_hold = io.guard_owned(journal.logical_owned_bytes()?)?;
            if &request.evidence != journal.as_resource() {
                return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root content evidence names another protected allocation"));
            }
            drop(journal);
            drop(journal_hold);
            let name_hold;
            let name = io.capability(&allocation.request.volume, "harness-")?;
            name_hold = io.guard_owned(name.capacity() as u64)?;
            let read = Self::owned_label("volume:read:", &name["harness-".len()..], &io)?;
            let write = Self::owned_label("volume:write:", &name["harness-".len()..], &io)?;
            if !context.original_root().grants.contains(&read) {
                return Err(native_error(crate::wire::ErrorCode::Unauthorized, "Root content original admission does not declare Project or Scratch READ"));
            }
            let writable = allocation.writable && context.original_root().grants.contains(&write);
            allocation.reauthenticate_with_io(&io).await?;
            let id = Self::owned_label("root-created:", &name, &io)?;
            drop(name);
            drop(name_hold);
            io.claim((if writable { 2 } else { 1 }) * std::mem::size_of::<String>() as u64)?;
            let capabilities = if writable { Capabilities::new([read, write]) } else {
                let discarded_capacity = write.capacity() as u64;
                drop(write);
                io.release(discarded_capacity)?;
                Capabilities::new([read])
            };
            let scope = {
                let mut observer = io.history_io();
                self.issuer.root_for_agent_with_work(context.agent(), &id, &capabilities,
                    &mut Some(&mut observer))?
            };
            let id_capacity = id.capacity() as u64;
            let capabilities_capacity = capabilities.logical_owned_bytes()?;
            drop(id);
            io.release(id_capacity)?;
            drop(capabilities);
            io.release(capabilities_capacity)?;
            let verifier = {
                let mut observer = io.history_io();
                self.issuer.verifier_with_work(&mut Some(&mut observer))?
            };
            io.claim(std::mem::size_of::<RootCreatedFilesystemAccess<A, O>>() as u64)?;
            let custody;
            let writer = if writable {
                let (volume, custody) = allocation.request.volume.clone_with_native_work(work)?;
                io.absorb_native_custody(custody)?;
                Some(FilesystemContentPublisher::new_with_io(self.host.clone(), volume,
                    &verifier, &scope, context.original_root().limits.file_bytes, Some(&io))?)
            } else { None };
            let reader = FilesystemContentVerifier::new_with_io(self.host.clone(), verifier, scope,
                context.original_root().limits.file_bytes, Some(&io))?;
            let capture = io.owned_bytes()?;
            custody = io.into_native_custody(capture)?;
            work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
            let access = Arc::new(RootCreatedFilesystemAccess {
                allocation: allocation.clone(), reader, writer, _custody: custody,
            });
            let mut receipt_custody = work.retain_owned((std::mem::size_of::<RootBootstrapContentReceipt>()
                - std::mem::size_of::<crate::resource_preparation::RootBootstrapContentRequest>()) as u64)?;
            let (request, mut request_custody) = request.clone_with_native_work(work)?;
            receipt_custody.absorb(&mut request_custody).map_err(|cause| Error::NativeWork {
                cause, custody: Some(request_custody),
            })?;
            let audience = {
                use crate::store::history::HistoryIo as _;
                let mut observer = crate::store::history::native_work_io(work)
                    .map_err(Error::from)?;
                let audience = observer.clone_authority(&allocation.audience)?;
                let mut custody = observer.into_custody();
                receipt_custody.absorb(&mut custody).map_err(|cause| Error::NativeWork {
                    cause, custody: Some(custody),
                })?;
                audience
            };
            Ok((RootBootstrapContentReceipt {
                request, audience, agent: context.agent(),
                binding: ContentBindings { reader: access.clone(),
                    writer: if writable { Some(access) } else { None } },
            }, receipt_custody))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemResourceCreationProvider<A, O> {
    async fn verify_with_io(
        &self, request: &ResourcePreparationRequest, proof: &ResourceCreationProofRef,
        io: Option<&PreparationIo>,
    ) -> Result<CreatedResourceReceipt> {
        let allocation = authenticate(&self.host, self.issuer.audience(), request, proof, io).await?;
        if let Some(io) = io {
            let mut observer = io.history_io();
            crate::core::resource_creation_clone_reservation(request, proof, &allocation.origin,
                &mut Some(&mut observer))?;
            let generation_bytes = allocation.generation.logical_owned_bytes()?;
            let resource_bytes = proof.resource.logical_owned_bytes()?;
            let evidence_bytes = proof.evidence.logical_owned_bytes()?;
            let bytes = allocation.volume.logical_owned_bytes()?.checked_mul(4)
                .and_then(|bytes| bytes.checked_add(generation_bytes))
                .and_then(|bytes| bytes.checked_add(self.issuer.audience().id.len() as u64))
                .and_then(|bytes| bytes.checked_add(resource_bytes))
                .and_then(|bytes| bytes.checked_add(evidence_bytes))
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64))
                .ok_or_else(|| Error::Invalid("creator capture ownership overflow".into()))?;
            io.claim(bytes)?;
            io.allocation_attempt(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)?;
            io.check_cancellation()?;
        }
        let scope = if let Some(io) = io {
            let name = io.capability(&allocation.volume, "harness-")?;
            let read = Self::owned_label("volume:read:", &name["harness-".len()..], io)?;
            let write = Self::owned_label("volume:write:", &name["harness-".len()..], io)?;
            self.created_scope(allocation.agent, "created:", proof, name, [read, write], io)?
        } else {
            self.issuer.root_for_agent(allocation.agent,
                format!("created:{}:{}", proof.preparation, allocation.volume.storage_name()?),
                Capabilities::new([allocation.volume.capability(VolumeOperation::Read)?,
                    allocation.volume.capability(VolumeOperation::Write)?]))
        };
        let verifier = if let Some(io) = io {
            let mut observer = io.history_io();
            self.issuer.verifier_with_work(&mut Some(&mut observer))?
        } else { self.issuer.verifier() };
        let writer = FilesystemContentPublisher::new_with_io(self.host.clone(), allocation.volume.clone(),
            &verifier, &scope, request.maximum_bytes, io)?;
        let reader = FilesystemContentVerifier::new_with_io(self.host.clone(), verifier, scope,
            request.maximum_bytes, io)?;
        if let Some(io) = io {
            let owned = reader.logical_owned_bytes()?.checked_add(writer.logical_owned_bytes()?)
                .ok_or_else(|| Error::Invalid("creator content ownership overflow".into()))?;
            if owned > io.owned_bytes()? {
                return Err(Error::Invalid("creator content exceeds its admitted capture quota".into()));
            }
        }
        let access = Arc::new(CreatedFilesystemAccess {
            host: self.host.clone(), owner: self.issuer.audience().clone(),
            request: request.clone(), proof: proof.clone(), origin: allocation.origin.clone(),
            generation: allocation.generation.clone(), prepared_generation: allocation.prepared_generation,
            volume: allocation.volume.clone(), agent: allocation.agent,
            creation_operation: allocation.creation_operation, reader, writer,
        });
        let count = allocation.reference_files.len();
        let operation_count = count.checked_add(2)
            .ok_or_else(|| Error::Invalid("creator receipt item count overflow".into()))?;
        if let Some(io) = io {
            let slots = operation_count.checked_mul(std::mem::size_of::<CreatedResourceOperation>())
                .and_then(|bytes| bytes.checked_add(count.checked_mul(std::mem::size_of::<CreatedReferenceBinding>())?))
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CreatedContentBinding>()))
                .ok_or_else(|| Error::Invalid("creator receipt slot overflow".into()))?;
            io.claim(slots as u64)?;
            io.allocation_attempt((operation_count * std::mem::size_of::<CreatedResourceOperation>()) as u64)?;
            io.allocation_attempt((count * std::mem::size_of::<CreatedReferenceBinding>()) as u64)?;
        }
        let mut operations = Vec::with_capacity(operation_count);
        let mut references = Vec::with_capacity(count);
        operations.push(CreatedResourceOperation::ReadVolume(allocation.volume.clone()));
        operations.push(CreatedResourceOperation::WriteVolume(allocation.volume.clone()));
        for file in allocation.reference_files {
            let source_agent = match file.volume().owner() {
                crate::conversation::VolumeOwner::Agent(agent) => *agent, _ => allocation.agent,
            };
            if let Some(io) = io {
                let bytes = file.logical_owned_bytes()?.checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CreatedFilesystemReference<A, O>>() as u64))
                    .ok_or_else(|| Error::Invalid("creator reference ownership overflow".into()))?;
                io.claim(bytes)?;
                io.allocation_attempt(std::mem::size_of::<CreatedFilesystemReference<A, O>>() as u64)?;
            }
            let scope = if let Some(io) = io {
                let suffix = io.capability(&file, "file:read:")?;
                let capability = Self::owned_label("", &suffix, io)?;
                self.created_scope(source_agent, "created-reference:", proof, suffix, [capability], io)?
            } else {
                self.issuer.root_for_agent(source_agent,
                    format!("created-reference:{}:{}", proof.preparation, file.read_capability()?),
                    Capabilities::new([file.read_capability()?]))
            };
            let verifier = if let Some(io) = io {
                let mut observer = io.history_io();
                self.issuer.verifier_with_work(&mut Some(&mut observer))?
            } else { self.issuer.verifier() };
            let verifier = FilesystemContentVerifier::new_with_io(self.host.clone(), verifier, scope, request.maximum_bytes, io)?;
            if let Some(io) = io {
                if verifier.logical_owned_bytes()? > io.owned_bytes()? {
                    return Err(Error::Invalid("creator reference exceeds its admitted capture quota".into()));
                }
            }
            let reader = Arc::new(CreatedFilesystemReference {
                allocation: access.clone(), file: file.clone(), reader: verifier,
            });
            operations.push(CreatedResourceOperation::ReadFile(file.clone()));
            references.push(CreatedReferenceBinding { file, reader });
        }
        if let Some(io) = io {
            io.allocation_attempt(std::mem::size_of::<CreatedContentBinding>() as u64)?;
        }
        Ok(CreatedResourceReceipt {
            origin: allocation.origin, resource: proof.resource.clone(), evidence: proof.evidence.clone(),
            allocation_generation: allocation.generation, operations, references,
            content: vec![CreatedContentBinding { volume: allocation.volume,
                authority: allocation.authority, default_writer: allocation.default_writer,
                artifact_writer: allocation.artifact_writer,
                scratch_creation: allocation.default_writer.then(||
                    crate::resource_preparation::ChildScratchCreationProof::new(access.clone())),
                binding: ContentBindings { reader: access.clone(), writer: Some(access) } }],
        })
    }

    fn created_scope<const N: usize>(
        &self, agent: crate::AgentId, prefix: &str, proof: &ResourceCreationProofRef,
        suffix: String, capabilities: [String; N], io: &PreparationIo,
    ) -> Result<Scope> {
        use std::fmt::Write as _;
        let length = prefix.len().checked_add(36 + 1).and_then(|length| length.checked_add(suffix.len()))
            .ok_or_else(|| Error::Invalid("creator scope identity overflow".into()))?;
        let copied = WorkCounters { bytes_copied: length as u64, ..Default::default() };
        io.preflight(copied)?;
        io.claim((length + N * std::mem::size_of::<String>()) as u64)?;
        io.allocation_attempt(length as u64)?;
        let mut id = String::with_capacity(length);
        write!(&mut id, "{prefix}{}:{suffix}", proof.preparation)
            .map_err(|_| Error::Invalid("creator scope formatting failed".into()))?;
        io.charge(copied)?;
        let capabilities = Capabilities::new(capabilities);
        let temporary = suffix.capacity() as u64 + id.capacity() as u64 + capabilities.logical_owned_bytes()?;
        let mut observer = io.history_io();
        let scope = self.issuer.root_for_agent_with_work(agent, &id, &capabilities, &mut Some(&mut observer))?;
        drop((suffix, id, capabilities));
        io.release(temporary)?;
        Ok(scope)
    }

    fn owned_label(prefix: &str, suffix: &str, io: &PreparationIo) -> Result<String> {
        let length = prefix.len().checked_add(suffix.len())
            .ok_or_else(|| Error::Invalid("creator label ownership overflow".into()))?;
        let copied = WorkCounters { bytes_copied: length as u64, ..Default::default() };
        io.preflight(copied)?;
        io.claim(length as u64)?;
        io.allocation_attempt(length as u64)?;
        let mut value = String::with_capacity(length);
        value.push_str(prefix);
        value.push_str(suffix);
        io.charge(copied)?;
        Ok(value)
    }
}

struct CreatedFilesystemAccess<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Authority,
    request: ResourcePreparationRequest,
    proof: ResourceCreationProofRef,
    origin: ResourceCreationOrigin,
    generation: GenerationRef,
    prepared_generation: GenerationRef,
    volume: VolumeRef,
    agent: crate::AgentId,
    creation_operation: acyclic_fs::OperationId,
    reader: FilesystemContentVerifier<A, O>,
    writer: FilesystemContentPublisher<A, O>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    crate::resource_preparation::ScratchCreationIdentity for CreatedFilesystemAccess<A,O>
{
    fn original_task(&self) -> crate::TaskId { crate::TaskId::from_bytes(self.origin.intended_child.into_bytes()) }
    fn agent(&self) -> crate::AgentId { self.agent }
    fn volume(&self) -> &VolumeRef { &self.volume }
    fn creation_operation(&self) -> acyclic_fs::OperationId { self.creation_operation }
    fn initial_generation(&self) -> &GenerationRef { &self.generation }
    fn origin(&self) -> &ResourceCreationOrigin { &self.origin }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CreatedFilesystemAccess<A, O> {
    fn validate_authenticated(&self, current: &AuthenticatedAllocation) -> Result<()> {
        if current.origin != self.origin || current.generation != self.generation
            || current.prepared_generation != self.prepared_generation || current.volume != self.volume
        {
            return Err(Error::Unauthorized("original created allocation evidence changed".into()));
        }
        Ok(())
    }
    async fn authenticate_file(&self, file: &FileRef, io: Option<&PreparationIo>) -> Result<()> {
        self.reauthenticate(file.volume(), io).await
    }
    async fn reauthenticate(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<()> {
        if volume != &self.volume {
            return Err(Error::Unauthorized("created content handle names another volume".into()));
        }
        let current = authenticate(&self.host, &self.owner, &self.request, &self.proof, io).await?;
        self.validate_authenticated(&current)
    }
}

/// Revalidates the original creator seal and admits only its one immutable file.
struct CreatedFilesystemReference<A, O> {
    allocation: Arc<CreatedFilesystemAccess<A, O>>,
    file: FileRef,
    reader: FilesystemContentVerifier<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CreatedFilesystemReference<A, O> {
    async fn authenticate_file(&self, file: &FileRef, io: Option<&PreparationIo>) -> Result<()> {
        if file != &self.file {
            return Err(Error::Unauthorized("created reference handle names another immutable file".into()));
        }
        let allocation = &self.allocation;
        let current = authenticate(&allocation.host, &allocation.owner,
            &allocation.request, &allocation.proof, io).await?;
        allocation.validate_authenticated(&current)?;
        if !current.reference_files.contains(file) {
            return Err(Error::Unauthorized("original inherited reference evidence changed".into()));
        }
        Ok(())
    }
}

macro_rules! created_reader {
    ($this:ident, $file:ident, $request:expr, $volume:expr) => {
    fn verify<'a>(&'a $this, $file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { $this.authenticate_file($file, None).await?; $this.reader.verify($file).await })
    }
    fn read<'a>(&'a $this, $file: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async move { $this.authenticate_file($file, None).await?; $this.reader.read($file).await })
    }
    fn supports_measured_read(&$this, volume: &VolumeRef, budget: WorkBudget, cancellation: &CancellationToken)
        -> MeasuredResult<OperationReceipt<bool>, Error> {
        if volume != $volume {
            return Ok(OperationReceipt { value: false, work: WorkCounters::default() });
        }
        $this.reader.supports_measured_read(volume, budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_read(&$this, volume: &VolumeRef, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<bool> {
        if volume != $volume { return Ok(false); }
        $this.reader.supports_native_read(volume, work)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work<'a>(&'a $this, $file: &'a FileRef, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            $this.authenticate_file($file, Some(&io)).await?;
            drop(io);
            $this.reader.read_with_native_work($file, maximum_bytes.min(($request).maximum_bytes), work)?.await
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(&'a $this, $file: &'a FileRef, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            $this.authenticate_file($file, Some(&io)).await?;
            drop(io);
            $this.reader.verify_with_native_work($file, maximum_bytes.min(($request).maximum_bytes), work)?.await
        })
    }
    fn read_measured<'a>(&'a $this, $file: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<acyclic_stream::OwnedBytes>, Error>> {
        Box::pin(async move {
            let io = content_io($request, budget, cancellation).map_err(OperationFailure::before_work)?;
            let result = async {
                $this.authenticate_file($file, Some(&io)).await?;
                io.receive_content($this.reader.read_measured($file, maximum_bytes.min(($request).maximum_bytes),
                    io.remaining()?, cancellation).await)
            }.await;
            content_result(&io, result)
        })
    }
    fn verify_measured<'a>(&'a $this, $file: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<()>, Error>> {
        Box::pin(async move {
            $this.read_measured($file, maximum_bytes, budget, cancellation).await
                .map(|receipt| OperationReceipt { value: (), work: receipt.work })
        })
    }
    fn verify_manifest<'a>(&'a $this, $file: &'a FileRef, count: u32, limits: &'a Limits) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { $this.authenticate_file($file, None).await?; $this.reader.verify_manifest($file, count, limits).await })
    }
    fn load_manifest<'a>(&'a $this, $file: &'a FileRef, count: u32) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move { $this.authenticate_file($file, None).await?; ContentResidencyVerifier::load_manifest(&$this.reader, $file, count).await })
    }
    };
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentResidencyVerifier for CreatedFilesystemReference<A, O> {
    created_reader!(self, file, &self.allocation.request, self.file.volume());
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentResidencyVerifier for CreatedFilesystemAccess<A, O> {
    created_reader!(self, file, &self.request, &self.volume);
    fn read_private_prefix<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str, generation: &'a GenerationRef, maximum_bytes: u64) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async move { self.reauthenticate(volume, None).await?; self.reader.read_private_prefix(volume, prefix, path, generation, maximum_bytes).await })
    }
    fn list_private_directory<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str, generation: Option<&'a GenerationRef>, after: Option<&'a str>, maximum_entries: u32) -> BoxFuture<'a, Result<PrivateDirectoryPage>> {
        Box::pin(async move { self.reauthenticate(volume, None).await?; ContentResidencyVerifier::list_private_directory(&self.reader, volume, prefix, path, generation, after, maximum_entries).await })
    }
    fn read_private_path<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str, generation: Option<&'a GenerationRef>) -> BoxFuture<'a, Result<(FileRef, acyclic_stream::OwnedBytes)>> {
        Box::pin(async move { self.reauthenticate(volume, None).await?; ContentResidencyVerifier::read_private_path(&self.reader, volume, prefix, path, generation).await })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> CreatedFilesystemAccess<A, O> {
    async fn publish(&self, operation: OperationId, path: &str, bytes: &[u8], media_type: &str,
        display_name: &str, source: Option<&FileRef>, io: Option<&PreparationIo>) -> Result<FileRef> {
        let volume = source.map_or(&self.volume, FileRef::volume);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            let authentication = PreparationIo::from_native_work(work)?;
            self.reauthenticate(volume, Some(&authentication)).await?;
        } else { self.reauthenticate(volume, io).await?; }
        #[cfg(target_arch = "wasm32")]
        self.reauthenticate(volume, io).await?;
        match source {
            Some(source) => self.writer.stage_at_with_io(operation, source, bytes, io, None, None).await,
            None => self.writer.stage_once_with_io(operation, path, bytes, media_type, display_name, io, None, None).await,
        }
    }
    async fn observe(&self, operation: OperationId, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<Option<FileRef>> {
        self.reauthenticate(&self.volume, io).await?;
        self.writer.observe_once_with_io(operation, maximum_bytes.min(self.request.maximum_bytes), io).await
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentPublisher for CreatedFilesystemAccess<A, O> {
    fn volume(&self) -> &VolumeRef { &self.volume }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_stage(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if self.writer.volume() != &self.volume { return Err(Error::Unauthorized("created writer is not its exact bound namespace".into())); }
        self.writer.supports_native_stage(work)
    }
    fn stage<'a>(&'a self, operation: OperationId, path: &'a str, bytes: &'a [u8], media_type: &'a str,
        display_name: &'a str) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move { self.reauthenticate(&self.volume, None).await?; self.writer.stage(operation, path, bytes, media_type, display_name).await })
    }
    created_publisher!(self, budget, cancellation, content_io(&self.request, budget, cancellation));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{VolumeClass, VolumeOwner};
    use crate::core::AggregateKind;

    #[cfg(not(target_arch = "wasm32"))]
    fn observation_work() -> Result<acyclic_fs::NativeMaterializationWork> {
        let units = 1_048_576;
        let bytes = 128 * 1_048_576;
        acyclic_fs::NativeMaterializationWork::new(WorkBudget {
            authority_records_read: units, authority_records_appended: units,
            authority_bytes_read: bytes, authority_bytes_written: bytes,
            object_probes: units, backend_read_operations: units, backend_write_operations: units,
            durability_operations: units, page_reads: units, page_writes: units,
            object_bytes_read: bytes, object_bytes_written: bytes,
            bytes_hashed: bytes, bytes_copied: bytes, bytes_encoded: bytes,
            source_bytes_read: bytes, source_path_components: units, source_entries_visited: units,
            output_bytes: bytes, items_examined: units, items_returned: units,
            allocation_operations: units, peak_allocation_bytes: bytes, materializations: units,
        }, &CancellationToken::new())
            .map_err(|(cause, _)| Error::NativeWork { cause, custody: None })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn original_root_fixture_authority(agent: AgentId, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<((Authority, AuthorityIssuer, crate::core::Scope), acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        let configured_audience = Authority { kind: AggregateKind::Conversation, id: "genuine-original-root-bootstrap".into() };
        let configured_issuer = AuthorityIssuer::new("created-owner", [80; 32], configured_audience.clone());
        let mut metadata = crate::store::history::native_work_io(work)
            .map_err(Error::from)?;
        metadata.retain_owned(std::mem::size_of::<(Authority, AuthorityIssuer, crate::core::Scope)>() as u64)?;
        let mut custody;
        let audience = metadata.clone_authority(&configured_audience)?;
        custody = metadata.into_custody();
        let (issuer, mut issuer_custody) = configured_issuer.clone_with_native_work(work)?;
        custody.absorb(&mut issuer_custody).map_err(|cause| Error::NativeWork {
            cause, custody: Some(issuer_custody),
        })?;
        let (scope, mut scope_custody) = issuer.root_for_agent_with_native_work(agent,
            "genuine-original-root-bootstrap", &Capabilities::new(["conversation:bind"]), work)?;
        custody.absorb(&mut scope_custody).map_err(|cause| Error::NativeWork {
            cause, custody: Some(scope_custody),
        })?;
        Ok(((audience, issuer, scope), custody))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn bind_genuine_original_root(
        fixture: &crate::durable_host::tests::created_resource_access::CreationFixture,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::durable_host::TaskJournalOwner<acyclic_stream::MemoryStream>> {
        let owner = fixture.host.journal_owner(fixture.root,
            (&fixture.root_lease.reservation).into()).await?;
        let agent = owner.authenticated_binding_agent_with_native_work(work)?.ok_or(native_error(crate::wire::ErrorCode::Unauthorized, "fixture has no original Root actor"))?;
        let authority_custody;
        let (authority, custody) = original_root_fixture_authority(agent, work)?;
        authority_custody = custody;
        let scratch = &fixture.original_root_births.as_ref().expect("real pre-admission births")[1];
        super::super::super::bind_original_root_conversation_with_native_work(
            &fixture.stream, authority, authority_custody, &owner,
            &scratch.volume, scratch.operation, fixture.root_scope.limits(), work,
        ).await?;
        Ok(owner)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn real_original_root_births_bind_readonly_project_and_actual_private_output() -> Result<()> {
        use crate::resource_preparation::{CreatedResourceAccess, RootBootstrapContentContext, RootBootstrapContentRole};
        use crate::source_authorization::{ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider};
        let work = observation_work()?;
        let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_original_root_births(&work).await?;
        let owner = bind_genuine_original_root(&fixture, &work).await?;
        let creator = &fixture.filesystem_creator;
        for (birth, role) in fixture.original_root_births.as_ref().unwrap().iter().zip([
            RootBootstrapContentRole::Project, RootBootstrapContentRole::Scratch,
        ]) {
            let token = creator.protect_root_bootstrap_with_native_work(&owner, &birth.volume, role,
                birth.operation, birth.config, birth.initial_generation, birth.selected_generation, None, &work)?.await?
                .into_native_shared(&work)?;
            creator.bind_root_bootstrap_allocation_with_native_work(token, &work)?;
        }
        let [project, scratch] = creator.bound_root_bootstrap_allocations(&work)?;
        assert_ne!(project.initial_generation, project.selected_generation);
        assert_ne!(scratch.initial_generation, scratch.selected_generation);
        assert!(!project.writable);
        assert!(scratch.writable);
        let admission_custody;
        let (admission, custody) = owner.original_root_admission_with_native_work(&work).await?;
        admission_custody = custody;
        let context = RootBootstrapContentContext::from_verified_root_with_native_work(
            &owner, &admission, project.witness.binding(), &work,
        ).await?;
        let mut accesses = Vec::new();
        for token in [project, scratch] {
            let (receipt, custody) = creator.verify_root_bootstrap_with_native_work(context, token.request(), &work)?.await?;
            accesses.push(CreatedResourceAccess::consume_root_bootstrap_with_native_work(
                context, token.request(), receipt, custody, &work,
            )?);
        }
        let scratch_access = accesses.pop().unwrap();
        let access = accesses.pop().unwrap().merge_root_bootstrap_with_native_work(scratch_access, &work)?;
        let scope_custody;
        let mut io = crate::store::history::native_work_io(&work)
            .map_err(Error::from)?;
        let (scope, retained) = fixture.root_scope.with_created_access_with_work(&admission, Some(access), &mut Some(&mut io))?;
        scope_custody = crate::store::history::HistoryIo::take_native_custody(&mut io, retained)?;
        assert_eq!(scope.grants(), fixture.root_scope.grants());
        assert_eq!(scope.current_created_private_volume_with_native_work(&work)?, Some(&scratch.request.volume));
        assert!(scope.created_content(Some(&project.request.volume)).unwrap().writer.is_none());
        assert!(scope.created_content(Some(&scratch.request.volume)).unwrap().writer.is_some());
        let reader = &scope.created_content(Some(&project.request.volume)).unwrap().reader;
        let (file, bytes, metadata) = reader.read_private_path_with_native_work(
            &project.request.volume, "", "root.txt", Some(&project.request.generation), 1024, &work,
        )?.await?;
        assert_eq!(bytes.as_ref(), b"original project import");
        assert_eq!(file.volume(), &project.request.volume);
        drop((file, bytes));
        drop(metadata);
        let private = scope.created_content(Some(&scratch.request.volume)).unwrap();
        let staged_custody;
        let (staged, custody) = private.writer.as_ref().unwrap().stage_once_with_native_work(
            OperationId::from_bytes([185; 16]), "root-output.txt", b"actual original root output",
            "text/plain", "root-output.txt", &work,
        )?.await?;
        staged_custody = custody;
        let output = private.reader.read_with_native_work(&staged, 1024, &work)?.await?;
        assert_eq!(output.as_ref(), b"actual original root output");
        assert_eq!(staged.volume(), &scratch.request.volume);
        assert_ne!(staged.volume(), &project.request.volume);
        drop(output);
        drop(staged);
        drop(staged_custody);
        let policy = super::super::super::root_source::FsRootProjectSourceProvider::new_with_native_work(
            fixture.filesystem.clone(), &project.owner, fixture.root, &work,
        )?.with_bootstrap_source_with_native_work(Arc::clone(project), &work)?;
        let selection = ProjectSourceSnapshot { project: project.request.volume.clone(),
            generation: project.request.generation.clone(), native_snapshot: None };
        policy.verify_with_native_work(RootProjectSourceContext {
            owner: &project.owner, caller: &admission, source: &admission, graph_root: fixture.root,
            operation_id: OperationId::from_bytes([180; 16]), maximum_work: 1_048_576,
            maximum_bytes: 1_048_576, cancellation: work.cancellation(), created_project_authenticated: false,
        }, &selection, &work)?.await?;
        drop(scope);
        drop(scope_custody);
        drop(admission);
        drop(admission_custody);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn root_bootstrap_wrong_creator_birth_selection_and_graph_never_install_access() -> Result<()> {
        use crate::resource_preparation::{RootBootstrapContentContext, RootBootstrapContentRole};
        let work = observation_work()?;
        let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_original_root_births(&work).await?;
        let owner = bind_genuine_original_root(&fixture, &work).await?;
        let birth = &fixture.original_root_births.as_ref().unwrap()[0];
        let bad_issuer = AuthorityIssuer::new("created-owner", [81; 32], fixture.filesystem_creator.issuer.audience().clone());
        let bad_creator = FilesystemResourceCreationProvider::new_with_native_work(fixture.filesystem.clone(), &bad_issuer, &work)?;
        assert!(bad_creator.protect_root_bootstrap_with_native_work(&owner, &birth.volume, RootBootstrapContentRole::Project,
            birth.operation, birth.config, birth.initial_generation, birth.selected_generation, None, &work)?.await.is_err());


        let creator = &fixture.filesystem_creator;
        assert!(creator.protect_root_bootstrap_with_native_work(&owner, &birth.volume, RootBootstrapContentRole::Project,
            acyclic_fs::OperationId::from_bytes([179; 16]), birth.config,
            birth.initial_generation, birth.selected_generation, None, &work)?.await.is_err());
        let mut wrong_initial = birth.initial_generation.digest().into_bytes();
        wrong_initial[0] ^= 1;
        assert!(creator.protect_root_bootstrap_with_native_work(&owner, &birth.volume, RootBootstrapContentRole::Project,
            birth.operation, birth.config, acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(wrong_initial)),
            birth.selected_generation, None, &work)?.await.is_err());
        let foreign_scratch = VolumeRef::new(fixture.filesystem_provider.clone(), "foreign-original-root-private",
            VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::from_bytes([184; 16])))?;
        let denied = creator.protect_root_bootstrap_with_native_work(&owner, &foreign_scratch, RootBootstrapContentRole::Scratch,
            birth.operation, birth.config, birth.initial_generation, birth.selected_generation, None, &work);
        assert!(match denied { Ok(future) => future.await.is_err(), Err(_) => true });


        assert!(creator.bound_root_bootstrap_allocations(&work).is_err());
        let token = creator.protect_root_bootstrap_with_native_work(&owner, &birth.volume, RootBootstrapContentRole::Project,
            birth.operation, birth.config, birth.initial_generation, birth.selected_generation, None, &work)?.await?
            .into_native_shared(&work)?;
        assert!(bad_creator.bind_root_bootstrap_allocation_with_native_work(Arc::clone(&token), &work).is_err());

        creator.bind_root_bootstrap_allocation_with_native_work(Arc::clone(&token), &work)?;
        let admission_custody;
        let (admission, custody) = owner.original_root_admission_with_native_work(&work).await?;
        admission_custody = custody;
        let context = RootBootstrapContentContext::from_verified_root_with_native_work(
            &owner, &admission, token.witness.binding(), &work,
        ).await?;
        for control in 0..5 {
            let (mut request, request_custody) = token.request().clone_with_native_work(&work)?;
            match control {
                0 => request.graph_root = TaskId::from_bytes([181; 16]),
                1 => request.root_operation = OperationId::from_bytes([182; 16]),
                2 => request.generation = GenerationRef::new(fixture.filesystem_provider.clone(),
                    birth.initial_generation.digest().into_bytes().to_vec(), None)?,
                3 => request.volume = VolumeRef::new(fixture.filesystem_provider.clone(), "foreign-root-project",
                    VolumeClass::Project, VolumeOwner::Project("created-project".into()))?,
                _ => request.role = RootBootstrapContentRole::Scratch,
            }
            let denied = creator.verify_root_bootstrap_with_native_work(context, &request, &work);
            assert!(match denied { Ok(future) => future.await.is_err(), Err(_) => true });


            drop(request);
            drop(request_custody);
        }
        drop(admission);
        drop(admission_custody);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn physical_root_birth_alone_and_superseded_protected_record_cannot_authenticate_source() -> Result<()> {
        use crate::resource_preparation::RootBootstrapContentRole;
        use crate::source_authorization::{ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider};
        let work = observation_work()?;
        let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_original_root_births(&work).await?;
        let creator = &fixture.filesystem_creator;
        let birth = &fixture.original_root_births.as_ref().unwrap()[0];
        let owner = fixture.host.journal_owner(fixture.root, (&fixture.root_lease.reservation).into()).await?;
        assert!(creator.protect_root_bootstrap_with_native_work(&owner, &birth.volume, RootBootstrapContentRole::Project,
            birth.operation, birth.config, birth.initial_generation, birth.selected_generation, None, &work)?.await.is_err());
        let admission_custody;
        let (admission, custody) = owner.original_root_admission_with_native_work(&work).await?;
        admission_custody = custody;
        let bare = super::super::super::root_source::FsRootProjectSourceProvider::new_with_native_work(
            fixture.filesystem.clone(), creator.issuer.audience(), fixture.root, &work,
        )?;
        let selection = ProjectSourceSnapshot { project: birth.volume.clone(),
            generation: GenerationRef::new(fixture.filesystem_provider.clone(),
                birth.selected_generation.digest().into_bytes().to_vec(), None)?, native_snapshot: None };
        let context = || RootProjectSourceContext {
            owner: creator.issuer.audience(), caller: &admission, source: &admission, graph_root: fixture.root,
            operation_id: OperationId::from_bytes([183; 16]), maximum_work: 1_048_576,
            maximum_bytes: 1_048_576, cancellation: work.cancellation(), created_project_authenticated: false,
        };
        let denied = bare.verify_with_native_work(context(), &selection, &work);
        assert!(match denied { Ok(future) => future.await.is_err(), Err(_) => true });

        let bound_owner = bind_genuine_original_root(&fixture, &work).await?;
        let token = creator.protect_root_bootstrap_with_native_work(&bound_owner, &birth.volume, RootBootstrapContentRole::Project,
            birth.operation, birth.config, birth.initial_generation, birth.selected_generation, None, &work)?.await?
            .into_native_shared(&work)?;
        let policy = bare.with_bootstrap_source_with_native_work(Arc::clone(&token), &work)?;
        policy.verify_with_native_work(context(), &selection, &work)?.await?;
        let initial_selection = ProjectSourceSnapshot { project: selection.project.clone(),
            generation: GenerationRef::new(fixture.filesystem_provider.clone(),
                birth.initial_generation.digest().into_bytes().to_vec(), None)?, native_snapshot: None };
        let denied = policy.verify_with_native_work(context(), &initial_selection, &work);
        assert!(match denied { Ok(future) => future.await.is_err(), Err(_) => true });


        let journal_io = PreparationIo::for_unmeasured(work.cancellation())?;
        let journal = allocation_ref(&creator.host.provider, &birth.volume, Some(&journal_io))?;
        let generation = fixture.filesystem.resolve(&journal).await?.generation;
        fixture.filesystem.apply(&journal, Some(&generation), &[
            super::super::super::WorkspaceMutation::PutFile {
                path: "/root-bootstrap.json".into(), bytes: b"superseded protected bootstrap".to_vec(),
            },
        ], &IdempotencyKey::new("supersede-genuine-original-root-bootstrap")?).await?;
        assert!(policy.verify_with_native_work(context(), &selection, &work)?.await.is_err());
        drop(admission);
        drop(admission_custody);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn canonical_original_root_binder_rejects_foreign_prior_bind_and_wrong_actor() -> Result<()> {
        use crate::core::{Action, Command, SchemaRegistry};
        for prior_foreign_bind in [false, true] {
            let work = observation_work()?;
            let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_original_root_births(&work).await?;
            let owner = fixture.host.journal_owner(fixture.root, (&fixture.root_lease.reservation).into()).await?;
            if prior_foreign_bind {
                let audience = Authority { kind: AggregateKind::Conversation, id: "genuine-original-root-bootstrap".into() };
                let issuer = AuthorityIssuer::new("created-owner", [80; 32], audience.clone());
                let foreign_agent = AgentId::from_bytes([186; 16]);
                let scope = issuer.root_for_agent(foreign_agent, "foreign-prior-root-bind", Capabilities::new(["conversation:bind"]));
                let mut aggregate = crate::store::StreamAggregate::open(
                    &fixture.stream, audience, issuer.verifier(), SchemaRegistry::new(),
                ).await?;
                aggregate.execute(Command { operation_id: OperationId::from_bytes([186; 16]),
                    idempotency_key: IdempotencyKey::new("genuine-foreign-prior-root-bind")?,
                    expected_revision: 0, causal_parent: None,
                    action: Action::BindConversation { agent: foreign_agent }, scope }).await?;
            }
            let actor = if prior_foreign_bind { AgentId::from_bytes([80; 16]) }
                else { AgentId::from_bytes([187; 16]) };
            let authority_custody;
            let (authority, custody) = original_root_fixture_authority(actor, &work)?;
            authority_custody = custody;
            let scratch = &fixture.original_root_births.as_ref().unwrap()[1];
            assert!(super::super::super::bind_original_root_conversation_with_native_work(
                &fixture.stream, authority, authority_custody, &owner,
                &scratch.volume, scratch.operation, fixture.root_scope.limits(), &work,
            ).await.is_err());
            assert!(fixture.filesystem_creator.bound_root_bootstrap_allocations(&work).is_err());
            assert!(owner.root_bootstrap_binding_witness_with_native_work(&work).await.is_err());
        }
        Ok(())
    }
}
