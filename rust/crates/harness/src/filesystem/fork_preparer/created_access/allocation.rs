//! Protected original Root allocation and lazy publication.
use super::*;

/// The protected original Root birth and its signed creation context.
pub struct FilesystemRootBootstrapAllocation<A, O> {
    pub(crate) host: Arc<FilesystemHost<A, O>>,
    pub(super) creator: crate::core::AuthorityVerifier,
    pub(crate) admission_digest: [u8; 32],
    pub(crate) request: crate::durable_host::TaskVolumeSnapshot,
    native_snapshot: Option<serde_json::Value>,
    record_digest: [u8; 32],
    pub(crate) operation: acyclic_fs::OperationId,
    pub(crate) config: acyclic_fs::VolumeConfig,
    pub(crate) initial_generation: acyclic_fs::GenerationId,
    pub(crate) native_source: Option<crate::filesystem::root_source::FsRootNativeSource<A, O>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) publication_lease: Option<Arc<dyn crate::resource_preparation::CreatedTaskLease>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) witness: Arc<crate::durable_host::RootBootstrapBindingWitness>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) _custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
async fn birth<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    volume: &VolumeRef,
    config: acyclic_fs::VolumeConfig,
    operation: acyclic_fs::OperationId,
    io: &PreparationIo,
) -> Result<Option<acyclic_fs::Generation<A, O>>> {
    let held;
    let name = bounded_io::volume_storage_name(volume, Some(io))?;
    held = io.guard_owned(name.capacity() as u64)?;
    let result = io.sdk(
        host.filesystem
            .workspace_for_creation_operation(&name, config, operation, io, io.cancellation())
            .await,
    );
    drop(name);
    drop(held);
    result
}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemResourceCreationProvider<A, O>
{
    pub fn protect_root_bootstrap_with_native_work<'a, P: StreamProvider>(
        &'a self, owner: &'a crate::durable_host::TaskJournalOwner<P>,
        volume: &'a VolumeRef, role: VolumeClass,
        operation: acyclic_fs::OperationId,
        config: acyclic_fs::VolumeConfig, initial_generation: acyclic_fs::GenerationId,
        selected_generation: acyclic_fs::GenerationId,
        native_import: Option<(&'a crate::filesystem::root_source::FsRootNativeSource<A, O>, acyclic_fs::LazySnapshotRef)>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, FilesystemRootBootstrapAllocation<A, O>>> {
        volume.validate(&mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)))?;
        let agent = owner.authenticated_binding_agent(&mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)))?;
        if owner.root_bootstrap_owner() != self.issuer.audience() || volume.provider() != &self.host.provider
            || !match role {
                crate::conversation::VolumeClass::Project =>
                    volume.class() == VolumeClass::Project,
                crate::conversation::VolumeClass::AgentPrivate =>
                    volume.class() == VolumeClass::AgentPrivate
                        && matches!(volume.owner(), VolumeOwner::Agent(owner) if Some(*owner) == agent),
                crate::conversation::VolumeClass::SessionShared => false,
            }
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root bootstrap differs from its configured original creator or namespace" });
        }
        if native_import.is_some_and(|(source, _)| !source.backing.matches_native_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root lazy source belongs to another original native ledger" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let creator_custody;
            let mut observer = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
            let creator = self.issuer.verifier(&mut Some(&mut observer))?;
            let custody = observer.into_custody()?;
            creator_custody = custody;
            owner.authenticate_root_bootstrap_creator_with_native_work(&creator, work).await?;
            drop(creator);
            drop(creator_custody);
            let witness = owner.root_bootstrap_binding_witness_with_native_work(work).await?;
            let root = witness.binding();
            let original_custody;
            let (original_root, custody) = owner.original_root_admission_with_native_work(work).await?;
            original_custody = custody;
            let context = owner.root_bootstrap_context(&original_root, root, work).await?;
            if role == crate::conversation::VolumeClass::AgentPrivate
                && volume.owner() != &VolumeOwner::Agent(root.agent()) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root bootstrap differs from its configured creator, actor or namespace" });
            }
            owner.require_volume_grant(volume, VolumeOperation::Read, &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)))?;
            let io = PreparationIo::from_native_work(work)?;
            let write = io.capability(volume, "volume:write:")?;
            let writable = context.original_root().grants.contains(&write);
            let write_bytes = write.capacity() as u64;
            drop(write);
            io.release(write_bytes)?;
            if writable { owner.require_volume_grant(volume, VolumeOperation::Write, &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)))?; }
            let publication_lease = if writable {
                Some(owner.root_bootstrap_publication_lease_with_native_work(work).await?)
            } else { None };
            let name_hold;
            let name = bounded_io::volume_storage_name(volume, Some(&io))?;
            name_hold = io.guard_owned(name.capacity() as u64)?;
            let birth = self.host.filesystem.workspace_for_creation_operation(
                &name, config, operation, work, io.cancellation(),
            ).await.map_err(Error::from)?
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root bootstrap has no original protected Filesystem birth" })?;
            if birth.id() != initial_generation {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root bootstrap initial generation differs from its actual birth" });
            }
            let selected = io.sdk(birth.workspace().generation(
                selected_generation, &io, io.cancellation(),
            ).await)?;
            let native_snapshot = if let Some((lazy, frozen)) = native_import {
                if lazy.backing.workspace_id() != birth.workspace_id() {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root bootstrap import names another original workspace" });
                }
                if frozen.authored_generation != selected.id() || frozen.workspace_id != birth.workspace_id() {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root bootstrap selected generation differs from its original import" });
                }
                lazy.backing.validate(frozen, &io)?.await?;
                Some(frozen)
            } else { None };
            drop(selected);
            drop(birth);
            drop(name);
            drop(name_hold);
            let mut record = io.encode(&(
                (owner.root_bootstrap_owner(), root.audience(), root.original_task(),
                    context.original_admission_digest(), root.admission_digest()),
                (role, root.agent(), root.bind_operation(), root.intent_digest(), root.fence()),
                (volume, operation, config, initial_generation, selected_generation, native_snapshot, writable),
            ), MAX_REQUEST_BYTES, true)?;
            let journal_hold;
            let journal = allocation_ref(&self.host.provider, volume, Some(&io))?;
            journal_hold = io.guard_owned(journal.logical_owned_bytes()?)?;
            let name = std::str::from_utf8(journal.as_resource().key()).map_err(|_| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "Root allocation journal name is not UTF-8" })?;
            let mut key_bytes = [0_u8; 32];
            hex::encode_to_slice(root.original_task().into_bytes(), &mut key_bytes).map_err(|_| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "Root allocation identity encoding failed" })?;
            let suffix = std::str::from_utf8(&key_bytes).map_err(|_| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "Root allocation identity is not ASCII" })?;
            let create = IdempotencyKey::new(crate::executor::with_history_io(Some(&io), |work|
                crate::contract::preparation_format(format_args!("root-bootstrap-journal:{suffix}"), work))?)?;
            let lifecycle = if self.host.filesystem.capabilities().durable {
                acyclic_fs::Lifecycle::Durable
            } else {
                acyclic_fs::Lifecycle::Ephemeral
            };
            let creation = acyclic_fs::OperationId::from_bytes(
                bounded_io::operation_key(&create, Some(&io))?.into_bytes(),
            );
            io.sdk(
                self.host
                    .filesystem
                    .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                        name,
                        acyclic_fs::VolumeConfig::portable(lifecycle),
                        creation,
                        &io,
                        io.cancellation(),
                        None,
                    )
                    .await,
            )?;
            match bounded_io::read(
                &self.host,
                &journal,
                "/claim.json",
                MAX_REQUEST_BYTES,
                Some(&io),
            )
            .await
            {
                Ok(_) => {
                    return Err(stock_boundary(
                        Some(&io),
                        crate::wire::ErrorCode::Unauthorized,
                        "child allocation cannot be claimed as original Root bootstrap",
                    ));
                }
                Err(error) if journal_absent(&error) => {}
                Err(error) => return Err(error),
            }
            let retained = match bounded_io::read(&self.host, &journal, "/root-bootstrap.json",
                MAX_REQUEST_BYTES, Some(&io)).await {
                Ok(bytes) => Some(bytes),
                Err(error) if journal_absent(&error) => None,
                Err(error) => return Err(error),
            };
            if let Some(bytes) = retained {
                if bytes != record {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "original Root bootstrap association changed" });
                }
            } else {
                let observed = bounded_io::resolve(&self.host, &journal, Some(&io)).await?;
                let key = IdempotencyKey::new(crate::executor::with_history_io(Some(&io), |work|
                    crate::contract::preparation_format(format_args!("root-bootstrap-claim:{suffix}"), work))?)?;
                let path = crate::executor::stock_string_copy("/root-bootstrap.json", Some(&io))?;
                let mutations = [WorkspaceMutation::PutFile { path, bytes: record }];
                let outcome = bounded_io::apply_generation(&self.host, &journal, Some(&observed.generation),
                    &mutations, &key, &io).await;
                let [WorkspaceMutation::PutFile { bytes: recovered, .. }] = mutations else {
                    unreachable!("the one Root association mutation is a file write")
                };
                record = recovered;
                match outcome {
                    Ok(_) => {},
                    Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {},
                    Err(error) => return Err(error),
                }
                let retained = bounded_io::read(&self.host, &journal, "/root-bootstrap.json",
                    MAX_REQUEST_BYTES, Some(&io)).await?;
                if retained != record {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "protected Root bootstrap association was not published exactly" });
                }
            }
            drop(journal);
            drop(journal_hold);
            let admission_digest = context.original_admission_digest();
            let native_source = native_import.map(|(source, _)| source.clone());
            io.retain_owned(std::mem::size_of::<FilesystemRootBootstrapAllocation<A, O>>() as u64)?;
            let creator = {
                let mut observer = io.history_io();
                self.issuer.verifier(&mut Some(&mut observer))?
            };
            let work = io.original_native_work().ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "Root bootstrap requires its original native ledger" })?;
            let volume = volume.copy(&mut Some(&mut io.history_io()))?;
            let generation = bounded_io::generation_ref(
                &self.host.provider, selected_generation, Some(&io),
            )?;
            let snapshot = if let Some(snapshot) = native_snapshot {
                let remaining = io.remaining()?;
                let value = crate::contract::json_value_bounded(&snapshot, crate::contract::JsonBounds {
                    maximum_bytes: remaining.bytes_encoded, maximum_items: remaining.items_examined, maximum_depth: 32,
                }, Some(crate::wire::ErrorCode::Invalid), &mut |charge| io.codec_charge(charge, true))?;
                io.release(std::mem::size_of::<serde_json::Value>() as u64)?;
                Some(value)
            } else { None };
            let request = crate::durable_host::TaskVolumeSnapshot { volume, generation };
            let record_digest = crate::model::hash_bytes(&record, Some(&io))?;
            drop(record);
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            drop(original_root);
            drop(original_custody);
            Ok(FilesystemRootBootstrapAllocation {
                host: self.host.clone(), creator, admission_digest, request, native_snapshot: snapshot,
                operation, config, initial_generation, native_source,
                publication_lease,
                record_digest, witness, _custody: custody,
            })
        })
    }
}

impl<A, O> FilesystemRootBootstrapAllocation<A, O> {
    pub(crate) fn owner(&self) -> &Authority {
        self.creator.audience()
    }
    pub(super) fn snapshot(&self) -> Option<acyclic_fs::LazySnapshotRef> {
        self.native_snapshot.as_ref().map(|value| {
            acyclic_fs::LazySnapshotRef::deserialize(value)
                .expect("snapshot came from fixed SDK data")
        })
    }
    /// Borrows the request protected by the original allocation journal.
    pub fn request(&self) -> &crate::durable_host::TaskVolumeSnapshot {
        &self.request
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool {
        self._custody.work().shares_allocation_context(work)
    }
    /// Shares the paid allocation on its original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self.matches_native_work(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "Root allocation belongs to another original ledger",
            });
        }
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..Default::default()
        })?;
        Ok(Arc::new(self))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn record(&self) -> impl Serialize + '_ {
        let root = self.witness.binding();
        let selected = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
            self.request
                .generation
                .as_resource()
                .key()
                .try_into()
                .expect("protected generation is a Filesystem digest"),
        ));
        (
            (
                self.owner(),
                root.audience(),
                root.original_task(),
                self.admission_digest,
                root.admission_digest(),
            ),
            (
                self.request.volume.class(),
                root.agent(),
                root.bind_operation(),
                root.intent_digest(),
                root.fence(),
            ),
            (
                &self.request.volume,
                self.operation,
                self.config,
                self.initial_generation,
                selected,
                &self.native_snapshot,
                self.publication_lease.is_some(),
            ),
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemRootBootstrapAllocation<A, O>
{
    pub(super) async fn reauthenticate_with_io(&self, io: &PreparationIo) -> Result<()> {
        let journal_hold;
        let journal = allocation_ref(&self.host.provider, &self.request.volume, Some(io))?;
        journal_hold = io.guard_owned(journal.logical_owned_bytes()?)?;
        let bytes = bounded_io::read(
            &self.host,
            &journal,
            "/root-bootstrap.json",
            MAX_REQUEST_BYTES,
            Some(io),
        )
        .await?;
        let bytes_hold = io.guard_bytes(&bytes)?;
        let record = io.encode(&self.record(), MAX_REQUEST_BYTES, true)?;
        let record_hold = io.guard_bytes(&record)?;
        let exact = bytes == record && crate::model::hash_bytes(&bytes, Some(io))? == self.record_digest;
        drop((bytes, record));
        drop((bytes_hold, record_hold));
        drop(journal);
        drop(journal_hold);
        if !exact {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "protected original Root bootstrap association changed",
            ));
        }
        let original = birth(
            &self.host,
            &self.request.volume,
            self.config,
            self.operation,
            io,
        )
        .await?;
        if !original
            .as_ref()
            .is_some_and(|birth| birth.id() == self.initial_generation)
        {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "Root content protected Filesystem birth changed",
            ));
        }
        Ok(())
    }

    pub(in crate::filesystem) fn source_generation(
        &self,
        file: Option<&FileRef>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<&GenerationRef>> {
        let Some(source) = &self.native_source else {
            return Ok(None);
        };
        let reject = |reason| stock_boundary(io, crate::wire::ErrorCode::Unauthorized, reason);
        let snapshot = self
            .snapshot()
            .ok_or_else(|| reject("Root lazy content has no original frozen snapshot"))?;
        if snapshot.workspace_id != source.backing.workspace_id()
            || self.request.generation.as_resource().key()
                != snapshot
                    .authored_generation
                    .digest()
                    .into_bytes()
                    .as_slice()
        {
            return Err(reject(
                "Root lazy publication basis differs from its protected allocation",
            ));
        }
        if let Some(io) = io {
            io.check_cancellation()?;
            if io.original_native_work().is_some_and(|work| {
                !self.matches_native_work(work) || !source.backing.matches_native_work(work)
            }) {
                return Err(reject(
                    "Root lazy publication belongs to another original ledger",
                ));
            }
        }
        if let Some(file) = file {
            if file.volume() != &self.request.volume {
                return Err(reject("Root source names another volume"));
            }
            if let Some(io) = io {
                io.charge(WorkCounters {
                    bytes_encoded: 64,
                    bytes_copied: 64,
                    ..Default::default()
                })?;
            }
            if file.version()
                != blake3::Hash::from_bytes(snapshot.id.into_bytes())
                    .to_hex()
                    .as_str()
            {
                return Ok(None);
            }
        }
        Ok(Some(&self.request.generation))
    }

    pub(in crate::filesystem) async fn publish_lazy_candidate_with_io(
        &self,
        mutations: &[WorkspaceMutation],
        key: &IdempotencyKey,
        permit: acyclic_fs::PublicationPermit,
        io: Option<&PreparationIo>,
    ) -> Result<(
        acyclic_fs::Generation<A, O>,
        Result<acyclic_fs::LazySnapshotRef>,
    )> {
        let owned;
        let io = match io {
            Some(io) => io,
            None => {
                owned = PreparationIo::for_unmeasured(self._custody.work().cancellation())?;
                &owned
            }
        };
        let lease = self.publication_lease.as_ref().ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "Root lazy content has no original publication authority",
            )
        })?;
        self.source_generation(None, Some(io))?
            .ok_or_else(|| io.invalid("eager Root does not select a lazy publication candidate"))?;
        lease.verify(Some(io))?.await?;
        self.reauthenticate_with_io(io).await?;
        let source = self
            .native_source
            .as_ref()
            .expect("authenticated original source");
        let snapshot = self.snapshot().expect("authenticated original snapshot");
        let operation = bounded_io::operation_key(key, Some(io))?;
        source
            .backing
            .publish(snapshot, mutations, operation, permit, io, lease)?
            .await
    }
}

use crate::executor::{stock_boundary, stock_string_copy};
