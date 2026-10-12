//! Protected original Root allocation and lazy publication.
use super::*;

/// The protected original Root birth and its signed creation context.
pub struct FilesystemRootBootstrapAllocation<A, O> {
    pub(crate) host: Arc<FilesystemHost<A, O>>,
    pub(super) creator: crate::core::AuthorityVerifier,
    pub(crate) admission_digest: [u8; 32],
    pub(crate) request: crate::resource_preparation::RootBootstrapContentRequest,
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
    let result = match io.original_native_work() {
        Some(work) => host
            .filesystem
            .workspace_for_creation_operation_with_native_work(
                &name,
                config,
                operation,
                work,
                io.cancellation(),
            )
            .await
            .map_err(|(source, custody)| Error::NativeWorkspace { source, custody }),
        None => io.receive(
            host.filesystem
                .workspace_for_creation_operation(
                    &name,
                    config,
                    operation,
                    io.remaining()?,
                    io.cancellation(),
                )
                .await,
        ),
    };
    drop(name);
    drop(held);
    result
}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemResourceCreationProvider<A, O>
{
    /// Protects a real pre-admission birth under its original Root owner.
    pub fn protect_root_bootstrap_with_native_work<'a, P: StreamProvider>(
        &'a self,
        owner: &'a crate::durable_host::TaskJournalOwner<P>,
        volume: &'a VolumeRef,
        role: crate::resource_preparation::RootBootstrapContentRole,
        operation: acyclic_fs::OperationId,
        config: acyclic_fs::VolumeConfig,
        initial_generation: acyclic_fs::GenerationId,
        selected_generation: acyclic_fs::GenerationId,
        native_import: Option<(
            &'a crate::filesystem::root_source::FsRootNativeSource<A, O>,
            acyclic_fs::LazySnapshotRef,
        )>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, FilesystemRootBootstrapAllocation<A, O>>> {
        use crate::resource_preparation::RootBootstrapContentRole;
        volume.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        let agent = owner.authenticated_binding_agent(&mut Some(
            &mut crate::store::history::native_work_io(work)?,
        ))?;
        let valid_role = match role {
            RootBootstrapContentRole::Project => volume.class() == VolumeClass::Project,
            RootBootstrapContentRole::Scratch => {
                volume.class() == VolumeClass::AgentPrivate
                    && matches!(volume.owner(), VolumeOwner::Agent(owner) if Some(*owner) == agent)
            }
        };
        if owner.root_bootstrap_owner() != self.issuer.audience()
            || volume.provider() != &self.host.provider
            || !valid_role
            || native_import.is_some_and(|(source, _)| !source.backing.matches_native_work(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "Root bootstrap differs from its original creator, namespace or ledger",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            io.retain_owned(std::mem::size_of::<FilesystemRootBootstrapAllocation<A, O>>() as u64)?;
            let creator = self.issuer.verifier(&mut Some(&mut io.history_io()))?;
            owner
                .authenticate_root_bootstrap_creator_with_native_work(&creator, work)
                .await?;
            let witness = owner
                .root_bootstrap_binding_witness_with_native_work(work)
                .await?;
            let admission_custody;
            let (admission, custody) = owner.original_root_admission_with_native_work(work).await?;
            admission_custody = custody;
            let context = crate::resource_preparation::RootBootstrapContentContext::from_verified_root_with_native_work(owner, &admission, witness.binding(), work).await?;
            if role == RootBootstrapContentRole::Scratch
                && volume.owner() != &VolumeOwner::Agent(witness.binding().agent())
            {
                return Err(bounded_io::failure(
                    Some(&io),
                    crate::wire::ErrorCode::Unauthorized,
                    "Root Scratch differs from its original actor",
                ));
            }
            owner.require_volume_grant(
                volume,
                VolumeOperation::Read,
                &mut Some(&mut io.history_io()),
            )?;
            let writable =
                io.contains_capability(&context.original_root().grants, volume, "volume:write:")?;
            let publication_lease = if writable {
                owner.require_volume_grant(
                    volume,
                    VolumeOperation::Write,
                    &mut Some(&mut io.history_io()),
                )?;
                Some(
                    owner
                        .root_bootstrap_publication_lease_with_native_work(work)
                        .await?,
                )
            } else {
                None
            };
            let born = birth(&self.host, volume, config, operation, &io)
                .await?
                .ok_or_else(|| {
                    bounded_io::failure(
                        Some(&io),
                        crate::wire::ErrorCode::Unauthorized,
                        "Root bootstrap has no protected Filesystem birth",
                    )
                })?;
            if born.id() != initial_generation {
                return Err(bounded_io::failure(
                    Some(&io),
                    crate::wire::ErrorCode::Unauthorized,
                    "Root bootstrap differs from its actual initial generation",
                ));
            }
            let selected = io.sdk(
                born.workspace()
                    .generation(selected_generation, &io, io.cancellation())
                    .await,
            )?;
            let snapshot = if let Some((source, snapshot)) = native_import {
                if source.backing.workspace_id() != born.workspace_id()
                    || snapshot.workspace_id != born.workspace_id()
                    || snapshot.authored_generation != selected.id()
                {
                    return Err(bounded_io::failure(
                        Some(&io),
                        crate::wire::ErrorCode::Unauthorized,
                        "Root import differs from its original workspace or selected generation",
                    ));
                }
                io.finish_future(source.backing.validate(snapshot, &io)?)
                    .await?;
                Some(snapshot)
            } else {
                None
            };
            drop(selected);
            drop(born);
            let graph_root = witness.binding().original_task();
            let admission_digest = context.original_admission_digest();
            let volume = volume.copy(&mut Some(&mut io.history_io()))?;
            let generation =
                bounded_io::generation_ref(&self.host.provider, selected_generation, Some(&io))?;
            let evidence = allocation_ref(&self.host.provider, &volume, Some(&io))?.into_resource();
            let native_snapshot = snapshot
                .map(|snapshot| io.value_json(&snapshot, MAX_REQUEST_BYTES))
                .transpose()?;
            if native_snapshot.is_some() {
                io.release(std::mem::size_of::<serde_json::Value>() as u64)?;
            }
            let request = crate::resource_preparation::RootBootstrapContentRequest {
                root_operation: OperationId::from_bytes(graph_root.into_bytes()),
                graph_root,
                role,
                volume,
                evidence,
                generation,
                native_snapshot,
            };
            let retained = io.owned_bytes()?;
            let proof = FilesystemRootBootstrapAllocation {
                host: self.host.clone(),
                creator,
                admission_digest,
                request,
                operation,
                config,
                initial_generation,
                native_source: native_import.map(|(source, _)| source.clone()),
                publication_lease,
                witness,
                _custody: io.take_native_custody(retained)?,
            };
            drop(admission);
            drop(admission_custody);
            let record = io.encode(&proof.record(), MAX_REQUEST_BYTES, true)?;
            let journal_hold;
            let journal = allocation_ref(&self.host.provider, &proof.request.volume, Some(&io))?;
            journal_hold = io.guard_owned(journal.logical_owned_bytes()?)?;
            let name = std::str::from_utf8(journal.as_resource().key())
                .map_err(|_| io.invalid("Root allocation journal is not UTF-8"))?;
            let mut identity = [0_u8; 32];
            hex::encode_to_slice(graph_root.into_bytes(), &mut identity)
                .map_err(|_| io.invalid("Root allocation identity encoding failed"))?;
            let suffix = std::str::from_utf8(&identity).expect("hex is ASCII");
            let creation = IdempotencyKey::new(crate::contract::preparation_format(
                format_args!("root-bootstrap-journal:{suffix}"),
                Some(&io),
            )?)?;
            let lifecycle = if self.host.filesystem.capabilities().durable {
                acyclic_fs::Lifecycle::Durable
            } else {
                acyclic_fs::Lifecycle::Ephemeral
            };
            let operation = acyclic_fs::OperationId::from_bytes(
                bounded_io::operation_key(&creation, Some(&io))?.into_bytes(),
            );
            self.host
                .filesystem
                .create_workspace_with_operation_with_native_work(
                    name,
                    acyclic_fs::VolumeConfig::portable(lifecycle),
                    operation,
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
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
                    return Err(bounded_io::failure(
                        Some(&io),
                        crate::wire::ErrorCode::Unauthorized,
                        "child allocation cannot be claimed as original Root bootstrap",
                    ));
                }
                Err(error) if journal_absent(&error) => {}
                Err(error) => return Err(error),
            }
            let prior = match bounded_io::read(
                &self.host,
                &journal,
                "/root-bootstrap.json",
                MAX_REQUEST_BYTES,
                Some(&io),
            )
            .await
            {
                Ok(bytes) => Some(bytes),
                Err(error) if journal_absent(&error) => None,
                Err(error) => return Err(error),
            };
            if let Some(prior) = prior {
                if prior != record {
                    return Err(bounded_io::failure(
                        Some(&io),
                        crate::wire::ErrorCode::Conflict,
                        "original Root bootstrap association changed",
                    ));
                }
            } else {
                let observed = bounded_io::resolve(&self.host, &journal, Some(&io)).await?;
                let key = IdempotencyKey::new(crate::contract::preparation_format(
                    format_args!("root-bootstrap-claim:{suffix}"),
                    Some(&io),
                )?)?;
                let path = bounded_io::clone_text("/root-bootstrap.json", Some(&io))?;
                match bounded_io::apply_generation(
                    &self.host,
                    &journal,
                    Some(&observed.generation),
                    &[WorkspaceMutation::PutFile {
                        path,
                        bytes: record.clone(),
                    }],
                    &key,
                    &io,
                )
                .await
                {
                    Ok(_) => {}
                    Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {}
                    Err(error) => return Err(error),
                }
                if bounded_io::read(
                    &self.host,
                    &journal,
                    "/root-bootstrap.json",
                    MAX_REQUEST_BYTES,
                    Some(&io),
                )
                .await?
                    != record
                {
                    return Err(bounded_io::failure(
                        Some(&io),
                        crate::wire::ErrorCode::Conflict,
                        "protected Root bootstrap association was not published exactly",
                    ));
                }
            }
            drop(journal);
            drop(journal_hold);
            Ok(proof)
        })
    }
}

impl<A, O> FilesystemRootBootstrapAllocation<A, O> {
    pub(crate) fn owner(&self) -> &Authority {
        self.creator.audience()
    }
    pub(super) fn snapshot(&self) -> Option<acyclic_fs::LazySnapshotRef> {
        self.request.native_snapshot.as_ref().map(|value| {
            acyclic_fs::LazySnapshotRef::deserialize(value)
                .expect("snapshot came from fixed SDK data")
        })
    }
    /// Borrows the request protected by the original allocation journal.
    pub fn request(&self) -> &crate::resource_preparation::RootBootstrapContentRequest {
        &self.request
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool {
        self._custody.matches_work(work)
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
                self.request.role,
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
                &self.request.native_snapshot,
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
        let exact = bytes == record;
        drop((bytes, record));
        drop((bytes_hold, record_hold));
        drop(journal);
        drop(journal_hold);
        if !exact {
            return Err(bounded_io::failure(
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
            return Err(bounded_io::failure(
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
        let reject = |reason| bounded_io::failure(io, crate::wire::ErrorCode::Unauthorized, reason);
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
            bounded_io::failure(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "Root lazy content has no original publication authority",
            )
        })?;
        self.source_generation(None, Some(io))?
            .ok_or_else(|| io.invalid("eager Root does not select a lazy publication candidate"))?;
        io.verify_publication_lease(lease.as_ref()).await?;
        self.reauthenticate_with_io(io).await?;
        let source = self
            .native_source
            .as_ref()
            .expect("authenticated original source");
        let snapshot = self.snapshot().expect("authenticated original snapshot");
        let operation = bounded_io::operation_key(key, Some(io))?;
        io.finish_future(
            source
                .backing
                .publish(snapshot, mutations, operation, permit, io, lease)?,
        )
        .await
    }
}
