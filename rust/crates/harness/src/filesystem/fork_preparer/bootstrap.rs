//! The protected original Root birth and lazy publication basis.
use super::*;
use crate::conversation::FileRef;
use crate::resources::GenerationRef;

/// Original Root content bound to its protected allocation journal.
pub struct FilesystemRootBootstrapAllocation<A, O> {
    pub(crate) host: Arc<FilesystemHost<A, O>>,
    pub(super) creator: AuthorityVerifier,
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
impl<A, O> FilesystemRootBootstrapAllocation<A, O> {
    pub(crate) fn owner(&self) -> &Authority {
        self.creator.audience()
    }
    /// Borrows the original selected content request.
    pub fn request(&self) -> &crate::resource_preparation::RootBootstrapContentRequest {
        &self.request
    }
    pub(super) fn snapshot(&self) -> Option<acyclic_fs::LazySnapshotRef> {
        self.request.native_snapshot.as_ref().map(|value| {
            acyclic_fs::LazySnapshotRef::deserialize(value).expect("SDK-produced snapshot")
        })
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
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..Default::default()
        })?;
        Ok(Arc::new(self))
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn record(&self) -> impl Serialize + '_ {
        let binding = self.witness.binding();
        let selected = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
            self.request
                .generation
                .as_resource()
                .key()
                .try_into()
                .expect("protected generation digest"),
        ));
        (
            (
                self.owner(),
                binding.audience(),
                binding.original_task(),
                self.admission_digest,
                binding.admission_digest(),
            ),
            (
                self.request.role,
                binding.agent(),
                binding.bind_operation(),
                binding.intent_digest(),
                binding.fence(),
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
        let journal = allocation_ref(&self.host.provider, &self.request.volume, Some(io))?;
        let journal_hold = io.guard_owned(journal.logical_owned_bytes()?)?;
        let workspace = (self
            .host
            .filesystem
            .open_workspace(
                std::str::from_utf8(journal.as_resource().key())
                    .map_err(|_| io.invalid("Root journal key is not UTF-8"))?,
                io,
                io.cancellation(),
            )
            .await)
            .and_then(|result| result.map_err(crate::Error::from))?;
        let actual = (workspace
            .read(
                "/root-bootstrap.json",
                MAX_REQUEST_BYTES,
                io,
                io.cancellation(),
            )
            .await)
            .and_then(|result| result.map_err(crate::Error::from))?;
        if io.original_native_work().is_none() {
            io.retain_owned(actual.retained_capacity().unwrap_or(0) as u64)?;
        }
        let actual_hold = io.guard_bytes(&actual)?;
        let expected = io.encode(&self.record(), MAX_REQUEST_BYTES, true)?;
        let expected_hold = io.guard_bytes(&expected)?;
        let exact = actual == expected;
        drop((actual, expected));
        drop((actual_hold, expected_hold));
        drop(journal);
        drop(journal_hold);
        if !exact {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "protected original Root bootstrap association changed",
            ));
        }
        let name = io.capability(&self.request.volume, "harness-")?;
        let held = io.guard_owned(name.capacity() as u64)?;
        let birth = (self
            .host
            .filesystem
            .workspace_for_creation_operation(
                &name,
                self.config,
                self.operation,
                io,
                io.cancellation(),
            )
            .await)
            .and_then(|result| result.map_err(crate::Error::from))?;
        drop(name);
        drop(held);
        if birth
            .as_ref()
            .is_none_or(|birth| birth.id() != self.initial_generation)
        {
            return Err(crate::executor::stock_boundary(
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
        let reject = |reason| {
            crate::executor::stock_boundary(io, crate::wire::ErrorCode::Unauthorized, reason)
        };
        let snapshot = self
            .snapshot()
            .ok_or_else(|| reject("Root lazy content has no original frozen snapshot"))?;
        if snapshot.workspace_id != source.backing.workspace_id()
            || self.request.generation.as_resource().key()
                != snapshot.authored_generation.digest().as_bytes()
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
                io.charge(acyclic_fs::WorkCounters {
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
        let ordinary = PreparationIo::for_unmeasured(self._custody.work().cancellation())?;
        let io = io.unwrap_or(&ordinary);
        let lease = self.publication_lease.as_ref().ok_or_else(|| {
            crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "Root lazy content has no original publication authority",
            )
        })?;
        self.source_generation(None, Some(io))?
            .ok_or_else(|| io.invalid("eager Root has no lazy publication basis"))?;
        lease.verify(Some(io))?.await?;
        self.reauthenticate_with_io(io).await?;
        self.native_source
            .as_ref()
            .expect("authenticated lazy source")
            .backing
            .publish(
                self.snapshot().expect("authenticated snapshot"),
                mutations,
                (|| -> crate::Result<_> {
                    let key = key;
                    let io: Option<&crate::filesystem::PreparationIo> = Some(io);

                    let work = acyclic_fs::WorkCounters {
                        bytes_hashed: key.as_str().len() as u64,
                        ..Default::default()
                    };
                    if let Some(io) = io {
                        io.preflight(work)?;
                    }
                    let key = crate::filesystem::filesystem_key(key);
                    if let Some(io) = io {
                        io.charge(work)?;
                    }
                    Ok::<_, crate::Error>(key)
                })()?,
                permit,
                io,
                lease,
            )?
            .await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemResourceCreationProvider<A, O>
{
    /// Protects the genuine pre-admission birth under its original Root owner.
    pub fn protect_root_bootstrap_with_native_work<'a, P: acyclic_stream::StreamProvider>(
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
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let deny = |reason| {
                crate::executor::stock_boundary(
                    Some(&io),
                    crate::wire::ErrorCode::Unauthorized,
                    reason,
                )
            };
            volume.validate(&mut Some(&mut io.history_io()))?;
            let creator = self.issuer.verifier(&mut Some(&mut io.history_io()))?;
            owner
                .authenticate_root_bootstrap_creator_with_native_work(&creator, work)
                .await?;
            let witness = owner
                .root_bootstrap_binding_witness_with_native_work(work)
                .await?;
            let (admission, admission_hold) =
                owner.original_root_admission_with_native_work(work).await?;
            let context = crate::resource_preparation::RootBootstrapContentContext::from_verified_root_with_native_work(
                owner, &admission, witness.binding(), work).await?;
            let valid_role = match role {
                crate::resource_preparation::RootBootstrapContentRole::Project => {
                    volume.class() == crate::conversation::VolumeClass::Project
                }
                crate::resource_preparation::RootBootstrapContentRole::Scratch => {
                    volume.class() == crate::conversation::VolumeClass::AgentPrivate
                        && volume.owner()
                            == &crate::conversation::VolumeOwner::Agent(context.agent())
                }
            };
            if owner.root_bootstrap_owner() != self.issuer.audience()
                || volume.provider() != &self.host.provider
                || !valid_role
            {
                return Err(deny(
                    "Root bootstrap differs from its configured creator or namespace",
                ));
            }
            owner.require_volume_grant(
                volume,
                VolumeOperation::Read,
                &mut Some(&mut io.history_io()),
            )?;
            let publication_lease =
                if io.contains_capability(&admission.grants, volume, "volume:write:")? {
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
            let name = io.capability(volume, "harness-")?;
            let name_hold = io.guard_owned(name.capacity() as u64)?;
            let birth = (self
                .host
                .filesystem
                .workspace_for_creation_operation(&name, config, operation, &io, io.cancellation())
                .await)
                .and_then(|result| result.map_err(crate::Error::from))?
                .ok_or_else(|| deny("Root has no protected Filesystem birth"))?;
            if birth.id() != initial_generation {
                return Err(deny(
                    "Root initial generation differs from its actual birth",
                ));
            }
            let selected = (birth
                .workspace()
                .generation(selected_generation, &io, io.cancellation())
                .await)
                .and_then(|result| result.map_err(crate::Error::from))?;
            let native_snapshot = if let Some((source, snapshot)) = native_import {
                if !source.backing.matches_native_work(work)
                    || source.backing.workspace_id() != birth.workspace_id()
                    || snapshot.workspace_id != birth.workspace_id()
                    || snapshot.authored_generation != selected.id()
                {
                    return Err(deny(
                        "Root import differs from its original workspace, generation or ledger",
                    ));
                }
                source.backing.validate(snapshot, &io)?.await?;
                Some(io.value_json(&snapshot, MAX_REQUEST_BYTES)?)
            } else {
                None
            };
            drop(selected);
            drop(birth);
            drop(name);
            drop(name_hold);
            let graph_root = witness.binding().original_task();
            let (generation, custody) = GenerationRef::new(
                &self.host.provider,
                selected_generation.digest().as_bytes(),
                None,
                Some(work),
            )?;
            io.absorb_native_custody(custody.expect("funded reference"))?;
            io.retain_owned(std::mem::size_of::<FilesystemRootBootstrapAllocation<A, O>>() as u64)?;
            let request = crate::resource_preparation::RootBootstrapContentRequest {
                root_operation: admission.operation_id,
                graph_root,
                role,
                volume: volume.copy(&mut Some(&mut io.history_io()))?,
                evidence: allocation_ref(&self.host.provider, volume, Some(&io))?.into_resource(),
                generation,
                native_snapshot,
            };
            if request.native_snapshot.is_some() {
                io.release(std::mem::size_of::<serde_json::Value>() as u64)?;
            }
            let allocation = FilesystemRootBootstrapAllocation {
                host: self.host.clone(),
                creator,
                admission_digest: context.original_admission_digest(),
                request,
                operation,
                config,
                initial_generation,
                native_source: native_import.map(|(source, _)| source.clone()),
                publication_lease,
                witness,
                _custody: io.take_native_custody(io.owned_bytes()?)?,
            };
            drop(admission);
            drop(admission_hold);
            let record = io.encode(&allocation.record(), MAX_REQUEST_BYTES, true)?;
            let journal = allocation_ref(&self.host.provider, volume, Some(&io))?;
            let journal_hold = io.guard_owned(journal.logical_owned_bytes()?)?;
            let name = std::str::from_utf8(journal.as_resource().key())
                .map_err(|_| io.invalid("Root journal key is not UTF-8"))?;
            let creation =
                IdempotencyKey::new(crate::executor::with_history_io(Some(&io), |work| {
                    crate::contract::preparation_format(
                        format_args!("root-bootstrap-journal:{}", graph_root),
                        work,
                    )
                })?)?;
            let lifecycle = if self.host.filesystem.capabilities().durable {
                acyclic_fs::Lifecycle::Durable
            } else {
                acyclic_fs::Lifecycle::Ephemeral
            };
            let operation = crate::filesystem::filesystem_key(&creation).into_bytes();
            io.charge(acyclic_fs::WorkCounters {
                bytes_hashed: creation.as_str().len() as u64,
                ..Default::default()
            })?;
            let initial = (self
                .host
                .filesystem
                .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                    name,
                    acyclic_fs::VolumeConfig::portable(lifecycle),
                    acyclic_fs::OperationId::from_bytes(operation),
                    &io,
                    io.cancellation(),
                    None,
                )
                .await)
                .and_then(|result| result.map_err(crate::Error::from))?;
            let workspace = initial.workspace();
            drop(initial);
            match (workspace
                .read(
                    "/root-bootstrap.json",
                    MAX_REQUEST_BYTES,
                    &io,
                    io.cancellation(),
                )
                .await)
                .and_then(|result| result.map_err(crate::Error::from))
            {
                Ok(prior) if prior == record => {}
                Ok(_) => return Err(deny("original Root bootstrap association changed")),
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => {
                    let key = IdempotencyKey::new(crate::executor::with_history_io(
                        Some(&io),
                        |work| {
                            crate::contract::preparation_format(
                                format_args!("root-bootstrap-claim:{}", graph_root),
                                work,
                            )
                        },
                    )?)?;
                    io.charge(acyclic_fs::WorkCounters {
                        bytes_hashed: key.as_str().len() as u64,
                        ..Default::default()
                    })?;
                    let head = (workspace.head(&io, io.cancellation()).await)
                        .and_then(|result| result.map_err(crate::Error::from))?;
                    let mut transaction = (workspace
                        .begin_transaction_at(
                            &head,
                            crate::filesystem::filesystem_key(&key),
                            &io,
                            io.cancellation(),
                        )
                        .await)
                        .and_then(|result| result.map_err(crate::Error::from))?;
                    (transaction
                        .write(
                            "/root-bootstrap.json",
                            record.clone(),
                            &io,
                            io.cancellation(),
                        )
                        .await)
                        .and_then(|result| result.map_err(crate::Error::from))?;
                    let committed = transaction
                        .commit_with_native_work(
                            None,
                            acyclic_fs::PublicationPermit::Unrestricted,
                            work,
                            io.cancellation(),
                            &mut |_| std::future::ready(Ok::<(), Error>(())),
                        )
                        .await?;
                    match committed {
                        acyclic_fs::TransactionCommit::Committed(_)
                        | acyclic_fs::TransactionCommit::AlreadyCommitted(_)
                        | acyclic_fs::TransactionCommit::Conflict { .. } => {}
                        _ => return Err(deny("Root bootstrap publication was rejected")),
                    }
                    if (workspace
                        .read(
                            "/root-bootstrap.json",
                            MAX_REQUEST_BYTES,
                            &io,
                            io.cancellation(),
                        )
                        .await)
                        .and_then(|result| result.map_err(crate::Error::from))?
                        != record
                    {
                        return Err(deny(
                            "protected Root bootstrap association was not published exactly",
                        ));
                    }
                }
                Err(error) => return Err(error),
            }
            drop(journal);
            drop(journal_hold);
            Ok(allocation)
        })
    }
}
