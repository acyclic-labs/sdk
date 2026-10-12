use super::*;

impl<P: StreamProvider> TaskJournalOwner<P> {
    #[allow(
        clippy::too_many_arguments,
        reason = "one engine validates both original acquisition decisions"
    )]
    /// Reserves or observes the original qualified publication gate.
    pub async fn publication<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
        &self,
        workspace: &acyclic_fs::Workspace<A, O>,
        volume: &VolumeRef,
        original_generation: acyclic_fs::GenerationId,
        original_operation: acyclic_fs::OperationId,
        work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
        recover: bool,
    ) -> Result<Option<(PublicationPermit, NativePublicationCleanup<A, O>)>> {
        let failure_slot = reserve_box::<NativePublicationReservationFailure<A, O>>(work)?;
        let workspace = workspace.clone();
        let mut reservation = None;
        let mut cleanup_binding = None;
        let result = match crate::NativeFutureBundle::new(work, || async {
            self.verify(
                false,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )
            .await?;
            self.require_volume_grant(
                volume,
                VolumeOperation::Write,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )?;
            cleanup_binding = Some((
                self.task_binding().0,
                native_publication_fence_digest(self, work)?,
                allocate(work, |io| volume.copy(&mut Some(&mut io.history_io())))?,
            ));
            {
                let (encoded, _encoded_custody) = encode_record(volume, work)?;
                work.charge(acyclic_fs::WorkCounters {
                    bytes_hashed: encoded.len() as u64,
                    ..acyclic_fs::WorkCounters::default()
                })?;
                let (name, _name_custody) =
                    work.display(&format_args!("harness-{}", blake3::hash(&encoded).to_hex(),))?;
                if workspace.name().as_str() != name {
                    return Err(boundary(
                        crate::wire::ErrorCode::Invalid,
                        "native reservation workspace differs from original volume",
                    ));
                }
            }
            cancellation
                .check()
                .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            let acquired = if recover {
                workspace
                    .publication_reservation_with_native_work(
                        original_operation,
                        work,
                        cancellation,
                    )
                    .await
            } else {
                workspace
                    .reserve_publication(original_operation, work, cancellation)
                    .await
                    .map(Some)
                    .map_err(|error| (error, None))
            };
            reservation = acquired.map_err(|(cause, custody)| Error::NativeWorkspace {
                source: cause,
                custody,
            })?;
            self.verify(
                false,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )
            .await?;
            self.require_volume_grant(
                volume,
                VolumeOperation::Write,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )?;
            if reservation.is_none() {
                return Ok(());
            }
            let head = workspace.head(work, cancellation).await.map_err(|source| {
                Error::NativeWorkspace {
                    source,
                    custody: None,
                }
            })?;
            if head.id() != original_generation {
                return Err(boundary(
                    crate::wire::ErrorCode::Invalid,
                    "native reservation generation differs from original selection",
                ));
            }
            drop(head);
            self.verify(
                false,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )
            .await?;
            self.require_volume_grant(
                volume,
                VolumeOperation::Write,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )?;
            cancellation
                .check()
                .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            work.remaining()?;
            Ok::<(), Error>(())
        }) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        let error = match result {
            Ok(()) => {
                return Ok(reservation.map(|reservation| {
                    let permit = PublicationPermit::Reservation {
                        operation_id: reservation.operation_id.into_bytes(),
                        gate_tail: reservation.gate_tail,
                        expected: reservation.expected,
                    };
                    let (task, fence, volume) =
                        cleanup_binding.expect("reservation follows original binding");
                    (
                        permit,
                        NativePublicationCleanup {
                            workspace,
                            reservation,
                            task,
                            fence,
                            volume,
                            failure_slot: Some(failure_slot),
                        },
                    )
                }));
            }
            Err(error) => error,
        };
        let failure = if let Some(reservation) = reservation {
            let (task, fence, volume) =
                cleanup_binding.expect("reservation follows original binding");
            let cleanup = NativePublicationCleanup {
                workspace,
                reservation,
                task,
                fence,
                volume,
                failure_slot: None,
            };
            if recover {
                NativePublicationReservationFailure {
                    error,
                    cleanup_error: None,
                    work: work.receipt(),
                    cleanup: Some(cleanup),
                    unwitnessed_workspace: None,
                }
            } else {
                let (result, released) =
                    cleanup.release_gate(self, work, cleanup_cancellation).await;
                NativePublicationReservationFailure {
                    error,
                    cleanup_error: result.err(),
                    work: work.receipt(),
                    cleanup: if released { None } else { Some(cleanup) },
                    unwitnessed_workspace: None,
                }
            }
        } else {
            NativePublicationReservationFailure {
                error,
                cleanup_error: None,
                work: work.receipt(),
                cleanup: None,
                unwitnessed_workspace: Some(workspace),
            }
        };
        Err(Error::NativeReservation {
            code: failure.error.code(),
            source: Box::write(failure_slot.0, failure),
            custody: Some(failure_slot.1),
        })
    }
}

/// Only settlement of an already acquired real SDK reservation is permitted.
pub struct NativePublicationCleanup<A, O> {
    pub(super) workspace: acyclic_fs::Workspace<A, O>,
    pub(super) reservation: acyclic_fs::PublicationReservation,
    pub(super) task: crate::TaskId,
    pub(super) fence: [u8; 32],
    pub(super) volume: (VolumeRef, acyclic_fs::NativeAllocation),
    pub(super) failure_slot: Option<(
        Box<std::mem::MaybeUninit<NativePublicationReservationFailure<A, O>>>,
        acyclic_fs::NativeAllocation,
    )>,
}

pub(super) fn native_publication_fence_digest<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    work: &NativeMaterializationWork,
) -> Result<[u8; 32]> {
    let encoded = encode_record(owner.task_binding().1, work)?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: encoded.0.len() as u64,
        ..Default::default()
    })?;
    Ok(*blake3::hash(&encoded.0).as_bytes())
}

/// Physical settlement uncertainty retains the exact cleanup capability.
pub struct NativePublicationReservationFailure<A, O> {
    pub error: Error,
    /// Additional actual release failure; `error` remains the factory's cause.
    pub cleanup_error: Option<Error>,
    pub work: acyclic_fs::WorkCounters,
    pub cleanup: Option<NativePublicationCleanup<A, O>>,
    /// Original SDK workspace retained without any witnessed gate proof.
    pub unwitnessed_workspace: Option<acyclic_fs::Workspace<A, O>>,
}

impl<A, O> std::fmt::Debug for NativePublicationReservationFailure<A, O> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativePublicationReservationFailure")
            .field("error", &self.error)
            .field("cleanup_error", &self.cleanup_error)
            .field("work", &self.work)
            .field("cleanup", &self.cleanup.is_some())
            .field(
                "unwitnessed_workspace",
                &self.unwitnessed_workspace.is_some(),
            )
            .finish()
    }
}

impl<A, O> std::fmt::Display for NativePublicationReservationFailure<A, O> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, formatter)
    }
}

impl<A: 'static, O: 'static> std::error::Error for NativePublicationReservationFailure<A, O> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

pub(in crate::filesystem) fn reserve_box<T>(
    work: &NativeMaterializationWork,
) -> Result<(Box<std::mem::MaybeUninit<T>>, acyclic_fs::NativeAllocation)> {
    let custody = work.claim(
        std::mem::size_of::<T>() as u64,
        u64::from((std::mem::size_of::<T>() as u64) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    Ok((Box::new_uninit(), custody))
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    NativePublicationReservationFailure<A, O>
{
    /// Borrows the actual retained failure without extracting its owner or custody.
    pub fn from_error(error: &Error) -> Option<&Self> {
        let Error::NativeReservation { source, .. } = error else {
            return None;
        };
        source.downcast_ref()
    }

    /// Retries only the retained actual gate while keeping the original Root error.
    pub async fn retry_cleanup_with_native_work<P: StreamProvider>(
        error: &mut Error,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<bool> {
        let Error::NativeReservation {
            source, custody, ..
        } = error
        else {
            return Ok(false);
        };
        if !custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_allocation_context(work))
        {
            return Err(boundary(
                crate::wire::ErrorCode::Invalid,
                "native cleanup differs from the original operation and resource",
            ));
        }
        let Some(failure) = source.downcast_mut::<Self>() else {
            return Ok(false);
        };
        let Some(cleanup) = failure.cleanup.as_ref() else {
            return Ok(false);
        };
        let (result, released) = cleanup.release_gate(owner, work, cancellation).await;
        if released {
            failure.cleanup = None;
        }
        result?;
        Ok(true)
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    NativePublicationCleanup<A, O>
{
    pub(super) fn verify_binding<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if owner.task_binding().0 != self.task
            || native_publication_fence_digest(owner, work)? != self.fence
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "native publication cleanup differs from the original task and fence",
            ));
        }
        owner.require_volume_grant(
            &self.volume.0,
            VolumeOperation::Write,
            &mut Some(&mut crate::store::history::native_work_io(work)?),
        )
    }

    /// Releases the actual gate without admitting a new failure carrier.
    pub async fn release<P: StreamProvider>(
        mut self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<()> {
        let slot = self
            .failure_slot
            .take()
            .expect("successful reservation retains its prepaid failure slot");
        let result = if !slot.1.work().shares_allocation_context(work) {
            Err(NativePublicationReservationFailure {
                error: boundary(
                    crate::wire::ErrorCode::Invalid,
                    "native cleanup differs from the original operation and resource",
                ),
                cleanup_error: None,
                work: work.receipt(),
                cleanup: Some(self),
                unwitnessed_workspace: None,
            })
        } else {
            let (result, released) = self.release_gate(owner, work, cancellation).await;
            result.map_err(|error| NativePublicationReservationFailure {
                error,
                cleanup_error: None,
                work: work.receipt(),
                cleanup: if released { None } else { Some(self) },
                unwitnessed_workspace: None,
            })
        };
        result.map_err(|failure| Error::NativeReservation {
            code: failure.error.code(),
            source: Box::write(slot.0, failure),
            custody: Some(slot.1),
        })
    }

    pub(super) async fn release_gate<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> (Result<()>, bool) {
        let mut released = false;
        let result = match crate::NativeFutureBundle::new(work, || async {
            self.verify_binding(owner, work)?;
            owner
                .verify(
                    true,
                    &mut Some(&mut crate::store::history::native_work_io(work)?),
                )
                .await?;
            self.workspace
                .release_publication(self.reservation, work, cancellation)
                .await
                .map_err(|source| Error::NativeWorkspace {
                    source,
                    custody: None,
                })?;
            released = true;
            owner
                .verify(
                    true,
                    &mut Some(&mut crate::store::history::native_work_io(work)?),
                )
                .await?;
            self.verify_binding(owner, work)
        }) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        (result, released)
    }
}

/// Serialized diagnostic data; the runtime failure retains its typed causes and custody separately.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationFailure {
    /// Exact admitted mapping whose integrity/publication check failed.
    pub destination: Option<NativeVolumeMapping>,
    /// Destinations whose existing sync returned success before this failure.
    pub completed: Vec<NativeVolumeMapping>,
    /// Exact filesystem capture/publication work across mappings.
    pub work: acyclic_fs::WorkCounters,
    /// Original provider/fence/conflict diagnostic, not an inferred OS outcome.
    pub message: String,
}

/// An actual publication failure before any fallible diagnostic rendering.
#[derive(Debug)]
pub struct NativeViewPublicationError {
    pub cause: Error,
    /// Actual reconciliation failure retained beside the original publication cause.
    pub accounting_failure: Option<Error>,
    /// Actual cumulative original work at the publication boundary.
    pub work: acyclic_fs::WorkCounters,
    pub destination: Option<NativeVolumeMapping>,
    pub completed: Vec<NativeVolumeMapping>,
    /// SDK-qualified final snapshots, including unchanged results and acknowledged prefixes.
    pub published: Vec<crate::filesystem::native_process::NativeProcessPublishedVolume>,
    /// Retained result already staged before a later destination failed.
    pub diagnostic: Option<crate::conversation::FileRef>,
    /// Absent only when admission failed before SDK effects and no custody was issued.
    pub custody: Option<acyclic_fs::NativeAllocation>,
}

impl std::fmt::Display for NativeViewPublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for NativeViewPublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// Actual successful publications remain actionable through caller postchecks.
pub struct NativeViewPublicationReceipt {
    /// Actual cumulative original work at the publication boundary.
    pub work: acyclic_fs::WorkCounters,
    pub completed: Vec<NativeVolumeMapping>,
    /// These immutable results are data, never permission to inspect or mutate a project.
    pub published: Vec<crate::filesystem::native_process::NativeProcessPublishedVolume>,
    pub custody: acyclic_fs::NativeAllocation,
}

impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn publish_original<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
        custody: Option<acyclic_fs::NativeAllocation>,
    ) -> std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError> {
        let mut custody = custody.expect("original publication has its funded custody");
        let mut completed = Vec::new();
        let mut published = Vec::new();
        let mut mappings = Vec::new();
        let mut mapping_storage = None;
        let mut failed_destination = None;
        let mut accounting_failure = None;
        let result: Result<()> = async {
            let (output, mut output_custody) = allocate(work, |io| {
                crate::filesystem::fork_preparer_io::vector(
                    self.preparation.manifest.volumes.len(),
                    Some(io),
                )
            })?;
            custody.absorb(&mut output_custody)?;
            completed = output;
            let (proofs, mut proofs_custody) = allocate(work, |io| {
                crate::filesystem::fork_preparer_io::vector(
                    self.preparation.manifest.volumes.len(),
                    Some(io),
                )
            })?;
            custody.absorb(&mut proofs_custody)?;
            published = proofs;
            let (copies, copies_custody) = allocate(work, |io| {
                crate::filesystem::fork_preparer_io::vector(
                    self.preparation.manifest.volumes.len(),
                    Some(io),
                )
            })?;
            mapping_storage = Some(copies_custody);
            mappings = copies;
            for mapping in &self.preparation.manifest.volumes {
                let (mapping, allocation) = copy_native_mapping_with_work(mapping, work)?;
                mappings.push(Some((mapping, None, allocation)));
            }
            for (index, (volume, mapping)) in self
                .volumes
                .iter()
                .zip(&self.preparation.manifest.volumes)
                .enumerate()
            {
                failed_destination = Some(index);
                self.work()
                    .cancellation()
                    .check()
                    .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
                owner
                    .verify(
                        false,
                        &mut Some(&mut crate::store::history::native_work_io(work)?),
                    )
                    .await?;
                owner.require_volume_grant(
                    &mapping.volume,
                    VolumeOperation::Read,
                    &mut Some(&mut crate::store::history::native_work_io(work)?),
                )?;
                if !mapping.writable {
                    let snapshot = match &volume.view {
                        NativePreparedVolumeBackend::Materialized { view, .. } => {
                            view.verify_unchanged(&volume._work).await?
                        }
                        NativePreparedVolumeBackend::HeldLazy(lazy) => {
                            let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace
                            else {
                                return Err(Error::from(
                                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                                ));
                            };
                            let qualified =
                                match lazy.publication_snapshot(lazy.original_work())?.await? {
                                    Ok(qualified) => qualified,
                                    Err(failure) => {
                                        mappings[index]
                                            .as_mut()
                                            .expect("original readonly destination")
                                            .1 = failure.publication;
                                        accounting_failure =
                                            failure.accounting_failure.map(Error::from);
                                        return Err(failure.cause);
                                    }
                                };
                            let actual = qualified
                                .lazy_snapshot(&mut acyclic_fs::kernel::KernelAccounting::native(
                                    lazy.original_work(),
                                    lazy.original_work().cancellation(),
                                ))?
                                .ok_or_else(|| {
                                    Error::from(
                                        acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                                    )
                                })?;
                            if actual.workspace_id != snapshot.workspace_id
                                || actual.source != snapshot.source
                                || actual.authored_generation != snapshot.authored_generation
                            {
                                return Err(Error::from(
                                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                                ));
                            }
                            qualified
                        }
                    };
                    mappings[index]
                        .as_mut()
                        .expect("original readonly destination")
                        .1 = Some(snapshot);
                    owner
                        .verify(
                            false,
                            &mut Some(&mut crate::store::history::native_work_io(work)?),
                        )
                        .await?;
                    owner.require_volume_grant(
                        &mapping.volume,
                        VolumeOperation::Read,
                        &mut Some(&mut crate::store::history::native_work_io(work)?),
                    )?;
                }
            }
            for (index, (volume, mapping)) in self
                .volumes
                .iter()
                .zip(&self.preparation.manifest.volumes)
                .enumerate()
            {
                if !mapping.writable {
                    let snapshot = mappings[index]
                        .as_mut()
                        .expect("original readonly destination")
                        .1
                        .take()
                        .expect("verified original readonly SDK cut");
                    published.push(
                        crate::filesystem::native_process::NativeProcessPublishedVolume {
                            index,
                            snapshot,
                        },
                    );
                    continue;
                }
                failed_destination = Some(index);
                let result = match &volume.view {
                    NativePreparedVolumeBackend::Materialized { view, callback } => {
                        let callback = Arc::clone(callback);
                        view.sync_with_permit(
                            volume.publication,
                            &volume._work,
                            move |actual: &NativeMaterializationWork| {
                                let callback = Arc::clone(&callback);
                                let actual = actual.clone();
                                async move { callback.check(&actual)?.await }
                            },
                        )
                        .await
                    }
                    NativePreparedVolumeBackend::HeldLazy(lazy) => {
                        owner
                            .verify(
                                false,
                                &mut Some(&mut crate::store::history::native_work_io(work)?),
                            )
                            .await?;
                        owner.require_volume_grant(
                            &mapping.volume,
                            VolumeOperation::Read,
                            &mut Some(&mut crate::store::history::native_work_io(work)?),
                        )?;
                        owner.require_volume_grant(
                            &mapping.volume,
                            VolumeOperation::Write,
                            &mut Some(&mut crate::store::history::native_work_io(work)?),
                        )?;
                        lazy.sync(volume.publication, lazy.original_work())?.await?
                    }
                };
                let snapshot = match result {
                    Ok(snapshot) => snapshot,
                    Err(failure) => {
                        if let Some(snapshot) = failure.publication {
                            published.push(
                                crate::filesystem::native_process::NativeProcessPublishedVolume {
                                    index,
                                    snapshot,
                                },
                            );
                        }
                        if failure.source_acknowledged {
                            let (mapping, _, mut allocation) = mappings[index]
                                .take()
                                .expect("original preadmitted acknowledged destination");
                            completed.push(mapping);
                            custody
                                .absorb(&mut allocation)
                                .expect("same original publication ledger and prepaid mapping");
                            failed_destination = None;
                        }
                        accounting_failure = failure.accounting_failure.map(Error::from);
                        return Err(failure.cause);
                    }
                };
                published.push(
                    crate::filesystem::native_process::NativeProcessPublishedVolume {
                        index,
                        snapshot,
                    },
                );
                let (mapping, _, mut allocation) = mappings[index]
                    .take()
                    .expect("original preadmitted destination");
                completed.push(mapping);
                custody.absorb(&mut allocation)?;
                work.remaining()?;
            }
            failed_destination = None;
            owner
                .verify(
                    false,
                    &mut Some(&mut crate::store::history::native_work_io(work)?),
                )
                .await?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                drop(mappings);
                drop(mapping_storage);
                Ok(NativeViewPublicationReceipt {
                    work: work.receipt(),
                    completed,
                    published,
                    custody,
                })
            }
            Err(cause) => {
                for (index, payload) in mappings.iter_mut().enumerate() {
                    if let Some(snapshot) = payload.as_mut().and_then(|payload| payload.1.take()) {
                        published.push(
                            crate::filesystem::native_process::NativeProcessPublishedVolume {
                                index,
                                snapshot,
                            },
                        );
                    }
                }
                let destination = failed_destination
                    .and_then(|index| mappings.get_mut(index))
                    .and_then(Option::take)
                    .map(|payload| {
                        let (mapping, _, mut allocation) = payload;
                        custody
                            .absorb(&mut allocation)
                            .expect("same original ledger and preadmitted total");
                        mapping
                    });
                drop(mappings);
                drop(mapping_storage);
                Err(NativeViewPublicationError {
                    cause,
                    accounting_failure,
                    work: work.receipt(),
                    destination,
                    completed,
                    published,
                    diagnostic: None,
                    custody: Some(custody),
                })
            }
        }
    }
}

pub(super) fn verify_binding<P: StreamProvider, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    owner: &TaskJournalOwner<P>,
    binding: &BorrowedNativeVolumeBinding<'_, A, O>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        if operation == VolumeOperation::Write && !binding.writable {
            continue;
        }
        owner.require_volume_grant(
            binding.volume,
            operation,
            &mut Some(&mut crate::store::history::native_work_io(work)?),
        )?;
        ContentGrant::authorize_volume(
            binding.verifier,
            binding.scope,
            binding.volume,
            operation,
            work.remaining()?.bytes_encoded,
            &mut Some(&mut crate::store::history::native_work_io(work)?),
        )?;
    }
    if binding.volume.provider() != &binding.host.provider
        || binding.generation.as_resource().provider() != &binding.host.provider
    {
        return Err(boundary(
            crate::wire::ErrorCode::Invalid,
            "resource belongs to another Filesystem provider",
        ));
    }
    Ok(())
}
