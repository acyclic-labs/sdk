use super::*;


/// Host-only selection. Credentials and publication permits never enter a command.
pub struct BorrowedNativeVolumeBinding<'a, A, O> {
    /// Exact provider capability used for this volume; a consumer router may compose heterogeneous stores under the existing provider traits.
    pub host: &'a FilesystemHost<A, O>,
    /// Owner-bound selected volume.
    pub volume: &'a VolumeRef,
    /// Exact immutable input generation.
    pub generation: &'a GenerationRef,
    /// Declared mechanism.
    pub namespace: &'a NativeVolumeNamespace,
    /// Relative native path below the prepared view root.
    pub path: &'a str,
    /// Whether captured edits may publish to this destination.
    pub writable: bool,
    /// Existing signed authority verifier.
    pub verifier: &'a AuthorityVerifier,
    /// Original read authority, and write authority for a destination.
    pub scope: &'a Scope,
    /// Existing Filesystem operation permit for destination publication.
    pub publication: PublicationPermit,
}

/// Explicit host allocation. Retained native paths never allocate or adopt a root.
#[derive(Clone, Debug)]
pub struct NativePreparationRoot {
    /// Existing physical namespace selected by the host.
    pub root: PathBuf,
    /// Same-volume service allocation outside the root, with empty target/backup directories.
    pub operation_directory: PathBuf,
    /// Actual root allocation witnessed by the host, not a provider result.
    pub expected_identity: acyclic_fs::NativeRootIdentity,
    /// Actual service allocation identities, witnessed before any staging writes.
    pub expected_preparation_identity: acyclic_fs::MaterializationPreparationIdentity,
}

impl NativePreparationRoot {
    /// Returns the stable preparation identity on the caller's ledger.
    pub fn operation(task: crate::TaskId, work: &NativeMaterializationWork) -> Result<acyclic_fs::OperationId> {
        const DOMAIN: &[u8] = b"harness/native-preparation/v1\0";
        work.charge(acyclic_fs::WorkCounters { bytes_hashed: (DOMAIN.len() + 16) as u64,
            ..Default::default() })?;
        let mut hash = blake3::Hasher::new();
        hash.update(DOMAIN);
        hash.update(&task.into_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        Ok(acyclic_fs::OperationId::from_bytes(bytes))
    }

    /// Observes host allocations using the caller's original native operation.
    pub fn witness<'a>(
        root: &'a std::path::Path,
        operation_directory: &'a std::path::Path,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Self, acyclic_fs::NativeAllocation)>> {
        use acyclic_fs::MaterializationBackend as _;
        if !work.remaining()?.has_finite_allowances() || !root.is_absolute() || !operation_directory.is_absolute() {
            return Err(boundary(crate::wire::ErrorCode::Invalid, "native allocation witness requires absolute paths and finite allowances"));
        }
        crate::NativeFutureBundle::new(work, || async {
        let (root, mut custody) = work.path(root, None)?;
        let (operation_directory, mut operation_custody) = work.path(operation_directory, None)?;
        custody.absorb(&mut operation_custody)?;
        let meter = work.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            let backend = acyclic_fs::NativeTreeMaterializationBackend::new(root, operation_directory, custody, meter.clone())?;
            backend.require_initial_empty()?;
            let expected_identity = acyclic_fs::NativeRootIdentity::from_bytes(
                backend.allocation_identity().ok_or(boundary(crate::wire::ErrorCode::Conflict, "native root allocation is unwitnessed"))?,
            );
            let expected_preparation_identity = backend.preparation_identity()
                .ok_or(boundary(crate::wire::ErrorCode::Conflict, "native staging allocation is unwitnessed"))?;
            let (root, mut custody) = meter.path(backend.root_path(), None)?;
            let (operation_directory, mut operation_custody) = meter.path(backend.operation_directory_path(), None)?;
            custody.absorb(&mut operation_custody)?;
            Ok::<_, Error>((Self { root, operation_directory, expected_identity, expected_preparation_identity }, custody))
        }, Some((work.runtime_meter(), false)))
            .map_err(|error| Error::from(work.runtime_error(error)))?
            .await.map_err(|error| Error::from(work.runtime_error(error)))?
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativePreparationIntent {
    pub(super) task: crate::TaskId,
    pub(super) original_fence: crate::scheduler::LeaseFence,
    pub(super) operation_directory: PathBuf,
    pub(super) manifest: NativeViewManifest,
}

/// Same-engine durable preparation progress; this evidence conveys no authority.
#[derive(Debug, serde::Serialize)]
pub struct NativePreparationReceipt {
    /// Original task and creation fence retained before physical edits.
    pub task: crate::TaskId,
    /// Original task reservation, not permission to attach under a stale fence.
    pub original_fence: crate::scheduler::LeaseFence,
    /// Original selected namespace and generations.
    pub manifest: NativeViewManifest,
    /// Original materializer operation and actual allocation/preparation identities.
    pub journal: MaterializationJournal,
    /// Independently observed physical-root status.
    pub root: acyclic_fs::MaterializationRootObservation,
    /// Actual original staging-allocation status, including replacement.
    pub preparation: acyclic_fs::MaterializationPreparationObservation,
    /// Independently observed captured endpoints. Uncaptured edits remain unknown.
    pub observations: Vec<acyclic_fs::materializer::MaterializationObservation>,
    #[serde(skip)]
    pub(super) _custody: acyclic_fs::NativeAllocation,
}

/// Owned output from the existing preparation journal; no host paths are opened.
pub struct NativeRecordedPreparation {
    pub task: crate::TaskId,
    pub original_fence: crate::scheduler::LeaseFence,
    pub manifest: NativeViewManifest,
    pub operation_directory: PathBuf,
    pub journal: MaterializationJournal,
    pub(super) _custody: acyclic_fs::NativeAllocation,
}

impl From<acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>> for Error {
    fn from(error: acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>) -> Self {
        use acyclic_fs::MaterializationError as E;
        let (code, reason) = match error {
            E::Store(error) => return error,
            E::Backend(error) => return Error::from(error),
            E::PlanConflict => (crate::wire::ErrorCode::Conflict, "materialization retry conflicts with its durable plan"),
            E::IncompatibleJournal => (crate::wire::ErrorCode::Storage, "materialization journal is incompatible"),
            E::Contended => (crate::wire::ErrorCode::Conflict, "materialization journal remained contended"),
            E::ExternalMutation => (crate::wire::ErrorCode::Conflict, "materialization paused because an external path mutation was observed"),
        };
        Error::NativeBoundary { code, reason }
    }
}

pub(super) fn preparation_backend(source: acyclic_stream::StreamError) -> Error {
    Error::NativeStream {
        code: if matches!(source, acyclic_stream::StreamError::IdempotencyMismatch) {
            crate::wire::ErrorCode::Conflict
        } else { crate::wire::ErrorCode::Storage },
        source, custody: None,
    }
}

pub(super) struct PreparedVolume<A, O> {
    pub(super) view: NativePreparedVolumeBackend<A, O>,
    pub(super) original_identity: acyclic_fs::NativeRootIdentity,
    pub(super) publication: PublicationPermit,
    pub(super) _work: NativeMaterializationWork,
}

pub(super) enum NativePreparedVolumeBackend<A, O> {
    Materialized { view: NativeWorkingSet<A, O>, callback: Arc<dyn crate::runtime::NativeVolumePublicationGuard> },
    HeldLazy(Arc<dyn NativeHeldLazyVolume>),
}

impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{

    /// Recovers the recorded host allocation on the caller's original ledger.
    pub fn recover_allocation<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, options: &'a NativeViewOptions, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<NativePreparationRoot>, acyclic_fs::NativeAllocation)>> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(boundary(crate::wire::ErrorCode::Invalid, "native allocation recovery requires finite original work"));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let store = NativePreparationStore { authority: NativePreparationAuthority::Owner { owner, settlement: true }, work: work.clone() };
            let operation = NativePreparationRoot::operation(owner.task_binding().0, work)?;
            let Some(journal) = store.load_record(operation, false).await
                ?
            else { return Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?)); };
            if journal.preparation_progress.is_some() {
                return Err(boundary(crate::wire::ErrorCode::Indeterminate, "native original staging allocation is incomplete or indeterminate"));
            }
            let intent = preparation_intent(&journal, work)?;
            if intent.0.task != owner.task_binding().0 || &intent.0.manifest.options != options {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "native allocation differs from original task or preparation options"));
            }
            let expected_root = journal.root_identity.ok_or(boundary(crate::wire::ErrorCode::Indeterminate, "native original root allocation is unwitnessed"))?;
            let expected_preparation_identity = journal.preparation_identity.ok_or(boundary(crate::wire::ErrorCode::Indeterminate, "native original staging allocation is unwitnessed"))?;
            store.verify_read_owner().await?;
            let expected_identity = work.root_identity(&intent.0.manifest.options.root)?;
            if expected_identity.to_bytes() != expected_root {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "native original root allocation was replaced"));
            }
            let NativePreparationIntent { original_fence, manifest, operation_directory, .. } = intent.0;
            let NativeViewManifest { options, volumes } = manifest;
            drop((volumes, original_fence));
            let mut allocation = (NativePreparationRoot {
                root: options.root, operation_directory, expected_identity, expected_preparation_identity,
            }, intent.1);
            let retained = allocation.0.root.capacity().checked_add(allocation.0.operation_directory.capacity())
                .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
            allocation.1.resize(retained as u64)?;
            let _backend = native_backend(&allocation.0, work, true).await?;
            store.verify_read_owner().await?;
            work.remaining()?;
            Ok((Some(allocation.0), allocation.1))
        })
    }


    /// Inspects the original preparation and optionally rolls it back.
    pub fn inspect_preparation<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, allocation: &'a NativePreparationRoot,
        work: &'a NativeMaterializationWork, cleanup: bool,
    ) -> Result<crate::NativeFutureBundle<'a, Option<NativePreparationReceipt>>> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(boundary(crate::wire::ErrorCode::Invalid, "native query requires finite original work"));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let store = NativePreparationStore { authority: NativePreparationAuthority::Owner { owner, settlement: true }, work: work.clone() };
            let operation = NativePreparationRoot::operation(owner.task_binding().0, work)?;
            let Some(mut journal) = store.load_record(operation, false).await
                ? else { return Ok(None); };
            let mut intent = preparation_intent(&journal, work)?;
            if intent.0.task != owner.task_binding().0 || intent.0.manifest.options.root != allocation.root
                || intent.0.operation_directory != allocation.operation_directory
            {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "native query differs from original preparation"));
            }
            require_allocation(&journal, allocation)?;
            store.verify_read_owner().await?;
            let backend = match native_backend(allocation, work, cleanup).await {
                Ok(backend) => backend,
                Err(error @ Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Work(_)
                        | acyclic_fs::NativeTreeMaterializationError::Cancelled
                        | acyclic_fs::NativeTreeMaterializationError::AllocationFailed(_),
                    ..
                }) => return Err(error),
                Err(error) if !cleanup => {
                    let mut root_message = work.display(&error)?;
                    let mut preparation_message = work.display(&root_message.0)?;
                    intent.1.absorb(&mut journal.custody.take().expect("loaded native journal custody"))?;
                    intent.1.absorb(&mut root_message.1)?;
                    intent.1.absorb(&mut preparation_message.1)?;
                    let receipt = NativePreparationReceipt {
                        task: intent.0.task, original_fence: intent.0.original_fence, manifest: intent.0.manifest,
                        journal, root: acyclic_fs::MaterializationRootObservation::Unavailable(root_message.0),
                        preparation: acyclic_fs::MaterializationPreparationObservation::Unavailable(preparation_message.0),
                        observations: Vec::new(), _custody: intent.1,
                    };
                    store.verify_read_owner().await?;
                    return Ok(Some(receipt));
                }
                Err(error) => return Err(error),
            };
            drop(journal);
            let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
            if cleanup {
                owner.verify(true, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                let rolled_back = {
                    let mut before_effect = |_write| async move {
                        owner.verify(true, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                        Ok::<(), Error>(())
                    };
                    materializer.recover(
                        operation, acyclic_fs::MaterializationRecovery::RollBack, &mut before_effect,
                    ).await
                }?
                    .ok_or(boundary(crate::wire::ErrorCode::Conflict, "native preparation journal disappeared"))?;
                owner.verify(true, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                materializer.backend().reset_target_directory("").await?;
                owner.verify(true, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                checkpoint_preparation(materializer.store(), rolled_back).await?;
            }
            let inspection = materializer.inspect(operation).await.map_err(Error::from)?
                .ok_or(boundary(crate::wire::ErrorCode::Conflict, "native preparation journal disappeared"))?;
            let receipt = preparation_receipt(intent.0, inspection, Some(intent.1))?;
            materializer.store().verify_read_owner().await?;
            Ok(Some(receipt))
        })
    }
}

/// Reads the authenticated original journal; absence leaves physical state unknown.
pub fn observe_preparation<'a, P: StreamProvider>(
    witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
    work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, Option<NativeRecordedPreparation>>> {
    if !work.remaining()?.has_finite_allowances() {
        return Err(boundary(crate::wire::ErrorCode::Invalid, "native admission query requires a finite original work allowance"));
    }
    crate::NativeFutureBundle::new(work, || async {
        witness.verify_with_native_work(work).await?;
        let scratch = witness.scratch_proof();
        if scratch.is_some_and(|scratch| witness.task() != scratch.original_task()) {
            return Err(boundary(crate::wire::ErrorCode::Storage, "native admission witness differs from its original scratch task"));
        }
        let operation = NativePreparationRoot::operation(witness.task(), work)?;
        let store = NativePreparationStore { authority: NativePreparationAuthority::Admission(witness), work: work.clone() };
        let record = store.load_record(operation, false).await?;
        let value = match record {
            None => None,
            Some(mut record) => {
                let mut intent = preparation_intent(&record, work)?;
                if intent.0.task != witness.task() {
                    return Err(boundary(crate::wire::ErrorCode::Storage,
                        "native journal original task differs from authenticated admission",
                    ));
                }
                if let Some(scratch) = scratch {
                    if witness.task() != scratch.original_task()
                        || !intent.0.manifest.volumes.iter().any(|mapping| mapping.writable
                            && &mapping.volume == scratch.volume())
                    {
                        return Err(boundary(crate::wire::ErrorCode::Storage,
                            "native journal does not retain the admitted original private scratch selection",
                        ));
                    }
                }
                let mut custody = record.custody.take().expect("loaded native journal custody");
                custody.absorb(&mut intent.1)?;
                Some(NativeRecordedPreparation {
                    task: intent.0.task, original_fence: intent.0.original_fence,
                    manifest: intent.0.manifest, operation_directory: intent.0.operation_directory,
                    journal: record, _custody: custody,
                })
            }
        };
        witness.verify_with_native_work(work).await?;
        work.remaining()?;
        Ok(value)
    })
}

pub(super) fn native_paths_overlap(left: &Path, right: &Path) -> bool {
    left.components().zip(right.components()).all(|(left, right)| {
        #[cfg(windows)]
        let equal = match (left.as_os_str().to_str(), right.as_os_str().to_str()) {
            (Some(left), Some(right)) => left.chars().flat_map(char::to_uppercase)
                .eq(right.chars().flat_map(char::to_uppercase)),
            _ => false,
        };
        #[cfg(not(windows))]
        let equal = left == right;
        equal
    })
}

pub(super) fn require_allocation(journal: &MaterializationJournal, allocation: &NativePreparationRoot) -> Result<()> {
    if journal.root_identity != Some(allocation.expected_identity.to_bytes())
        || journal.preparation_identity.as_ref() != Some(&allocation.expected_preparation_identity)
    {
        return Err(boundary(crate::wire::ErrorCode::Conflict, "native preparation allocation differs from original journal"));
    }
    Ok(())
}

pub(super) async fn native_backend(
    allocation: &NativePreparationRoot, work: &NativeMaterializationWork, require_original: bool,
) -> Result<Arc<acyclic_fs::NativeTreeMaterializationBackend>> {
    use acyclic_fs::MaterializationBackend as _;
    let (root, mut custody) = work.path(&allocation.root, None)?;
    let (operation_directory, mut operation_custody) = work.path(&allocation.operation_directory, None)?;
    custody.absorb(&mut operation_custody)?;
    let meter = work.clone();
    let backend = acyclic_native_runtime::run_blocking_io(move || {
        acyclic_fs::NativeTreeMaterializationBackend::new(root, operation_directory, custody, meter)
    }, Some((work.runtime_meter(), false)))
        .map_err(|error| Error::from(work.runtime_error(error)))?
        .await.map_err(|error| Error::from(work.runtime_error(error)))??;
    if require_original && (backend.allocation_identity() != Some(allocation.expected_identity.to_bytes())
        || backend.preparation_identity().as_ref() != Some(&allocation.expected_preparation_identity))
    {
        return Err(boundary(crate::wire::ErrorCode::Conflict, "native allocation was physically replaced"));
    }
    Ok(backend)
}

pub(super) fn preparation_intent(
    journal: &MaterializationJournal, work: &NativeMaterializationWork,
) -> Result<(NativePreparationIntent, acyclic_fs::NativeAllocation)> {
    let acyclic_fs::MaterializationIntent::NativePreparation { selection, original } = &journal.plan.intent else {
        return Err(boundary(crate::wire::ErrorCode::Storage, "native preparation original qualified intent is missing"));
    };
    let intent: (NativePreparationIntent, _) = decode_record(
        original, std::mem::size_of::<NativeVolumeMapping>().max(std::mem::size_of::<u8>()), work,
        native_intent_control_bytes,
    )?;
    intent.0.manifest.validate()?;
    if selection.len() != intent.0.manifest.volumes.len()
        || journal.plan.edits.iter().any(|edit| !matches!(edit,
            acyclic_fs::MaterializationEdit::Install { image, .. }
                if image.as_slice() == acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX
        ))
    {
        return Err(boundary(crate::wire::ErrorCode::Storage, "native preparation original selections or edit intents disagree"));
    }
    for (selected, mapping) in selection.iter().zip(&intent.0.manifest.volumes) {
        let identity = encode_record(mapping.volume.provider(), work)?;
        if identity.0 != selected.provider_identity
            || selected.generation.digest().as_bytes().as_slice() != mapping.generation.as_resource().key()
        {
            return Err(boundary(crate::wire::ErrorCode::Storage, "native preparation original qualified volume differs"));
        }
    }
    Ok(intent)
}

pub(super) fn preparation_receipt(
    intent: NativePreparationIntent, inspection: acyclic_fs::MaterializationInspection,
    custody: Option<acyclic_fs::NativeAllocation>,
) -> Result<NativePreparationReceipt> {
    let mut journal = inspection.journal;
    let custody = match (custody, journal.custody.take()) {
        (Some(mut custody), Some(mut journal_custody)) => {
            custody.absorb(&mut journal_custody)?;
            custody
        }
        (Some(custody), None) | (None, Some(custody)) => custody,
        (None, None) => return Err(boundary(crate::wire::ErrorCode::Storage, "native preparation inspection lost its original journal custody")),
    };
    Ok(NativePreparationReceipt {
        task: intent.task, original_fence: intent.original_fence, manifest: intent.manifest,
        journal, root: inspection.root, preparation: inspection.preparation,
        observations: inspection.observations, _custody: custody,
    })
}
