use super::*;
use crate::kernel::NamespacePath;
use crate::lazy_workspace::NativeSnapshotContext;
use std::future::Future;

mod changes;
mod preparation;
mod recovery;
type SelectedPublication<'a> = Option<(
    crate::LazySnapshotRef,
    &'a crate::SelectedImportPlan,
    crate::OperationId,
)>;
type CommittedPublication = (
    crate::OperationId,
    crate::GenerationId,
    Option<SelectedImportProof>,
    Option<crate::OperationId>,
    Option<crate::OperationId>,
);
type MountedRecords = BTreeMap<FileId, (FileRecord, FileMetadata)>;
struct PreparedPublication<'a, A, O, E> {
    _records: Vec<(FileRecord, FileMetadata)>,
    _effects: E,
    changes: Option<Vec<NamespacePath>>,
    paths: Vec<String>,
    candidate: CheckoutCandidate<'a, A, O>,
    operation: crate::OperationId,
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) async fn publish<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        before_publication: &mut F,
        selected: Option<(
            crate::LazySnapshotRef,
            &crate::SelectedImportPlan,
            crate::OperationId,
        )>,
    ) -> Result<crate::NativePublicationSnapshot, (E, Option<MountSourceError>)>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let mut accounting_failure = None;
        let result = self
            .apply_publication(
                permit,
                &mut crate::kernel::KernelAccounting::native(work, cancellation),
                acknowledged,
                before_publication,
                selected,
                &mut accounting_failure,
            )
            .await;
        if result.is_err()
            && let Err(error) = self.cleanup_publication(work, cancellation).await
        {
            accounting_failure = accounting_failure.or(Some(error));
        }
        let accounting = work
            .admit(crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork);
        match (result, accounting) {
            (Ok(snapshot), Ok(())) => Ok(snapshot),
            (Ok(_), Err(error)) => Err((E::from(error), accounting_failure)),
            (Err(error), Ok(())) => Err((error, accounting_failure)),
            (Err(error), Err(accounting)) => Err((error, accounting_failure.or(Some(accounting)))),
        }
    }

    async fn apply_publication<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        before_publication: &mut F,
        selected: SelectedPublication<'_>,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<crate::NativePublicationSnapshot, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } = &*accounting
        else {
            return Err(MountSourceError::OriginalSnapshotUnavailable.into());
        };
        let (work, cancellation) = (*work, *cancellation);
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let selected_proof = selected
            .map(|(destination, plan, _)| {
                plan.input_digest(destination, accounting)
                    .map(|input| SelectedImportProof {
                        destination,
                        source: plan.source,
                        config: plan.source_config,
                        input,
                    })
                    .map_err(lazy_error)
            })
            .transpose()?;
        if let Some(published) = self
            .recover_publication(
                permit,
                accounting,
                before_publication,
                acknowledged,
                (selected, selected_proof),
                accounting_failure,
            )
            .await?
        {
            return Ok(published);
        }
        let mut input_carrier = adapter::NativeDemandCarrier::Native(work);
        let mut input = adapter::InputBuffers::new(&mut input_carrier)?;
        let (removals, captured_records) = self.removal_inputs(&mut input)?;
        let dirty = self.dirty_detached_records(work).await?;
        if removals.is_empty() && dirty.value.is_empty() && selected.is_none() {
            return self
                .publish_clean(
                    permit,
                    accounting,
                    before_publication,
                    acknowledged,
                    accounting_failure,
                )
                .await;
        }
        let PreparedPublication {
            operation,
            mut candidate,
            paths,
            _effects,
            changes,
            _records,
        } = self
            .prepare_publication(
                removals,
                &dirty.value,
                captured_records,
                (selected, selected_proof),
                &mut input,
                accounting,
            )
            .await?;
        let context = self
            .publication_context(operation, None, selected_proof, accounting)
            .await?;
        let mut scratch_carrier = adapter::NativeDemandCarrier::Native(work);
        let mut scratch = adapter::InputBuffers::new(&mut scratch_carrier)?;
        let (_namespace_allocations, _namespaces, installed, _allocation) = self
            .publication_changes(
                &candidate,
                &paths,
                changes.as_deref(),
                &mut scratch,
                accounting,
                &mut carrier,
            )
            .await?;
        candidate.operation = Some(operation);
        self.install_publication(candidate, &installed, &mut carrier, cancellation)
            .await?;
        self.complete_publication(
            permit,
            (context, operation, selected_proof),
            accounting,
            before_publication,
            acknowledged,
            accounting_failure,
        )
        .await
    }
    pub(super) fn clear_mount_removals(&self) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        removals.paths.clear();
        removals.records.clear();
        removals.publication = None;
        Ok(())
    }

    pub(super) async fn prepare_removed_identity_candidate(
        &self,
        removals: &[(String, FileId)],
        dirty: &[DirtyDetachedRecord],
        work: &crate::NativeMaterializationWork,
    ) -> Result<
        (
            crate::OperationId,
            CheckoutCandidate<'_, A, O>,
            Vec<(FileRecord, FileMetadata)>,
        ),
        MountSourceError,
    > {
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let mut transaction = self
            .authored
            .shared_checkout()
            .candidate(
                None,
                &mut carrier,
                work.cancellation(),
                Some(self.authored.cancellation()),
            )
            .await?;
        let candidate = &mut transaction.checkout;
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        let mut identity_records = BTreeMap::new();
        for (path, removed_id) in removals {
            let mut namespace = self
                .lazy
                .namespace_path(path, &mut accounting)
                .map_err(lazy_error)?;
            let lookup = candidate
                .lookup_no_follow_with_metadata(&namespace.value, work, work.cancellation())
                .await
                .map_err(mount_engine_error)?;
            let Some(lookup) = lookup else {
                continue;
            };
            if lookup.record.file_id != *removed_id {
                continue;
            }
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::Stale);
            }
            identity_records
                .entry(*removed_id)
                .or_insert((lookup.record, lookup.metadata));
            let mut operations =
                crate::kernel::Owned::allocate(1, &mut accounting).map_err(mount_engine_error)?;
            operations.bytes = operations
                .bytes
                .checked_add(namespace.bytes)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            operations
                .allocation
                .absorb(&mut namespace.allocation)
                .map_err(mount_engine_error)?;
            operations.value.push(Mutation::Remove {
                path: namespace.value,
                expected_file_id: MetadataField::Value(*removed_id),
            });
            candidate
                .apply_mutations(operations, &mut accounting, work.cancellation())
                .await
                .map_err(mount_engine_error)?;
        }
        // Update surviving hard links from the detached identity, without rereading the source.
        let mut replacements = Vec::new();
        for (record, _, _) in dirty {
            let live = match candidate
                .read_file_record_by_id(record.file_id, work, work.cancellation())
                .await
            {
                Ok(record) => Some(record),
                Err(crate::FsError::NotFound) => None,
                Err(error) => return Err(mount_engine_error(error)),
            };
            if let Some(live) = live {
                if live.kind != record.kind {
                    return Err(MountSourceError::Stale);
                }
                replacements.push(Mutation::File {
                    file_id: record.file_id,
                    mutation: FileMutation::ReplaceRecord { record: *record },
                });
            }
        }
        if !replacements.is_empty() {
            let maximum =
                usize::try_from(candidate.volume_config().limits.maximum_mutations_per_batch)
                    .unwrap_or(usize::MAX)
                    .max(1);
            for chunk in replacements.chunks(maximum) {
                let mut operations = crate::kernel::Owned::allocate(chunk.len(), &mut accounting)
                    .map_err(mount_engine_error)?;
                operations.value.extend(chunk.iter().cloned());
                candidate
                    .apply_mutations(operations, &mut accounting, work.cancellation())
                    .await
                    .map_err(mount_engine_error)?;
            }
        }
        let identity_records = identity_records.into_values().collect();
        let operation_id = crate::OperationId::new();
        Ok((operation_id, transaction, identity_records))
    }
}
