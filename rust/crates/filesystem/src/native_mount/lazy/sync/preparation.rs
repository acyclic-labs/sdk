use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) fn removal_inputs(
        &self,
        input: &mut adapter::InputBuffers<'_, '_>,
    ) -> Result<(Vec<(String, FileId)>, MountedRecords), MountSourceError> {
        let state = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let mut paths = input.vec(state.paths.len())?;
        for (path, (id, _)) in &state.paths {
            input.items(1)?;
            let copied = crate::materializer::NativeResourceCredit::Copy.delta(path.len() as u64);
            input.admit(copied)?;
            let mut bytes = input.vec(path.len())?;
            bytes.extend_from_slice(path.as_bytes());
            input.record(copied)?;
            paths.push((
                String::from_utf8(bytes).map_err(|_| {
                    MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    )
                })?,
                *id,
            ));
        }
        let records = state.records.clone();
        Ok((paths, records))
    }
    pub(super) async fn prepare_publication(
        &self,
        removals: Vec<(String, FileId)>,
        dirty: &[DirtyDetachedRecord],
        captured_records: MountedRecords,
        selection: (SelectedPublication<'_>, Option<SelectedImportProof>),
        input: &mut adapter::InputBuffers<'_, '_>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<PreparedPublication<'_, A, O, impl Sized + Send + use<A, O, D, S>>, MountSourceError>
    {
        let crate::kernel::KernelAccounting::NativeOriginal { work, .. } = &*accounting else {
            return Err(MountSourceError::OriginalSnapshotUnavailable);
        };
        let work = *work;
        let (selected, selected_proof) = selection;
        let (fresh_operation, mut candidate, identities) = self
            .prepare_removed_identity_candidate(&removals, dirty, work)
            .await?;
        let mut paths = input.vec(removals.len())?;
        paths.extend(removals.into_iter().map(|(path, _)| path));
        let operation = selected.map_or(fresh_operation, |(_, _, operation)| operation);
        let mut effects = match selected {
            Some((destination, plan, _)) => Some(
                self.lazy
                    .prepare_selected_import_into_checkout(
                        destination,
                        plan,
                        &mut candidate.checkout,
                        operation,
                        accounting,
                    )
                    .await
                    .map_err(lazy_error)?,
            ),
            None => None,
        };
        let mut records = captured_records;
        records.extend(
            identities
                .into_iter()
                .map(|(record, metadata)| (record.file_id, (record, metadata))),
        );
        if let Some(effects) = &effects {
            records.extend(
                effects
                    .records
                    .iter()
                    .map(|(record, metadata)| (record.file_id, (*record, *metadata))),
            );
        }
        records.extend(
            dirty
                .iter()
                .map(|(record, metadata, _)| (record.file_id, (*record, *metadata))),
        );
        let records = records.into_values().collect::<Vec<_>>();
        if let Err(error) = self.stage_detached_records(&records, dirty, work).await {
            self.abort_detached_publication()?;
            return Err(error);
        }
        self.removals
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .publication = Some((operation, selected_proof));
        self.lazy
            .prepare_mount_removals(
                &paths,
                effects
                    .as_ref()
                    .map_or(&[], |effects| effects.paths.as_slice()),
                &records,
                IdempotencyKey::from_bytes(operation.into_bytes()),
                accounting,
            )
            .await
            .map_err(lazy_error)?;
        let changes = effects
            .as_mut()
            .map(|effects| std::mem::take(&mut effects.changes));
        Ok(PreparedPublication {
            operation,
            candidate,
            paths,
            _effects: effects,
            changes,
            _records: records,
        })
    }
}
