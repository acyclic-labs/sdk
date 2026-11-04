use super::*;

/// Canonically ordered portable paths for the selected stable file identities.
/// The original work source funds these paths until this value is dropped.
pub struct FileIdentityPaths {
    paths: crate::kernel::allocation::retained::Retained<Vec<(FileId, Vec<String>)>>,
    complete: bool,
}

impl FileIdentityPaths {
    /// Selected identities and their admitted aliases, in canonical order.
    pub fn entries(&self) -> &[(FileId, Vec<String>)] {
        &self.paths.value
    }

    /// Whether every selected identity's authenticated aliases were returned.
    pub const fn is_complete(&self) -> bool {
        self.complete
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Workspace<A, O> {
    pub(super) async fn first_path_at_with_accounting(
        &self,
        generation: GenerationId,
        file_id: FileId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::kernel::Owned<NamespacePath>>, WorkspaceError> {
        let mut accounting = accounting.allocation_scope()?;
        let result = async {
            let checkout = self
                .open_checkout(
                    GenerationSelector::Exact(generation),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let result = super::namespace_view::NamespaceView {
                objects: self.volume.fs.objects(),
                config: self.volume.config(),
                root: checkout.value.root(),
            }
            .first_path(file_id, &mut accounting, cancellation)
            .await;
            let retired = checkout.retire(&mut accounting);
            result
                .and_then(|path| retired.map(|()| path))
                .map_err(Into::into)
        }
        .await;
        if result.is_ok() {
            accounting.keep();
        }
        result
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Generation<A, O> {
    /// Finds portable paths through each selected identity's authenticated parent edges.
    /// Limits and cancellation apply to both traversal and portable projection.
    pub async fn paths_for_file_ids<W: crate::facade::FsWork>(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileIdentityPaths, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .paths_for_file_ids_with_accounting(
                file_ids,
                maximum_entries,
                &mut accounting,
                cancellation,
            )
            .await;
        work.finish(result, &accounting)
    }

    pub(super) async fn paths_for_file_ids_with_accounting(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileIdentityPaths, WorkspaceError> {
        let mut accounting = accounting.allocation_scope()?;
        let ids = sorted_changed_file_ids(file_ids, &mut accounting)?;
        let search = self
            .namespace_records_with_accounting(
                &ids.0,
                maximum_entries,
                &mut accounting,
                cancellation,
            )
            .await;
        let bytes = workspace_vec_bytes(&ids.0);
        reverse_drop(ids, bytes, &mut accounting)?;
        let search = search?;
        let complete = search.0.complete;
        // The authenticated producer already groups and orders identities and
        // aliases. Project once into exact-capacity vectors, with no second map.
        let mut projected = crate::kernel::Owned::<Vec<(FileId, Vec<String>)>>::allocate(
            search.0.records.len(),
            &mut accounting,
        )?;
        for (file_id, records) in &search.0.records {
            accounting.charge_items(1)?;
            let mut paths =
                crate::kernel::Owned::<Vec<String>>::allocate(records.len(), &mut accounting)?;
            for (path, _) in records {
                accounting.charge_items(1)?;
                let copied = copy_work(std::mem::size_of::<String>() as u64);
                accounting.admit(copied)?;
                let (text, allocation) =
                    path.unicode_text_with_accounting(&mut accounting, cancellation)?;
                let bytes = text.capacity() as u64;
                let text = crate::kernel::Owned::new(text, allocation, bytes).transfer_to(
                    &mut paths.allocation,
                    &mut paths.bytes,
                    &mut accounting,
                )?;
                paths.value.push(text);
                accounting.record_completed(copied)?;
            }
            let copied = copy_work(std::mem::size_of::<(FileId, Vec<String>)>() as u64);
            accounting.admit(copied)?;
            let paths = paths.transfer_to(
                &mut projected.allocation,
                &mut projected.bytes,
                &mut accounting,
            )?;
            projected.value.push((*file_id, paths));
            accounting.record_completed(copied)?;
        }
        // Physical temporaries die before the scope retires their scratch. Only
        // the returned vectors and strings become independently retained work.
        drop(search);
        Ok(FileIdentityPaths {
            paths: crate::kernel::allocation::retained::Retained::from_owned(
                projected,
                &mut accounting,
            )?,
            complete,
        })
    }

    pub(super) async fn namespace_records_with_accounting(
        &self,
        ids: &[FileId],
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if ids.is_empty() || maximum_entries == 0 {
            return Ok((
                NamespaceRecordSearch {
                    records: NamespaceRecords::default(),
                    complete: ids.is_empty(),
                },
                accounting.claim_owned(0, 0)?,
            ));
        }
        let checkout = self
            .workspace
            .open_checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                accounting,
                cancellation,
            )
            .await?;
        let result = namespace_records_to_file_root_with_accounting(
            &self.workspace.volume,
            checkout.value.root(),
            ids,
            maximum_entries,
            accounting,
            cancellation,
        )
        .await;
        checkout.retire(accounting)?;
        result.map_err(WorkspaceError::from)
    }
}
