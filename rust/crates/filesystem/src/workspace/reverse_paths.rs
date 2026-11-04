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
        let result = async {
            let paths = self
                .unaccounted_paths_for_file_ids(file_ids, maximum_entries)
                .await?;
            let mut retained =
                crate::kernel::allocation::retained::Retained::allocate(0, &mut accounting)?;
            retained.value = paths.into_iter().collect();
            Ok(FileIdentityPaths {
                paths: retained,
                complete: true,
            })
        }
        .await;
        work.finish(result, &accounting)
    }

    async fn unaccounted_paths_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<BTreeMap<FileId, Vec<String>>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let ids = sorted_changed_file_ids(file_ids, &mut accounting)?;
        let search = self
            .namespace_records_with_accounting(
                &ids.0,
                maximum_entries,
                &mut accounting,
                &cancellation,
            )
            .await;
        let bytes = workspace_vec_bytes(&ids.0);
        reverse_drop(ids, bytes, &mut accounting)?;
        search?
            .0
            .records
            .into_iter()
            .map(|(file_id, records)| {
                records
                    .into_iter()
                    .map(|(path, _)| namespace_path_text(&path))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|paths| (file_id, paths))
            })
            .collect()
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
