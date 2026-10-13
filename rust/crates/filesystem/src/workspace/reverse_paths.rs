use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Finds paths for one stable file identity under supplied work limits.
    pub async fn paths_for_file_id(
        &self,
        file_id: FileId,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<String>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let search = self
                .namespace_records_with_accounting(
                    &[file_id],
                    maximum_entries,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let mut paths = crate::kernel::Owned::allocate(0, &mut accounting)?;
            if let Some(records) =
                binary_search_with_accounting(&search.0.records, &mut accounting, |record| {
                    record.0.cmp(&file_id)
                })?
                .ok()
                .map(|index| &search.0.records[index].1)
            {
                for (path, _) in records {
                    paths.ensure_for_push(records.len(), &mut accounting)?;
                    let (text, mut allocation) =
                        path.unicode_text_with_accounting(&mut accounting, cancellation)?;
                    paths.allocation.absorb(&mut allocation)?;
                    paths.bytes = paths
                        .bytes
                        .checked_add(text.capacity() as u64)
                        .ok_or(crate::WorkError::Overflow)?;
                    paths.value.push(text);
                }
            }
            Ok(paths.value)
        }
        .await;
        crate::facade::measured(result, &accounting)
    }

    /// Finds portable paths for multiple stable file identities in one namespace traversal.
    pub async fn paths_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<BTreeMap<FileId, Vec<String>>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let ids = sorted_changed_file_ids(file_ids, &mut accounting)?;
        self.namespace_records_with_accounting(
            &ids.0,
            maximum_entries,
            &mut accounting,
            &cancellation,
        )
        .await?
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
        let generation_root = crate::ObjectId {
            kind: crate::ObjectKind::GenerationRoot,
            digest: self.id.digest(),
        };
        let result = namespace_records_to_file_root_with_accounting(
            &self.workspace.volume,
            checkout.value.root(),
            generation_root,
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
