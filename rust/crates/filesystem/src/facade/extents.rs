use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ResolvedFile<A, O> {

    /// Plans sparse physical spans without another namespace lookup.
    pub async fn plan_extents(
        &self,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        self.reader
            .plan_record_extents(self.record, range, maximum_spans, budget, cancellation)
            .await
    }

    /// Plans sparse spans on the same original ledger without another lookup.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn plan_extents_with_native_work(
        &self, range: ByteRange, maximum_spans: u32, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<(ExtentPlan, NativeAllocation)>, FsError> {
        self.reader.plan_record_extents_with_native_work(self.record, range, maximum_spans, work, cancellation).await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {

    /// Plans actual sparse spans, returning the original-owned span vector.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn plan_record_extents_with_native_work(
        &self, record: FileRecord, range: ByteRange, maximum_spans: u32, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<(ExtentPlan, NativeAllocation)>, FsError> {
        if maximum_spans == 0 || range.length > self.volume.config.limits.maximum_generation_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        accounting.check()?;
        let (logical_bytes, root) = match record.payload {
            FilePayload::InlineRegular(data) => {
                validate_planned_file_range(usize_to_u64(data.as_bytes().len()), range)?;
                return Ok(None);
            }
            FilePayload::Regular { logical_bytes, extents } => (logical_bytes, extents),
            _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
        };
        let (plan, allocation) = plan_extent_range_with_accounting(
            &self.volume.fs.inner.objects, ExtentRangeRequest {
                root, file_size: logical_bytes, range, maximum_spans,
                limits: DecodeLimits::for_volume(self.volume.config), budget: accounting.remaining()?,
            }, &mut accounting, cancellation,
        ).await?;
        Ok(Some((plan, native_pinned_allocation(allocation)?)))
    }

    /// Plans sparse physical spans without reading content bodies.
    pub async fn plan_file_extents(
        &self, path: &NamespacePath, range: ByteRange, maximum_spans: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self.resolve_file(path, &mut accounting, cancellation).await;
        let work = accounting.spent();
        let record = result.map_err(|error| OperationFailure::new(error, work))?;
        let plan = self.plan_record_extents(record, range, maximum_spans, remaining(work, budget)?, cancellation)
            .await.map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        Ok(FsReceipt { value: plan.value, work: add(work, plan.work)? })
    }

    pub(super) async fn plan_record_extents(
        &self,
        record: FileRecord,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        if maximum_spans == 0 || range.length > self.volume.config.limits.maximum_generation_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let mut work = WorkCounters::default();
        let (logical_bytes, extents) = match record.payload {
            FilePayload::InlineRegular(data) => {
                validate_planned_file_range(
                    u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX),
                    range,
                ).map_err(|error| OperationFailure::new(error, work))?;
                return Ok(FsReceipt { value: None, work });
            }
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => (logical_bytes, extents),
            _ => {
                return Err(OperationFailure::new(
                    FsError::FileRead(FileRangeReadError::NotRegular),
                    work,
                ));
            }
        };
        let mut plan = plan_extent_range(
            &self.volume.fs.inner.objects,
            ExtentRangeRequest {
                root: extents,
                file_size: logical_bytes,
                range,
                maximum_spans,
                limits: DecodeLimits::for_volume(self.volume.config),
                budget: remaining(work, budget)?,
            },
            cancellation,
        )
        .await
        .map_err(|failure| {
            failure.map_with_prior_work(work, |error| {
                FsError::FileRead(FileRangeReadError::Extent(error))
            })
        })?;
        work = add(work, plan.work)?;
        plan.work = work;
        Ok(FsReceipt {
            value: Some(plan),
            work,
        })
    }
}

pub(super) async fn seek_record_extent_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, record: FileRecord, offset: u64, target: ExtentSeekTarget,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<u64>, FsError> {
    accounting.check()?;
    match record.payload {
        FilePayload::InlineRegular(data) => {
            let logical_bytes = data.as_bytes().len() as u64;
            if offset > logical_bytes { return Ok(None); }
            Ok(match target {
                ExtentSeekTarget::Data => (offset < logical_bytes).then_some(offset),
                ExtentSeekTarget::Hole => Some(logical_bytes),
            })
        }
        FilePayload::Regular { logical_bytes, extents } => {
            if offset > logical_bytes { return Ok(None); }
            let before = readonly_standalone_live(accounting);
            let result = crate::kernel::seek_extent_with_accounting(
                &volume.fs.inner.objects,
                ExtentSeekRequest {
                    root: extents, file_size: logical_bytes, offset, target,
                    limits: DecodeLimits::for_volume(volume.config), budget: accounting.remaining()?,
                }, accounting, cancellation,
            ).await;
            readonly_cleanup_standalone(accounting, before, 0)?;
            result
        }
        _ => Err(FsError::FileRead(FileRangeReadError::NotRegular)),
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {

    /// Plans one bounded sparse range by stable file identity without reading
    pub async fn plan_file_extents_by_id(
        &mut self,
        file_id: FileId,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        if maximum_spans == 0 || range.length > self.volume.config.limits.maximum_generation_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let record = self.lookup_file_record_by_id_with_accounting(file_id, &mut accounting, cancellation).await?
                .ok_or(FsError::NotFound)?;
            self.plan_record_extents(record, range, maximum_spans, &mut accounting, cancellation).await
                .map(|(plan, allocation)| { drop(allocation); plan })
        }.await;
        measured(result, &accounting)
    }

    pub(super) async fn plan_record_extents(
        &mut self, record: FileRecord, range: ByteRange, maximum_spans: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(Option<ExtentPlan>, KernelOwnedAllocation), FsError> {
        let mut planned = match record.payload {
            FilePayload::InlineRegular(data) => {
                validate_planned_file_range(data.as_bytes().len() as u64, range)?;
                (None, KernelOwnedAllocation::Standalone)
            }
            FilePayload::Regular { logical_bytes, extents } => {
                let (plan, allocation) = crate::kernel::plan_extent_range_with_accounting(
                    &self.volume.fs.inner.objects,
                    ExtentRangeRequest {
                        root: extents, file_size: logical_bytes, range, maximum_spans,
                        limits: DecodeLimits::for_volume(self.volume.config), budget: accounting.remaining()?,
                    },
                    accounting, cancellation,
                ).await?;
                (Some(plan), allocation)
            }
            _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
        };
        if self.tracks_observations() && range.length != 0 {
            self.observe_base_regular_range_with_accounting(record.file_id, range, accounting, cancellation).await?;
        }
        if let Some(plan) = &mut planned.0 { plan.work = accounting.spent(); }
        Ok(planned)
    }

    /// Finds the first sparse data or hole boundary by stable file identity.
    pub async fn seek_file_extent_by_id<W: FsWork>(
        &mut self, file_id: FileId, offset: u64, target: ExtentSeekTarget,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<u64>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let record = self.lookup_file_record_by_id_with_accounting(file_id, accounting, cancellation).await?
                .ok_or(FsError::NotFound)?;
            let value = seek_record_extent_with_accounting(&self.volume, record, offset, target, accounting, cancellation).await?;
            if self.tracks_observations() {
                self.observe_identity_regions_with_accounting(
                    [Some(DependencyRegion::SparseSeek { file_id, offset, target }), None], accounting, cancellation,
                ).await?;
            }
            Ok(value)
        }.await;
        budget.finish(result, &accounting)
    }

    /// Plans the sparse physical representation of one exact regular-file range.
    pub async fn plan_file_extents(
        &mut self,
        path: &NamespacePath,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        if maximum_spans == 0 || range.length > self.volume.config.limits.maximum_generation_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (lookup, allocation) = self.lookup_no_follow_with_accounting(
                path, &mut accounting, cancellation, false,
            ).await?;
            drop(allocation);
            let record = lookup.record.ok_or(FsError::NotFound)?;
            self.plan_record_extents(record, range, maximum_spans, &mut accounting, cancellation).await
                .map(|(plan, allocation)| { drop(allocation); plan })
        }.await;
        measured(result, &accounting)
    }

    /// Finds the first sparse data or hole boundary after one logical offset.
    pub async fn seek_file_extent(
        &mut self, path: &NamespacePath, offset: u64, target: ExtentSeekTarget,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<Option<u64>> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let accounting = &mut accounting;
            let (lookup, allocation) = self.lookup_no_follow_with_accounting(
                path, accounting, cancellation, false,
            ).await?;
            drop(allocation);
            let record = lookup.record.ok_or(FsError::NotFound)?;
            let value = seek_record_extent_with_accounting(&self.volume, record, offset, target, accounting, cancellation).await?;
            if self.tracks_observations() {
                self.observe_identity_regions_with_accounting(
                    [Some(DependencyRegion::SparseSeek { file_id: record.file_id, offset, target }), None],
                    accounting, cancellation,
                ).await?;
            }
            Ok(value)
        }.await;
        measured(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {

    /// Finds the first sparse data or hole boundary without a namespace binding.
    pub async fn seek<W: FsWork>(
        &self,
        offset: u64,
        target: ExtentSeekTarget,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<u64>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = seek_record_extent_with_accounting(&self.volume, self.record, offset, target, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }
}

pub(super) fn regular_range_regions(
    record: Option<FileRecord>,
    offset: u64,
    length: u64,
) -> [Option<DependencyRegion>; 2] {
    let Some(record) = record else {
        return [None, None];
    };
    let logical_bytes = match record.payload {
        FilePayload::InlineRegular(data) => {
            u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. } => logical_bytes,
        FilePayload::Directory { .. }
        | FilePayload::SymbolicLink { .. }
        | FilePayload::Device { .. }
        | FilePayload::Empty
        | FilePayload::ReparsePoint { .. } => {
            return [Some(DependencyRegion::FileRecord(record.file_id)), None];
        }
    };
    let overlap = logical_bytes.saturating_sub(offset).min(length);
    let content = (overlap != 0).then_some(DependencyRegion::ContentRange {
        file_id: record.file_id,
        offset,
        length: overlap,
    });
    let extends = offset
        .checked_add(length)
        .is_none_or(|end| end > logical_bytes);
    let file_length = extends.then_some(DependencyRegion::FileLength(record.file_id));
    [content, file_length]
}

pub(super) fn validate_planned_file_range(
    logical_bytes: u64,
    range: ByteRange,
) -> Result<(), FsError> {
    let end = range.offset.checked_add(range.length)
        .ok_or(FsError::FileRead(FileRangeReadError::InvalidRange))?;
    if end > logical_bytes {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    Ok(())
}

