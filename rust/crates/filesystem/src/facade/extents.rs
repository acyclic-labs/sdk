use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ResolvedFile<A, O> {
    /// Plans sparse physical spans without another namespace lookup.
    pub async fn plan_extents<W: FsWork>(
        &self,
        range: ByteRange,
        maximum_spans: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Option<ExtentPlan>, crate::FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut accounting = accounting.allocation_scope()?;
            let result = async {
                validate_extent_plan_request(self.reader.volume.config, range, maximum_spans)?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    self.reader.validate_native_work(original)?;
                }
                plan_record_extents_with_accounting(
                    &self.reader.volume,
                    self.record,
                    range,
                    maximum_spans,
                    std::convert::identity,
                    &mut accounting,
                    cancellation,
                )
                .await
            }
            .await;
            if result.is_ok() {
                accounting.keep();
            }
            result
        }
        .await;
        work.finish_owned::<_, crate::FsError>(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {
    /// Plans sparse physical spans without reading content bodies.
    pub async fn plan_file_extents<W: FsWork>(
        &self,
        path: &NamespacePath,
        range: ByteRange,
        maximum_spans: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Option<ExtentPlan>, crate::FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut accounting = accounting.allocation_scope()?;
            let result = async {
                validate_extent_plan_request(self.volume.config, range, maximum_spans)?;
                let record = self
                    .resolve_file(path, &mut accounting, cancellation)
                    .await?;
                plan_record_extents_with_accounting(
                    &self.volume,
                    record,
                    range,
                    maximum_spans,
                    std::convert::identity,
                    &mut accounting,
                    cancellation,
                )
                .await
            }
            .await;
            if result.is_ok() {
                accounting.keep();
            }
            result
        }
        .await;
        work.finish_owned::<_, crate::FsError>(result, &accounting)
    }
}

fn validate_extent_plan_request(
    config: VolumeConfig,
    range: ByteRange,
    maximum_spans: u32,
) -> Result<(), FsError> {
    if maximum_spans == 0 || range.length > config.limits.maximum_generation_bytes {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    Ok(())
}

async fn plan_record_extents_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore, T>(
    volume: &Volume<A, O>,
    record: FileRecord,
    range: ByteRange,
    maximum_spans: u32,
    project: impl Fn(crate::kernel::ExtentSlice) -> T,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Option<ExtentPlan<T>>, KernelOwnedAllocation), FsError> {
    accounting.check()?;
    let (logical_bytes, root) = match record.payload {
        FilePayload::InlineRegular(data) => {
            validate_planned_file_range(usize_to_u64(data.as_bytes().len()), range)?;
            return Ok((None, accounting.claim_owned(0, 0)?));
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => (logical_bytes, extents),
        _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
    };
    let (plan, allocation) = ExtentPlan::project_with_accounting(
        &volume.fs.inner.objects,
        ExtentRangeRequest {
            root,
            file_size: logical_bytes,
            range,
            maximum_spans,
            limits: DecodeLimits::for_volume(volume.config),
            budget: accounting.remaining()?,
        },
        project,
        accounting,
        cancellation,
    )
    .await?;
    Ok((Some(plan), allocation))
}

pub(crate) async fn seek_record_extent_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    volume: &Volume<A, O>,
    record: FileRecord,
    offset: u64,
    target: ExtentSeekTarget,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<u64>, FsError> {
    accounting.check()?;
    match record.payload {
        FilePayload::InlineRegular(data) => {
            let logical_bytes = data.as_bytes().len() as u64;
            if offset > logical_bytes {
                return Ok(None);
            }
            Ok(match target {
                ExtentSeekTarget::Data => (offset < logical_bytes).then_some(offset),
                ExtentSeekTarget::Hole => Some(logical_bytes),
            })
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            if offset > logical_bytes {
                return Ok(None);
            }
            crate::kernel::seek_extent_with_accounting(
                &volume.fs.inner.objects,
                ExtentSeekRequest {
                    root: extents,
                    file_size: logical_bytes,
                    offset,
                    target,
                    limits: DecodeLimits::for_volume(volume.config),
                    budget: accounting.remaining()?,
                },
                accounting,
                cancellation,
            )
            .await
        }
        _ => Err(FsError::FileRead(FileRangeReadError::NotRegular)),
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Checkout<A, O> {
    pub(crate) async fn plan_record_extents(
        &mut self,
        record: FileRecord,
        range: ByteRange,
        maximum_spans: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Option<ExtentPlan>, KernelOwnedAllocation), FsError> {
        self.plan_record_extents_projected(
            record,
            range,
            maximum_spans,
            std::convert::identity,
            accounting,
            cancellation,
        )
        .await
    }

    async fn plan_record_extents_projected<T>(
        &mut self,
        record: FileRecord,
        range: ByteRange,
        maximum_spans: u32,
        project: impl Fn(crate::kernel::ExtentSlice) -> T,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Option<ExtentPlan<T>>, KernelOwnedAllocation), FsError> {
        let mut planned = plan_record_extents_with_accounting(
            &self.volume,
            record,
            range,
            maximum_spans,
            project,
            accounting,
            cancellation,
        )
        .await?;
        if self.tracks_observations() && range.length != 0 {
            self.observe_base_regular_range_with_accounting(
                record.file_id,
                range,
                accounting,
                cancellation,
            )
            .await?;
        }
        if let Some(plan) = &mut planned.0 {
            plan.work = accounting.spent();
        }
        Ok(planned)
    }

    /// Plans the sparse physical representation of one exact regular-file range.
    pub async fn plan_file_extents<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        range: ByteRange,
        maximum_spans: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<Option<ExtentPlan>, crate::FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut accounting = accounting.allocation_scope()?;
            let result = self
                .plan_file_extents_projected_with_accounting(
                    &file,
                    range,
                    maximum_spans,
                    std::convert::identity,
                    &mut accounting,
                    cancellation,
                )
                .await;
            if result.is_ok() {
                accounting.keep();
            }
            result
        }
        .await;
        work.finish_owned::<_, crate::FsError>(result, &accounting)
    }

    pub(crate) async fn plan_file_extents_projected_with_accounting<F: ContentFile, T>(
        &mut self,
        file: &F,
        range: ByteRange,
        maximum_spans: u32,
        project: impl Fn(crate::kernel::ExtentSlice) -> T,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Option<ExtentPlan<T>>, KernelOwnedAllocation), FsError> {
        validate_extent_plan_request(self.volume.config, range, maximum_spans)?;
        let record = self
            .lookup_content_record(file, accounting, cancellation)
            .await?;
        self.plan_record_extents_projected(
            record,
            range,
            maximum_spans,
            project,
            accounting,
            cancellation,
        )
        .await
    }

    /// Finds the first sparse data or hole boundary after one logical offset.
    pub async fn seek_file_extent<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        offset: u64,
        target: ExtentSeekTarget,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<u64>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let record = self
                .lookup_content_record(&file, &mut accounting, cancellation)
                .await?;
            let value = seek_record_extent_with_accounting(
                &self.volume,
                record,
                offset,
                target,
                &mut accounting,
                cancellation,
            )
            .await?;
            if self.tracks_observations() {
                self.observe_identity_regions_with_accounting(
                    [
                        Some(DependencyRegion::SparseSeek {
                            file_id: record.file_id,
                            offset,
                            target,
                        }),
                        None,
                    ],
                    &mut accounting,
                    cancellation,
                )
                .await?;
            }
            Ok(value)
        }
        .await;
        budget.finish(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> DetachedFile<A, O> {
    /// Finds the first sparse data or hole boundary without a namespace binding.
    pub async fn seek<W: FsWork>(
        &self,
        offset: u64,
        target: ExtentSeekTarget,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<u64>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = seek_record_extent_with_accounting(
            &self.volume,
            self.record,
            offset,
            target,
            &mut accounting,
            cancellation,
        )
        .await;
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
    let end = range
        .offset
        .checked_add(range.length)
        .ok_or(FsError::FileRead(FileRangeReadError::InvalidRange))?;
    if end > logical_bytes {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    Ok(())
}
