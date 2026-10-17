use super::*;

pub(super) async fn read_selected<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    W: crate::facade::FsWork,
>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    maximum_bytes: u64,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace
            .open_read_path(selector, path, &mut accounting, cancellation)
            .await?;
        let lookup = {
            let (lookup, _allocation) = checkout
                .value
                .lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, true)
                .await?;
            lookup
        };
        let record = lookup.record.ok_or(WorkspaceError::NotFound)?;
        if record.kind != FileKind::Regular {
            return Err(WorkspaceError::NotRegularFile);
        }
        let length = regular_file_bytes(record)?;
        if length > maximum_bytes {
            return Err(WorkspaceError::ReadLimitExceeded);
        }
        if length > checkout.value.volume_config().limits.maximum_read_bytes {
            return Err(
                crate::FsError::FileRead(crate::kernel::FileRangeReadError::InvalidRange).into(),
            );
        }
        let range = crate::ByteRange { offset: 0, length };
        checkout
            .value
            .read_record_range_with_accounting(record, range, range, &mut accounting, cancellation)
            .await
            .map(|read| read.bytes)
            .map_err(WorkspaceError::from)
    }
    .await;
    work.finish(result, &accounting)
}

pub(super) async fn read_range<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    W: crate::facade::FsWork,
>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    range: crate::ByteRange,
    unchanged_since: Option<GenerationId>,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let unchanged_since =
        unchanged_since.filter(|original| selector != GenerationSelector::Exact(*original));
    let result = async {
        let (mut checkout, path) = workspace
            .open_read_path(selector, path, &mut accounting, cancellation)
            .await?;
        if range.length > checkout.value.volume_config().limits.maximum_read_bytes {
            return Err(FsError::FileRead(crate::kernel::FileRangeReadError::InvalidRange).into());
        }
        let record = checkout
            .value
            .lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, true)
            .await?
            .0
            .record
            .ok_or(WorkspaceError::NotFound)?;
        if let Some(original) = unchanged_since {
            let mut original = workspace
                .open_checkout(
                    GenerationSelector::Exact(original),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let expected = original
                .value
                .lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, false)
                .await?
                .0
                .record
                .ok_or(WorkspaceError::NotFound)?;
            let matches = expected.kind == record.kind && expected.payload == record.payload;
            original.retire(&mut accounting)?;
            if !matches {
                return Err(WorkspaceError::StaleIdentity);
            }
        }
        let read = checkout
            .value
            .read_record_range_with_accounting(record, range, range, &mut accounting, cancellation)
            .await?;
        let bytes = read.logical_owned_bytes();
        Ok::<_, WorkspaceError>(crate::kernel::OwnedSourceChunk::claimed(read.bytes, bytes))
    }
    .await;
    work.finish(result, &accounting)
}

pub(super) async fn read_link<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    W: crate::facade::FsWork,
>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace
            .open_read_path(selector, path, &mut accounting, cancellation)
            .await?;
        checkout
            .value
            .read_symbolic_link_with_accounting(&path.value, &mut accounting, cancellation)
            .await
            .map_err(WorkspaceError::from)
    }
    .await;
    work.finish(result, &accounting)
}

async fn lookup_stat<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<WorkspaceStat>, WorkspaceError> {
    let (mut checkout, path) = workspace
        .open_read_path(selector, path, accounting, cancellation)
        .await?;
    checkout
        .value
        .lookup_no_follow_with_metadata_with_accounting(&path.value, accounting, cancellation)
        .await?
        .map(|lookup| {
            let logical_bytes = match lookup.record.payload {
                FilePayload::InlineRegular(bytes) => Some(
                    u64::try_from(bytes.as_bytes().len())
                        .map_err(|_| crate::WorkError::Overflow)?,
                ),
                FilePayload::Regular { logical_bytes, .. } => Some(logical_bytes),
                FilePayload::SymbolicLink { target_bytes, .. } => Some(target_bytes),
                FilePayload::ReparsePoint { payload_bytes, .. } => Some(payload_bytes),
                FilePayload::Directory { .. } | FilePayload::Device { .. } | FilePayload::Empty => {
                    None
                }
            };
            Ok(WorkspaceStat {
                file_id: lookup.record.file_id,
                kind: lookup.record.kind,
                link_count: lookup.record.link_count,
                logical_bytes,
                metadata: WorkspaceMetadata::from_engine(lookup.metadata),
            })
        })
        .transpose()
}

pub(super) async fn stat<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<WorkspaceStat, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = lookup_stat(workspace, selector, path, &mut accounting, cancellation)
        .await
        .and_then(|value| value.ok_or(WorkspaceError::NotFound));
    work.finish(result, &accounting)
}

pub(super) async fn list<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    after: Option<&LogicalName>,
    maximum: u32,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace
            .open_read_path(selector, path, &mut accounting, cancellation)
            .await?;
        let (page, allocation) = checkout
            .value
            .list_directory_with_accounting(
                &path.value,
                after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                maximum,
                &mut accounting,
                cancellation,
            )
            .await?;
        let allocation = workspace_allocation(allocation, &accounting)?;
        Ok::<_, WorkspaceError>((page, allocation))
    }
    .await;
    work.finish(result, &accounting)
}

pub(super) async fn plan<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    maximum_spans: u32,
) -> Result<WorkspaceExtentPlan, WorkspaceError> {
    let mut checkout = workspace
        .checkout(
            selector,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let path = customer_path(path, checkout.volume_config())?;
    let plan = checkout
        .plan_file_extents(
            &path,
            crate::ByteRange { offset, length },
            maximum_spans,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?
        .value;
    let spans = match plan {
        Some(plan) => plan
            .spans
            .into_iter()
            .map(|span| WorkspaceExtentSpan {
                offset: span.offset,
                length: span.length,
                source_end: span.source_end,
                kind: match span.kind {
                    ExtentKind::Hole => WorkspaceExtentKind::Hole,
                    ExtentKind::AllocatedZero => WorkspaceExtentKind::AllocatedZero,
                    ExtentKind::Content { .. } => WorkspaceExtentKind::Content,
                },
            })
            .collect(),
        None if length == 0 => Vec::new(),
        None => vec![WorkspaceExtentSpan {
            offset,
            length,
            source_end: offset
                .checked_add(length)
                .ok_or_else(|| WorkspaceError::Engine("file range overflows".to_owned()))?,
            kind: WorkspaceExtentKind::Content,
        }],
    };
    Ok(WorkspaceExtentPlan { spans })
}

#[cfg(test)]
mod tests;
