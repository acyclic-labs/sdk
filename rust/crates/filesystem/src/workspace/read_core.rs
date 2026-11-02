use super::*;

pub(super) async fn lookup_paths<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    paths: &[NamespacePath],
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(crate::kernel::PathBatchLookup, crate::AuthorityAllocation), WorkspaceError> {
    let mut scope = accounting.allocation_scope()?;
    let result = async {
        let mut checkout = workspace
            .open_checkout(
                selector,
                CheckoutMode::read_only_pinned(),
                &mut scope,
                cancellation,
            )
            .await?;
        let (lookup, allocation) = checkout
            .value
            .lookup_batch_no_follow_with_accounting::<true>(paths, false, &mut scope, cancellation)
            .await?;
        checkout.retire(&mut scope)?;
        let allocation = workspace_allocation(allocation, &scope)?;
        Ok((lookup, allocation))
    }
    .await;
    let result = authorize(workspace, result, &mut scope, cancellation).await;
    if result.is_ok() {
        scope.keep();
    } else {
        drop(scope);
    }
    result.map(|(mut lookup, allocation)| {
        lookup.work = accounting.spent();
        (lookup, allocation)
    })
}

pub(super) async fn read_selected<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    W: crate::facade::FsWork,
>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    maximum_bytes: Option<u64>,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let mut scope = accounting.allocation_scope()?;
        let result = async {
            let (mut checkout, path) = workspace
                .open_read_path(selector, path, &mut scope, cancellation)
                .await?;
            let lookup = {
                let (lookup, _allocation) = checkout
                    .value
                    .lookup_no_follow_with_accounting(&path.value, &mut scope, cancellation, true)
                    .await?;
                lookup
            };
            let record = lookup.record.ok_or(WorkspaceError::NotFound)?;
            if record.kind != FileKind::Regular {
                return Err(WorkspaceError::NotRegularFile);
            }
            let length = regular_file_bytes(record)?;
            if maximum_bytes.is_some_and(|maximum| length > maximum) {
                return Err(WorkspaceError::ReadLimitExceeded);
            }
            let range = crate::ByteRange { offset: 0, length };
            let read = checkout
                .value
                .read_record_range_with_accounting(record, range, range, &mut scope, cancellation)
                .await?;
            path.retire(&mut scope)?;
            checkout.retire(&mut scope)?;
            Ok::<_, WorkspaceError>(read.bytes)
        }
        .await;
        let result = authorize(workspace, result, &mut scope, cancellation).await;
        if result.is_ok() {
            scope.keep();
        }
        result
    }
    .await;
    work.finish(result, &accounting)
}

pub(super) async fn read_range<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
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
        let mut scope = accounting.allocation_scope()?;
        let result = async {
            let (mut checkout, path) = workspace
                .open_read_path(selector, path, &mut scope, cancellation)
                .await?;
            let record = checkout
                .value
                .lookup_no_follow_with_accounting(&path.value, &mut scope, cancellation, true)
                .await?
                .0
                .record
                .ok_or(WorkspaceError::NotFound)?;
            if let Some(original) = unchanged_since {
                let mut original = workspace
                    .open_checkout(
                        GenerationSelector::Exact(original),
                        CheckoutMode::read_only_pinned(),
                        &mut scope,
                        cancellation,
                    )
                    .await?;
                let expected = original
                    .value
                    .lookup_no_follow_with_accounting(&path.value, &mut scope, cancellation, false)
                    .await?
                    .0
                    .record
                    .ok_or(WorkspaceError::NotFound)?;
                let matches = expected.kind == record.kind && expected.payload == record.payload;
                original.retire(&mut scope)?;
                if !matches {
                    return Err(WorkspaceError::StaleIdentity);
                }
            }
            let read = checkout
                .value
                .read_record_range_with_accounting(record, range, range, &mut scope, cancellation)
                .await?;
            let bytes = read.logical_owned_bytes();
            path.retire(&mut scope)?;
            checkout.retire(&mut scope)?;
            Ok::<_, WorkspaceError>(crate::kernel::OwnedSourceChunk::claimed(read.bytes, bytes))
        }
        .await;
        let result = authorize(workspace, result, &mut scope, cancellation).await;
        if result.is_ok() {
            scope.keep();
        }
        result
    }
    .await;
    work.finish(result, &accounting)
}

pub(super) async fn matches_content<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    W: crate::facade::FsWork,
>(
    workspace: &Workspace<A, O>,
    selected: GenerationId,
    original: GenerationId,
    path: &str,
    exact_payload: bool,
    allow_absent: bool,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<bool, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let mut scope = accounting.allocation_scope()?;
        let result = async {
            let (mut checkout, path) = workspace
                .open_read_path(
                    GenerationSelector::Exact(selected),
                    path,
                    &mut scope,
                    cancellation,
                )
                .await?;
            let record = checkout
                .value
                .lookup_no_follow_with_accounting(&path.value, &mut scope, cancellation, false)
                .await?
                .0
                .record;
            let mut original = workspace
                .open_checkout(
                    GenerationSelector::Exact(original),
                    CheckoutMode::read_only_pinned(),
                    &mut scope,
                    cancellation,
                )
                .await?;
            let expected = original
                .value
                .lookup_no_follow_with_accounting(&path.value, &mut scope, cancellation, false)
                .await?
                .0
                .record;
            let matches = match (record, expected) {
                (Some(record), Some(expected)) if exact_payload => {
                    record.file_id == expected.file_id
                        && record.kind == expected.kind
                        && record.payload == expected.payload
                }
                (Some(record), Some(expected)) => {
                    crate::kernel::records_match_content(
                        workspace.volume.fs.objects(),
                        record,
                        expected,
                        workspace.volume_config(),
                        &mut scope,
                        cancellation,
                    )
                    .await?
                }
                (None, None) => allow_absent,
                _ => false,
            };
            original.retire(&mut scope)?;
            checkout.retire(&mut scope)?;
            Ok(matches)
        }
        .await;
        authorize(workspace, result, &mut scope, cancellation).await
    }
    .await;
    work.finish(result, &accounting)
}

pub(super) async fn read_link<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
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
        let mut scope = accounting.allocation_scope()?;
        let result = async {
            let (mut checkout, path) = workspace
                .open_read_path(selector, path, &mut scope, cancellation)
                .await?;
            let read = checkout
                .value
                .read_symbolic_link_with_accounting(&path.value, &mut scope, cancellation)
                .await?;
            path.retire(&mut scope)?;
            checkout.retire(&mut scope)?;
            Ok::<_, WorkspaceError>(read)
        }
        .await;
        let result = authorize(workspace, result, &mut scope, cancellation).await;
        if result.is_ok() {
            scope.keep();
        }
        result
    }
    .await;
    work.finish(result, &accounting)
}

async fn lookup_stat<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
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

pub(super) async fn stat<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    W: crate::facade::FsWork,
>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<WorkspaceStat, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let mut scope = accounting.allocation_scope()?;
        let result = lookup_stat(workspace, selector, path, &mut scope, cancellation)
            .await
            .and_then(|value| value.ok_or(WorkspaceError::NotFound));
        authorize(workspace, result, &mut scope, cancellation).await
    }
    .await;
    work.finish(result, &accounting)
}

pub(super) async fn list<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    W: crate::facade::FsWork,
>(
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
        let mut scope = accounting.allocation_scope()?;
        let result = async {
            let (mut checkout, path) = workspace
                .open_read_path(selector, path, &mut scope, cancellation)
                .await?;
            let (page, allocation) = checkout
                .value
                .list_directory_with_accounting(
                    &path.value,
                    after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    maximum,
                    &mut scope,
                    cancellation,
                )
                .await?;
            path.retire(&mut scope)?;
            checkout.retire(&mut scope)?;
            let allocation = workspace_allocation(allocation, &scope)?;
            Ok::<_, WorkspaceError>((page, allocation))
        }
        .await;
        let result = authorize(workspace, result, &mut scope, cancellation).await;
        if result.is_ok() {
            scope.keep();
        }
        result
    }
    .await;
    let result = result.map(|(mut page, allocation)| {
        page.work = accounting.spent();
        (page, allocation)
    });
    work.finish(result, &accounting)
}

pub(super) async fn plan<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    W: crate::facade::FsWork,
>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    maximum_spans: u32,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<(WorkspaceExtentPlan, crate::AuthorityAllocation), WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let mut scope = accounting.allocation_scope()?;
        let result = async {
            let (mut checkout, path) = workspace
                .open_read_path(selector, path, &mut scope, cancellation)
                .await?;
            let (plan, allocation) = checkout
                .value
                .plan_file_extents_projected_with_accounting(
                    &path.value,
                    crate::ByteRange { offset, length },
                    Some(maximum_spans),
                    |span| WorkspaceExtentSpan {
                        offset: span.offset,
                        length: span.length,
                        source_end: span.source_end,
                        kind: match span.kind {
                            ExtentKind::Hole => WorkspaceExtentKind::Hole,
                            ExtentKind::AllocatedZero => WorkspaceExtentKind::AllocatedZero,
                            ExtentKind::Content { .. } => WorkspaceExtentKind::Content,
                        },
                    },
                    &mut scope,
                    cancellation,
                )
                .await?;
            let spans = match plan {
                Some(plan) => crate::kernel::Owned::new(
                    plan.spans,
                    allocation,
                    plan.retained_allocation_bytes,
                ),
                None => {
                    drop(allocation);
                    let mut spans = crate::kernel::Owned::<Vec<WorkspaceExtentSpan>>::allocate(
                        usize::from(length != 0),
                        &mut scope,
                    )?;
                    if length != 0 {
                        let returned = crate::WorkCounters {
                            items_returned: 1,
                            ..crate::WorkCounters::UNCHARGED
                        };
                        scope.admit(returned)?;
                        let source_end = offset
                            .checked_add(length)
                            .ok_or(crate::WorkError::Overflow)?;
                        spans.value.push(WorkspaceExtentSpan {
                            offset,
                            length,
                            source_end,
                            kind: WorkspaceExtentKind::Content,
                        });
                        scope.record_completed(returned)?;
                    }
                    spans
                }
            };
            path.retire(&mut scope)?;
            checkout.retire(&mut scope)?;
            let allocation = workspace_allocation(spans.allocation, &scope)?;
            Ok::<_, WorkspaceError>((WorkspaceExtentPlan { spans: spans.value }, allocation))
        }
        .await;
        let result = authorize(workspace, result, &mut scope, cancellation).await;
        if result.is_ok() {
            scope.keep();
        }
        result
    }
    .await;
    work.finish(result, &accounting)
}

async fn authorize<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static, T>(
    workspace: &Workspace<A, O>,
    result: Result<T, WorkspaceError>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<T, WorkspaceError> {
    let authorization = workspace
        .check_current_access(accounting, cancellation)
        .await;
    authorization.and(result)
}

#[cfg(test)]
mod tests;
