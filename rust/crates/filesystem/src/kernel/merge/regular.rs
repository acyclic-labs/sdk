fn is_extent_regular(record: Option<FileRecord>) -> bool {
    matches!(
        record.map(|record| record.payload),
        Some(FilePayload::Regular { .. })
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
#[allow(
    clippy::too_many_lines,
    reason = "The merge frame retains both sides and their admitted data until the merged record is complete."
)]
fn merge_regular_record_async<'a, 'accounting, S: AsyncObjectStore>(
    store: &'a S,
    base: FileRecord,
    ours: FileRecord,
    theirs: FileRecord,
    maximum_changes: u32,
    limits: DecodeLimits,
    accounting: &'a mut KernelAccounting<'accounting>,
    cancellation: &'a CancellationToken,
) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<(FileRecord, u32)>, FsError>> {
    Box::pin(async move {
        let Some(kind) = resolve_three(&base.kind, &ours.kind, &theirs.kind) else {
            return Ok(None);
        };
        let Some(metadata) = merge_metadata_async(
            store,
            Some(base.metadata),
            ours.metadata,
            theirs.metadata,
            limits,
            accounting,
            cancellation,
        )
        .await?
        else {
            return Ok(None);
        };
        let (
            FilePayload::Regular {
                logical_bytes: base_bytes,
                extents: base_root,
            },
            FilePayload::Regular {
                logical_bytes: ours_bytes,
                extents: ours_root,
            },
            FilePayload::Regular {
                logical_bytes: theirs_bytes,
                extents: theirs_root,
            },
        ) = (base.payload, ours.payload, theirs.payload)
        else {
            return Ok(None);
        };
        let Some(logical_bytes) = resolve_three(&base_bytes, &ours_bytes, &theirs_bytes) else {
            return Ok(None);
        };
        if base_bytes != ours_bytes || base_bytes != theirs_bytes || logical_bytes == 0 {
            return Ok(None);
        }
        let request = move |root| ExtentRangeRequest {
            root,
            file_size: logical_bytes,
            range: ByteRange {
                offset: 0,
                length: logical_bytes,
            },
            maximum_spans: maximum_changes,
            limits,
            budget: WorkBudget::UNBOUNDED,
        };
        let (base_plan, allocation) =
            plan_extent_range_with_accounting(store, request(base_root), accounting, cancellation)
                .await?;
        let bytes = base_plan.retained_allocation_bytes;
        let base_plan = Owned::new(base_plan, allocation, bytes);
        let result = {
            let base_plan = &base_plan;
            let accounting = &mut *accounting;
            async move {
                let (ours_plan, allocation) = plan_extent_range_with_accounting(
                    store,
                    request(ours_root),
                    accounting,
                    cancellation,
                )
                .await?;
                let bytes = ours_plan.retained_allocation_bytes;
                let ours_plan = Owned::new(ours_plan, allocation, bytes);
                let result = {
                    let ours_plan = &ours_plan;
                    let accounting = &mut *accounting;
                    async move {
                        let (theirs_plan, allocation) = plan_extent_range_with_accounting(
                            store,
                            request(theirs_root),
                            accounting,
                            cancellation,
                        )
                        .await?;
                        let bytes = theirs_plan.retained_allocation_bytes;
                        let theirs_plan = Owned::new(theirs_plan, allocation, bytes);
                        let mut mutations = Owned::allocate(0, accounting)?;
                        let result = {
                            let theirs_plan = &theirs_plan;
                            let mutations = &mut mutations;
                            let accounting = &mut *accounting;
                            async move {
                                let plans = [
                                    &base_plan.value.spans,
                                    &ours_plan.value.spans,
                                    &theirs_plan.value.spans,
                                ];
                                let mut indices = [0_usize; 3];
                                let mut offset = 0;
                                while offset < logical_bytes {
                                    accounting.charge(WorkCounters {
                                        items_examined: 1,
                                        ..WorkCounters::UNCHARGED
                                    })?;
                                    let spans = [
                                        plans[0].get(indices[0]).ok_or(FsError::InvalidDiff)?,
                                        plans[1].get(indices[1]).ok_or(FsError::InvalidDiff)?,
                                        plans[2].get(indices[2]).ok_or(FsError::InvalidDiff)?,
                                    ];
                                    let end = spans
                                        .iter()
                                        .map(|span| span.offset.saturating_add(span.length))
                                        .min()
                                        .ok_or(FsError::InvalidDiff)?;
                                    if end <= offset {
                                        return Err(FsError::InvalidDiff);
                                    }
                                    let kinds = [
                                        sliced_extent_kind(spans[0], offset)?,
                                        sliced_extent_kind(spans[1], offset)?,
                                        sliced_extent_kind(spans[2], offset)?,
                                    ];
                                    let Some(resolved) =
                                        resolve_three(&kinds[0], &kinds[1], &kinds[2])
                                    else {
                                        return Ok(None);
                                    };
                                    if resolved != kinds[1] {
                                        if mutations.value.len() >= maximum_changes as usize {
                                            return Err(FsError::MergeChangeLimit);
                                        }
                                        mutations.ensure_for_push(
                                            maximum_changes as usize,
                                            accounting,
                                        )?;
                                        mutations.value.push(ExtentMutation::Replace {
                                            offset,
                                            length: end - offset,
                                            kind: resolved,
                                            extend: false,
                                        });
                                    }
                                    offset = end;
                                    for (index, span) in indices.iter_mut().zip(spans) {
                                        if span.offset + span.length == end {
                                            *index += 1;
                                        }
                                    }
                                }
                                let root = if mutations.value.is_empty() {
                                    ours_root
                                } else {
                                    apply_extent_mutations_with_accounting(
                                        store,
                                        (ours_root, ours_bytes),
                                        &mutations.value,
                                        maximum_changes,
                                        limits,
                                        accounting,
                                        cancellation,
                                    )
                                    .await?
                                    .root
                                };
                                Ok(Some((
                                    root,
                                    u32::try_from(mutations.value.len())
                                        .map_err(|_| FsError::MergeChangeLimit)?,
                                )))
                            }
                        }
                        .await;
                        mutations.retire(accounting)?;
                        theirs_plan.retire(accounting)?;
                        result
                    }
                }
                .await;
                ours_plan.retire(accounting)?;
                result
            }
        }
        .await;
        base_plan.retire(accounting)?;
        result.map(|merged| {
            merged.map(|(root, count)| {
                (
                    FileRecord {
                        file_id: ours.file_id,
                        kind,
                        link_count: ours.link_count,
                        metadata,
                        payload: FilePayload::Regular {
                            logical_bytes,
                            extents: root,
                        },
                    },
                    count,
                )
            })
        })
    })
}

fn sliced_extent_kind(span: &ExtentSlice, offset: u64) -> Result<ExtentKind, FsError> {
    let delta = offset
        .checked_sub(span.offset)
        .ok_or(FsError::InvalidDiff)?;
    Ok(match span.kind {
        ExtentKind::Hole => ExtentKind::Hole,
        ExtentKind::AllocatedZero => ExtentKind::AllocatedZero,
        ExtentKind::Content {
            object,
            object_offset,
        } => ExtentKind::Content {
            object,
            object_offset: object_offset
                .checked_add(delta)
                .ok_or(FsError::InvalidDiff)?,
        },
    })
}
