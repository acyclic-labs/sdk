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
    (maximum_changes, content): (u32, ContentMerge),
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
                                    match resolve_three(&kinds[0], &kinds[1], &kinds[2]) {
                                        Some(resolved) if resolved != kinds[1] => {
                                            push_extent_replace(
                                                mutations,
                                                (offset, end - offset, resolved),
                                                maximum_changes,
                                                accounting,
                                            )?;
                                        }
                                        Some(_) => {}
                                        // Three different extents can still hold
                                        // bytes only one side changed, as when a
                                        // side rewrote the whole file.
                                        None if content == ContentMerge::Extents => {
                                            return Ok(None);
                                        }
                                        None => {
                                            let span = ContentSpan {
                                                kinds,
                                                offset,
                                                end,
                                            };
                                            if !merge_span_bytes(
                                                store,
                                                &span,
                                                mutations,
                                                maximum_changes,
                                                limits,
                                                accounting,
                                                cancellation,
                                            )
                                            .await?
                                            {
                                                return Ok(None);
                                            }
                                        }
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

fn push_extent_replace(
    mutations: &mut Owned<Vec<ExtentMutation>>,
    (offset, length, kind): (u64, u64, ExtentKind),
    maximum_changes: u32,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if mutations.value.len() >= maximum_changes as usize {
        return Err(FsError::MergeChangeLimit);
    }
    mutations.ensure_for_push(maximum_changes as usize, accounting)?;
    mutations.value.push(ExtentMutation::Replace {
        offset,
        length,
        kind,
        extend: false,
    });
    Ok(())
}

/// Bytes compared per side at a time when three extents differ.
const CONTENT_MERGE_WINDOW: u64 = 64 * 1024;

/// One logical interval where base, ours and theirs map three different
/// extents, each sliced at `offset`.
struct ContentSpan {
    kinds: [ExtentKind; 3],
    offset: u64,
    end: u64,
}

impl ContentSpan {
    /// Blob and blob offset of side `side` at logical `position`.
    fn source(&self, side: usize, position: u64) -> Option<(ObjectId, u64)> {
        let ExtentKind::Content {
            object,
            object_offset,
        } = *self.kinds.get(side)?
        else {
            return None;
        };
        Some((object, object_offset.checked_add(position.checked_sub(self.offset)?)?))
    }

    /// Theirs' extent sliced at logical `position`.
    fn theirs_at(&self, position: u64) -> Result<ExtentKind, FsError> {
        let (object, object_offset) = self.source(2, position).ok_or(FsError::InvalidDiff)?;
        Ok(ExtentKind::Content {
            object,
            object_offset,
        })
    }
}

type WindowRead = Option<(acyclic_stream::OwnedBytes, u64)>;

/// Each side's bytes of one window, with the output charge each read retains.
struct SpanWindow([WindowRead; 3]);

impl SpanWindow {
    /// Reads `length` bytes of each side's blob at logical `position`.
    async fn read<S: AsyncObjectStore>(
        store: &S,
        span: &ContentSpan,
        (position, length): (u64, u64),
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let mut window = Self([None, None, None]);
        for (side, slot) in window.0.iter_mut().enumerate() {
            let read = match span.source(side, position) {
                Some((object, offset)) => {
                    super::blob::read_blob_range_with_accounting(
                        store,
                        object,
                        ByteRange { offset, length },
                        limits,
                        accounting,
                        cancellation,
                        true,
                    )
                    .await
                }
                None => Err(FsError::InvalidDiff),
            };
            let read = read.and_then(|read| {
                if crate::foundation::usize_to_u64(read.bytes.len()) == length {
                    Ok(read)
                } else {
                    Err(FsError::InvalidDiff)
                }
            });
            match read {
                Ok(read) => *slot = Some((read.bytes, read.logical_owned_bytes)),
                Err(error) => {
                    window.release(accounting)?;
                    return Err(error);
                }
            }
        }
        Ok(window)
    }

    /// Base, ours and theirs bytes of the window.
    fn sides(&self) -> Option<[&[u8]; 3]> {
        let [Some((base, _)), Some((ours, _)), Some((theirs, _))] = &self.0 else {
            return None;
        };
        Some([base.as_ref(), ours.as_ref(), theirs.as_ref()])
    }

    fn release(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        self.0.into_iter().flatten().try_for_each(|(bytes, retained)| {
            drop(bytes);
            super::file_read::release_standalone_read_bytes(retained, accounting)
        })
    }
}

/// Merges one interval whose three extents all differ by comparing bytes:
/// a byte theirs left as base, or both sides wrote alike, keeps ours; a
/// byte only theirs changed takes theirs; a byte both changed differently
/// conflicts. Theirs' runs become replacements sliced from theirs' extent.
/// Returns false on a conflict, or when an extent is not stored content.
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
async fn merge_span_bytes<S: AsyncObjectStore>(
    store: &S,
    span: &ContentSpan,
    mutations: &mut Owned<Vec<ExtentMutation>>,
    maximum_changes: u32,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    if (0..3).any(|side| span.source(side, span.offset).is_none()) {
        return Ok(false);
    }
    let mut theirs_from: Option<u64> = None;
    let mut position = span.offset;
    while position < span.end {
        cancellation.check()?;
        let length = (span.end - position).min(CONTENT_MERGE_WINDOW);
        let window = SpanWindow::read(
            store,
            span,
            (position, length),
            limits,
            accounting,
            cancellation,
        )
        .await?;
        let compared = (|| -> Result<bool, FsError> {
            let [base, ours, theirs] = window.sides().ok_or(FsError::InvalidDiff)?;
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let sides = base.iter().zip(ours.iter()).zip(theirs.iter());
            for (at, ((base, ours), theirs)) in (position..).zip(sides) {
                let Some(takes_theirs) = byte_takes_theirs(*base, *ours, *theirs) else {
                    return Ok(false);
                };
                match (takes_theirs, theirs_from) {
                    (true, None) => theirs_from = Some(at),
                    (false, Some(from)) => {
                        theirs_from = None;
                        let replaced = (from, at - from, span.theirs_at(from)?);
                        push_extent_replace(mutations, replaced, maximum_changes, accounting)?;
                    }
                    _ => {}
                }
            }
            Ok(true)
        })();
        window.release(accounting)?;
        if !compared? {
            return Ok(false);
        }
        position += length;
    }
    if let Some(from) = theirs_from {
        let kind = span.theirs_at(from)?;
        push_extent_replace(
            mutations,
            (from, span.end - from, kind),
            maximum_changes,
            accounting,
        )?;
    }
    Ok(true)
}

/// The three-way rule for one byte: `Some(false)` keeps ours (theirs left
/// it as base, or both wrote it alike), `Some(true)` takes theirs (only
/// theirs changed it), and `None` is a conflict (both changed it
/// differently).
fn byte_takes_theirs(base: u8, ours: u8, theirs: u8) -> Option<bool> {
    if ours == theirs || theirs == base {
        Some(false)
    } else if ours == base {
        Some(true)
    } else {
        None
    }
}

/// Merges three tiny regular files of one length byte by byte, the same
/// rule extent-backed content uses where its extents all differ.
async fn merge_inline_record_async<S: AsyncObjectStore>(
    store: &S,
    [base, ours, theirs]: [FileRecord; 3],
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let (
        FilePayload::InlineRegular(base_data),
        FilePayload::InlineRegular(ours_data),
        FilePayload::InlineRegular(theirs_data),
    ) = (base.payload, ours.payload, theirs.payload)
    else {
        return Ok(None);
    };
    let [base_bytes, ours_bytes, theirs_bytes] =
        [base_data.as_bytes(), ours_data.as_bytes(), theirs_data.as_bytes()];
    if base_bytes.len() != ours_bytes.len() || base_bytes.len() != theirs_bytes.len() {
        return Ok(None);
    }
    let Some(kind) = resolve_three(&base.kind, &ours.kind, &theirs.kind) else {
        return Ok(None);
    };
    accounting.charge(WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let mut merged = [0_u8; super::file_table::MAXIMUM_INLINE_FILE_BYTES];
    let sides = base_bytes.iter().zip(ours_bytes).zip(theirs_bytes);
    for (out, ((base, ours), theirs)) in merged.iter_mut().zip(sides) {
        let Some(takes_theirs) = byte_takes_theirs(*base, *ours, *theirs) else {
            return Ok(None);
        };
        *out = if takes_theirs { *theirs } else { *ours };
    }
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
    let merged = merged
        .get(..ours_bytes.len())
        .ok_or(FsError::InvalidDiff)
        .and_then(|bytes| super::file_table::InlineFileData::new(bytes).map_err(|_| FsError::InvalidDiff))?;
    Ok(Some(FileRecord {
        file_id: ours.file_id,
        kind,
        link_count: ours.link_count,
        metadata,
        payload: FilePayload::InlineRegular(merged),
    }))
}
