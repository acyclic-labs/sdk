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
        let lengths = [base_bytes, ours_bytes, theirs_bytes];
        // Extent identity compares one interval of three equal files; content
        // also merges a length one side changed, past which a side is absent.
        let resized = lengths.iter().any(|length| *length != logical_bytes);
        if lengths.contains(&0) || (resized && content == ContentMerge::Extents) {
            return Ok(None);
        }
        let request = move |root, length| ExtentRangeRequest {
            root,
            file_size: length,
            range: ByteRange { offset: 0, length },
            maximum_spans: maximum_changes,
            limits,
            budget: WorkBudget::UNBOUNDED,
        };
        let (base_plan, allocation) =
            plan_extent_range_with_accounting(store, request(base_root, base_bytes), accounting, cancellation)
                .await?;
        let bytes = base_plan.retained_allocation_bytes;
        let base_plan = Owned::new(base_plan, allocation, bytes);
        let result = {
            let base_plan = &base_plan;
            let accounting = &mut *accounting;
            async move {
                let (ours_plan, allocation) = plan_extent_range_with_accounting(
                    store,
                    request(ours_root, ours_bytes),
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
                            request(theirs_root, theirs_bytes),
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
                                    base_plan.value.spans.as_slice(),
                                    ours_plan.value.spans.as_slice(),
                                    theirs_plan.value.spans.as_slice(),
                                ];
                                let walk = MergeWalk {
                                    lengths,
                                    merged_bytes: logical_bytes,
                                    maximum_changes,
                                    content,
                                };
                                if !walk
                                    .run(store, plans, mutations, limits, accounting, cancellation)
                                    .await?
                                {
                                    return Ok(None);
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

/// The interval walk of a three-way extent merge over base, ours and
/// theirs of `lengths`, producing the mutations that turn ours into the
/// merge of `merged_bytes`. Past a side's length that side is absent, which
/// the three-way rule treats like any other value.
struct MergeWalk {
    lengths: [u64; 3],
    merged_bytes: u64,
    maximum_changes: u32,
    content: ContentMerge,
}

impl MergeWalk {
    /// Walks every interval where no side's extent or presence changes.
    /// Returns false on a conflict.
    async fn run<S: AsyncObjectStore>(
        &self,
        store: &S,
        plans: [&[ExtentSlice]; 3],
        mutations: &mut Owned<Vec<ExtentMutation>>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let longest = self.lengths.into_iter().max().unwrap_or(0);
        let mut indices = [0_usize; 3];
        let mut offset = 0;
        while offset < longest {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let mut spans = [None; 3];
            for (side, slot) in spans.iter_mut().enumerate() {
                let present = self.lengths.get(side).is_some_and(|length| offset < *length);
                if present {
                    let plan = plans.get(side).ok_or(FsError::InvalidDiff)?;
                    let index = *indices.get(side).ok_or(FsError::InvalidDiff)?;
                    *slot = Some(plan.get(index).ok_or(FsError::InvalidDiff)?);
                }
            }
            let end = spans
                .iter()
                .flatten()
                .map(|span| span.offset.saturating_add(span.length))
                .min()
                .ok_or(FsError::InvalidDiff)?;
            if end <= offset {
                return Err(FsError::InvalidDiff);
            }
            let mut kinds = [None, None, None];
            for (kind, span) in kinds.iter_mut().zip(spans) {
                *kind = span.map(|span| sliced_extent_kind(span, offset)).transpose()?;
            }
            let interval = ContentSpan {
                kinds,
                offset,
                end,
                ours_bytes: self.ours_bytes(),
                merged_bytes: self.merged_bytes,
            };
            if !self
                .merge_interval(store, &interval, mutations, limits, accounting, cancellation)
                .await?
            {
                return Ok(false);
            }
            offset = end;
            for (index, span) in indices.iter_mut().zip(spans) {
                if span.is_some_and(|span| span.offset + span.length == end) {
                    *index += 1;
                }
            }
        }
        if self.merged_bytes < self.ours_bytes() {
            push_extent_mutation(
                mutations,
                ExtentMutation::Resize {
                    logical_bytes: self.merged_bytes,
                },
                self.maximum_changes,
                accounting,
            )?;
        }
        Ok(true)
    }

    fn ours_bytes(&self) -> u64 {
        let [_, ours, _] = self.lengths;
        ours
    }

    /// Merges one interval: by extent identity, then, for content merges,
    /// by bytes where all three extents differ.
    async fn merge_interval<S: AsyncObjectStore>(
        &self,
        store: &S,
        interval: &ContentSpan,
        mutations: &mut Owned<Vec<ExtentMutation>>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let [base, ours, theirs] = &interval.kinds;
        match resolve_three(base, ours, theirs) {
            // The merge must hold bytes exactly up to its length.
            Some(resolved) if resolved.is_some() != (interval.offset < self.merged_bytes) => {
                Ok(false)
            }
            Some(Some(resolved)) if Some(&resolved) != ours.as_ref() => {
                let replaced = (interval.offset, interval.end - interval.offset, resolved);
                interval.push_replace(mutations, replaced, self.maximum_changes, accounting)?;
                Ok(true)
            }
            // Ours already holds the merge, or theirs cut what the final
            // resize removes.
            Some(_) => Ok(true),
            // Three different extents can still hold bytes only one side
            // changed, as when a side rewrote the whole file.
            None if self.content == ContentMerge::Extents => Ok(false),
            None => {
                merge_span_bytes(
                    store,
                    interval,
                    mutations,
                    self.maximum_changes,
                    limits,
                    accounting,
                    cancellation,
                )
                .await
            }
        }
    }
}

fn push_extent_mutation(
    mutations: &mut Owned<Vec<ExtentMutation>>,
    mutation: ExtentMutation,
    maximum_changes: u32,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if mutations.value.len() >= maximum_changes as usize {
        return Err(FsError::MergeChangeLimit);
    }
    mutations.ensure_for_push(maximum_changes as usize, accounting)?;
    mutations.value.push(mutation);
    Ok(())
}

/// Bytes compared per side at a time when three extents differ.
const CONTENT_MERGE_WINDOW: u64 = 64 * 1024;

/// One logical interval of base, ours and theirs, each side's extent sliced
/// at `offset`, or `None` past that side's length.
struct ContentSpan {
    kinds: [Option<ExtentKind>; 3],
    offset: u64,
    end: u64,
    ours_bytes: u64,
    merged_bytes: u64,
}

impl ContentSpan {
    /// Blob and blob offset of side `side` at logical `position`.
    fn source(&self, side: usize, position: u64) -> Option<(ObjectId, u64)> {
        let Some(ExtentKind::Content {
            object,
            object_offset,
        }) = *self.kinds.get(side)?
        else {
            return None;
        };
        Some((object, object_offset.checked_add(position.checked_sub(self.offset)?)?))
    }

    /// Whether side `side` has bytes in this interval.
    fn present(&self, side: usize) -> bool {
        self.kinds.get(side).is_some_and(Option::is_some)
    }

    /// Replaces ours' `length` bytes at `offset`, extending ours where the
    /// merge is longer.
    fn push_replace(
        &self,
        mutations: &mut Owned<Vec<ExtentMutation>>,
        (offset, length, kind): (u64, u64, ExtentKind),
        maximum_changes: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let replace = ExtentMutation::Replace {
            offset,
            length,
            kind,
            extend: offset.saturating_add(length) > self.ours_bytes,
        };
        push_extent_mutation(mutations, replace, maximum_changes, accounting)
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
            if !span.present(side) {
                continue;
            }
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

    /// Base, ours and theirs bytes of the window; `None` for a side past its
    /// length.
    fn sides(&self) -> [Option<&[u8]>; 3] {
        let [base, ours, theirs] = &self.0;
        [base, ours, theirs].map(|side| side.as_ref().map(|(bytes, _)| bytes.as_ref()))
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
/// conflicts. A side past its length has no byte there, so an untouched
/// suffix one side appended or cut merges like any other byte. Theirs' runs
/// become replacements sliced from theirs' extent. Returns false on a
/// conflict, or when a present extent is not stored content.
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
    if (0..3).any(|side| span.present(side) && span.source(side, span.offset).is_none()) {
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
            let sides = window.sides();
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            for (at, index) in (position..position + length).zip(0_usize..) {
                let [base, ours, theirs] = sides.map(|side| side.and_then(|bytes| bytes.get(index)));
                let Some(takes_theirs) = byte_takes_theirs(base, ours, theirs) else {
                    return Ok(false);
                };
                let merged = if takes_theirs { theirs } else { ours };
                if merged.is_some() != (at < span.merged_bytes) {
                    return Ok(false);
                }
                // A run ends where theirs keeps ours, or where theirs is cut.
                match (takes_theirs && theirs.is_some(), theirs_from) {
                    (true, None) => theirs_from = Some(at),
                    (false, Some(from)) => {
                        theirs_from = None;
                        let replaced = (from, at - from, span.theirs_at(from)?);
                        span.push_replace(mutations, replaced, maximum_changes, accounting)?;
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
        span.push_replace(
            mutations,
            (from, span.end - from, kind),
            maximum_changes,
            accounting,
        )?;
    }
    Ok(true)
}

/// The three-way rule for one byte, or its absence past a side's length:
/// `Some(false)` keeps ours (theirs left it as base, or both wrote it
/// alike), `Some(true)` takes theirs (only theirs changed it), and `None`
/// is a conflict (both changed it differently).
fn byte_takes_theirs<T: PartialEq + Copy>(base: T, ours: T, theirs: T) -> Option<bool> {
    if ours == theirs || theirs == base {
        Some(false)
    } else if ours == base {
        Some(true)
    } else {
        None
    }
}

/// Merges three tiny regular files byte by byte, the same rule
/// extent-backed content uses where its extents all differ: the length
/// merges like a field, a position past a side's length has no byte, and
/// the merged bytes must end exactly at the merged length.
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
    let sides = [base_data.as_bytes(), ours_data.as_bytes(), theirs_data.as_bytes()];
    let [base_length, ours_length, theirs_length] = sides.map(<[u8]>::len);
    let Some(length) = resolve_three(&base_length, &ours_length, &theirs_length) else {
        return Ok(None);
    };
    let Some(kind) = resolve_three(&base.kind, &ours.kind, &theirs.kind) else {
        return Ok(None);
    };
    accounting.charge(WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let mut merged = [0_u8; super::file_table::MAXIMUM_INLINE_FILE_BYTES];
    for (position, out) in merged.iter_mut().enumerate() {
        let [base, ours, theirs] = sides.map(|side| side.get(position));
        let Some(takes_theirs) = byte_takes_theirs(base, ours, theirs) else {
            return Ok(None);
        };
        match (if takes_theirs { theirs } else { ours }, position < length) {
            (Some(byte), true) => *out = *byte,
            (None, false) => {}
            _ => return Ok(None),
        }
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
        .get(..length)
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
