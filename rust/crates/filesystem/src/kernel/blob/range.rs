use super::*;

/// Successful authenticated logical blob range and exact work evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobRead {
    /// Requested bytes. Native aliases retain the original physical allocation guard.
    pub bytes: acyclic_stream::OwnedBytes,
    /// Actual controlled output-vector capacity retained by `bytes`.
    pub logical_owned_bytes: u64,
    /// Exact completed kernel and backend work in the supplied accounting context.
    pub work: WorkCounters,
    /// Exact authenticated forward page or chunk exposed by this traversal.
    pub next_residency: Option<ResidencyHint>,
}

impl BlobRead {
    /// Exact logical payload capacity measured before freezing the byte carrier.
    pub fn logical_owned_bytes(&self) -> u64 {
        self.logical_owned_bytes
    }
}

/// Reads only blob-index pages and chunks intersecting one exact range.
pub async fn read_blob_range<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    range: ByteRange,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<BlobRead, BlobReadFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    read_blob_range_with_accounting(
        store,
        root,
        range,
        limits,
        &mut accounting,
        cancellation,
        true,
    )
    .await
    .map_err(|error| failed(blob_error(error), accounting.spent()))
}

pub(crate) async fn read_blob_range_with_accounting<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    range: ByteRange,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    publish_output: bool,
) -> Result<BlobRead, FsError> {
    let mut machine = BlobRangeMachine::new(root, range, limits, accounting)?;
    machine.publish_output = publish_output;
    machine
        .drive(store, limits, accounting, cancellation)
        .await?;
    machine.finish(accounting)
}

/// Appends one authenticated blob range to `output`, whose spare capacity
/// already holds it, copying each chunk's bytes exactly once. Output bytes
/// are the caller's to publish and charge.
pub(crate) async fn append_blob_range_with_accounting<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    range: ByteRange,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    output: &mut Vec<u8>,
) -> Result<(), FsError> {
    let mut machine = BlobRangeMachine::with_output(
        root,
        range,
        limits,
        accounting,
        ReadOwned {
            value: std::mem::take(output),
            custody: KernelOwnedAllocation::Standalone,
        },
    )?;
    machine.publish_output = false;
    machine
        .drive(store, limits, accounting, cancellation)
        .await?;
    *output = machine.finish_appended(accounting)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum PendingBlobRead {
    Page {
        page: ObjectId,
        expected_first: u64,
        expected_end: Option<u64>,
        height: u16,
    },
    Chunk(BlobChunkRef),
}

impl PendingBlobRead {
    fn request(self, limits: DecodeLimits) -> (ObjectId, u64) {
        match self {
            Self::Page { page, .. } => (page, limits.maximum_page_object_bytes()),
            Self::Chunk(chunk) => (chunk.chunk, chunk.end_offset - chunk.first_offset),
        }
    }
}

struct BlobRangeMachine {
    range: ByteRange,
    range_end: u64,
    limits: DecodeLimits,
    maximum_pending: usize,
    pending: Vec<PendingBlobRead>,
    pending_capacity: LogicalVecCapacity,
    visited: Option<crate::kernel::persistent_point::Owned<VisitedObjectSet>>,
    output: Vec<u8>,
    /// Bytes `output` held before this range; the range is appended after them.
    base: usize,
    successor: Option<(u64, ResidencyHint)>,
    publish_output: bool,
    // Every physical payload is dropped before the corresponding native guard.
    pending_custody: KernelOwnedAllocation,
    output_custody: KernelOwnedAllocation,
}

impl BlobRangeMachine {
    fn new(
        root: ObjectId,
        range: ByteRange,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let (maximum_pending, maximum_visited) = Self::validate(root, range, limits, accounting)?;
        let allocation = usize::try_from(range.length).map_err(|_| BlobReadError::InvalidRange)?;
        let visited = VisitedObjectSet::new_with_accounting(maximum_visited, accounting)?;
        let output = allocate_read_buffer(allocation, accounting)?;
        Self::start(
            root,
            range,
            limits,
            maximum_pending,
            visited,
            output,
            accounting,
        )
    }

    /// Checks the root, range and limits; returns the pending and visited bounds.
    fn validate(
        root: ObjectId,
        range: ByteRange,
        limits: DecodeLimits,
        accounting: &KernelAccounting<'_>,
    ) -> Result<(usize, usize), FsError> {
        accounting.check()?;
        if root.kind != ObjectKind::Blob {
            return Err(BlobReadError::WrongRootKind.into());
        }
        range
            .offset
            .checked_add(range.length)
            .ok_or(BlobReadError::InvalidRange)?;
        usize::try_from(range.length).map_err(|_| BlobReadError::InvalidRange)?;
        if !limits.page_limits_valid(1) {
            return Err(BlobReadError::InvalidLimits.into());
        }
        let maximum_visited = usize::try_from(limits.maximum_visited_pages)
            .map_err(|_| BlobReadError::InvalidLimits)?;
        let maximum_pending = maximum_visited
            .checked_add(
                usize::try_from(limits.maximum_page_items)
                    .map_err(|_| BlobReadError::InvalidLimits)?,
            )
            .ok_or(BlobReadError::InvalidLimits)?;
        Ok((maximum_pending, maximum_visited))
    }

    /// A machine appending to `output`, which already has spare capacity for the range.
    fn with_output(
        root: ObjectId,
        range: ByteRange,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        output: ReadOwned<Vec<u8>>,
    ) -> Result<Self, FsError> {
        let (maximum_pending, maximum_visited) = Self::validate(root, range, limits, accounting)?;
        let length = usize::try_from(range.length).map_err(|_| BlobReadError::InvalidRange)?;
        if output.value.capacity() - output.value.len() < length {
            return Err(BlobReadError::InvalidRange.into());
        }
        let visited = VisitedObjectSet::new_with_accounting(maximum_visited, accounting)?;
        Self::start(
            root,
            range,
            limits,
            maximum_pending,
            visited,
            output,
            accounting,
        )
    }

    fn start(
        root: ObjectId,
        range: ByteRange,
        limits: DecodeLimits,
        maximum_pending: usize,
        visited: crate::kernel::persistent_point::Owned<VisitedObjectSet>,
        output: ReadOwned<Vec<u8>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let range_end = range
            .offset
            .checked_add(range.length)
            .ok_or(BlobReadError::InvalidRange)?;
        let mut machine = Self {
            range,
            range_end,
            limits,
            maximum_pending,
            pending: Vec::new(),
            pending_capacity: LogicalVecCapacity::default(),
            visited: Some(visited),
            base: output.value.len(),
            output: output.value,
            successor: None,
            pending_custody: KernelOwnedAllocation::Standalone,
            output_custody: output.custody,
            publish_output: true,
        };
        machine.push(
            PendingBlobRead::Page {
                page: root,
                expected_first: 0,
                expected_end: None,
                height: 1,
            },
            accounting,
        )?;
        Ok(machine)
    }

    async fn drive<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let machine = self;
        while !machine.pending.is_empty() {
            if machine
                .read_chunk_batch(store, accounting, cancellation)
                .await?
            {
                continue;
            }
            let pending = machine.prepare(accounting)?;
            let (object, maximum) = pending.request(limits);
            if let PendingBlobRead::Page {
                expected_first,
                expected_end,
                height,
                ..
            } = pending
            {
                let key = DecodedCacheKey::new::<BlobPage>(object, limits);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    original.remaining()?;
                }
                let cached = store.decoded_cache_get(key);
                if let Some(cached) = cached.map_err(crate::kernel::file_read::read_store_error)? {
                    let page = cached
                        .value
                        .downcast_ref::<BlobPage>()
                        .ok_or_else(|| FsError::BlobRead(ObjectStoreError::Corrupt.into()))?;
                    machine.accept_decoded_page(
                        page,
                        expected_first,
                        expected_end,
                        height,
                        accounting,
                    )?;

                    drop(cached);
                    continue;
                }
                let (read, retained) =
                    read_object_with_accounting(store, object, maximum, accounting, cancellation)
                        .await?;
                let decoded = ReadOwned::decoded(decode_blob_page_with_accounting(
                    &read, limits, accounting,
                )?);
                let (cached, allocated) =
                    crate::kernel::file_read::cache_decoded_read(store, key, decoded, accounting)?;
                let page = cached
                    .value
                    .downcast_ref::<BlobPage>()
                    .ok_or_else(|| FsError::BlobRead(ObjectStoreError::Corrupt.into()))?;
                machine.accept_decoded_page(
                    page,
                    expected_first,
                    expected_end,
                    height,
                    accounting,
                )?;

                drop(cached);
                crate::kernel::file_read::release_standalone_read_bytes(allocated, accounting)?;
                drop(read);
                crate::kernel::file_read::release_standalone_read_bytes(retained, accounting)?;
            } else if let PendingBlobRead::Chunk(chunk) = pending {
                let (read, retained) =
                    read_object_with_accounting(store, object, maximum, accounting, cancellation)
                        .await?;
                machine.accept_chunk(&read, chunk, accounting)?;
                drop(read);
                crate::kernel::file_read::release_standalone_read_bytes(retained, accounting)?;
            }
        }
        Ok(())
    }

    fn push(
        &mut self,
        pending: PendingBlobRead,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        self.pending_capacity.ensure_for_push_with_accounting(
            &mut self.pending,
            self.maximum_pending,
            &mut self.pending_custody,
            accounting,
        )?;
        self.pending.push(pending);
        Ok(())
    }

    fn prepare(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<PendingBlobRead, FsError> {
        accounting.check()?;
        let pending = self.pending.pop().ok_or(BlobReadError::TraversalState)?;
        if let PendingBlobRead::Page { page, height, .. } = pending {
            if height > self.limits.maximum_page_height {
                return Err(BlobReadError::CycleOrHeight.into());
            }
            let visited_owner = self.visited.as_mut().ok_or(BlobReadError::TraversalState)?;
            let visited =
                VisitedObjectSet::insert_with_accounting(visited_owner, page, accounting)?;
            if !visited.inserted {
                return Err(BlobReadError::CycleOrHeight.into());
            }
            accounting.charge(WorkCounters {
                page_reads: 1,
                ..WorkCounters::UNCHARGED
            })?;
        }
        Ok(pending)
    }

    fn accept_decoded_page(
        &mut self,
        page: &BlobPage,
        expected_first: u64,
        expected_end: Option<u64>,
        height: u16,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if page.first_offset != expected_first
            || expected_end.is_some_and(|end| page.end_offset != end)
        {
            return Err(BlobReadError::ChildBoundsMismatch.into());
        }
        if expected_end.is_none() && self.range_end > page.end_offset {
            return Err(BlobReadError::InvalidRange.into());
        }
        match &page.node {
            BlobNode::Internal(children) => {
                let height = height.checked_add(1).ok_or(BlobReadError::CycleOrHeight)?;
                for child in children.iter().rev() {
                    accounting.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    if child.first_offset < self.range_end && child.end_offset > self.range.offset {
                        self.push(
                            PendingBlobRead::Page {
                                page: child.page,
                                expected_first: child.first_offset,
                                expected_end: Some(child.end_offset),
                                height,
                            },
                            accounting,
                        )?;
                    } else if self.range.length != 0 && child.first_offset >= self.range_end {
                        self.retain_successor(
                            child.first_offset,
                            ObjectReadRequest {
                                object_id: child.page,
                                maximum_bytes: self.limits.maximum_page_object_bytes(),
                            },
                        );
                    }
                }
            }
            BlobNode::Leaf(chunks) => {
                for chunk in chunks.iter().rev() {
                    accounting.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    if chunk.first_offset < self.range_end && chunk.end_offset > self.range.offset {
                        self.push(PendingBlobRead::Chunk(*chunk), accounting)?;
                    } else if self.range.length != 0 && chunk.first_offset >= self.range_end {
                        self.retain_successor(
                            chunk.first_offset,
                            ObjectReadRequest {
                                object_id: chunk.chunk,
                                maximum_bytes: chunk.end_offset - chunk.first_offset,
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn retain_successor(&mut self, offset: u64, request: ObjectReadRequest) {
        if self.successor.is_none_or(|(current, _)| offset < current) {
            self.successor = Some((
                offset,
                ResidencyHint {
                    request,
                    reason: ResidencyReason::SequentialRange,
                },
            ));
        }
    }

    fn accept_chunk(
        &mut self,
        bytes: &[u8],
        chunk: BlobChunkRef,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if usize_to_u64(bytes.len()) != chunk.end_offset - chunk.first_offset {
            return Err(BlobReadError::ChunkLengthMismatch.into());
        }
        let start = usize::try_from(self.range.offset.max(chunk.first_offset) - chunk.first_offset)
            .map_err(|_| BlobReadError::InvalidRange)?;
        let end = usize::try_from(self.range_end.min(chunk.end_offset) - chunk.first_offset)
            .map_err(|_| BlobReadError::InvalidRange)?;
        let region = bytes.get(start..end).ok_or(BlobReadError::InvalidRange)?;
        if (self.output.len() - self.base)
            .checked_add(region.len())
            .map(usize_to_u64)
            .is_none_or(|length| length > self.range.length)
        {
            return Err(BlobReadError::IncompleteCoverage.into());
        }
        // Admitted capacity covers the range, so the copy never reallocates.
        if self.output.capacity() - self.output.len() < region.len() {
            return Err(BlobReadError::InvalidRange.into());
        }
        let copied = WorkCounters {
            bytes_copied: usize_to_u64(region.len()),
            output_bytes: if self.publish_output {
                usize_to_u64(region.len())
            } else {
                0
            },
            items_returned: 1,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        #[cfg(test)]
        crate::kernel::file_read::read_work::record(
            "chunk",
            self.output.len() - self.base,
            region.len(),
        );
        self.output.extend_from_slice(region);
        accounting.record_completed(copied)
    }

    async fn read_chunk_batch<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        if !matches!(self.pending.last(), Some(PendingBlobRead::Chunk(_))) {
            return Ok(false);
        }
        let count = self
            .pending
            .iter()
            .rev()
            .take_while(|pending| matches!(pending, PendingBlobRead::Chunk(_)))
            .count();
        if count < 2 {
            return Ok(false);
        }
        #[cfg(not(target_arch = "wasm32"))]
        let native = accounting.native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        let mut observed = crate::kernel::file_read::OriginalReadAccounting::new(accounting)?;
        let mut chunks = Vec::new();
        let mut requests = Vec::new();
        let prepared = (|| -> Result<(), FsError> {
            reserve_decoded_items(&mut chunks, count, &mut |step| observed.observe(step))?;
            reserve_decoded_items(&mut requests, count, &mut |step| observed.observe(step))?;
            let copied = usize_to_u64(copied_per_chunk());
            for pending in self.pending.iter().rev().take(count) {
                let PendingBlobRead::Chunk(chunk) = pending else {
                    return Err(FsError::from(BlobReadError::TraversalState));
                };
                let completed = WorkCounters {
                    items_examined: 1,
                    bytes_copied: copied,
                    ..WorkCounters::UNCHARGED
                };
                observed.admit(completed)?;
                chunks.push(*chunk);
                requests.push(ObjectReadRequest {
                    object_id: chunk.chunk,
                    maximum_bytes: chunk.end_offset - chunk.first_offset,
                });
                observed.record_completed(completed)?;
            }
            Ok(())
        })();
        if let Err(error) = prepared {
            drop(chunks);
            drop(requests);
            observed.release_failed()?;
            return Err(read_decode_error(error));
        }
        #[cfg(test)]
        crate::kernel::file_read::read_work::record("batch", count, count * copied_per_chunk());
        let (custody, scratch_bytes) = observed.into_parts();
        let batch = ReadOwned {
            value: (chunks, requests, scratch_bytes),
            custody,
        };
        let read_result = crate::kernel::file_read::read_objects_with_accounting(
            store,
            &batch.value.1,
            accounting,
            cancellation,
        )
        .await;
        let applied = match read_result {
            Err(error) => Err(error),
            Ok(reads) => {
                let retained = if native {
                    Ok(0)
                } else {
                    crate::kernel::file_read::retained_object_reads_bytes(
                        &reads.value,
                        reads.value.capacity(),
                    )
                };
                let applied = (|| -> Result<bool, FsError> {
                    if reads.value.len() != count {
                        return Err(BlobReadError::TraversalState.into());
                    }
                    self.pending.truncate(self.pending.len() - count);
                    for (chunk, read) in batch.value.0.iter().zip(&reads.value) {
                        self.accept_chunk(&read.bytes, *chunk, accounting)?;
                    }
                    Ok(true)
                })();
                drop(reads.value);
                let retired = retained
                    .and_then(|bytes| release_read_allocation(bytes, reads.custody, accounting));
                retired.and(applied)
            }
        };
        drop(batch.value);
        release_read_allocation(scratch_bytes, batch.custody, accounting)?;
        applied
    }

    /// Returns the appended output after retiring the traversal state.
    fn finish_appended(
        mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Vec<u8>, FsError> {
        if usize_to_u64(self.output.len() - self.base) != self.range.length {
            return Err(BlobReadError::IncompleteCoverage.into());
        }
        let output = std::mem::take(&mut self.output);
        let pending_bytes = self.pending_capacity.logical_bytes();
        drop(self.pending);
        release_read_allocation(pending_bytes, self.pending_custody, accounting)?;
        if let Some(visited) = self.visited {
            visited.retire(accounting)?;
        }
        Ok(output)
    }

    fn finish(mut self, accounting: &mut KernelAccounting<'_>) -> Result<BlobRead, FsError> {
        if usize_to_u64(self.output.len() - self.base) != self.range.length {
            return Err(BlobReadError::IncompleteCoverage.into());
        }
        let logical_owned_bytes = usize_to_u64(self.output.capacity());
        let output = ReadOwned {
            value: std::mem::take(&mut self.output),
            custody: std::mem::replace(&mut self.output_custody, KernelOwnedAllocation::Standalone),
        };
        let bytes = owned_read_bytes(output)?;
        let pending_bytes = self.pending_capacity.logical_bytes();
        drop(self.pending);
        release_read_allocation(pending_bytes, self.pending_custody, accounting)?;
        if let Some(visited) = self.visited {
            visited.retire(accounting)?;
        }
        Ok(BlobRead {
            bytes,
            logical_owned_bytes,
            work: accounting.spent(),
            next_residency: self.successor.map(|(_, hint)| hint),
        })
    }
}

/// Chunk-reference bytes a batched chunk read copies into its request.
fn copied_per_chunk() -> usize {
    size_of::<BlobChunkRef>() + size_of::<ObjectReadRequest>()
}
