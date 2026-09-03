//! Object-store-backed authenticated dependency probes.

use super::{
    AsyncRebaseProbe, BlobReadError, DecodeLimits, Dependency, DependencyError, DependencyRegion,
    DependencyState, DirectoryReadError, ExtentKind, ExtentReadError, ExtentSeekRequest,
    ExtentSeekTarget, FilePayload, FileRecord, FileRecordReadError, GenerationRoot, ProbeReceipt,
    RebaseProbe, TreeReadError, decode_generation_root, encode_tree_page, list_tree_entries,
    list_tree_entries_async, lookup_file_record, lookup_file_record_async, lookup_tree_entry,
    lookup_tree_entry_async, plan_extent_range, plan_extent_range_async, read_blob_range,
    read_blob_range_async, seek_extent, seek_extent_async,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::{Digest, FileId, GenerationId};
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ByteRange, ObjectId, ObjectKind, ObjectStoreError};
use std::collections::HashMap;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::Mutex;
use thiserror::Error;

const ZERO_HASH_BLOCK: [u8; 8 * 1024] = [0; 8 * 1024];
const CONTENT_RANGE_DOMAIN: &[u8] = b"acyclic-fs-dependency-content-range-v1\0";

/// Computes canonical layout-independent evidence for one non-empty logical
/// content range already present in memory.
///
/// This function is the normative cross-language vector surface. Sparse and
/// inline content use the same domain, length prefix, and logical byte stream.
///
/// # Errors
///
/// Rejects a zero maximum, empty/excessive content, or exact hash work outside
/// the admitted budget.
pub fn capture_content_range_bytes(
    bytes: &[u8],
    maximum_bytes: u64,
    budget: WorkBudget,
) -> Result<ProbeReceipt, ProbeFailure> {
    if maximum_bytes == 0 {
        return Err(failed(
            AuthenticatedProbeError::InvalidLimits,
            WorkCounters::default(),
        ));
    }
    if bytes.is_empty() {
        return Err(failed(
            AuthenticatedProbeError::InvalidDependency(DependencyError::EmptyContentRange),
            WorkCounters::default(),
        ));
    }
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(failed(
            AuthenticatedProbeError::ContentRangeTooLarge,
            WorkCounters::default(),
        ));
    }
    hash_content_range(bytes, budget, WorkCounters::default())
}

/// Authenticated exact-region resolver with disposable per-instance summary caches.
///
/// Caches contain only immutable decoded summaries. Cache loss affects
/// performance, never correctness or authority.
pub struct AuthenticatedGenerationProbe<'a, S> {
    store: &'a S,
    limits: ProbeLimits,
    generations: Mutex<HashMap<GenerationId, GenerationRoot>>,
    records: Mutex<HashMap<(GenerationId, FileId), Option<FileRecord>>>,
}

/// Hard bounds for one disposable authenticated dependency probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProbeLimits {
    /// Canonical decoder and page traversal bounds.
    pub decode: DecodeLimits,
    /// Maximum retained immutable generation summaries.
    pub maximum_cached_generations: u32,
    /// Maximum retained `(generation, file)` summaries.
    pub maximum_cached_records: u32,
    /// Maximum sparse spans produced for one content dependency.
    pub maximum_extent_spans: u32,
    /// Maximum semantic file bytes in one observed content range.
    pub maximum_content_payload_bytes: u64,
    /// Maximum entries in one observed directory page.
    pub maximum_directory_entries: u32,
}

impl Default for ProbeLimits {
    fn default() -> Self {
        Self {
            decode: DecodeLimits::default(),
            maximum_cached_generations: 64,
            maximum_cached_records: 4_096,
            maximum_extent_spans: 4_096,
            maximum_content_payload_bytes: 16 * 1024 * 1024,
            maximum_directory_entries: 1_024,
        }
    }
}

impl<'a, S> AuthenticatedGenerationProbe<'a, S> {
    /// Creates a bounded resolver over one immutable-object backend.
    ///
    /// # Errors
    ///
    /// Rejects zero cache or extent-output limits before backend access.
    pub fn new(store: &'a S, limits: ProbeLimits) -> Result<Self, AuthenticatedProbeError> {
        if limits.maximum_cached_generations == 0
            || limits.maximum_cached_records == 0
            || limits.maximum_extent_spans == 0
            || limits.maximum_content_payload_bytes == 0
            || limits.maximum_directory_entries == 0
            || !limits.decode.page_limits_valid(1)
        {
            return Err(AuthenticatedProbeError::InvalidLimits);
        }
        Ok(Self {
            store,
            limits,
            generations: Mutex::new(HashMap::new()),
            records: Mutex::new(HashMap::new()),
        })
    }

    fn validate_region(&self, region: &DependencyRegion) -> Result<FileId, ProbeFailure> {
        region.validate().map_err(|error| {
            failed(
                AuthenticatedProbeError::InvalidDependency(error),
                WorkCounters::default(),
            )
        })?;
        match region {
            DependencyRegion::ContentRange { length, .. }
                if *length > self.limits.maximum_content_payload_bytes =>
            {
                return Err(failed(
                    AuthenticatedProbeError::ContentRangeTooLarge,
                    WorkCounters::default(),
                ));
            }
            DependencyRegion::DirectoryRange {
                maximum_entries, ..
            } if *maximum_entries > self.limits.maximum_directory_entries => {
                return Err(failed(
                    AuthenticatedProbeError::DirectoryPageTooLarge,
                    WorkCounters::default(),
                ));
            }
            _ => {}
        }
        Ok(match region {
            DependencyRegion::FileRecord(file_id)
            | DependencyRegion::Metadata(file_id)
            | DependencyRegion::FileLength(file_id)
            | DependencyRegion::ContentRange { file_id, .. }
            | DependencyRegion::SparseSeek { file_id, .. } => *file_id,
            DependencyRegion::DirectoryName { directory_id, .. }
            | DependencyRegion::DirectoryRange { directory_id, .. } => *directory_id,
        })
    }
}

impl<S: crate::ImmediateObjectStore> AuthenticatedGenerationProbe<'_, S> {
    fn generation(
        &self,
        generation: GenerationId,
        budget: WorkBudget,
    ) -> Result<(GenerationRoot, WorkCounters), ProbeFailure> {
        if let Some(root) = self
            .generations
            .lock()
            .map_err(|_| {
                failed(
                    AuthenticatedProbeError::CachePoisoned,
                    WorkCounters::default(),
                )
            })?
            .get(&generation)
            .cloned()
        {
            return Ok((root, WorkCounters::default()));
        }
        let object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        let receipt = self
            .store
            .read(object, self.limits.decode.maximum_object_bytes, budget)
            .map_err(|failure| failed(failure.error.into(), *failure.work))?;
        let root = decode_generation_root(&receipt.value, self.limits.decode)
            .map_err(|error| failed(error.into(), receipt.work))?;
        let mut generations = self
            .generations
            .lock()
            .map_err(|_| failed(AuthenticatedProbeError::CachePoisoned, receipt.work))?;
        clear_at_capacity(&mut generations, self.limits.maximum_cached_generations);
        generations.insert(generation, root.clone());
        Ok((root, receipt.work))
    }

    fn record(
        &self,
        generation: GenerationId,
        file_id: FileId,
        budget: WorkBudget,
    ) -> Result<(Option<FileRecord>, WorkCounters), ProbeFailure> {
        if let Some(record) = self
            .records
            .lock()
            .map_err(|_| {
                failed(
                    AuthenticatedProbeError::CachePoisoned,
                    WorkCounters::default(),
                )
            })?
            .get(&(generation, file_id))
            .copied()
        {
            return Ok((record, WorkCounters::default()));
        }
        let (root, mut work) = self.generation(generation, budget)?;
        let remaining = work
            .remaining(budget)
            .map_err(|error| failed(error.into(), work))?;
        let lookup = lookup_file_record(
            self.store,
            root.file_table,
            file_id,
            self.limits.decode,
            remaining,
        )
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = work
            .checked_add(lookup.work)
            .map_err(|error| failed(error.into(), work))?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| failed(AuthenticatedProbeError::CachePoisoned, work))?;
        clear_at_capacity(&mut records, self.limits.maximum_cached_records);
        records.insert((generation, file_id), lookup.record);
        Ok((lookup.record, work))
    }
}

pub(crate) fn capture_file_record_state(
    record: FileRecord,
    budget: WorkBudget,
    mut work: WorkCounters,
) -> Result<ProbeReceipt, ProbeFailure> {
    let encoded =
        super::file_table::encode_file_table_leaf_records(std::slice::from_ref(&record), 1)
            .map_err(|error| failed(error.into(), work))?;
    let domain = b"acyclic-fs-dependency-file-record-v1\0";
    let value = DependencyState::Present(semantic_digest(domain, &encoded));
    work = work
        .checked_add(WorkCounters {
            bytes_encoded: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
            bytes_hashed: u64::try_from(encoded.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(domain.len()).unwrap_or(u64::MAX))
                .saturating_add(8),
            allocation_operations: 1,
            peak_allocation_bytes: u64::try_from(encoded.capacity()).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), work))?;
    work.verify(budget)
        .map_err(|error| failed(error.into(), work))?;
    Ok(ProbeReceipt { value, work })
}

pub(crate) fn capture_sparse_seek_state(
    target: ExtentSeekTarget,
    result: Option<u64>,
    budget: WorkBudget,
    mut work: WorkCounters,
) -> Result<ProbeReceipt, ProbeFailure> {
    let mut encoded = [0_u8; 10];
    encoded[0] = match target {
        ExtentSeekTarget::Data => 0,
        ExtentSeekTarget::Hole => 1,
    };
    if let Some(offset) = result {
        encoded[1] = 1;
        encoded[2..].copy_from_slice(&offset.to_le_bytes());
    }
    let domain = b"acyclic-fs-dependency-sparse-seek-v1\0";
    let value = DependencyState::Present(semantic_digest(domain, &encoded));
    work = work
        .checked_add(WorkCounters {
            bytes_encoded: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
            bytes_hashed: u64::try_from(encoded.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(domain.len()).unwrap_or(u64::MAX))
                .saturating_add(8),
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), work))?;
    work.verify(budget)
        .map_err(|error| failed(error.into(), work))?;
    Ok(ProbeReceipt { value, work })
}

fn capture_file_length_state(
    record: FileRecord,
    budget: WorkBudget,
    mut work: WorkCounters,
) -> Result<ProbeReceipt, ProbeFailure> {
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
            return capture_file_record_state(record, budget, work);
        }
    };
    let domain = b"acyclic-fs-dependency-file-length-v1\0";
    let encoded = logical_bytes.to_le_bytes();
    let value = DependencyState::Present(semantic_digest(domain, &encoded));
    work = work
        .checked_add(WorkCounters {
            bytes_encoded: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
            bytes_hashed: u64::try_from(encoded.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(domain.len()).unwrap_or(u64::MAX))
                .saturating_add(8),
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), work))?;
    work.verify(budget)
        .map_err(|error| failed(error.into(), work))?;
    Ok(ProbeReceipt { value, work })
}

pub(crate) fn capture_directory_name_state(
    entry: &super::TreeEntry,
    budget: WorkBudget,
    mut work: WorkCounters,
) -> Result<ProbeReceipt, ProbeFailure> {
    let encoded = super::tree::encode_tree_leaf_entries(std::slice::from_ref(entry), 1)
        .map_err(|error| failed(error.into(), work))?;
    let domain = b"acyclic-fs-dependency-directory-name-v1\0";
    let value = DependencyState::Present(semantic_digest(domain, &encoded));
    work = work
        .checked_add(WorkCounters {
            bytes_encoded: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
            bytes_hashed: u64::try_from(encoded.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(domain.len()).unwrap_or(u64::MAX))
                .saturating_add(8),
            allocation_operations: 1,
            peak_allocation_bytes: u64::try_from(encoded.capacity()).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), work))?;
    work.verify(budget)
        .map_err(|error| failed(error.into(), work))?;
    Ok(ProbeReceipt { value, work })
}

impl<S: crate::ImmediateObjectStore> AuthenticatedGenerationProbe<'_, S> {
    fn content_state(
        &self,
        record: FileRecord,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        mut work: WorkCounters,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        if let FilePayload::InlineRegular(data) = record.payload {
            return Self::inline_content_state(data, offset, length, budget, work);
        }
        let FilePayload::Regular {
            logical_bytes,
            extents,
        } = record.payload
        else {
            return capture_file_record_state(record, budget, work);
        };
        let range = ByteRange { offset, length };
        let hash_work = WorkCounters {
            bytes_hashed: u64::try_from(CONTENT_RANGE_DOMAIN.len())
                .unwrap_or(u64::MAX)
                .saturating_add(8)
                .saturating_add(length),
            ..WorkCounters::default()
        };
        let reserved = work
            .checked_add(hash_work)
            .map_err(|error| failed(error.into(), work))?;
        reserved
            .verify(budget)
            .map_err(|error| failed(error.into(), work))?;
        let remaining = reserved
            .remaining(budget)
            .map_err(|error| failed(error.into(), work))?;
        let plan = plan_extent_range(
            self.store,
            super::ExtentRangeRequest {
                root: extents,
                file_size: logical_bytes,
                range,
                maximum_spans: self.limits.maximum_extent_spans,
                limits: self.limits.decode,
                budget: remaining,
            },
        )
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = work
            .checked_add(plan.work)
            .map_err(|error| failed(error.into(), work))?;
        self.hash_content_spans(plan.spans, length, hash_work, budget, work)
    }

    fn sparse_seek_state(
        &self,
        record: FileRecord,
        offset: u64,
        target: ExtentSeekTarget,
        budget: WorkBudget,
        mut work: WorkCounters,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        let result = match record.payload {
            FilePayload::InlineRegular(data) => inline_seek_result(data, offset, target),
            FilePayload::Regular {
                logical_bytes,
                extents,
            } if offset <= logical_bytes => {
                let receipt = seek_extent(
                    self.store,
                    ExtentSeekRequest {
                        root: extents,
                        file_size: logical_bytes,
                        offset,
                        target,
                        limits: self.limits.decode,
                        budget: work
                            .remaining(budget)
                            .map_err(|error| failed(error.into(), work))?,
                    },
                )
                .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
                work = work
                    .checked_add(receipt.work)
                    .map_err(|error| failed(error.into(), work))?;
                receipt.value
            }
            FilePayload::Regular { .. } => None,
            FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::Empty
            | FilePayload::Device { .. }
            | FilePayload::ReparsePoint { .. } => {
                return capture_file_record_state(record, budget, work);
            }
        };
        capture_sparse_seek_state(target, result, budget, work)
    }
}

impl<S> AuthenticatedGenerationProbe<'_, S> {
    fn inline_content_state(
        data: super::InlineFileData,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        work: WorkCounters,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        let end = offset.checked_add(length).ok_or_else(|| {
            failed(
                AuthenticatedProbeError::Extent(ExtentReadError::InvalidRange),
                work,
            )
        })?;
        let bytes = data.as_bytes();
        if end > u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
            return Err(failed(
                AuthenticatedProbeError::Extent(ExtentReadError::InvalidRange),
                work,
            ));
        }
        let start = usize::try_from(offset).map_err(|_| {
            failed(
                AuthenticatedProbeError::Extent(ExtentReadError::InvalidRange),
                work,
            )
        })?;
        let end = usize::try_from(end).map_err(|_| {
            failed(
                AuthenticatedProbeError::Extent(ExtentReadError::InvalidRange),
                work,
            )
        })?;
        hash_content_range(&bytes[start..end], budget, work)
    }
}

impl<S: crate::ImmediateObjectStore> AuthenticatedGenerationProbe<'_, S> {
    fn hash_content_spans(
        &self,
        spans: Vec<super::ExtentSlice>,
        length: u64,
        hash_work: WorkCounters,
        budget: WorkBudget,
        mut work: WorkCounters,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(CONTENT_RANGE_DOMAIN);
        hasher.update(&length.to_le_bytes());
        for span in spans {
            match span.kind {
                ExtentKind::Hole | ExtentKind::AllocatedZero => {
                    let mut remaining_zeroes = span.length;
                    while remaining_zeroes != 0 {
                        let count = usize::try_from(
                            remaining_zeroes
                                .min(u64::try_from(ZERO_HASH_BLOCK.len()).unwrap_or(u64::MAX)),
                        )
                        .unwrap_or(ZERO_HASH_BLOCK.len());
                        hasher.update(&ZERO_HASH_BLOCK[..count]);
                        remaining_zeroes -= u64::try_from(count).unwrap_or(0);
                    }
                }
                ExtentKind::Content {
                    object,
                    object_offset,
                } => {
                    let reserved = work
                        .checked_add(hash_work)
                        .map_err(|error| failed(error.into(), work))?;
                    let remaining = reserved
                        .remaining(budget)
                        .map_err(|error| failed(error.into(), work))?;
                    let read = read_blob_range(
                        self.store,
                        object,
                        ByteRange {
                            offset: object_offset,
                            length: span.length,
                        },
                        self.limits.decode,
                        remaining,
                    )
                    .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
                    work = work
                        .checked_add(read.work)
                        .map_err(|error| failed(error.into(), work))?;
                    hasher.update(&read.bytes);
                }
            }
        }
        work = work
            .checked_add(hash_work)
            .map_err(|error| failed(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| failed(error.into(), work))?;
        Ok(ProbeReceipt {
            value: DependencyState::Present(Digest::from_bytes(*hasher.finalize().as_bytes())),
            work,
        })
    }

    fn directory_page_state(
        &self,
        record: FileRecord,
        after: Option<&super::LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        mut work: WorkCounters,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        let FilePayload::Directory { entries } = record.payload else {
            return capture_file_record_state(record, budget, work);
        };
        let remaining = work
            .remaining(budget)
            .map_err(|error| failed(error.into(), work))?;
        let page = list_tree_entries(
            self.store,
            entries,
            after,
            maximum_entries,
            self.limits.decode,
            remaining,
        )
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = work
            .checked_add(page.work)
            .map_err(|error| failed(error.into(), work))?;
        let mut encoded = encode_tree_page(&super::TreePage::Leaf(page.entries), maximum_entries)
            .map_err(|error| failed(error.into(), work))?;
        encoded.push(u8::from(page.has_more));
        let domain = b"acyclic-fs-dependency-directory-page-v1\0";
        let digest = semantic_digest(domain, &encoded);
        work = work
            .checked_add(WorkCounters {
                bytes_encoded: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
                bytes_hashed: u64::try_from(encoded.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::try_from(domain.len()).unwrap_or(u64::MAX))
                    .saturating_add(8),
                allocation_operations: 1,
                peak_allocation_bytes: u64::try_from(encoded.capacity()).unwrap_or(u64::MAX),
                ..WorkCounters::default()
            })
            .map_err(|error| failed(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| failed(error.into(), work))?;
        Ok(ProbeReceipt {
            value: DependencyState::Present(digest),
            work,
        })
    }
}

impl<S: AsyncObjectStore> AuthenticatedGenerationProbe<'_, S> {
    /// Captures a bounded set of exact regions through one shared immutable
    /// generation and file-record cache.
    ///
    /// Regions are admitted as a complete batch before allocation or backend
    /// access. Input order and duplicates are preserved; repeated regions and
    /// regions sharing a file identity reuse the same authenticated frontiers.
    ///
    /// # Errors
    ///
    /// Rejects a zero/exceeded batch bound, malformed region, allocation,
    /// cancellation, authentication, storage, or exact-work failure.
    pub async fn capture_many_async(
        &self,
        generation: GenerationId,
        regions: Vec<DependencyRegion>,
        maximum_regions: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Vec<Dependency>>, ProbeFailure> {
        cancellation
            .check()
            .map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::default()))?;
        validate_capture_batch(regions.len(), maximum_regions)?;
        for region in &regions {
            self.validate_region(region)?;
        }
        let (mut captured, mut work) = allocate_capture_output(&regions, budget)?;

        for region in regions {
            let receipt = self
                .probe_async(
                    generation,
                    &region,
                    work.remaining(budget)
                        .map_err(|error| failed(error.into(), work))?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = work
                .checked_add(receipt.work)
                .map_err(|error| failed(error.into(), work))?;
            captured.push(Dependency {
                region,
                expected: receipt.value,
            });
        }
        Ok(OperationReceipt {
            value: captured,
            work,
        })
    }

    /// Captures a bounded batch from records already authenticated by the
    /// caller's path or identity frontier, without rereading the generation or
    /// file table.
    ///
    /// Input order and duplicates are preserved. Every present record must own
    /// its region; absence is an explicit authenticated caller fact.
    ///
    /// # Errors
    ///
    /// Rejects the complete batch before semantic backend access when its
    /// bound, region, record identity, allocation, cancellation, or work is
    /// invalid.
    pub async fn capture_records_many_async(
        &self,
        regions: Vec<(Option<FileRecord>, DependencyRegion)>,
        maximum_regions: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Vec<Dependency>>, ProbeFailure> {
        cancellation
            .check()
            .map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::default()))?;
        validate_capture_batch(regions.len(), maximum_regions)?;
        for (record, region) in &regions {
            let expected_file_id = self.validate_region(region)?;
            if record.is_some_and(|record| record.file_id != expected_file_id) {
                return Err(failed(
                    AuthenticatedProbeError::RecordIdentityMismatch,
                    WorkCounters::default(),
                ));
            }
        }

        let (mut captured, mut work) = allocate_capture_output(&regions, budget)?;

        for (record, region) in regions {
            let receipt = self
                .capture_validated_record_async(
                    record,
                    &region,
                    work.remaining(budget)
                        .map_err(|error| failed(error.into(), work))?,
                    WorkCounters::default(),
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = work
                .checked_add(receipt.work)
                .map_err(|error| failed(error.into(), work))?;
            captured.push(Dependency {
                region,
                expected: receipt.value,
            });
        }
        Ok(OperationReceipt {
            value: captured,
            work,
        })
    }

    async fn generation_async(
        &self,
        generation: GenerationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(GenerationRoot, WorkCounters), ProbeFailure> {
        cancellation
            .check()
            .map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::default()))?;
        if let Some(root) = self
            .generations
            .lock()
            .map_err(|_| {
                failed(
                    AuthenticatedProbeError::CachePoisoned,
                    WorkCounters::default(),
                )
            })?
            .get(&generation)
            .cloned()
        {
            return Ok((root, WorkCounters::default()));
        }
        let object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        let receipt = AsyncObjectStore::read(
            self.store,
            object,
            self.limits.decode.maximum_object_bytes,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), Into::into))?;
        let root = decode_generation_root(&receipt.value, self.limits.decode)
            .map_err(|error| failed(error.into(), receipt.work))?;
        let mut generations = self
            .generations
            .lock()
            .map_err(|_| failed(AuthenticatedProbeError::CachePoisoned, receipt.work))?;
        clear_at_capacity(&mut generations, self.limits.maximum_cached_generations);
        generations.insert(generation, root.clone());
        Ok((root, receipt.work))
    }

    async fn record_async(
        &self,
        generation: GenerationId,
        file_id: FileId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(Option<FileRecord>, WorkCounters), ProbeFailure> {
        if let Some(record) = self
            .records
            .lock()
            .map_err(|_| {
                failed(
                    AuthenticatedProbeError::CachePoisoned,
                    WorkCounters::default(),
                )
            })?
            .get(&(generation, file_id))
            .copied()
        {
            return Ok((record, WorkCounters::default()));
        }
        let (root, mut work) = self
            .generation_async(generation, budget, cancellation)
            .await?;
        let remaining = work
            .remaining(budget)
            .map_err(|error| failed(error.into(), work))?;
        let lookup = lookup_file_record_async(
            self.store,
            root.file_table,
            file_id,
            self.limits.decode,
            remaining,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = work
            .checked_add(lookup.work)
            .map_err(|error| failed(error.into(), work))?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| failed(AuthenticatedProbeError::CachePoisoned, work))?;
        clear_at_capacity(&mut records, self.limits.maximum_cached_records);
        records.insert((generation, file_id), lookup.record);
        Ok((lookup.record, work))
    }

    async fn content_state_async(
        &self,
        record: FileRecord,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        mut work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        if let FilePayload::InlineRegular(data) = record.payload {
            return Self::inline_content_state(data, offset, length, budget, work);
        }
        let FilePayload::Regular {
            logical_bytes,
            extents,
        } = record.payload
        else {
            return capture_file_record_state(record, budget, work);
        };
        let range = ByteRange { offset, length };
        let hash_work = WorkCounters {
            bytes_hashed: u64::try_from(CONTENT_RANGE_DOMAIN.len())
                .unwrap_or(u64::MAX)
                .saturating_add(8)
                .saturating_add(length),
            ..WorkCounters::default()
        };
        let reserved = work
            .checked_add(hash_work)
            .map_err(|error| failed(error.into(), work))?;
        reserved
            .verify(budget)
            .map_err(|error| failed(error.into(), work))?;
        let remaining = reserved
            .remaining(budget)
            .map_err(|error| failed(error.into(), work))?;
        let plan = plan_extent_range_async(
            self.store,
            super::ExtentRangeRequest {
                root: extents,
                file_size: logical_bytes,
                range,
                maximum_spans: self.limits.maximum_extent_spans,
                limits: self.limits.decode,
                budget: remaining,
            },
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = work
            .checked_add(plan.work)
            .map_err(|error| failed(error.into(), work))?;
        self.hash_content_spans_async(plan.spans, length, hash_work, budget, work, cancellation)
            .await
    }

    async fn sparse_seek_state_async(
        &self,
        record: FileRecord,
        offset: u64,
        target: ExtentSeekTarget,
        budget: WorkBudget,
        mut work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        let result = match record.payload {
            FilePayload::InlineRegular(data) => inline_seek_result(data, offset, target),
            FilePayload::Regular {
                logical_bytes,
                extents,
            } if offset <= logical_bytes => {
                let receipt = seek_extent_async(
                    self.store,
                    ExtentSeekRequest {
                        root: extents,
                        file_size: logical_bytes,
                        offset,
                        target,
                        limits: self.limits.decode,
                        budget: work
                            .remaining(budget)
                            .map_err(|error| failed(error.into(), work))?,
                    },
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
                work = work
                    .checked_add(receipt.work)
                    .map_err(|error| failed(error.into(), work))?;
                receipt.value
            }
            FilePayload::Regular { .. } => None,
            FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::Empty
            | FilePayload::Device { .. }
            | FilePayload::ReparsePoint { .. } => {
                return capture_file_record_state(record, budget, work);
            }
        };
        capture_sparse_seek_state(target, result, budget, work)
    }

    async fn hash_content_spans_async(
        &self,
        spans: Vec<super::ExtentSlice>,
        length: u64,
        hash_work: WorkCounters,
        budget: WorkBudget,
        mut work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(CONTENT_RANGE_DOMAIN);
        hasher.update(&length.to_le_bytes());
        for span in spans {
            cancellation
                .check()
                .map_err(|_| failed(AuthenticatedProbeError::Cancelled, work))?;
            match span.kind {
                ExtentKind::Hole | ExtentKind::AllocatedZero => {
                    let mut remaining_zeroes = span.length;
                    while remaining_zeroes != 0 {
                        cancellation
                            .check()
                            .map_err(|_| failed(AuthenticatedProbeError::Cancelled, work))?;
                        let count = usize::try_from(
                            remaining_zeroes
                                .min(u64::try_from(ZERO_HASH_BLOCK.len()).unwrap_or(u64::MAX)),
                        )
                        .unwrap_or(ZERO_HASH_BLOCK.len());
                        hasher.update(&ZERO_HASH_BLOCK[..count]);
                        remaining_zeroes -= u64::try_from(count).unwrap_or(0);
                    }
                }
                ExtentKind::Content {
                    object,
                    object_offset,
                } => {
                    let reserved = work
                        .checked_add(hash_work)
                        .map_err(|error| failed(error.into(), work))?;
                    let remaining = reserved
                        .remaining(budget)
                        .map_err(|error| failed(error.into(), work))?;
                    let read = read_blob_range_async(
                        self.store,
                        object,
                        ByteRange {
                            offset: object_offset,
                            length: span.length,
                        },
                        self.limits.decode,
                        remaining,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
                    work = work
                        .checked_add(read.work)
                        .map_err(|error| failed(error.into(), work))?;
                    hasher.update(&read.bytes);
                }
            }
        }
        work = work
            .checked_add(hash_work)
            .map_err(|error| failed(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| failed(error.into(), work))?;
        Ok(ProbeReceipt {
            value: DependencyState::Present(Digest::from_bytes(*hasher.finalize().as_bytes())),
            work,
        })
    }

    async fn directory_page_state_async(
        &self,
        record: FileRecord,
        after: Option<&super::LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        mut work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        let FilePayload::Directory { entries } = record.payload else {
            return capture_file_record_state(record, budget, work);
        };
        let remaining = work
            .remaining(budget)
            .map_err(|error| failed(error.into(), work))?;
        let page = list_tree_entries_async(
            self.store,
            entries,
            after,
            maximum_entries,
            self.limits.decode,
            remaining,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = work
            .checked_add(page.work)
            .map_err(|error| failed(error.into(), work))?;
        let mut encoded = encode_tree_page(&super::TreePage::Leaf(page.entries), maximum_entries)
            .map_err(|error| failed(error.into(), work))?;
        encoded.push(u8::from(page.has_more));
        let domain = b"acyclic-fs-dependency-directory-page-v1\0";
        let digest = semantic_digest(domain, &encoded);
        work = work
            .checked_add(WorkCounters {
                bytes_encoded: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
                bytes_hashed: u64::try_from(encoded.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::try_from(domain.len()).unwrap_or(u64::MAX))
                    .saturating_add(8),
                allocation_operations: 1,
                peak_allocation_bytes: u64::try_from(encoded.capacity()).unwrap_or(u64::MAX),
                ..WorkCounters::default()
            })
            .map_err(|error| failed(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| failed(error.into(), work))?;
        Ok(ProbeReceipt {
            value: DependencyState::Present(digest),
            work,
        })
    }

    /// Resolves one exact dependency through nonblocking immutable-object I/O.
    ///
    /// This is the browser/remote equivalent of [`RebaseProbe::probe`] and
    /// shares all canonical decoders and frontier machines with it.
    ///
    /// # Errors
    ///
    /// Returns exact typed authentication, cancellation, and work failures.
    pub async fn probe_async(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        cancellation
            .check()
            .map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::default()))?;
        let file_id = self.validate_region(region)?;
        let (record, work) = self
            .record_async(generation, file_id, budget, cancellation)
            .await?;
        self.capture_validated_record_async(record, region, budget, work, cancellation)
            .await
    }

    /// Captures one exact region from a file record already authenticated by
    /// the caller's generation/path frontier.
    ///
    /// This is the zero-reread mutation/read precondition path. The region is
    /// validated and must name the supplied record identity. `None` represents
    /// an authenticated absent identity.
    ///
    /// # Errors
    ///
    /// Returns a typed identity mismatch, malformed region, cancellation,
    /// storage, authentication, allocation, or exact-work failure.
    pub async fn capture_record_async(
        &self,
        record: Option<FileRecord>,
        region: &DependencyRegion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        cancellation
            .check()
            .map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::default()))?;
        let expected_file_id = self.validate_region(region)?;
        if record.is_some_and(|record| record.file_id != expected_file_id) {
            return Err(failed(
                AuthenticatedProbeError::RecordIdentityMismatch,
                WorkCounters::default(),
            ));
        }
        self.capture_validated_record_async(
            record,
            region,
            budget,
            WorkCounters::default(),
            cancellation,
        )
        .await
    }

    async fn capture_validated_record_async(
        &self,
        record: Option<FileRecord>,
        region: &DependencyRegion,
        budget: WorkBudget,
        mut work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        let Some(record) = record else {
            return Ok(ProbeReceipt {
                value: DependencyState::Absent,
                work,
            });
        };
        let value = match region {
            DependencyRegion::FileRecord(_) => {
                return capture_file_record_state(record, budget, work);
            }
            DependencyRegion::Metadata(_) => DependencyState::Present(record.metadata.digest),
            DependencyRegion::FileLength(_) => {
                return capture_file_length_state(record, budget, work);
            }
            DependencyRegion::DirectoryName { name, .. } => {
                let FilePayload::Directory { entries } = record.payload else {
                    return capture_file_record_state(record, budget, work);
                };
                let remaining = work
                    .remaining(budget)
                    .map_err(|error| failed(error.into(), work))?;
                let lookup = lookup_tree_entry_async(
                    self.store,
                    entries,
                    name,
                    self.limits.decode,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
                work = work
                    .checked_add(lookup.work)
                    .map_err(|error| failed(error.into(), work))?;
                match lookup.entry {
                    None => DependencyState::Absent,
                    Some(entry) => {
                        return capture_directory_name_state(&entry, budget, work);
                    }
                }
            }
            DependencyRegion::ContentRange { offset, length, .. } => {
                return self
                    .content_state_async(record, *offset, *length, budget, work, cancellation)
                    .await;
            }
            DependencyRegion::SparseSeek { offset, target, .. } => {
                return self
                    .sparse_seek_state_async(record, *offset, *target, budget, work, cancellation)
                    .await;
            }
            DependencyRegion::DirectoryRange {
                after,
                maximum_entries,
                ..
            } => {
                return self
                    .directory_page_state_async(
                        record,
                        after.as_ref(),
                        *maximum_entries,
                        budget,
                        work,
                        cancellation,
                    )
                    .await;
            }
        };
        Ok(ProbeReceipt { value, work })
    }
}

impl<S: AsyncObjectStore> AsyncRebaseProbe for AuthenticatedGenerationProbe<'_, S> {
    type Error = AuthenticatedProbeError;

    async fn probe_async(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, OperationFailure<Self::Error>> {
        AuthenticatedGenerationProbe::probe_async(self, generation, region, budget, cancellation)
            .await
    }
}

impl<S: crate::ImmediateObjectStore> RebaseProbe for AuthenticatedGenerationProbe<'_, S> {
    type Error = AuthenticatedProbeError;

    fn probe(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
    ) -> Result<ProbeReceipt, OperationFailure<Self::Error>> {
        let file_id = self.validate_region(region)?;
        let (record, mut work) = self.record(generation, file_id, budget)?;
        let Some(record) = record else {
            return Ok(ProbeReceipt {
                value: DependencyState::Absent,
                work,
            });
        };
        let value = match region {
            DependencyRegion::FileRecord(_) => {
                return capture_file_record_state(record, budget, work);
            }
            DependencyRegion::Metadata(_) => DependencyState::Present(record.metadata.digest),
            DependencyRegion::FileLength(_) => {
                return capture_file_length_state(record, budget, work);
            }
            DependencyRegion::DirectoryName { name, .. } => {
                let FilePayload::Directory { entries } = record.payload else {
                    return capture_file_record_state(record, budget, work);
                };
                let remaining = work
                    .remaining(budget)
                    .map_err(|error| failed(error.into(), work))?;
                let lookup =
                    lookup_tree_entry(self.store, entries, name, self.limits.decode, remaining)
                        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
                work = work
                    .checked_add(lookup.work)
                    .map_err(|error| failed(error.into(), work))?;
                match lookup.entry {
                    None => DependencyState::Absent,
                    Some(entry) => {
                        return capture_directory_name_state(&entry, budget, work);
                    }
                }
            }
            DependencyRegion::ContentRange { offset, length, .. } => {
                return self.content_state(record, *offset, *length, budget, work);
            }
            DependencyRegion::SparseSeek { offset, target, .. } => {
                return self.sparse_seek_state(record, *offset, *target, budget, work);
            }
            DependencyRegion::DirectoryRange {
                after,
                maximum_entries,
                ..
            } => {
                return self.directory_page_state(
                    record,
                    after.as_ref(),
                    *maximum_entries,
                    budget,
                    work,
                );
            }
        };
        Ok(ProbeReceipt { value, work })
    }
}

fn clear_at_capacity<K: Eq + Hash, V>(map: &mut HashMap<K, V>, maximum: u32) {
    if u32::try_from(map.len()).unwrap_or(u32::MAX) >= maximum {
        map.clear();
    }
}

fn inline_seek_result(
    data: super::InlineFileData,
    offset: u64,
    target: ExtentSeekTarget,
) -> Option<u64> {
    let logical_bytes = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
    if offset > logical_bytes {
        return None;
    }
    match target {
        ExtentSeekTarget::Data => (offset < logical_bytes).then_some(offset),
        ExtentSeekTarget::Hole => Some(logical_bytes),
    }
}

fn hash_content_range(
    bytes: &[u8],
    budget: WorkBudget,
    work: WorkCounters,
) -> Result<ProbeReceipt, ProbeFailure> {
    let hash_work = WorkCounters {
        bytes_hashed: u64::try_from(CONTENT_RANGE_DOMAIN.len())
            .unwrap_or(u64::MAX)
            .saturating_add(8)
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
        ..WorkCounters::default()
    };
    let next = work
        .checked_add(hash_work)
        .map_err(|error| failed(error.into(), work))?;
    next.verify(budget)
        .map_err(|error| failed(error.into(), work))?;
    Ok(ProbeReceipt {
        value: DependencyState::Present(semantic_digest(CONTENT_RANGE_DOMAIN, bytes)),
        work: next,
    })
}

fn semantic_digest(domain: &[u8], bytes: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(bytes);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

type ProbeFailure = OperationFailure<AuthenticatedProbeError>;

fn failed(error: AuthenticatedProbeError, work: WorkCounters) -> ProbeFailure {
    OperationFailure::new(error, work)
}

fn validate_capture_batch(count: usize, maximum: u32) -> Result<(), ProbeFailure> {
    if maximum == 0 {
        return Err(failed(
            AuthenticatedProbeError::ZeroBatchLimit,
            WorkCounters::default(),
        ));
    }
    if u32::try_from(count).unwrap_or(u32::MAX) > maximum {
        return Err(failed(
            AuthenticatedProbeError::TooManyRegions { maximum },
            WorkCounters::default(),
        ));
    }
    Ok(())
}

fn allocate_capture_output<T>(
    input: &Vec<T>,
    budget: WorkBudget,
) -> Result<(Vec<Dependency>, WorkCounters), ProbeFailure> {
    let input_bytes = input
        .capacity()
        .checked_mul(size_of::<T>())
        .map(crate::foundation::usize_to_u64)
        .ok_or_else(|| {
            failed(
                AuthenticatedProbeError::AllocationFailed,
                WorkCounters::default(),
            )
        })?;
    let mut captured = Vec::new();
    captured.try_reserve_exact(input.len()).map_err(|_| {
        failed(
            AuthenticatedProbeError::AllocationFailed,
            WorkCounters::default(),
        )
    })?;
    let output_bytes = captured
        .capacity()
        .checked_mul(size_of::<Dependency>())
        .map(crate::foundation::usize_to_u64)
        .ok_or_else(|| {
            failed(
                AuthenticatedProbeError::AllocationFailed,
                WorkCounters::default(),
            )
        })?;
    let work = WorkCounters {
        allocation_operations: u64::from(!captured.is_empty()),
        peak_allocation_bytes: input_bytes.checked_add(output_bytes).ok_or_else(|| {
            failed(
                AuthenticatedProbeError::AllocationFailed,
                WorkCounters::default(),
            )
        })?,
        ..WorkCounters::default()
    };
    work.verify(budget)
        .map_err(|error| failed(error.into(), WorkCounters::default()))?;
    Ok((captured, work))
}

/// Authenticated generation-probe failures.
#[derive(Debug, Error)]
pub enum AuthenticatedProbeError {
    /// Cooperative cancellation occurred before the next storage or hashing boundary.
    #[error("authenticated dependency probing was cancelled")]
    Cancelled,
    /// Cache or extent-output limits are zero.
    #[error("authenticated probe limits are invalid")]
    InvalidLimits,
    /// A dependency-capture batch requires a positive region bound.
    #[error("authenticated dependency batch limit must be positive")]
    ZeroBatchLimit,
    /// A dependency-capture batch exceeds its admitted region count.
    #[error("authenticated dependency batch exceeds maximum {maximum}")]
    TooManyRegions {
        /// Admitted maximum region count.
        maximum: u32,
    },
    /// Bounded dependency output allocation failed or overflowed.
    #[error("authenticated dependency batch allocation failed")]
    AllocationFailed,
    /// A directly supplied record does not own the requested dependency region.
    #[error("authenticated record identity does not match dependency region")]
    RecordIdentityMismatch,
    /// Content dependency exceeds the configured byte bound.
    #[error("authenticated content dependency exceeds its byte bound")]
    ContentRangeTooLarge,
    /// Directory dependency exceeds the configured entry bound.
    #[error("authenticated directory dependency exceeds its entry bound")]
    DirectoryPageTooLarge,
    /// Dependency shape is empty, overflowing, or otherwise invalid.
    #[error(transparent)]
    InvalidDependency(#[from] DependencyError),
    /// Process-local immutable summary cache was poisoned.
    #[error("authenticated probe cache is poisoned")]
    CachePoisoned,
    /// Immutable object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Generation or canonical semantic encoding failed.
    #[error(transparent)]
    Decode(#[from] super::CanonicalDecodeError),
    /// File-table frontier authentication failed.
    #[error(transparent)]
    FileRecord(#[from] FileRecordReadError),
    /// Directory frontier authentication failed.
    #[error(transparent)]
    Tree(#[from] TreeReadError),
    /// Bounded directory frontier authentication failed.
    #[error(transparent)]
    Directory(#[from] DirectoryReadError),
    /// Sparse extent frontier authentication failed.
    #[error(transparent)]
    Extent(#[from] ExtentReadError),
    /// Authenticated content blob reading failed.
    #[error(transparent)]
    Blob(#[from] BlobReadError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/probe.rs"]
mod tests;
