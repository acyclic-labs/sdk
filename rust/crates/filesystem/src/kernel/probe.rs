//! Object-store-backed authenticated dependency probes.


use super::{
    AsyncRebaseProbe, BlobReadError, DecodeLimits, Dependency, DependencyError, DependencyRegion,
    DependencyState, DirectoryReadError, ExtentKind, ExtentReadError, ExtentSeekRequest,
    ExtentSeekTarget, FilePayload, FileRecord, FileRecordReadError, ProbeReceipt,
    TreeReadError,
};
use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::persistent_point::{Owned, retire_allocation};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::{Digest, FileId, GenerationId, usize_to_u64};
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ByteRange, ObjectId, ObjectKind, ObjectStoreError};
use std::mem::size_of;
use std::sync::Mutex;
use thiserror::Error;

const ZERO_HASH_BLOCK: [u8; 8 * 1024] = [0; 8 * 1024];
const CONTENT_RANGE_DOMAIN: &[u8] = b"acyclic-fs-dependency-content-range-v1\0";

/// Computes canonical layout-independent evidence for one non-empty logical content range already present in memory.
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
    if usize_to_u64(bytes.len()) > maximum_bytes {
        return Err(failed(
            AuthenticatedProbeError::ContentRangeTooLarge,
            WorkCounters::default(),
        ));
    }
    hash_content_range(bytes, budget, WorkCounters::default())
}

/// Authenticated exact-region resolver with disposable per-instance summary caches.
pub struct AuthenticatedGenerationProbe<'a, S> {
    store: &'a S,
    limits: ProbeLimits,
    data_custody: Option<&'a super::ProofDataCustody>,
    async_generations: Mutex<ProbeSummaryCache<GenerationId, ObjectId>>,
    async_records: Mutex<ProbeSummaryCache<(GenerationId, FileId), Option<FileRecord>>>,
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
            data_custody: None,
            async_generations: Mutex::new(ProbeSummaryCache::new()),
            async_records: Mutex::new(ProbeSummaryCache::new()),
        })
    }

    pub(crate) fn new_with_accounting(
        store: &'a S,
        limits: ProbeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        accounting.with_allocations(|accounting| {
            let allocation = accounting.claim_owned(0, 0)?;
            let probe = Self::new(store, limits).map_err(crate::FsError::from)?;
            Ok((probe, allocation))
        })
    }

    pub(crate) fn retire_with_accounting(
        self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let Self { async_generations, async_records, .. } = self;
        let generations = async_generations.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
        let records = async_records.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
        let generations = generations.retire(accounting);
        let records = records.retire(accounting);
        generations?;
        records
    }

    pub(crate) fn with_data_custody(mut self, custody: Option<&'a super::ProofDataCustody>) -> Self {
        self.data_custody = custody;
        self
    }

    fn validate_region_shape(&self, region: &DependencyRegion) -> Result<FileId, AuthenticatedProbeError> {
        region.validate()?;
        match region {
            DependencyRegion::ContentRange { length, .. }
                if *length > self.limits.maximum_content_payload_bytes =>
            {
                return Err(AuthenticatedProbeError::ContentRangeTooLarge);
            }
            DependencyRegion::DirectoryRange { maximum_entries, .. }
                if *maximum_entries > self.limits.maximum_directory_entries =>
            {
                return Err(AuthenticatedProbeError::DirectoryPageTooLarge);
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

pub(crate) fn capture_file_record_state(
    record: FileRecord,
    budget: WorkBudget,
    work: WorkCounters,
    data_custody: Option<&super::ProofDataCustody>,
    standing: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<ProbeReceipt, ProbeFailure> {
    let cached = data_custody.map_or(0, super::ProofDataCustody::operation_cache_bytes);
    let standing = standing.max(cached);
    let _scope = ProbeNameResidency::new(data_custody, standing - cached)
        .map_err(|error| failed(error.into(), work))?;
    project_accounted_capture(budget, work, standing, cancellation, |accounting| {
        capture_file_record_state_with_accounting(record, accounting, cancellation, data_custody)
    })
}

pub(crate) fn capture_file_record_state_with_accounting(
    record: FileRecord,
    accounting: &mut KernelAccounting<'_>,
    cancellation: Option<&CancellationToken>,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<DependencyState, crate::FsError> {
    cancellation.map_or(Ok(()), CancellationToken::check)?;
    let (bytes, allocation) = crate::kernel::codec::encode_owned(accounting, |observe, reserve| super::file_table::encode_file_table_leaf_records(std::slice::from_ref(&record), 1, data_custody, observe, reserve))?;
    let capacity = usize_to_u64(bytes.capacity());
    capture_encoded_state_with_accounting(
        Owned::new(bytes, allocation, capacity),
        b"acyclic-fs-dependency-file-record-v1\0", accounting, cancellation, data_custody,
    )
}

pub(crate) fn capture_sparse_seek_state(
    target: ExtentSeekTarget,
    result: Option<u64>,
    budget: WorkBudget,
    mut work: WorkCounters,
) -> Result<ProbeReceipt, ProbeFailure> {
    let domain = b"acyclic-fs-dependency-sparse-seek-v1\0";
    let completed = work.checked_add(WorkCounters {
        bytes_encoded: 10,
        bytes_hashed: 10 + usize_to_u64(domain.len()) + 8,
        ..WorkCounters::default()
    }).map_err(|error| failed(error.into(), work))?;
    completed.verify(budget).map_err(|error| failed(error.into(), work))?;
    let mut encoded = [0_u8; 10];
    encoded[0] = match target {
        ExtentSeekTarget::Data => 0,
        ExtentSeekTarget::Hole => 1,
    };
    if let Some(offset) = result {
        encoded[1] = 1;
        encoded[2..].copy_from_slice(&offset.to_le_bytes());
    }
    let value = DependencyState::Present(semantic_digest(domain, &encoded));
    work = completed;
    Ok(ProbeReceipt { value, work })
}

fn capture_file_length_state(
    record: FileRecord,
    budget: WorkBudget,
    mut work: WorkCounters,
    data_custody: Option<&super::ProofDataCustody>,
    cancellation: Option<&CancellationToken>,
) -> Result<ProbeReceipt, ProbeFailure> {
    let logical_bytes = match record.payload {
        FilePayload::InlineRegular(data) => usize_to_u64(data.as_bytes().len()),
        FilePayload::Regular { logical_bytes, .. } => logical_bytes,
        FilePayload::Directory { .. }
        | FilePayload::SymbolicLink { .. }
        | FilePayload::Device { .. }
        | FilePayload::Empty
        | FilePayload::ReparsePoint { .. } => {
            return capture_file_record_state(record, budget, work, data_custody, 0, cancellation);
        }
    };
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(failed(AuthenticatedProbeError::Cancelled, work));
    }
    let domain = b"acyclic-fs-dependency-file-length-v1\0";
    let encoded = logical_bytes.to_le_bytes();
    let delta = WorkCounters {
        bytes_encoded: usize_to_u64(encoded.len()),
        bytes_hashed: usize_to_u64(encoded.len())
            .saturating_add(usize_to_u64(domain.len()))
            .saturating_add(8),
        ..WorkCounters::default()
    };
    work.charge(&delta, &budget).map_err(|error| failed(error.into(), work))?;
    if let Some(owner) = data_custody {
        owner.record_completed_leaf_work(delta).map_err(|error| failed(error.into(), work))?;
    }
    let value = DependencyState::Present(semantic_digest(domain, &encoded));
    Ok(ProbeReceipt { value, work })
}

pub(crate) fn capture_directory_name_state(
    entry: &super::TreeEntry,
    budget: WorkBudget,
    work: WorkCounters,
    data_custody: Option<&super::ProofDataCustody>,
    standing: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<ProbeReceipt, ProbeFailure> {
    let cached = data_custody.map_or(0, super::ProofDataCustody::operation_cache_bytes);
    let external = standing.max(cached);
    let standing = external.checked_add(usize_to_u64(entry.name.retained_bytes()))
        .ok_or_else(|| failed(WorkError::Overflow.into(), work))?;
    let _name = ProbeNameResidency::new(data_custody, external - cached)
        .map_err(|error| failed(error.into(), work))?;
    project_accounted_capture(budget, work, standing, cancellation, |accounting| {
        capture_directory_name_state_with_accounting(entry, accounting, cancellation, data_custody)
    })
}

pub(crate) fn capture_directory_name_state_with_accounting(
    entry: &super::TreeEntry,
    accounting: &mut KernelAccounting<'_>,
    cancellation: Option<&CancellationToken>,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<DependencyState, crate::FsError> {
    cancellation.map_or(Ok(()), CancellationToken::check)?;
    let _name = ProbeNameResidency::new(data_custody, usize_to_u64(entry.name.retained_bytes()))?;
    let (bytes, allocation) = crate::kernel::codec::encode_owned(accounting, |observe, _reserve| super::tree::encode_tree_leaf_entries(std::slice::from_ref(entry), 1, None, data_custody, observe, crate::FsError::EncodedAllocationFailed))?;
    let capacity = usize_to_u64(bytes.capacity());
    capture_encoded_state_with_accounting(
        Owned::new(bytes, allocation, capacity),
        b"acyclic-fs-dependency-directory-name-v1\0", accounting, cancellation, data_custody,
    )
}

fn project_accounted_capture(
    budget: WorkBudget, mut work: WorkCounters, standing: u64,
    cancellation: Option<&CancellationToken>,
    capture: impl FnOnce(&mut KernelAccounting<'_>) -> Result<DependencyState, crate::FsError>,
) -> Result<ProbeReceipt, ProbeFailure> {
    let unobserved = CancellationToken::new();
    let mut allocations = super::allocation::AllocationLedger::default();
    allocations.claim_bytes(standing, 0, &mut work, budget)
        .map_err(|error| failed(error.into_work_or(AuthenticatedProbeError::AllocationFailed), work))?;
    let result = capture(&mut KernelAccounting::borrowed_standalone(
        &mut allocations, &mut work, budget, cancellation.unwrap_or(&unobserved),
    ));
    result.map(|value| ProbeReceipt { value, work }).map_err(|error| {
        let error = match error {
            crate::FsError::Probe(error) => error,
            crate::FsError::Decode(error) => AuthenticatedProbeError::Decode(error),
            crate::FsError::Work(error) => AuthenticatedProbeError::Work(error),
            crate::FsError::Cancelled(_) => AuthenticatedProbeError::Cancelled,
            crate::FsError::EncodedAllocationFailed(error) => AuthenticatedProbeError::EncodedAllocationFailed(error),
            #[cfg(not(target_arch = "wasm32"))]
            crate::FsError::NativeKernel(error) => AuthenticatedProbeError::Storage(ObjectStoreError::Native(error)),
            _ => unreachable!("standalone capture leaves return only typed decode, allocation, work, probe, native or cancellation errors"),
        };
        failed(error, work)
    })
}

fn capture_encoded_state_with_accounting(
    encoded: Owned<Vec<u8>>, domain: &[u8], accounting: &mut KernelAccounting<'_>,
    cancellation: Option<&CancellationToken>, data_custody: Option<&super::ProofDataCustody>,
) -> Result<DependencyState, crate::FsError> {
    let result = (|| {
        cancellation.map_or(Ok(()), CancellationToken::check)?;
        let hash = WorkCounters {
            bytes_hashed: usize_to_u64(encoded.value.len()).checked_add(usize_to_u64(domain.len()))
                .and_then(|bytes| bytes.checked_add(8)).ok_or(WorkError::Overflow)?,
            ..WorkCounters::default()
        };
        accounting.admit(hash)?;
        let value = DependencyState::Present(semantic_digest(domain, &encoded.value));
        let completed = accounting.record_completed(hash);
        let attributed = data_custody.map_or(Ok(()), |owner| owner.record_completed_leaf_work(hash));
        completed?;
        attributed?;
        Ok(value)
    })();
    // The canonical digest is scalar; only the real temporary encoder is owned.
    let retired = encoded.retire(accounting);
    match result {
        Err(error) => Err(error),
        Ok(value) => { retired?; Ok(value) }
    }
}
fn state_encoding_budget(work: WorkCounters, budget: WorkBudget, standing: u64) -> Result<WorkBudget, ProbeFailure> {
    let mut remaining = work.remaining(budget).map_err(|error| failed(error.into(), work))?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(standing)
        .ok_or_else(|| failed(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", observed: standing, maximum: budget.peak_allocation_bytes,
        }.into(), work))?;
    Ok(remaining)
}

fn capture_directory_page_encoding(
    entries: Vec<super::TreeEntry>, has_more: bool, maximum_entries: u32,
    budget: WorkBudget, work: WorkCounters, data_custody: Option<&super::ProofDataCustody>,
    cancellation: Option<&CancellationToken>,
) -> Result<ProbeReceipt, ProbeFailure> {
    let owned = usize_to_u64(entries.capacity()).checked_mul(size_of::<super::TreeEntry>() as u64)
        .and_then(|bytes| entries.iter().try_fold(bytes, |total, entry| {
            total.checked_add(usize_to_u64(entry.name.retained_bytes()))
        })).ok_or_else(|| failed(WorkError::Overflow.into(), work))?;
    let cached = data_custody.map_or(0, super::ProofDataCustody::operation_cache_bytes);
    let standing = cached.checked_add(owned).ok_or_else(|| failed(WorkError::Overflow.into(), work))?;
    let _scope = ProbeNameResidency::new(data_custody, owned).map_err(|error| failed(error.into(), work))?;
    // The moved entries are declared after their report guard: actual buffers
    // are destroyed before their operation-residency registration retires.
    let entries = entries;
    let remaining = state_encoding_budget(work, budget, standing)?;
    let encoded = crate::kernel::encode(remaining, cancellation, 0, |observe| super::tree::encode_tree_leaf_entries(&entries, maximum_entries, Some(u8::from(has_more)), None, observe, crate::kernel::EncodeError::AllocationFailed)).map_err(|failure| {
        let completed = match work.with_backend(*failure.work, standing) {
            Ok(completed) => completed,
            Err(error) => return failed(error.into(), work),
        };
        if let Some(owner) = data_custody {
            if let Err(error) = owner.record_completed_leaf_work(*failure.work) {
                return failed(error.into(), completed);
            }
        }
        let error = match failure.error {
            super::EncodeError::Canonical(error) => error.into(),
            super::EncodeError::Work(error) => error.into(),
            super::EncodeError::Cancelled(_) => AuthenticatedProbeError::Cancelled,
            super::EncodeError::AllocationFailed(error) => AuthenticatedProbeError::EncodedAllocationFailed(error),
        };
        failed(error, completed)
    })?;
    capture_encoded_state(encoded, b"acyclic-fs-dependency-directory-page-v1\0", budget, work, standing, data_custody, cancellation)
}

struct ProbeNameResidency<'a> {
    token: Option<&'a super::ProofDataCustody>,
    bytes: u64,
}

impl<'a> ProbeNameResidency<'a> {
    fn new(token: Option<&'a super::ProofDataCustody>, bytes: u64) -> Result<Self, WorkError> {
        if let Some(token) = token { token.add_operation_cache_bytes(bytes)?; }
        Ok(Self { token, bytes })
    }
}

impl Drop for ProbeNameResidency<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.token { token.release_operation_cache_bytes(self.bytes); }
    }
}

fn capture_encoded_state(
    encoded: crate::OperationReceipt<Vec<u8>>, domain: &[u8], budget: WorkBudget,
    work: WorkCounters, standing: u64, data_custody: Option<&super::ProofDataCustody>,
    cancellation: Option<&CancellationToken>,
) -> Result<ProbeReceipt, ProbeFailure> {
    if let Some(owner) = data_custody {
        owner.record_completed_leaf_work(encoded.work).map_err(|error| failed(error.into(), work))?;
    }
    let mut work = work.with_backend(encoded.work, standing).map_err(|error| failed(error.into(), work))?;
    work.verify(budget).map_err(|error| failed(error.into(), work))?;
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(failed(AuthenticatedProbeError::Cancelled, work));
    }
    let hash = WorkCounters {
        bytes_hashed: usize_to_u64(encoded.value.len()).checked_add(usize_to_u64(domain.len()))
            .and_then(|bytes| bytes.checked_add(8)).ok_or_else(|| failed(WorkError::Overflow.into(), work))?,
        ..WorkCounters::default()
    };
    work.charge(&hash, &budget).map_err(|error| failed(error.into(), work))?;
    if let Some(owner) = data_custody {
        owner.record_completed_leaf_work(hash).map_err(|error| failed(error.into(), work))?;
    }
    let value = DependencyState::Present(semantic_digest(domain, &encoded.value));
    Ok(ProbeReceipt { value, work })
}

struct ProbeSummaryCache<K, V> {
    entries: Owned<Vec<(K, V)>>,
}

impl<K: Copy + Eq, V: Copy> ProbeSummaryCache<K, V> {
    fn new() -> Self {
        Self { entries: Owned::new(Vec::new(), KernelOwnedAllocation::Standalone, 0) }
    }

    fn check_scope(&self, accounting: &KernelAccounting<'_>) -> Result<(), crate::FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.entries.bytes != 0 {
            let valid = match (&self.entries.allocation, accounting.native_work()) {
                (KernelOwnedAllocation::Standalone, None) => true,
                (KernelOwnedAllocation::NativeOriginal(guard), Some(original)) =>
                    guard.work().shares_ledger(original),
                _ => false,
            };
            if !valid { return Err(crate::NativeTreeMaterializationError::InvalidLayout.into()); }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = accounting;
        Ok(())
    }

    fn get_with_accounting(
        &self, key: &K, accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<V>, crate::FsError> {
        self.check_scope(accounting)?;
        for (existing, value) in &self.entries.value {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if existing == key { return Ok(Some(*value)); }
        }
        Ok(None)
    }

    fn insert_with_accounting(
        &mut self, key: K, value: V, maximum: u32, accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        self.check_scope(accounting)?;
        let maximum = usize::try_from(maximum).map_err(|_| WorkError::Overflow)?;
        if self.entries.value.len() >= maximum { self.entries.value.clear(); }
        if self.entries.value.len() == self.entries.value.capacity() {
            let capacity = self.entries.value.capacity().saturating_mul(2).max(1).min(maximum);
            let mut next = Owned::allocate(capacity, accounting)?;
            let copied = WorkCounters {
                bytes_copied: usize_to_u64(self.entries.value.len().checked_mul(size_of::<(K, V)>())
                    .ok_or(WorkError::Overflow)?),
                ..WorkCounters::UNCHARGED
            };
            if let Err(error) = accounting.charge(copied) {
                next.retire(accounting)?;
                return Err(error);
            }
            next.value.extend_from_slice(&self.entries.value);
            let previous = std::mem::replace(&mut self.entries, next);
            if previous.bytes == 0 { drop(previous); } else { previous.retire(accounting)?; }
        }
        self.entries.value.push((key, value));
        Ok(())
    }

    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        if self.entries.bytes == 0 { drop(self); Ok(()) } else { self.entries.retire(accounting) }
    }
}

impl<S: AsyncObjectStore> AuthenticatedGenerationProbe<'_, S> {
    fn standalone_accounting<'c>(
        &self, budget: WorkBudget, cancellation: &'c CancellationToken, input_bytes: u64,
    ) -> Result<KernelAccounting<'c>, ProbeFailure> {
        let cached = self.async_generations.lock()
            .map_err(|_| failed(AuthenticatedProbeError::CachePoisoned, WorkCounters::UNCHARGED))?
            .entries.bytes.checked_add(self.async_records.lock()
                .map_err(|_| failed(AuthenticatedProbeError::CachePoisoned, WorkCounters::UNCHARGED))?
                .entries.bytes).ok_or_else(|| failed(WorkError::Overflow.into(), WorkCounters::UNCHARGED))?;
        let standing = cached.checked_add(self.data_custody.map_or(0, super::ProofDataCustody::operation_cache_bytes))
            .and_then(|bytes| bytes.checked_add(input_bytes))
            .ok_or_else(|| failed(WorkError::Overflow.into(), WorkCounters::UNCHARGED))?;
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        accounting.with_allocations(|accounting| {
            accounting.claim_owned(standing, 0).map(drop)
        }).map_err(|error| failed(project_probe_error(error), accounting.spent()))?;
        Ok(accounting)
    }

    /// Captures a bounded batch, preserving order, duplicates and moved names.
    pub async fn capture_many_async(
        &self, generation: GenerationId, regions: Vec<DependencyRegion>, maximum_regions: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Vec<Dependency>>, ProbeFailure> {
        cancellation.check().map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::UNCHARGED))?;
        validate_capture_batch(regions.len(), maximum_regions)?;
        for region in &regions { self.validate_region_shape(region).map_err(|error| failed(error, WorkCounters::UNCHARGED))?; }
        let input_bytes = capture_input_bytes(&regions, |region| region)?;
        let mut accounting = self.standalone_accounting(budget, cancellation, input_bytes)?;
        let result = self.capture_many_with_accounting(
            generation, regions, maximum_regions, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(value, _)| OperationReceipt { value, work })
            .map_err(|error| failed(project_probe_error(error), work))
    }

    pub(crate) async fn capture_many_with_accounting(
        &self, generation: GenerationId, regions: Vec<DependencyRegion>, maximum_regions: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(Vec<Dependency>, KernelOwnedAllocation), crate::FsError> {
        cancellation.check()?;
        accounting.check()?;
        validate_capture_batch_error(regions.len(), maximum_regions)?;
        for region in &regions { self.validate_region_error(region)?; }
        let mut captured = Owned::<Vec<Dependency>>::allocate(regions.len(), accounting)?;
        let result = async {
            for region in regions {
                let expected = self.probe_async_with_accounting(
                    generation, &region, accounting, cancellation,
                ).await?;
                let returned = WorkCounters {
                    bytes_copied: usize_to_u64(size_of::<Dependency>()),
                    items_returned: 1,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(returned)?;
                captured.value.push(Dependency { region, expected });
                let completed = accounting.record_completed(returned);
                let traced = self.data_custody.map_or(Ok(()), |owner| owner.record_completed_leaf_work(returned));
                completed?;
                traced?;
            }
            Ok(())
        }.await;
        if let Err(error) = result { captured.retire(accounting)?; return Err(error); }
        Ok((captured.value, captured.allocation))
    }

    /// Captures records already authenticated by the caller without rereading their generation or file table.
    pub async fn capture_records_many_async(
        &self, regions: Vec<(Option<FileRecord>, DependencyRegion)>, maximum_regions: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Vec<Dependency>>, ProbeFailure> {
        cancellation.check().map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::UNCHARGED))?;
        validate_capture_batch(regions.len(), maximum_regions)?;
        for (record, region) in &regions { self.validate_record(*record, region)?; }
        let input_bytes = capture_input_bytes(&regions, |(_, region)| region)?;
        let mut accounting = self.standalone_accounting(budget, cancellation, input_bytes)?;
        let result = self.capture_records_many_with_accounting(
            regions, maximum_regions, &mut accounting, cancellation, self.data_custody,
        ).await;
        let work = accounting.spent();
        result.map(|(value, _)| OperationReceipt { value, work })
            .map_err(|error| failed(project_probe_error(error), work))
    }

    pub(crate) async fn capture_records_many_with_accounting(
        &self, regions: Vec<(Option<FileRecord>, DependencyRegion)>, maximum_regions: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
        custody: Option<&super::ProofDataCustody>,
    ) -> Result<(Vec<Dependency>, KernelOwnedAllocation), crate::FsError> {
        cancellation.check()?;
        accounting.check()?;
        validate_capture_batch_error(regions.len(), maximum_regions)?;
        for (record, region) in &regions { self.validate_record_error(*record, region)?; }
        let mut captured = Owned::<Vec<Dependency>>::allocate(regions.len(), accounting)?;
        let result = async {
            for (record, region) in regions {
                let expected = self.capture_validated_record_with_accounting(
                    record, &region, accounting, cancellation, custody,
                ).await?;
                let returned = WorkCounters {
                    bytes_copied: usize_to_u64(size_of::<Dependency>()),
                    items_returned: 1,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(returned)?;
                captured.value.push(Dependency { region, expected });
                let completed = accounting.record_completed(returned);
                let traced = custody.map_or(Ok(()), |owner| owner.record_completed_leaf_work(returned));
                completed?;
                traced?;
            }
            Ok(())
        }.await;
        if let Err(error) = result { captured.retire(accounting)?; return Err(error); }
        Ok((captured.value, captured.allocation))
    }

    fn validate_region_error(&self, region: &DependencyRegion) -> Result<FileId, crate::FsError> {
        self.validate_region_shape(region).map_err(crate::FsError::Probe)
    }

    fn validate_record(
        &self, record: Option<FileRecord>, region: &DependencyRegion,
    ) -> Result<(), ProbeFailure> {
        self.validate_record_error(record, region)
            .map_err(|error| failed(project_probe_error(error), WorkCounters::UNCHARGED))
    }

    fn validate_record_error(
        &self, record: Option<FileRecord>, region: &DependencyRegion,
    ) -> Result<(), crate::FsError> {
        let expected = self.validate_region_error(region)?;
        if record.is_some_and(|record| record.file_id != expected) {
            return Err(AuthenticatedProbeError::RecordIdentityMismatch.into());
        }
        Ok(())
    }

    async fn generation_with_accounting(
        &self, generation: GenerationId, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, crate::FsError> {
        cancellation.check()?;
        accounting.check()?;
        if let Some(root) = self.async_generations.lock()
            .map_err(|_| crate::FsError::Probe(AuthenticatedProbeError::CachePoisoned))?
            .get_with_accounting(&generation, accounting)? {
            return Ok(root);
        }
        let object = ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() };
        let (root, allocation) = super::generation::read_generation_root_with_limits_and_accounting(
            self.store, object, self.limits.decode, accounting, cancellation,
        ).await?;
        let retained = usize_to_u64(root.parents.capacity().checked_mul(size_of::<GenerationId>())
            .ok_or(WorkError::Overflow)?);
        let root = Owned::new(root, allocation, retained);
        let file_table = root.value.file_table;
        root.retire(accounting)?;
        self.async_generations.lock()
            .map_err(|_| crate::FsError::Probe(AuthenticatedProbeError::CachePoisoned))?
            .insert_with_accounting(generation, file_table, self.limits.maximum_cached_generations, accounting)?;
        Ok(file_table)
    }

    async fn record_with_accounting(
        &self, generation: GenerationId, file_id: FileId, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, crate::FsError> {
        cancellation.check()?;
        accounting.check()?;
        if let Some(record) = self.async_records.lock()
            .map_err(|_| crate::FsError::Probe(AuthenticatedProbeError::CachePoisoned))?
            .get_with_accounting(&(generation, file_id), accounting)? {
            return Ok(record);
        }
        let file_table = self.generation_with_accounting(generation, accounting, cancellation).await?;
        let (record, allocation) = super::file_table::lookup_file_record_async_with_accounting(
            self.store, file_table, file_id, self.limits.decode, accounting, cancellation, self.data_custody,
        ).await?;
        // A file record is scalar, including its fixed-capacity inline payload.
        retire_allocation(allocation, 0, accounting)?;
        self.async_records.lock()
            .map_err(|_| crate::FsError::Probe(AuthenticatedProbeError::CachePoisoned))?
            .insert_with_accounting((generation, file_id), record, self.limits.maximum_cached_records, accounting)?;
        Ok(record)
    }

    /// Resolves the full exact-region algorithm through nonblocking canonical I/O.
    pub async fn probe_async(
        &self, generation: GenerationId, region: &DependencyRegion, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        cancellation.check().map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::UNCHARGED))?;
        self.validate_region_shape(region).map_err(|error| failed(error, WorkCounters::UNCHARGED))?;
        let mut accounting = self.standalone_accounting(budget, cancellation, region_owned_bytes(region))?;
        let result = self.probe_async_with_accounting(generation, region, &mut accounting, cancellation).await;
        let work = accounting.spent();
        result.map(|value| ProbeReceipt { value, work })
            .map_err(|error| failed(project_probe_error(error), work))
    }

    pub(crate) async fn probe_async_with_accounting(
        &self, generation: GenerationId, region: &DependencyRegion,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<DependencyState, crate::FsError> {
        cancellation.check()?;
        accounting.check()?;
        let file_id = self.validate_region_error(region)?;
        let record = self.record_with_accounting(generation, file_id, accounting, cancellation).await?;
        self.capture_validated_record_with_accounting(
            record, region, accounting, cancellation, self.data_custody,
        ).await
    }

    /// Captures an already authenticated record without rereading its frontier.
    pub async fn capture_record_async(
        &self, record: Option<FileRecord>, region: &DependencyRegion, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, ProbeFailure> {
        cancellation.check().map_err(|_| failed(AuthenticatedProbeError::Cancelled, WorkCounters::UNCHARGED))?;
        self.validate_record(record, region)?;
        let mut accounting = self.standalone_accounting(budget, cancellation, region_owned_bytes(region))?;
        let result = self.capture_validated_record_with_accounting(
            record, region, &mut accounting, cancellation, self.data_custody,
        ).await;
        let work = accounting.spent();
        result.map(|value| ProbeReceipt { value, work })
            .map_err(|error| failed(project_probe_error(error), work))
    }

    async fn capture_validated_record_with_accounting(
        &self, record: Option<FileRecord>, region: &DependencyRegion,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
        custody: Option<&super::ProofDataCustody>,
    ) -> Result<DependencyState, crate::FsError> {
        cancellation.check()?;
        accounting.check()?;
        let Some(record) = record else { return Ok(DependencyState::Absent); };
        match region {
            DependencyRegion::FileRecord(_) => {
                capture_file_record_state_with_accounting(record, accounting, Some(cancellation), custody)
            }
            DependencyRegion::Metadata(_) => Ok(DependencyState::Present(record.metadata.digest)),
            DependencyRegion::FileLength(_) => {
                let length = match record.payload {
                    FilePayload::InlineRegular(data) => usize_to_u64(data.as_bytes().len()),
                    FilePayload::Regular { logical_bytes, .. } => logical_bytes,
                    _ => return capture_file_record_state_with_accounting(record, accounting, Some(cancellation), custody),
                };
                capture_stack_state_with_accounting(
                    || length.to_le_bytes(), b"acyclic-fs-dependency-file-length-v1\0",
                    accounting, cancellation, custody,
                )
            }
            DependencyRegion::DirectoryName { name, .. } => {
                let FilePayload::Directory { entries } = record.payload else {
                    return capture_file_record_state_with_accounting(record, accounting, Some(cancellation), custody);
                };
                let (entry, allocation) = super::read::lookup_tree_entry_async_with_accounting(
                    self.store, entries, name, self.limits.decode, accounting, cancellation, custody,
                ).await?;
                let retained = entry.as_ref().map_or(0, |entry| usize_to_u64(entry.name.retained_bytes()));
                let entry = Owned::new(entry, allocation, retained);
                let result = match &entry.value {
                    Some(entry) => capture_directory_name_state_with_accounting(
                        entry, accounting, Some(cancellation), custody,
                    ),
                    None => Ok(DependencyState::Absent),
                };
                entry.retire(accounting)?;
                result
            }
            DependencyRegion::ContentRange { offset, length, .. } => {
                self.content_state_with_accounting(record, *offset, *length, accounting, cancellation, custody).await
            }
            DependencyRegion::SparseSeek { offset, target, .. } => {
                let result = match record.payload {
                    FilePayload::InlineRegular(data) => inline_seek_result(data, *offset, *target),
                    FilePayload::Regular { logical_bytes, extents } if *offset <= logical_bytes => {
                        super::range::seek_extent_with_accounting(
                            self.store, ExtentSeekRequest {
                                root: extents, file_size: logical_bytes, offset: *offset, target: *target,
                                limits: self.limits.decode, budget: WorkBudget::UNCHARGED,
                            }, accounting, cancellation,
                        ).await?
                    }
                    FilePayload::Regular { .. } => None,
                    _ => return capture_file_record_state_with_accounting(record, accounting, Some(cancellation), custody),
                };
                capture_stack_state_with_accounting(
                    || {
                        let [a, b, c, d, e, f, g, h] = result.unwrap_or(0).to_le_bytes();
                        [
                            match target { ExtentSeekTarget::Data => 0, ExtentSeekTarget::Hole => 1 },
                            u8::from(result.is_some()), a, b, c, d, e, f, g, h,
                        ]
                    },
                    b"acyclic-fs-dependency-sparse-seek-v1\0", accounting, cancellation, custody,
                )
            }
            DependencyRegion::DirectoryRange { after, maximum_entries, .. } => {
                let FilePayload::Directory { entries } = record.payload else {
                    return capture_file_record_state_with_accounting(record, accounting, Some(cancellation), custody);
                };
                let (page, allocation) = super::list::list_tree_entries_async_with_accounting(
                    self.store, entries, after.as_ref(), *maximum_entries, self.limits.decode,
                    accounting, cancellation.clone(), custody,
                ).await?;
                let retained = directory_entries_bytes(&page.entries)?;
                let page = Owned::new(page, allocation, retained);
                let result = capture_directory_page_with_accounting(
                    &page.value.entries, page.value.has_more, *maximum_entries, accounting, cancellation, custody,
                );
                page.retire(accounting)?;
                result
            }
        }
    }

    async fn content_state_with_accounting(
        &self, record: FileRecord, offset: u64, length: u64,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
        custody: Option<&super::ProofDataCustody>,
    ) -> Result<DependencyState, crate::FsError> {
        if let FilePayload::InlineRegular(data) = record.payload {
            let invalid = || AuthenticatedProbeError::Extent(ExtentReadError::InvalidRange);
            let end = offset.checked_add(length).ok_or_else(invalid)?;
            let start = usize::try_from(offset).map_err(|_| invalid())?;
            let end = usize::try_from(end).map_err(|_| invalid())?;
            let bytes = data.as_bytes().get(start..end).ok_or_else(invalid)?;
            let hash = content_hash_work(length)?;
            accounting.charge(hash)?;
            let value = DependencyState::Present(semantic_digest(CONTENT_RANGE_DOMAIN, bytes));
            if let Some(owner) = custody { owner.record_completed_leaf_work(hash)?; }
            return Ok(value);
        }
        let FilePayload::Regular { logical_bytes, extents } = record.payload else {
            return capture_file_record_state_with_accounting(record, accounting, Some(cancellation), custody);
        };
        accounting.admit(content_hash_work(length)?)?;
        let (plan, allocation) = super::range::plan_extent_range_with_accounting(
            self.store, super::ExtentRangeRequest {
                root: extents, file_size: logical_bytes, range: ByteRange { offset, length },
                maximum_spans: self.limits.maximum_extent_spans, limits: self.limits.decode,
                budget: WorkBudget::UNCHARGED,
            }, accounting, cancellation,
        ).await?;
        let retained = plan.retained_allocation_bytes;
        let plan = Owned::new(plan, allocation, retained);
        let result = async {
            let mut hasher = blake3::Hasher::new();
            hash_probe_bytes(&mut hasher, CONTENT_RANGE_DOMAIN, accounting, cancellation, custody)?;
            hash_probe_bytes(&mut hasher, &length.to_le_bytes(), accounting, cancellation, custody)?;
            for span in &plan.value.spans {
                cancellation.check()?;
                match span.kind {
                    ExtentKind::Hole | ExtentKind::AllocatedZero => {
                        let mut remaining = span.length;
                        while remaining != 0 {
                            let count = usize::try_from(remaining.min(usize_to_u64(ZERO_HASH_BLOCK.len())))
                                .map_err(|_| WorkError::Overflow)?;
                            let zeroes = ZERO_HASH_BLOCK.get(..count).ok_or(WorkError::Overflow)?;
                            hash_probe_bytes(&mut hasher, zeroes, accounting, cancellation, custody)?;
                            remaining -= usize_to_u64(count);
                        }
                    }
                    ExtentKind::Content { object, object_offset } => {
                        let read = super::blob::read_blob_range_with_accounting(
                            self.store, object, ByteRange { offset: object_offset, length: span.length },
                            self.limits.decode, accounting, cancellation,
                        ).await?;
                        let result = hash_probe_bytes(&mut hasher, &read.bytes, accounting, cancellation, custody);
                        let retained = read.logical_owned_bytes;
                        drop(read);
                        super::file_read::release_standalone_read_bytes(retained, accounting)?;
                        result?;
                    }
                }
            }
            Ok(DependencyState::Present(Digest::from_bytes(*hasher.finalize().as_bytes())))
        }.await;
        plan.retire(accounting)?;
        result
    }
}

impl<S: AsyncObjectStore> super::rebase::AccountingRebaseProbe for AuthenticatedGenerationProbe<'_, S> {
    type Error = crate::FsError;

    async fn probe_with_accounting(
        &self, generation: GenerationId, region: &DependencyRegion,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<DependencyState, super::rebase::ClassifyRebaseFailure<Self::Error>> {
        self.probe_async_with_accounting(generation, region, accounting, cancellation).await
            .map_err(super::rebase::ClassifyRebaseFailure::Accounting)
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

fn inline_seek_result(
    data: super::InlineFileData,
    offset: u64,
    target: ExtentSeekTarget,
) -> Option<u64> {
    let logical_bytes = usize_to_u64(data.as_bytes().len());
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
        bytes_hashed: usize_to_u64(CONTENT_RANGE_DOMAIN.len())
            .saturating_add(8)
            .saturating_add(usize_to_u64(bytes.len())),
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
    hasher.update(&usize_to_u64(bytes.len()).to_le_bytes());
    hasher.update(bytes);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

type ProbeFailure = OperationFailure<AuthenticatedProbeError>;

fn failed(error: AuthenticatedProbeError, work: WorkCounters) -> ProbeFailure {
    OperationFailure::new(error, work)
}

fn validate_capture_batch(count: usize, maximum: u32) -> Result<(), ProbeFailure> {
    validate_capture_batch_error(count, maximum)
        .map_err(|error| failed(project_probe_error(error), WorkCounters::UNCHARGED))
}

fn validate_capture_batch_error(count: usize, maximum: u32) -> Result<(), crate::FsError> {
    if maximum == 0 { return Err(AuthenticatedProbeError::ZeroBatchLimit.into()); }
    if u32::try_from(count).unwrap_or(u32::MAX) > maximum {
        return Err(AuthenticatedProbeError::TooManyRegions { maximum }.into());
    }
    Ok(())
}

fn region_owned_bytes(region: &DependencyRegion) -> u64 {
    match region {
        DependencyRegion::DirectoryName { name, .. } => usize_to_u64(name.retained_bytes()),
        DependencyRegion::DirectoryRange { after: Some(name), .. } => usize_to_u64(name.retained_bytes()),
        _ => 0,
    }
}

fn capture_input_bytes<T>(
    regions: &Vec<T>, region: impl Fn(&T) -> &DependencyRegion,
) -> Result<u64, ProbeFailure> {
    let backing = regions.capacity().checked_mul(size_of::<T>()).map(usize_to_u64)
        .ok_or_else(|| failed(WorkError::Overflow.into(), WorkCounters::UNCHARGED))?;
    regions.iter().try_fold(backing, |bytes, value| {
        bytes.checked_add(region_owned_bytes(region(value)))
    }).ok_or_else(|| failed(WorkError::Overflow.into(), WorkCounters::UNCHARGED))
}

fn directory_entries_bytes(entries: &Vec<super::TreeEntry>) -> Result<u64, crate::FsError> {
    let backing = entries.capacity().checked_mul(size_of::<super::TreeEntry>()).map(usize_to_u64)
        .ok_or(WorkError::Overflow)?;
    entries.iter().try_fold(backing, |bytes, entry| {
        bytes.checked_add(usize_to_u64(entry.name.retained_bytes()))
    }).ok_or_else(|| WorkError::Overflow.into())
}

fn content_hash_work(length: u64) -> Result<WorkCounters, crate::FsError> {
    Ok(WorkCounters {
        bytes_hashed: usize_to_u64(CONTENT_RANGE_DOMAIN.len()).checked_add(8)
            .and_then(|bytes| bytes.checked_add(length)).ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    })
}

fn hash_probe_bytes(
    hasher: &mut blake3::Hasher, bytes: &[u8], accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken, custody: Option<&super::ProofDataCustody>,
) -> Result<(), crate::FsError> {
    cancellation.check()?;
    let work = WorkCounters { bytes_hashed: usize_to_u64(bytes.len()), ..WorkCounters::UNCHARGED };
    accounting.charge(work)?;
    hasher.update(bytes);
    if let Some(owner) = custody { owner.record_completed_leaf_work(work)?; }
    Ok(())
}

fn capture_stack_state_with_accounting<const N: usize>(
    encode: impl FnOnce() -> [u8; N], domain: &[u8], accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken, custody: Option<&super::ProofDataCustody>,
) -> Result<DependencyState, crate::FsError> {
    cancellation.check()?;
    let encoded_work = WorkCounters { bytes_encoded: usize_to_u64(N), ..WorkCounters::UNCHARGED };
    let hash_work = WorkCounters {
        bytes_hashed: usize_to_u64(N).checked_add(usize_to_u64(domain.len()))
            .and_then(|bytes| bytes.checked_add(8)).ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(encoded_work.checked_add(hash_work)?)?;
    let encoded = encode();
    let completed = accounting.record_completed(encoded_work);
    let traced = custody.map_or(Ok(()), |owner| owner.record_completed_leaf_work(encoded_work));
    completed?;
    traced?;
    cancellation.check()?;
    accounting.admit(hash_work)?;
    let value = DependencyState::Present(semantic_digest(domain, &encoded));
    let completed = accounting.record_completed(hash_work);
    let traced = custody.map_or(Ok(()), |owner| owner.record_completed_leaf_work(hash_work));
    completed?;
    traced?;
    Ok(value)
}

fn capture_directory_page_with_accounting(
    entries: &Vec<super::TreeEntry>, has_more: bool, maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    custody: Option<&super::ProofDataCustody>,
) -> Result<DependencyState, crate::FsError> {
    cancellation.check()?;
    let _entries = ProbeNameResidency::new(custody, directory_entries_bytes(entries)?)?;
    let (encoded, allocation) = crate::kernel::codec::encode_owned(accounting, |observe, _reserve| super::tree::encode_tree_leaf_entries(entries, maximum_entries, Some(u8::from(has_more)), custody, observe, crate::FsError::EncodedAllocationFailed))?;
    let bytes = usize_to_u64(encoded.capacity());
    capture_encoded_state_with_accounting(
        Owned::new(encoded, allocation, bytes), b"acyclic-fs-dependency-directory-page-v1\0",
        accounting, Some(cancellation), custody,
    )
}

fn project_probe_error(error: crate::FsError) -> AuthenticatedProbeError {
    match error {
        crate::FsError::Probe(error) => error,
        crate::FsError::Object(error) => AuthenticatedProbeError::Storage(error),
        crate::FsError::Decode(error) => AuthenticatedProbeError::Decode(error),
        crate::FsError::Work(error) => AuthenticatedProbeError::Work(error),
        crate::FsError::Cancelled(_) => AuthenticatedProbeError::Cancelled,
        crate::FsError::EncodedAllocationFailed(error) => AuthenticatedProbeError::EncodedAllocationFailed(error),
        crate::FsError::FileRecord(error) => AuthenticatedProbeError::FileRecord(error),
        crate::FsError::Path(super::PathLookupError::Tree(error)) => AuthenticatedProbeError::Tree(error),
        crate::FsError::Directory(error) => AuthenticatedProbeError::Directory(error),
        crate::FsError::FileRead(super::FileRangeReadError::Extent(error)) => AuthenticatedProbeError::Extent(error),
        crate::FsError::BlobRead(error) => AuthenticatedProbeError::Blob(error),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => AuthenticatedProbeError::Storage(ObjectStoreError::Native(error)),
        error => AuthenticatedProbeError::Context(Box::new(error)),
    }
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
    /// A canonical state encoder retains the allocator's actual failure cause.
    #[error("authenticated dependency encoding allocation failed: {0}")]
    EncodedAllocationFailed(#[source] std::collections::TryReserveError),
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
    /// A shared accounting boundary retains its typed facade cause.
    #[error(transparent)]
    Context(Box<crate::FsError>),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/probe.rs"]
mod tests;
