//! Disposable generation/path query acceleration.

use crate::kernel::allocation::{AllocationError, KernelAccounting, KernelOwnedAllocation};
use crate::kernel::{FileRecord, LogicalName, NameEncoding, NamespacePath};
use crate::model::VolumeLimits;
use crate::{FileId, GenerationId, WorkCounters, WorkError};
use std::sync::Mutex;

const PATH_QUERY_HEADER: &[u8] = b"APQ1";
const MAXIMUM_PATH_INDEX_ENTRIES: usize = 256;
const MAXIMUM_PATH_INDEX_RETAINED_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_PATH_INDEX_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
enum PathIndexError {
    #[error(transparent)]
    Boundary(#[from] crate::FsError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("disposable cache miss")]
    Miss,
}

impl From<WorkError> for PathIndexError {
    fn from(error: WorkError) -> Self { Self::Boundary(error.into()) }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<crate::NativeTreeMaterializationError> for PathIndexError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self { Self::Boundary(error.into()) }
}

impl From<AllocationError> for PathIndexError {
    fn from(error: AllocationError) -> Self {
        Self::Boundary(error.into_work_or(crate::FsError::HandleAllocationFailed))
    }
}

fn accounting_error(error: PathIndexError) -> crate::FsError {
    match error {
        PathIndexError::Boundary(error) => error,
        PathIndexError::Io(_) | PathIndexError::Miss => crate::FsError::HandleAllocationFailed,
    }
}

/// All producer work runs outside the original ledger mutex.
struct PathIndexWork<'a, 'ctx> {
    accounting: &'a mut KernelAccounting<'ctx>,
    retained: u64,
    allocation: KernelOwnedAllocation,
}

impl<'a, 'ctx> PathIndexWork<'a, 'ctx> {
    fn new(accounting: &'a mut KernelAccounting<'ctx>)
        -> Result<Self, PathIndexError>
    {
        let allocation = accounting.claim_owned(0, 0).map_err(projected_error)?;
        Ok(Self { accounting, retained: 0, allocation })
    }

    fn exact_vec<T>(&mut self, count: usize) -> Result<Vec<T>, PathIndexError> {
        self.accounting.check().map_err(projected_error)?;
        let requested = count.checked_mul(std::mem::size_of::<T>())
            .ok_or(WorkError::Overflow)? as u64;
        let mut allocation = self.accounting.claim_owned(requested, u64::from(count != 0))
            .map_err(projected_error)?;
        let mut value = Vec::<T>::new();
        if let Err(error) = value.try_reserve_exact(count) {
            drop(value);
            self.accounting.release_owned(&mut allocation, requested).map_err(projected_error)?;
            #[cfg(not(target_arch = "wasm32"))]
            if self.accounting.native_work().is_some() {
                return Err(PathIndexError::from(crate::NativeTreeMaterializationError::AllocationFailed(error)));
            }
            let _ = error;
            return Err(PathIndexError::Boundary(crate::FsError::HandleAllocationFailed));
        }
        let actual = value.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or(WorkError::Overflow)? as u64;
        if let Err(error) = self.accounting.reconcile_owned(&mut allocation, requested, actual) {
            drop(value);
            self.accounting.release_owned(&mut allocation, actual).map_err(projected_error)?;
            return Err(projected_error(error));
        }
        if let Err(error) = self.allocation.absorb(&mut allocation) {
            drop(value);
            self.accounting.release_owned(&mut allocation, actual).map_err(projected_error)?;
            return Err(projected_error(error));
        }
        self.retained = self.retained.checked_add(actual).ok_or(WorkError::Overflow)?;
        Ok(value)
    }

    fn take_allocation(&mut self) -> KernelOwnedAllocation {
        self.retained = 0;
        std::mem::replace(&mut self.allocation, KernelOwnedAllocation::Standalone)
    }
}

impl Drop for PathIndexWork<'_, '_> {
    fn drop(&mut self) {
        let _ = release_borrowed(self.accounting, self.retained);
    }
}

/// Decodes a bounded cache entry without scratch allocations.
pub(crate) fn decode(
    bytes: &[u8], generation: GenerationId, file_ids: &[FileId], limits: VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Option<Vec<(FileId, NamespacePath)>>, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    if bytes.len() > MAXIMUM_PATH_INDEX_VALUE_BYTES {
        return Ok((None, accounting.claim_owned(0, 0)?));
    }
    let mut work = PathIndexWork::new(accounting).map_err(accounting_error)?;
    let result = (|| {
        let mut parser = QueryParser { bytes, work: &mut work };
        let count = parser.document(generation, file_ids, limits, None)?;
        let mut output = parser.work.exact_vec(count)?;
        parser.bytes = bytes;
        parser.document(generation, file_ids, limits, Some(&mut output))?;
        Ok(output)
    })();
    match result {
        Ok(output) => Ok((Some(output), work.take_allocation())),
        Err(PathIndexError::Miss) => {
            drop(work);
            Ok((None, accounting.claim_owned(0, 0)?))
        }
        Err(error) => Err(accounting_error(error)),
    }
}


struct QueryParser<'a, 'work, 'ctx> {
    bytes: &'a [u8],
    work: &'a mut PathIndexWork<'work, 'ctx>,
}

impl<'bytes> QueryParser<'bytes, '_, '_> {
    fn take(&mut self, length: usize) -> Result<&'bytes [u8], PathIndexError> {
        let (bytes, remaining) = self.bytes.split_at_checked(length).ok_or(PathIndexError::Miss)?;
        let delta = WorkCounters { items_examined: length as u64, ..WorkCounters::UNCHARGED };
        self.work.accounting.admit(delta).map_err(projected_error)?;
        self.bytes = remaining;
        self.work.accounting.record_completed(delta).map_err(projected_error)?;
        Ok(bytes)
    }

    fn number(&mut self) -> Result<usize, PathIndexError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(|_| PathIndexError::Miss)?) as usize)
    }

    fn path(&mut self, limits: VolumeLimits, materialize: bool) -> Result<Option<NamespacePath>, PathIndexError> {
        let depth = self.number()?;
        if depth > usize::from(limits.maximum_path_depth) { return Err(PathIndexError::Miss); }
        let mut names = if materialize { Some(self.work.exact_vec::<LogicalName>(depth)?) } else { None };
        let mut path_bytes = 1_u64;
        for index in 0..depth {
            let encoding = NameEncoding::from_tag(self.take(1)?[0]).map_err(|_| PathIndexError::Miss)?;
            let length = self.number()?;
            if length == 0 || length as u64 > u64::from(limits.maximum_component_bytes) {
                return Err(PathIndexError::Miss);
            }
            path_bytes = path_bytes.checked_add(length as u64 + u64::from(index != 0))
                .ok_or(PathIndexError::Miss)?;
            if path_bytes > u64::from(limits.maximum_path_bytes) { return Err(PathIndexError::Miss); }
            let source = self.take(length)?;
            if let Some(names) = names.as_mut() {
                let mut bytes = self.work.exact_vec::<u8>(length)?;
                let delta = WorkCounters {
                    bytes_copied: length as u64, items_examined: length as u64,
                    ..WorkCounters::UNCHARGED
                };
                self.work.accounting.admit(delta).map_err(projected_error)?;
                bytes.extend_from_slice(source);
                let name = LogicalName::new(encoding, bytes, limits.maximum_component_bytes);
                self.work.accounting.record_completed(delta).map_err(projected_error)?;
                let name = name.map_err(|_| PathIndexError::Miss)?;
                let delta = WorkCounters {
                    bytes_copied: std::mem::size_of::<LogicalName>() as u64, ..WorkCounters::UNCHARGED
                };
                self.work.accounting.admit(delta).map_err(projected_error)?;
                names.push(name);
                self.work.accounting.record_completed(delta).map_err(projected_error)?;
            }
        }
        if path_bytes > u64::from(limits.maximum_path_bytes) { return Err(PathIndexError::Miss); }
        names.map(|names| NamespacePath::new(names, limits).map_err(|_| PathIndexError::Miss)).transpose()
    }

    fn document(
        &mut self, generation: GenerationId, file_ids: &[FileId],
        limits: VolumeLimits, mut output: Option<&mut Vec<(FileId, NamespacePath)>>,
    ) -> Result<usize, PathIndexError> {
        if self.take(PATH_QUERY_HEADER.len())? != PATH_QUERY_HEADER
            || self.take(32)? != generation.digest().as_bytes() { return Err(PathIndexError::Miss); }
        let mut paths = 0_usize;
        for &file_id in file_ids {
            if self.take(16)? != file_id.into_bytes() { return Err(PathIndexError::Miss); }
            let count = self.number()?;
            for _ in 0..count {
                let path = self.path(limits, output.is_some())?;
                paths = paths.checked_add(1).ok_or(PathIndexError::Miss)?;
                if let Some(output) = output.as_deref_mut() {
                    let delta = WorkCounters {
                        bytes_copied: std::mem::size_of::<(FileId, NamespacePath)>() as u64,
                        ..WorkCounters::UNCHARGED
                    };
                    self.work.accounting.admit(delta).map_err(projected_error)?;
                    output.push((file_id, path.ok_or(PathIndexError::Miss)?));
                    self.work.accounting.record_completed(delta).map_err(projected_error)?;
                }
            }
        }
        if !self.bytes.is_empty() { return Err(PathIndexError::Miss); }
        Ok(paths)
    }
}

/// Counting and producing use the same canonical encoder.
struct QueryWriter<'a, 'work, 'ctx> {
    output: Option<&'a mut Vec<u8>>,
    length: usize,
    work: &'a mut PathIndexWork<'work, 'ctx>,
}

impl QueryWriter<'_, '_, '_> {
    fn append(&mut self, bytes: &[u8]) -> Result<(), PathIndexError> {
        self.work.accounting.check().map_err(projected_error)?;
        let length = self.length.checked_add(bytes.len())
            .ok_or(PathIndexError::from(WorkError::Overflow))?;
        if length > MAXIMUM_PATH_INDEX_VALUE_BYTES { return Err(PathIndexError::Miss); }
        let delta = WorkCounters {
            bytes_encoded: bytes.len() as u64,
            bytes_copied: if self.output.is_some() { bytes.len() as u64 } else { 0 },
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        };
        if self.output.as_ref().is_some_and(|output| length > output.capacity()) {
            return Err(PathIndexError::Boundary(crate::FsError::HandleAllocationFailed));
        }
        self.work.accounting.admit(delta).map_err(projected_error)?;
        if let Some(output) = self.output.as_deref_mut() {
            output.extend_from_slice(bytes);
        }
        self.length = length;
        self.work.accounting.record_completed(delta).map_err(projected_error)?;
        Ok(())
    }

    fn number(&mut self, value: usize) -> Result<(), PathIndexError> {
        let value = u32::try_from(value).map_err(|_| PathIndexError::from(WorkError::Overflow))?;
        self.append(&value.to_le_bytes())
    }

    fn document(
        &mut self, generation: GenerationId, file_ids: &[FileId],
        records: &[(FileId, Vec<(NamespacePath, FileRecord)>)],
    ) -> Result<(), PathIndexError> {
        self.append(PATH_QUERY_HEADER)?;
        self.append(generation.digest().as_bytes())?;
        let mut record_index = 0_usize;
        for &file_id in file_ids {
            self.append(&file_id.into_bytes())?;
            while records.get(record_index).is_some_and(|entry| entry.0 < file_id) {
                let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                self.work.accounting.admit(delta).map_err(projected_error)?;
                record_index += 1;
                self.work.accounting.record_completed(delta).map_err(projected_error)?;
            }
            let paths = records.get(record_index).filter(|entry| entry.0 == file_id)
                .map_or(&[][..], |entry| entry.1.as_slice());
            self.number(paths.len())?;
            for (path, _) in paths {
                self.number(path.components().len())?;
                for name in path.components() {
                    self.append(&[name.encoding().tag()])?;
                    self.number(name.as_bytes().len())?;
                    self.append(name.as_bytes())?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn encode_records(
    generation: GenerationId, file_ids: &[FileId],
    records: &[(FileId, Vec<(NamespacePath, FileRecord)>)],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Option<Vec<u8>>, KernelOwnedAllocation), crate::FsError> {
    let mut work = PathIndexWork::new(accounting).map_err(accounting_error)?;
    let length = {
        let mut counter = QueryWriter { output: None, length: 0, work: &mut work };
        match counter.document(generation, file_ids, records) {
            Ok(()) => counter.length,
            Err(PathIndexError::Miss) => {
                drop(counter);
                return Ok((None, work.take_allocation()));
            }
            Err(error) => return Err(accounting_error(error)),
        }
    };
    let mut output = work.exact_vec::<u8>(length).map_err(accounting_error)?;
    let result = {
        let mut writer = QueryWriter { output: Some(&mut output), length: 0, work: &mut work };
        writer.document(generation, file_ids, records)
    };
    match result {
        Ok(()) => Ok((Some(output), work.take_allocation())),
        Err(error) => {
            drop(output);
            drop(work);
            match error {
                PathIndexError::Miss => Ok((None, accounting.claim_owned(0, 0)?)),
                error => Err(accounting_error(error)),
            }
        }
    }
}

/// Candidate roots never reuse a complete alias set from another namespace.
pub(crate) fn cache_key(
    generation: GenerationId, root_file_id: FileId, file_table: crate::ObjectId,
    file_ids: &[FileId], accounting: &mut KernelAccounting<'_>,
) -> Result<[u8; 32], crate::FsError> {
    let mut hasher = blake3::Hasher::new();
    for bytes in [b"acyclic-generation-path-root-v2\0".as_slice(), generation.digest().as_bytes(),
        &root_file_id.into_bytes(), &[file_table.kind.canonical_tag()], file_table.digest.as_bytes()] {
        hash_bytes(&mut hasher, bytes, accounting).map_err(accounting_error)?;
    }
    for file_id in file_ids {
        hash_bytes(&mut hasher, &file_id.into_bytes(), accounting).map_err(accounting_error)?;
    }
    Ok(*hasher.finalize().as_bytes())
}

fn hash_bytes(
    hasher: &mut blake3::Hasher, bytes: &[u8], accounting: &mut KernelAccounting<'_>,
) -> Result<(), PathIndexError> {
    accounting.check().map_err(projected_error)?;
    let delta = WorkCounters {
        bytes_hashed: bytes.len() as u64, items_examined: 1, ..WorkCounters::UNCHARGED
    };
    accounting.admit(delta).map_err(projected_error)?;
    hasher.update(bytes);
    accounting.record_completed(delta).map_err(projected_error)
}

/// Best-effort derived cache.
pub(crate) trait GenerationPathIndex: Send + Sync {
    fn load(
        &self, key: [u8; 32], accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Option<Vec<u8>>, KernelOwnedAllocation), crate::FsError>;

    fn store(
        &self, key: [u8; 32], value: &[u8], accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError>;
}

fn cache_find<T>(
    entries: &[([u8; 32], T)], key: [u8; 32], accounting: &mut KernelAccounting<'_>,
) -> Result<Result<usize, usize>, PathIndexError> {
    let mut left = 0;
    let mut right = entries.len();
    while left < right {
        accounting.check().map_err(projected_error)?;
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        accounting.admit(delta).map_err(projected_error)?;
        let index = left + (right - left) / 2;
        let (existing, _) = &entries[index];
        let ordering = existing.cmp(&key);
        accounting.record_completed(delta).map_err(projected_error)?;
        match ordering {
            std::cmp::Ordering::Equal => return Ok(Ok(index)),
            std::cmp::Ordering::Greater => right = index,
            std::cmp::Ordering::Less => left = index + 1,
        }
    }
    Ok(Err(left))
}

fn cache_move(count: usize, width: usize) -> Result<WorkCounters, PathIndexError> {
    let bytes = count.checked_mul(width)
        .ok_or(crate::WorkError::Overflow)?;
    Ok(WorkCounters { bytes_copied: bytes as u64, ..WorkCounters::UNCHARGED })
}

fn cache_copy(
    value: &[u8], accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), PathIndexError> {
    let mut meter = PathIndexWork::new(accounting)?;
    let delta = cache_move(value.len(), std::mem::size_of::<u8>())?;
    meter.accounting.admit(delta).map_err(projected_error)?;
    let mut copied = meter.exact_vec::<u8>(value.len())?;
    copied.extend_from_slice(value);
    meter.accounting.record_completed(delta).map_err(projected_error)?;
    Ok((copied, meter.take_allocation()))
}

fn projected_error(error: crate::FsError) -> PathIndexError {
    PathIndexError::Boundary(match error {
        error @ (crate::FsError::Work(_) | crate::FsError::Cancelled(_)) => error,
        #[cfg(not(target_arch = "wasm32"))]
        error @ crate::FsError::NativeKernel(_) => error,
        _ => crate::FsError::HandleAllocationFailed,
    })
}

// Cleanup cannot be vetoed by cancellation after the physical payload drop.
fn release_borrowed(
    accounting: &mut KernelAccounting<'_>, bytes: u64,
) -> Result<(), crate::FsError> {
    match accounting {
        KernelAccounting::Standalone { allocations, .. } =>
            allocations.release(bytes).map_err(|error| accounting_error(error.into())),
        KernelAccounting::BorrowedStandalone { allocations, .. } =>
            allocations.release(bytes).map_err(|error| accounting_error(error.into())),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => Ok(()),
    }
}

fn retire_cache_payload(
    payload: (Vec<u8>, KernelOwnedAllocation), accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let bytes = payload.0.capacity() as u64;
    drop(payload);
    release_borrowed(accounting, bytes)?;
    Ok(())
}

fn cache_reserve<T>(
    entries: &mut Vec<T>, allocation: &mut Option<KernelOwnedAllocation>, capacity: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    if entries.capacity() >= capacity { return Ok(()); }
    let old_bytes = entries.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let mut meter = PathIndexWork::new(accounting).map_err(accounting_error)?;
    let delta = cache_move(entries.len(), std::mem::size_of::<T>()).map_err(accounting_error)?;
    meter.accounting.admit(delta).map_err(projected_error).map_err(accounting_error)?;
    let mut replacement = meter.exact_vec::<T>(capacity).map_err(accounting_error)?;
    replacement.append(entries);
    let guard = meter.take_allocation();
    drop(meter);
    let previous_guard = allocation.replace(guard);
    let previous = std::mem::replace(entries, replacement);
    let completed = accounting.record_completed(delta);
    drop(previous);
    drop(previous_guard);
    release_borrowed(accounting, old_bytes)?;
    completed
}

#[derive(Default)]
pub(crate) struct MemoryGenerationPathIndex {
    state: Mutex<MemoryPathIndexState>,
}

#[derive(Default)]
struct MemoryPathIndexState {
    entries: Vec<([u8; 32], (Vec<u8>, KernelOwnedAllocation))>,
    // Physical container capacity is destroyed before original custody.
    allocation: Option<KernelOwnedAllocation>,
}

impl MemoryPathIndexState {
    fn owned_bytes(&self) -> Result<u64, PathIndexError> {
        let container = self.entries.capacity().checked_mul(
            std::mem::size_of::<([u8; 32], (Vec<u8>, KernelOwnedAllocation))>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        self.entries.iter().try_fold(container, |total, (_, value)| {
            total.checked_add(value.0.capacity() as u64)
                .ok_or_else(|| crate::WorkError::Overflow.into())
        })
    }
}

impl GenerationPathIndex for MemoryGenerationPathIndex {
    fn load(
        &self, key: [u8; 32], accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Option<Vec<u8>>, KernelOwnedAllocation), crate::FsError> {
        accounting.check()?;
        let Ok(state) = self.state.lock() else {
            return Ok((None, accounting.claim_owned(0, 0)
                .map_err(|error| accounting_error(projected_error(error)))?));
        };
        let retained = state.owned_bytes().map_err(accounting_error)?;
        let borrowed = accounting.standalone_parts().is_some();
        if borrowed { let _ = accounting.claim_owned(retained, 0)?; }
        let result = (|| {
            match cache_find(&state.entries, key, accounting).map_err(accounting_error)? {
                Ok(index) => cache_copy(&state.entries[index].1.0,
                    accounting).map(|(bytes, allocation)| (Some(bytes), allocation))
                    .map_err(accounting_error),
                Err(_) => Ok((None, accounting.claim_owned(0, 0)
                    .map_err(|error| accounting_error(projected_error(error)))?)),
            }
        })();
        release_borrowed(accounting, if borrowed { retained } else { 0 })?;
        result
    }

    fn store(
        &self, key: [u8; 32], value: &[u8], accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        if value.len() > MAXIMUM_PATH_INDEX_VALUE_BYTES { return Ok(()); }
        let Ok(mut state) = self.state.lock() else { return Ok(()); };
        let borrowed = accounting.standalone_parts().is_some();
        if borrowed {
            let retained = state.owned_bytes().map_err(accounting_error)?;
            let _ = accounting.claim_owned(retained, 0)?;
        }
        let result = (|| {
            let found = cache_find(&state.entries, key, accounting).map_err(accounting_error)?;
            let MemoryPathIndexState { entries, allocation, .. } = &mut *state;
            cache_reserve(entries, allocation, MAXIMUM_PATH_INDEX_ENTRIES,
                accounting)?;
            let (bytes, allocation) = cache_copy(value, accounting).map_err(accounting_error)?;
            let copied = (bytes, allocation);
            let mutation = (|| {
                let visited = WorkCounters { items_examined: state.entries.len() as u64, ..WorkCounters::UNCHARGED };
                accounting.admit(visited)?;
                let mut retained_bytes: usize = state.entries.iter().map(|(_, value)| value.0.len()).sum();
                accounting.record_completed(visited)?;
                if let Ok(index) = found {
                    let delta = cache_move(state.entries.len() - index - 1, std::mem::size_of::<([u8; 32], (Vec<u8>, KernelOwnedAllocation))>()).map_err(accounting_error)?;
                    accounting.admit(delta)?;
                    let (_, previous) = state.entries.remove(index);
                    retained_bytes -= previous.0.len();
                    let completed = accounting.record_completed(delta);
                    retire_cache_payload(previous, accounting)?;
                    completed?;
                }
                while state.entries.len() >= MAXIMUM_PATH_INDEX_ENTRIES
                    || retained_bytes.saturating_add(value.len()) > MAXIMUM_PATH_INDEX_RETAINED_BYTES
                {
                    let delta = cache_move(state.entries.len() - 1, std::mem::size_of::<([u8; 32], (Vec<u8>, KernelOwnedAllocation))>()).map_err(accounting_error)?;
                    accounting.admit(delta)?;
                    let (_, evicted) = state.entries.remove(0);
                    retained_bytes -= evicted.0.len();
                    let completed = accounting.record_completed(delta);
                    retire_cache_payload(evicted, accounting)?;
                    completed?;
                }
                let index = cache_find(&state.entries, key, accounting)
                    .map_err(accounting_error)?.unwrap_or_else(|index| index);
                let delta = cache_move(state.entries.len() - index, std::mem::size_of::<([u8; 32], (Vec<u8>, KernelOwnedAllocation))>()).map_err(accounting_error)?;
                accounting.admit(delta)?;
                Ok((index, delta))
            })();
            match mutation {
                Ok((index, delta)) => {
                    state.entries.insert(index, (key, copied));
                    accounting.record_completed(delta)
                }
                Err(error) => {
                    retire_cache_payload(copied, accounting)?;
                    Err(error)
                }
            }
        })();
        if borrowed {
            let retained = state.owned_bytes().map_err(accounting_error)?;
            release_borrowed(accounting, retained)?;
        }
        result
    }

}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) struct NativeGenerationPathIndex {
    root: std::path::PathBuf,
    state: Mutex<NativePathIndexState>,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Default)]
struct NativePathIndexState {
    entries: Vec<([u8; 32], usize)>,
    // The resident vector is physically owned, never post-hoc adopted.
    allocation: Option<KernelOwnedAllocation>,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl NativePathIndexState {
    fn owned_bytes(&self) -> Result<u64, PathIndexError> {
        self.entries.capacity().checked_mul(std::mem::size_of::<([u8; 32], usize)>())
            .map(|bytes| bytes as u64).ok_or_else(|| crate::WorkError::Overflow.into())
    }


}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
enum CacheCleanupWrite {
    Standalone(u64),
    Native(crate::materializer::NativeCleanupWrite),
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl crate::native_host::NativeHostWork for PathIndexWork<'_, '_> {
    type Error = PathIndexError;
    type Allocation = (KernelOwnedAllocation, u64);
    type CleanupWrite = CacheCleanupWrite;
    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        match error { PathIndexError::Io(error) => Some(error.kind()), _ => None }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        PathIndexError::from(crate::NativeTreeMaterializationError::AllocationFailed(error))
    }
    fn check(&self) -> Result<(), Self::Error> { self.accounting.check().map_err(projected_error) }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> { self.accounting.admit(delta).map_err(projected_error) }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> { self.accounting.record_completed(delta).map_err(projected_error) }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.accounting.charge(delta).map_err(projected_error)
    }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
        let allocation = self.accounting.claim_owned(bytes, u64::from(bytes != 0))
            .map_err(projected_error)?;
        Ok((allocation, bytes))
    }
    fn reserve_opaque_decode(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
        let allocation = self.accounting.claim_owned(bytes, 0).map_err(projected_error)?;
        Ok((allocation, bytes))
    }
    fn reconcile_allocation(&mut self, allocation: &mut Self::Allocation, actual: u64)
        -> Result<(), Self::Error> {
        let result = self.accounting.reconcile_owned(
            &mut allocation.0, allocation.1, actual).map_err(projected_error);
        allocation.1 = actual;
        result
    }
    fn release(&mut self, mut allocation: Self::Allocation) -> Result<(), Self::Error> {
        self.accounting.release_owned(&mut allocation.0, allocation.1)
            .map_err(projected_error)
    }
    fn attempt_io<T>(&mut self, delta: WorkCounters, operation: impl FnOnce() -> std::io::Result<T>)
        -> Result<T, Self::Error> {
        self.started_io(delta)?;
        match operation() {
            Ok(value) => { self.check()?; Ok(value) }
            Err(error) => Err(error.into()),
        }
    }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        self.check()?;
        if let Some(work) = self.accounting.native_work() {
            return work.reserve_cleanup_write().map(CacheCleanupWrite::Native).map_err(Into::into);
        }
        let (_, work, budget, _) = self.accounting.standalone_parts()
            .ok_or(PathIndexError::Boundary(crate::FsError::HandleAllocationFailed))?;
        work.admit(&WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }, &budget)?;
        let original = budget.backend_write_operations;
        match self.accounting {
            KernelAccounting::Standalone { budget, .. }
            | KernelAccounting::BorrowedStandalone { budget, .. } => {
                budget.backend_write_operations = original.checked_sub(1).ok_or(WorkError::Overflow)?;
            }
            KernelAccounting::NativeOriginal { .. } => unreachable!(),
        }
        Ok(CacheCleanupWrite::Standalone(original))
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        if let CacheCleanupWrite::Standalone(original) = reservation {
            match self.accounting {
                KernelAccounting::Standalone { budget, .. }
                | KernelAccounting::BorrowedStandalone { budget, .. } => {
                    budget.backend_write_operations = original;
                }
                KernelAccounting::NativeOriginal { .. } => unreachable!(),
            }
        }
    }
    fn attempt_cleanup_write<T>(&mut self, reservation: Self::CleanupWrite, operation: impl FnOnce() -> T)
        -> (T, Result<(), Self::Error>) {
        match reservation {
            CacheCleanupWrite::Native(reservation) => {
                let (result, recorded) = reservation.attempt(operation);
                (result, recorded.map_err(Into::into))
            }
            reservation @ CacheCleanupWrite::Standalone(_) => {
                self.cancel_cleanup_write(reservation);
                let result = operation();
                let recorded = match self.accounting.standalone_parts() {
                    Some((_, work, budget, _)) => work.try_add_assign(&WorkCounters {
                        backend_write_operations: 1, ..WorkCounters::UNCHARGED })
                        .and_then(|()| work.verify(budget)).map_err(Into::into),
                    None => Err(PathIndexError::Boundary(crate::FsError::HandleAllocationFailed)),
                };
                (result, recorded)
            }
        }
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn cache_io_miss(result: Result<(), PathIndexError>) -> Result<(), PathIndexError> {
    match result { Err(PathIndexError::Io(_)) => Ok(()), result => result }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn cache_name<'a>(
    key: [u8; 32], temporary: bool, buffer: &'a mut [u8; 85],
    meter: &mut PathIndexWork<'_, '_>,
) -> Result<&'a std::path::Path, PathIndexError> {
    use std::io::Write as _;
    let pid = if temporary { std::process::id() } else { 0 };
    let length = 69 + if temporary { 1 + pid.checked_ilog10().unwrap_or(0) as usize + 1 } else { 0 };
    let delta = WorkCounters { bytes_encoded: length as u64, ..WorkCounters::UNCHARGED };
    meter.accounting.admit(delta).map_err(projected_error)?;
    let encoded: Result<usize, PathIndexError> = (|| {
        hex::encode_to_slice(key, &mut buffer[..64])
            .map_err(|_| PathIndexError::Boundary(crate::FsError::HandleAllocationFailed))?;
        // The fixed buffer holds the hex key, a u32 PID and either suffix.
        let mut suffix = std::io::Cursor::new(&mut buffer[64..]);
        if temporary { write!(suffix, ".{pid}.next")?; } else { suffix.write_all(b".json")?; }
        Ok(length)
    })();
    meter.accounting.record_completed(delta).map_err(projected_error)?;
    let length = encoded?;
    Ok(std::path::Path::new(std::str::from_utf8(&buffer[..length]).expect("cache filenames are ASCII")))
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn evict_native_entries(
    state: &mut NativePathIndexState, directory: &cap_std::fs::Dir,
    meter: &mut PathIndexWork<'_, '_>,
) -> Result<(), PathIndexError> {
    let visited = WorkCounters { items_examined: state.entries.len() as u64, ..WorkCounters::UNCHARGED };
    meter.accounting.admit(visited).map_err(projected_error)?;
    let mut retained_bytes: u128 = state.entries.iter().map(|(_, bytes)| *bytes as u128).sum();
    meter.accounting.record_completed(visited).map_err(projected_error)?;
    while state.entries.len() > MAXIMUM_PATH_INDEX_ENTRIES
        || retained_bytes > MAXIMUM_PATH_INDEX_RETAINED_BYTES as u128
    {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        meter.accounting.admit(delta).map_err(projected_error)?;
        let Some(&(oldest, _)) = state.entries.first() else { break; };
        meter.accounting.record_completed(delta).map_err(projected_error)?;
        let mut buffer = [0_u8; 85];
        let leaf = cache_name(oldest, false, &mut buffer, meter)?;
        // A failed eviction remains a disposable disk entry, as before.
        cache_io_miss(crate::native_host::remove_leaf_with_work(directory, leaf, meter))?;
        let delta = cache_move(state.entries.len() - 1, std::mem::size_of::<([u8; 32], usize)>())?;
        meter.accounting.admit(delta).map_err(projected_error)?;
        let (_, bytes) = state.entries.remove(0);
        retained_bytes -= bytes as u128;
        meter.accounting.record_completed(delta).map_err(projected_error)?;
    }
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl NativeGenerationPathIndex {
    pub(crate) fn new(root: std::path::PathBuf) -> Self {
        let mut state = NativePathIndexState::default();
        if let Ok(entries) = std::fs::read_dir(&root) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(key) = path
                    .file_stem()
                    .and_then(std::ffi::OsStr::to_str)
                    .and_then(|stem| hex::decode(stem).ok())
                    .and_then(|bytes| bytes.try_into().ok())
                else {
                    continue;
                };
                let Ok(metadata) = entry.metadata() else {
                    continue;
                };
                let Ok(bytes) = usize::try_from(metadata.len()) else {
                    let _ = std::fs::remove_file(path);
                    continue;
                };
                if bytes > MAXIMUM_PATH_INDEX_VALUE_BYTES {
                    let _ = std::fs::remove_file(path);
                    continue;
                }
                match state.entries.binary_search_by_key(&key, |(key, _)| *key) {
                    Ok(index) => state.entries[index].1 = bytes,
                    Err(index) => state.entries.insert(index, (key, bytes)),
                }
            }
        }
        let _ = (|| -> Result<(), PathIndexError> {
            let token = crate::CancellationToken::new();
            let retained = state.owned_bytes()?.checked_add(root.capacity() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &token);
            let _ = accounting.claim_owned(retained, 0).map_err(projected_error)?;
            let result = (|| {
                let mut meter = PathIndexWork::new(&mut accounting)?;
                let directory = crate::native_host::open_root_directory_with_work(&root, &mut meter)?;
                evict_native_entries(&mut state, &directory, &mut meter)
            })();
            release_borrowed(&mut accounting, retained).map_err(projected_error)?;
            cache_io_miss(result)
        })();
        state.entries.shrink_to_fit();
        Self { root, state: Mutex::new(state) }
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl GenerationPathIndex for NativeGenerationPathIndex {
    fn load(
        &self, key: [u8; 32], accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Option<Vec<u8>>, KernelOwnedAllocation), crate::FsError> {
        (|| -> Result<_, PathIndexError> {
            use std::io::Read as _;
            accounting.check().map_err(projected_error)?;
            let borrowed = accounting.standalone_parts().is_some();
            let root_bytes = if borrowed { self.root.capacity() as u64 } else { 0 };
            if borrowed { let _ = accounting.claim_owned(root_bytes, 0).map_err(projected_error)?; }
            let result = (|| {
                let mut meter = PathIndexWork::new(accounting)?;
                let directory = crate::native_host::open_root_directory_with_work(&self.root, &mut meter)?;
                let mut name = [0_u8; 85];
                let leaf = cache_name(key, false, &mut name, &mut meter)?;
                let mut file = crate::native_host::open_leaf_with_work(&directory, leaf,
                    &crate::native_host::NativeOpenOptions::Read, &mut meter)?;
                let metadata = crate::native_host::NativeHostWork::attempt_io(&mut meter, WorkCounters {
                    backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || file.metadata())?;
                let length = match usize::try_from(metadata.len()) {
                    Ok(length) if length <= MAXIMUM_PATH_INDEX_VALUE_BYTES => length,
                    _ => return Ok((None, meter.take_allocation())),
                };
                let mut bytes = meter.exact_vec::<u8>(length)?;
                bytes.resize(length, 0);
                let mut consumed = 0;
                while consumed < length {
                    let remaining = &mut bytes[consumed..];
                    meter.accounting.admit(WorkCounters { backend_read_operations: 1,
                        source_bytes_read: remaining.len() as u64, ..WorkCounters::UNCHARGED }).map_err(projected_error)?;
                    let read = file.read(remaining);
                    let count = read.as_ref().map_or(0, |count| *count);
                    meter.accounting.record_completed(WorkCounters { backend_read_operations: 1,
                        source_bytes_read: count as u64, ..WorkCounters::UNCHARGED }).map_err(projected_error)?;
                    match read {
                        Ok(0) => { bytes.truncate(consumed); break; }
                        Ok(count) => consumed += count,
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                Ok((Some(bytes), meter.take_allocation()))
            })();
            release_borrowed(accounting, root_bytes).map_err(projected_error)?;
            match result {
                Err(PathIndexError::Io(_)) => Ok((None,
                    accounting.claim_owned(0, 0).map_err(projected_error)?)),
                result => result,
            }
        })().map_err(accounting_error)
    }

    fn store(
        &self, key: [u8; 32], value: &[u8], accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        if value.len() > MAXIMUM_PATH_INDEX_VALUE_BYTES { return Ok(()); }
        let Ok(mut state) = self.state.lock() else { return Ok(()); };
        let state = &mut *state;
        (|| -> Result<_, PathIndexError> {
            use std::io::Write as _;
            accounting.check().map_err(projected_error)?;
            let borrowed = accounting.standalone_parts().is_some();
            let root_bytes = self.root.capacity() as u64;
            if borrowed {
                let retained = state.owned_bytes()?.checked_add(root_bytes).ok_or(WorkError::Overflow)?;
                let _ = accounting.claim_owned(retained, 0).map_err(projected_error)?;
            }
            let result = (|| {
                cache_reserve(&mut state.entries, &mut state.allocation,
                    MAXIMUM_PATH_INDEX_ENTRIES + 1, accounting).map_err(projected_error)?;
                let mut meter = PathIndexWork::new(accounting)?;
                let directory = match crate::native_host::open_root_directory_with_work(&self.root, &mut meter) {
                    Ok(directory) => directory,
                    Err(PathIndexError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        crate::native_host::create_directories(&self.root, &mut meter)?;
                        crate::native_host::open_root_directory_with_work(&self.root, &mut meter)?
                    }
                    Err(error) => return Err(error),
                };
                evict_native_entries(state, &directory, &mut meter)?;
                let found = cache_find(&state.entries, key, meter.accounting)?;
                let copied = cache_move(match found {
                    Ok(_) => 0, Err(index) => state.entries.len() - index,
                }, std::mem::size_of::<([u8; 32], usize)>())?;
                meter.accounting.admit(copied).map_err(projected_error)?;
                let mut name = [0_u8; 85];
                let mut temporary = [0_u8; 85];
                let leaf = cache_name(key, false, &mut name, &mut meter)?;
                let temporary_leaf = cache_name(key, true, &mut temporary, &mut meter)?;
                let stored = (|| {
                    let mut file = crate::native_host::open_leaf_with_work(&directory, temporary_leaf,
                        &crate::native_host::NativeOpenOptions::ReadWriteCreate, &mut meter)?;
                    crate::native_host::NativeHostWork::attempt_io(&mut meter, WorkCounters {
                        backend_write_operations: 1, ..WorkCounters::UNCHARGED }, || file.set_len(0))?;
                    let mut remaining = value;
                    while !remaining.is_empty() {
                        meter.accounting.admit(WorkCounters { backend_write_operations: 1,
                            output_bytes: remaining.len() as u64, ..WorkCounters::UNCHARGED }).map_err(projected_error)?;
                        let written = file.write(remaining);
                        let count = written.as_ref().map_or(0, |count| *count);
                        meter.accounting.record_completed(WorkCounters { backend_write_operations: 1,
                            output_bytes: count as u64, ..WorkCounters::UNCHARGED }).map_err(projected_error)?;
                        match written {
                            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
                            Ok(count) => {
                                remaining = remaining.get(count..).ok_or(PathIndexError::Boundary(crate::FsError::HandleAllocationFailed))?;
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                    drop(file);
                    cache_io_miss(crate::native_host::remove_leaf_with_work(&directory, leaf, &mut meter))?;
                    crate::native_host::rename_no_replace_held(&directory, temporary_leaf,
                        &directory, leaf, &mut meter)
                })();
                match stored {
                    Err(error @ (PathIndexError::Boundary(_) | PathIndexError::Miss)) => return Err(error),
                    Err(PathIndexError::Io(_)) => {
                        cache_io_miss(crate::native_host::remove_leaf_with_work(
                            &directory, temporary_leaf, &mut meter))?;
                        return Ok(());
                    }
                    Ok(()) => {}
                }
                match found {
                    Ok(index) => state.entries[index].1 = value.len(),
                    Err(index) => state.entries.insert(index, (key, value.len())),
                }
                meter.accounting.record_completed(copied).map_err(projected_error)?;
                evict_native_entries(state, &directory, &mut meter)
            })();
            if borrowed {
                let retained = state.owned_bytes()?.checked_add(root_bytes).ok_or(WorkError::Overflow)?;
                release_borrowed(accounting, retained).map_err(projected_error)?;
            }
            cache_io_miss(result)
        })().map_err(accounting_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CancellationToken, WorkBudget};
    use crate::kernel::allocation::AllocationLedger;

    fn codec_fixture() -> (GenerationId, Vec<FileId>, Vec<(FileId, Vec<(NamespacePath, FileRecord)>)>) {
        let generation = GenerationId::new(crate::Digest::from_bytes([7; 32]));
        let file_id = FileId::from_bytes([3; 16]);
        let limits = VolumeLimits::default();
        let path = NamespacePath::new(vec![
            LogicalName::new(NameEncoding::PosixBytes, vec![0xff, b'x'], limits.maximum_component_bytes).expect("opaque POSIX"),
            LogicalName::new(NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8], limits.maximum_component_bytes).expect("opaque UTF16"),
        ], limits).expect("path");
        let record = FileRecord {
            file_id, kind: crate::kernel::FileKind::Fifo, link_count: 1,
            metadata: crate::storage::ObjectId {
                kind: crate::storage::ObjectKind::Metadata,
                digest: crate::Digest::from_bytes([9; 32]),
            },
            payload: crate::kernel::FilePayload::Empty,
        };
        (generation, vec![file_id, FileId::from_bytes([4; 16])], vec![(file_id, vec![(path, record)])])
    }

    fn fixture_bytes() -> (GenerationId, Vec<FileId>, Vec<u8>) {
        let (generation, file_ids, records) = codec_fixture();
        let bytes = encode_records(generation, &file_ids, &records, &mut KernelAccounting::borrowed_standalone(&mut AllocationLedger::default(), &mut WorkCounters::default(), WorkBudget::UNBOUNDED, &CancellationToken::new())).map(|(bytes, _)| bytes).map_err(projected_error).expect("encode").expect("cacheable");
        (generation, file_ids, bytes)
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn original_disk_cache_guards_resident_container_and_loaded_bytes() -> Result<(), crate::FsError> {
        let directory = tempfile::tempdir().expect("cache directory");
        let index = NativeGenerationPathIndex::new(directory.path().join("cache"));
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &token);
        index.store([9; 32], b"disk bytes", &mut accounting)?;
        let resident = original.live_allocation_bytes();
        assert!(resident > baseline);
        let (bytes, guard) = index.load([9; 32], &mut accounting)?;
        assert_eq!(bytes.as_deref(), Some(b"disk bytes".as_slice()));
        assert_eq!(original.live_allocation_bytes(), resident + 10);
        drop(bytes);
        drop(guard);
        assert_eq!(original.live_allocation_bytes(), resident);
        drop(index);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_decode_late_capacity_denial_retires_dropped_payload() -> Result<(), crate::FsError> {
        let (generation, ids, bytes) = fixture_bytes();
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &token,
        ).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &token);
        let result = decode(
            &bytes, generation, &ids, VolumeLimits::default(), &mut accounting);
        assert!(result.is_err());
        drop(result);
        assert_eq!(original.receipt().allocation_operations, 1);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn root_qualified_keys_separate_candidate_namespaces() -> Result<(), crate::FsError> {
        let token = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token);
        let (generation, ids, _) = codec_fixture();
        let table = crate::ObjectId {
            kind: crate::storage::ObjectKind::FileTablePage,
            digest: crate::Digest::from_bytes([1; 32]),
        };
        let root = FileId::from_bytes([2; 16]);
        let first = cache_key(generation, root, table, &ids, &mut accounting)?;
        let different_root = cache_key(
            generation, FileId::from_bytes([3; 16]), table, &ids, &mut accounting)?;
        let different_table = cache_key(
            generation, root, crate::ObjectId { digest: crate::Digest::from_bytes([2; 32]), ..table },
            &ids, &mut accounting)?;
        assert_ne!(first, different_root);
        assert_ne!(first, different_table);
        assert_eq!(first, cache_key(
            generation, root, table, &ids, &mut accounting)?);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_memory_cache_retains_only_physically_owned_capacity() -> Result<(), crate::FsError> {
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &token);
        let index = MemoryGenerationPathIndex::default();
        index.store([1; 32], b"original", &mut accounting)?;
        let resident = original.live_allocation_bytes();
        assert!(resident >= baseline + 8);
        let (bytes, guard) = index.load([1; 32], &mut accounting)?;
        assert_eq!(bytes.as_deref(), Some(b"original".as_slice()));
        assert_eq!(original.live_allocation_bytes(), resident + 8);
        drop(bytes);
        drop(guard);
        assert_eq!(original.live_allocation_bytes(), resident);
        index.store([1; 32], b"x", &mut accounting)?;
        assert_eq!(original.live_allocation_bytes(), resident - 7);
        drop(index);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_codec_returns_actual_wire_and_path_custody() -> Result<(), crate::FsError> {
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &token);
        let (generation, ids, records) = codec_fixture();
        let (wire, wire_guard) = encode_records(
            generation, &ids, &records, &mut accounting)?;
        let wire = wire.expect("cacheable");
        let wire_live = original.live_allocation_bytes();
        assert_eq!(wire_live, baseline + wire.capacity() as u64);
        let (paths, paths_guard) = decode(
            &wire, generation, &ids, VolumeLimits::default(), &mut accounting)?;
        assert_eq!(paths.as_ref().expect("valid").len(), 1);
        assert!(original.live_allocation_bytes() > wire_live);
        drop(paths);
        drop(paths_guard);
        assert_eq!(original.live_allocation_bytes(), wire_live);
        let (miss, miss_guard) = decode(
            b"corrupt", generation, &ids, VolumeLimits::default(), &mut accounting)?;
        assert!(miss.is_none());
        drop(miss);
        drop(miss_guard);
        assert_eq!(original.live_allocation_bytes(), wire_live);
        drop(wire);
        drop(wire_guard);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn measured_codec_nested_capacity_denial_releases_only_operation_owned_buffers() {
        let (generation, file_ids, bytes) = fixture_bytes();
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        ledger.claim_bytes(bytes.len() as u64, 0, &mut work, WorkBudget::UNBOUNDED).expect("input");
        assert!(matches!(decode(&bytes, generation, &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &token)).map(|(paths, _)| paths).map_err(projected_error),
            Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(work.allocation_operations, 1);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(ledger.live_bytes(), bytes.len() as u64);
        let (generation, file_ids, records) = codec_fixture();
        let mut work = WorkCounters::default();
        let mut output_ledger = AllocationLedger::default();
        assert!(matches!(encode_records(generation, &file_ids, &records, &mut KernelAccounting::borrowed_standalone(&mut output_ledger, &mut work, WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED }, &token)).map(|(bytes, _)| bytes).map_err(projected_error),
            Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(work.allocation_operations, 1);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(output_ledger.live_bytes(), 0);
    }

    #[test]
    fn measured_codec_opaque_names_round_trip_under_full_finite_bounds() {
        let (generation, file_ids, records) = codec_fixture();
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let bytes = encode_records(generation, &file_ids, &records, &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)).map(|(bytes, _)| bytes).map_err(projected_error).expect("encode").expect("cacheable");
        let encode_budget = work;
        assert!(encode_budget.has_finite_allowances());
        let mut finite_work = WorkCounters::default();
        let finite = encode_records(generation, &file_ids, &records, &mut KernelAccounting::borrowed_standalone(&mut AllocationLedger::default(), &mut finite_work, encode_budget, &token)).map(|(bytes, _)| bytes).map_err(projected_error)
            .expect("finite encode").expect("cacheable");
        assert_eq!(finite, bytes);
        assert_eq!(finite_work, encode_budget);
        let baseline = work;
        let decoded = decode(&bytes, generation, &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)).map(|(paths, _)| paths).map_err(projected_error)
            .expect("decode").expect("valid");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1, records[0].1[0].0);
        let retained = (decoded.capacity() * std::mem::size_of::<(FileId, NamespacePath)>()) as u64
            + decoded.iter().map(|(_, path)| path.retained_bytes()).sum::<u64>();
        assert_eq!(ledger.live_bytes(), bytes.capacity() as u64 + retained);
        let mut finite_ledger = AllocationLedger::default();
        let mut finite_work = baseline;
        finite_ledger.claim_bytes(bytes.capacity() as u64, 0, &mut finite_work, work).expect("caller bytes");
        let finite = decode(&bytes, generation, &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut finite_ledger, &mut finite_work, work, &token)).map(|(paths, _)| paths).map_err(projected_error)
            .expect("full finite decode").expect("valid");
        assert_eq!(finite, decoded);
        assert_eq!(finite_work, work);
    }

    #[test]
    fn measured_codec_denies_output_allocation_before_copy() {
        let (generation, file_ids, bytes) = fixture_bytes();
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        ledger.claim_bytes(bytes.len() as u64, 0, &mut work, WorkBudget::UNBOUNDED).expect("input");
        let budget = WorkBudget { peak_allocation_bytes: bytes.len() as u64, ..WorkBudget::UNBOUNDED };
        assert!(matches!(decode(&bytes, generation, &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token)).map(|(paths, _)| paths).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(work.allocation_operations, 0);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(ledger.live_bytes(), bytes.len() as u64);
        let (generation, file_ids, records) = codec_fixture();
        let mut work = WorkCounters::default();
        assert!(matches!(encode_records(generation, &file_ids, &records, &mut KernelAccounting::borrowed_standalone(&mut AllocationLedger::default(), &mut work, WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &token)).map(|(bytes, _)| bytes).map_err(projected_error),
            Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(work.allocation_operations, 0);
    }

    #[test]
    fn measured_codec_malformed_identity_and_path_bounds_remain_misses() {
        let (generation, file_ids, bytes) = fixture_bytes();
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        for invalid in [b"{}".as_slice(), &bytes[..bytes.len() - 1], b"{\"version\":01}"] {
            assert!(decode(invalid, generation, &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)).map(|(paths, _)| paths).map_err(projected_error)
                .expect("invalid miss").is_none());
            assert_eq!(ledger.live_bytes(), 0);
        }
        for limits in [
            VolumeLimits { maximum_path_depth: 1, ..VolumeLimits::default() },
            VolumeLimits { maximum_component_bytes: 1, ..VolumeLimits::default() },
            VolumeLimits { maximum_path_bytes: 5, ..VolumeLimits::default() },
        ] {
            assert!(decode(&bytes, generation, &file_ids, limits, &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)).map(|(paths, _)| paths).map_err(projected_error).expect("bounded miss").is_none());
            assert_eq!(ledger.live_bytes(), 0);
        }
        assert!(decode(&bytes, GenerationId::new(crate::Digest::from_bytes([8; 32])), &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)).map(|(paths, _)| paths).map_err(projected_error)
            .expect("generation miss").is_none());
        assert!(decode(&bytes, generation, &file_ids[..1], VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)).map(|(paths, _)| paths).map_err(projected_error)
            .expect("query miss").is_none());
        let mut invalid_name = bytes.clone();
        let offset = invalid_name.windows(2).rposition(|chunk| chunk == [0xff, b'x']).expect("name");
        invalid_name[offset] = 0;
        assert!(decode(&invalid_name, generation, &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)).map(|(paths, _)| paths).map_err(projected_error)
            .expect("name miss").is_none());
        assert_eq!(ledger.live_bytes(), 0);
    }

    #[test]
    fn measured_codec_cancellation_and_denial_preserve_actual_work_prefix() {
        let (generation, file_ids, bytes) = fixture_bytes();
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        assert!(matches!(decode(&bytes, generation, &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget { items_examined: 8, ..WorkBudget::UNBOUNDED }, &token)).map(|(paths, _)| paths).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(ledger.live_bytes(), 0);
        token.cancel();
        let mut work = WorkCounters::default();
        assert!(matches!(decode(&bytes, generation, &file_ids, VolumeLimits::default(), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)).map(|(paths, _)| paths).map_err(projected_error),
            Err(PathIndexError::Boundary(crate::FsError::Cancelled(_)))));
        assert_eq!(work, WorkCounters::default());
    }

    #[test]
    fn memory_index_has_exact_entry_and_byte_bounds() {
        let index = MemoryGenerationPathIndex::default();
        for ordinal in 0..=MAXIMUM_PATH_INDEX_ENTRIES {
            let mut key = [0_u8; 32];
            key[..8].copy_from_slice(&u64::try_from(ordinal).expect("ordinal").to_be_bytes());
            index.store(key, b"value", &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("store");
        }
        let state = index.state.lock().expect("path index");
        assert_eq!(state.entries.len(), MAXIMUM_PATH_INDEX_ENTRIES);
        assert_eq!(state.entries.iter().map(|(_, value)| value.0.len()).sum::<usize>(), MAXIMUM_PATH_INDEX_ENTRIES * 5);
        assert!(!state.entries.iter().any(|(key, _)| *key == [0; 32]));
        drop(state);

        index.store([0xff; 32], &vec![0; MAXIMUM_PATH_INDEX_VALUE_BYTES + 1], &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("store");
        assert!(index.load([0xff; 32], &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("load").0.is_none());
    }

    #[test]
    fn measured_memory_copy_preserves_caller_and_cache_overlap() {
        let index = MemoryGenerationPathIndex::default();
        index.store([1; 32], b"cached", &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("store");
        let cache_bytes = index.state.lock().expect("cache").owned_bytes().expect("owned");
        let token = CancellationToken::new();
        let mut work = WorkCounters::default();
        let mut ledger = AllocationLedger::default();
        ledger.claim_bytes(11, 0, &mut work, WorkBudget::UNBOUNDED).expect("caller");
        let budget = WorkBudget { peak_allocation_bytes: cache_bytes + 11 + 6,
            ..WorkBudget::UNBOUNDED };
        let loaded = index.load([1; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token)).map(|(bytes, _)| bytes).map_err(projected_error)
            .expect("finite load").expect("cached");
        assert_eq!(loaded, b"cached");
        assert_eq!(work.bytes_copied, 6);
        assert_eq!(work.peak_allocation_bytes, cache_bytes + 17);
        assert_eq!(ledger.live_bytes(), 17);
        ledger.release(loaded.capacity() as u64).expect("release returned bytes");
        assert_eq!(ledger.live_bytes(), 11);

        let mut denied_work = WorkCounters::default();
        let denied = WorkBudget { peak_allocation_bytes: cache_bytes + 16, ..budget };
        assert!(matches!(index.load([1; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut denied_work, denied, &token)).map(|(bytes, _)| bytes).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(denied_work.bytes_copied, 0);
        assert_eq!(ledger.live_bytes(), 11);
        assert_eq!(index.load([1; 32], &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("load").0.expect("retained entry"), b"cached");
    }

    #[test]
    fn measured_memory_store_rejects_copy_before_retained_mutation() {
        let index = MemoryGenerationPathIndex::default();
        index.store([1; 32], b"original", &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("store");
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let token = CancellationToken::new();
        let budget = WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED };
        assert!(matches!(index.store([1; 32], b"replacement", &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token)).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(ledger.live_bytes(), 0);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(index.load([1; 32], &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("load").0.expect("original retained"), b"original");
        token.cancel();
        let mut cancelled_work = WorkCounters::default();
        assert!(matches!(index.load([1; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut cancelled_work, WorkBudget::UNBOUNDED, &token)).map(|(bytes, _)| bytes).map_err(projected_error),
            Err(PathIndexError::Boundary(crate::FsError::Cancelled(_)))));
        assert_eq!(cancelled_work, WorkCounters::default());
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    fn finite_provider_budget() -> WorkBudget {
        WorkBudget {
            backend_read_operations: 128, backend_write_operations: 128,
            bytes_copied: 1024 * 1024, bytes_encoded: 1024 * 1024,
            source_bytes_read: 1024 * 1024, source_path_components: 1024,
            output_bytes: 1024 * 1024, items_examined: 65536,
            allocation_operations: 1024, peak_allocation_bytes: 1024 * 1024,
            ..WorkBudget::default()
        }
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn measured_native_cache_replays_actual_finite_read_and_write_engine() {
        let directory = tempfile::tempdir().expect("cache directory");
        let index = NativeGenerationPathIndex::new(directory.path().join("cache"));
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut stored_work = WorkCounters::default();
        let budget = finite_provider_budget();
        index.store([7; 32], b"cached", &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut stored_work, budget, &token)).map_err(projected_error).expect("finite native store");
        assert_eq!(stored_work.output_bytes, 6);
        assert!(stored_work.backend_write_operations > 0);
        assert!(stored_work.allocation_operations > 0);
        assert_eq!(ledger.live_bytes(), 0);
        stored_work.verify(budget).expect("finite store receipt");

        let mut measured = WorkCounters::default();
        let bytes = index.load([7; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut measured, budget, &token)).map(|(bytes, _)| bytes).map_err(projected_error).expect("finite read").expect("warm cache");
        assert_eq!(bytes, b"cached");
        assert_eq!(measured.source_bytes_read, 6);
        assert!(measured.backend_read_operations > 0);
        assert_eq!(ledger.live_bytes(), bytes.capacity() as u64);
        ledger.release(bytes.capacity() as u64).expect("return ownership");
        let mut replayed = WorkCounters::default();
        let replay = index.load([7; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut replayed, measured, &token)).map(|(bytes, _)| bytes).map_err(projected_error).expect("exact finite read").expect("warm replay");
        assert_eq!(replay, bytes);
        assert_eq!(replayed, measured);
        ledger.release(replay.capacity() as u64).expect("replay ownership");

        let denied = WorkBudget { source_bytes_read: 5, ..budget };
        let mut denied_work = WorkCounters::default();
        assert!(matches!(index.load([7; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut denied_work, denied, &token)).map(|(bytes, _)| bytes).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(denied_work.source_bytes_read, 0);
        assert!(denied_work.backend_read_operations > 0);
        assert_eq!(ledger.live_bytes(), 0);
        denied_work.verify(denied).expect("source denial prefix");
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn measured_native_failure_is_a_miss_only_for_io() {
        let directory = tempfile::tempdir().expect("cache directory");
        let index = NativeGenerationPathIndex::new(directory.path().join("missing"));
        let token = CancellationToken::new();
        let budget = finite_provider_budget();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        assert!(index.load([3; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token)).map(|(bytes, _)| bytes).map_err(projected_error).expect("missing cache").is_none());
        assert!(work.backend_read_operations > 0);
        assert_eq!(ledger.live_bytes(), 0);

        let denied = WorkBudget { backend_read_operations: 0, ..budget };
        let mut work = WorkCounters::default();
        assert!(matches!(index.load([3; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, denied, &token)).map(|(bytes, _)| bytes).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(work.backend_read_operations, 0);
        assert_eq!(ledger.live_bytes(), 0);
        token.cancel();
        let mut work = WorkCounters::default();
        assert!(matches!(index.store([3; 32], b"value", &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token)).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Cancelled(_)))));
        assert_eq!(work, WorkCounters::default());
        assert!(!directory.path().join("missing").exists());
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn measured_native_write_denial_preserves_previous_cache_entry() {
        let directory = tempfile::tempdir().expect("cache directory");
        let index = NativeGenerationPathIndex::new(directory.path().to_path_buf());
        index.store([8; 32], b"original", &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("store");
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let token = CancellationToken::new();
        let budget = WorkBudget { output_bytes: 0, ..finite_provider_budget() };
        assert!(matches!(index.store([8; 32], b"replacement", &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token)).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        assert_eq!(work.output_bytes, 0);
        assert_eq!(ledger.live_bytes(), 0);
        assert_eq!(index.load([8; 32], &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("load").0.expect("original remains"), b"original");
        index.store([8; 32], b"replacement", &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("store");
        assert_eq!(index.load([8; 32], &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("load").0.expect("scratch reused"), b"replacement");
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn measured_native_vec_storage_preserves_lexicographic_eviction() {
        let directory = tempfile::tempdir().expect("cache directory");
        let index = NativeGenerationPathIndex::new(directory.path().to_path_buf());
        let token = CancellationToken::new();
        let budget = finite_provider_budget();
        for ordinal in (0..=MAXIMUM_PATH_INDEX_ENTRIES).rev() {
            let mut key = [0_u8; 32];
            key[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
            let mut ledger = AllocationLedger::default();
            let mut work = WorkCounters::default();
            index.store(key, b"value", &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token)).map_err(projected_error).expect("finite bounded insertion");
            assert_eq!(ledger.live_bytes(), 0);
            work.verify(budget).expect("finite insertion receipt");
        }
        let state = index.state.lock().expect("cache state");
        assert_eq!(state.entries.len(), MAXIMUM_PATH_INDEX_ENTRIES);
        assert_eq!(state.entries.iter().map(|(_, bytes)| *bytes).sum::<usize>(), MAXIMUM_PATH_INDEX_ENTRIES * 5);
        assert!(!state.entries.iter().any(|(key, _)| *key == [0; 32]));
        drop(state);
        assert!(index.load([0; 32], &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("load").0.is_none());
        assert_eq!(std::fs::read_dir(directory.path()).expect("native entries")
            .count(), MAXIMUM_PATH_INDEX_ENTRIES);
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn cache_host_preserves_physical_failure_after_late_cancellation() {
        let directory = tempfile::tempdir().expect("held cache fixture");
        let missing = directory.path().join("missing");
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let budget = WorkBudget { backend_read_operations: 1, ..WorkBudget::default() };
        let mut accounting = KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token);
        let mut meter = PathIndexWork::new(&mut accounting).expect("meter");
        let error = crate::native_host::NativeHostWork::attempt_io(&mut meter, WorkCounters {
            backend_read_operations: 1, ..WorkCounters::UNCHARGED
        }, || {
            let result = std::fs::metadata(&missing);
            token.cancel();
            result
        }).expect_err("actual missing entry wins over later cancellation");
        assert!(matches!(error, PathIndexError::Io(error)
            if error.kind() == std::io::ErrorKind::NotFound));
        assert_eq!(meter.accounting.spent().backend_read_operations, 1);
        meter.accounting.spent().verify(budget).expect("actual attempted prefix");
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn cache_host_cleanup_reservation_restores_after_cancellation() {
        use crate::native_host::NativeHostWork as _;
        let file = tempfile::tempfile().expect("cleanup file");
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let budget = WorkBudget { backend_write_operations: 2, ..WorkBudget::default() };
        let mut accounting = KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token);
        let mut meter = PathIndexWork::new(&mut accounting).expect("meter");
        let cleanup = meter.reserve_cleanup_write().expect("reserve restoration");
        assert_eq!(meter.accounting.spent().backend_write_operations, 0);
        assert_eq!(meter.accounting.standalone_parts().expect("standalone").2.backend_write_operations, 1);
        crate::native_host::NativeHostWork::attempt_io(&mut meter, WorkCounters { backend_write_operations: 1,
            ..WorkCounters::UNCHARGED }, || file.set_len(8)).expect("actual mutation");
        assert!(matches!(meter.accounting.admit(WorkCounters { backend_write_operations: 1,
            ..WorkCounters::UNCHARGED }).map_err(projected_error), Err(PathIndexError::Boundary(crate::FsError::Work(_)))));
        token.cancel();
        let (restored, recorded) = meter.attempt_cleanup_write(cleanup, || file.set_len(0));
        restored.expect("mandatory physical restoration");
        recorded.expect("admitted restoration receipt");
        assert_eq!(meter.accounting.spent().backend_write_operations, 2);
        assert_eq!(meter.accounting.standalone_parts().expect("standalone").2.backend_write_operations, 2);
        assert_eq!(file.metadata().expect("restored file").len(), 0);
        meter.accounting.spent().verify(budget).expect("exact cleanup receipt");
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn cache_host_cancelled_reservation_spends_no_write() {
        use crate::native_host::NativeHostWork as _;
        let file = tempfile::tempfile().expect("write file");
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let budget = WorkBudget { backend_write_operations: 1, ..WorkBudget::default() };
        let mut accounting = KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token);
        let mut meter = PathIndexWork::new(&mut accounting).expect("meter");
        let cleanup = meter.reserve_cleanup_write().expect("reserve write");
        assert_eq!(meter.accounting.standalone_parts().expect("standalone").2.backend_write_operations, 0);
        meter.cancel_cleanup_write(cleanup);
        assert_eq!(meter.accounting.spent().backend_write_operations, 0);
        crate::native_host::NativeHostWork::attempt_io(&mut meter, WorkCounters { backend_write_operations: 1,
            ..WorkCounters::UNCHARGED }, || file.set_len(3)).expect("released write allowance");
        assert_eq!(meter.accounting.spent().backend_write_operations, 1);
        assert_eq!(file.metadata().expect("actual write").len(), 3);
    }
}
