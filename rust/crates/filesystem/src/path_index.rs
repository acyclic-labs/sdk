//! Disposable generation/path query acceleration.
//!
//! Generation identity and filesystem semantics never depend on this cache.
//! Missing, corrupt, or unwritable entries fall back to the authenticated
//! namespace traversal; complete results can then be reused by every context
//! sharing the same filesystem deployment.

use crate::kernel::allocation::{AllocationError, AllocationLedger};
use crate::kernel::{FileRecord, LogicalName, NameEncoding, NamespacePath};
use crate::model::VolumeLimits;
use crate::{CancellationToken, FileId, GenerationId, WorkBudget, WorkCounters, WorkError};
use std::collections::BTreeMap;
use std::sync::Mutex;

const PATH_QUERY_VERSION: u32 = 1;
const MAXIMUM_PATH_INDEX_ENTRIES: usize = 256;
const MAXIMUM_PATH_INDEX_RETAINED_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_PATH_INDEX_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PathIndexError {
    #[error(transparent)]
    Work(#[from] WorkError),
    #[error("path index operation cancelled")]
    Cancelled,
    #[error("path index allocation failed")]
    AllocationFailed,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<AllocationError> for PathIndexError {
    fn from(error: AllocationError) -> Self {
        error.into_work_or(Self::AllocationFailed)
    }
}

fn check(token: &CancellationToken) -> Result<(), PathIndexError> {
    token.check().map_err(|_| PathIndexError::Cancelled)
}

fn copied(bytes: usize, work: &mut WorkCounters, budget: WorkBudget) -> Result<(), PathIndexError> {
    work.charge(&WorkCounters { bytes_copied: bytes as u64, ..WorkCounters::default() }, &budget)?;
    Ok(())
}

fn exact_vec<T>(
    count: usize, ledger: &mut AllocationLedger, work: &mut WorkCounters,
    budget: WorkBudget, token: &CancellationToken,
) -> Result<Vec<T>, PathIndexError> {
    check(token)?;
    let bytes = ledger.claim_elements::<T>(count, work, budget)?;
    let mut value = Vec::new();
    if value.try_reserve_exact(count).is_err() {
        ledger.release(bytes)?;
        return Err(PathIndexError::AllocationFailed);
    }
    Ok(value)
}

/// The canonical cache wire format is scanned without a JSON DOM or string
/// scratch buffers. Replay allocates only exact, already admitted capacities.
pub(crate) struct GenerationPathQuery;

impl GenerationPathQuery {
    pub(crate) fn decode_measured(
        bytes: &[u8], generation: GenerationId, file_ids: &[FileId], limits: VolumeLimits,
        ledger: &mut AllocationLedger, work: &mut WorkCounters, budget: WorkBudget,
        token: &CancellationToken,
    ) -> Result<Option<Vec<(FileId, NamespacePath)>>, PathIndexError> {
        check(token)?;
        if bytes.len() > MAXIMUM_PATH_INDEX_VALUE_BYTES { return Ok(None); }
        let query = query_id_measured(file_ids, work, budget, token)?;
        let initial_live = ledger.live_bytes();
        let result = (|| {
            let mut parser = QueryParser { bytes, position: 0, work, budget, token };
            let count = parser.document(generation, file_ids, query, limits, ledger, None)?;
            let mut output = exact_vec(count, ledger, parser.work, budget, token)?;
            parser.position = 0;
            parser.document(generation, file_ids, query, limits, ledger, Some(&mut output))?;
            Ok(output)
        })();
        match result {
            Ok(output) => Ok(Some(output)),
            Err(error) => {
                ledger.release(ledger.live_bytes() - initial_live)?;
                match error {
                    DecodeError::Invalid => Ok(None),
                    DecodeError::Boundary(error) => Err(error),
                }
            }
        }
    }
}

enum DecodeError {
    Invalid,
    Boundary(PathIndexError),
}

impl From<PathIndexError> for DecodeError {
    fn from(error: PathIndexError) -> Self { Self::Boundary(error) }
}

struct QueryParser<'a> {
    bytes: &'a [u8],
    position: usize,
    work: &'a mut WorkCounters,
    budget: WorkBudget,
    token: &'a CancellationToken,
}

impl QueryParser<'_> {
    fn take(&mut self) -> Result<u8, DecodeError> {
        check(self.token)?;
        let value = *self.bytes.get(self.position).ok_or(DecodeError::Invalid)?;
        self.work.charge_items(1, &self.budget).map_err(PathIndexError::from)?;
        self.position += 1;
        Ok(value)
    }

    fn whitespace(&mut self) -> Result<(), DecodeError> {
        while self.bytes.get(self.position).is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t')) {
            self.take()?;
        }
        Ok(())
    }

    fn literal(&mut self, literal: &[u8]) -> Result<(), DecodeError> {
        let mut quoted = false;
        for &expected in literal {
            if !quoted { self.whitespace()?; }
            if self.take()? != expected { return Err(DecodeError::Invalid); }
            if expected == b'"' { quoted = !quoted; }
        }
        Ok(())
    }

    fn number(&mut self) -> Result<u32, DecodeError> {
        self.whitespace()?;
        let first = self.take()?;
        if !first.is_ascii_digit() { return Err(DecodeError::Invalid); }
        let mut value = u32::from(first - b'0');
        if first == b'0' && self.bytes.get(self.position).is_some_and(u8::is_ascii_digit) {
            return Err(DecodeError::Invalid);
        }
        while self.bytes.get(self.position).is_some_and(u8::is_ascii_digit) {
            value = value.checked_mul(10).and_then(|n| n.checked_add(u32::from(self.bytes[self.position] - b'0')))
                .ok_or(DecodeError::Invalid)?;
            self.take()?;
        }
        Ok(value)
    }

    fn next(&mut self, first: &mut bool) -> Result<bool, DecodeError> {
        self.whitespace()?;
        if self.bytes.get(self.position) == Some(&b']') {
            self.take()?;
            return Ok(false);
        }
        if !*first { self.literal(b",")?; }
        *first = false;
        Ok(true)
    }

    fn byte_array(&mut self, mut output: Option<&mut Vec<u8>>) -> Result<usize, DecodeError> {
        self.literal(b"[")?;
        let mut first = true;
        let mut count = 0_usize;
        while self.next(&mut first)? {
            let byte = u8::try_from(self.number()?).map_err(|_| DecodeError::Invalid)?;
            count = count.checked_add(1).ok_or(DecodeError::Invalid)?;
            if let Some(output) = output.as_deref_mut() {
                if output.len() == output.capacity() { return Err(DecodeError::Invalid); }
                copied(1, self.work, self.budget)?;
                output.push(byte);
            }
        }
        Ok(count)
    }

    fn identity(&mut self, expected: &[u8; 32]) -> Result<(), DecodeError> {
        self.literal(b"[")?;
        for (index, byte) in expected.iter().enumerate() {
            if index != 0 { self.literal(b",")?; }
            if self.number()? != u32::from(*byte) { return Err(DecodeError::Invalid); }
        }
        self.literal(b"]")
    }

    fn file_id(&mut self, expected: FileId) -> Result<(), DecodeError> {
        self.literal(b"\"")?;
        let start = self.position;
        while self.bytes.get(self.position).is_some_and(|byte| byte.is_ascii_hexdigit() || *byte == b'-') {
            self.take()?;
        }
        let value = std::str::from_utf8(&self.bytes[start..self.position]).map_err(|_| DecodeError::Invalid)?;
        let uuid = uuid::Uuid::parse_str(value).map_err(|_| DecodeError::Invalid)?;
        if self.take()? != b'"' { return Err(DecodeError::Invalid); }
        if uuid.into_bytes() != expected.into_bytes() { return Err(DecodeError::Invalid); }
        Ok(())
    }

    fn name(&mut self, limits: VolumeLimits) -> Result<(NameEncoding, usize, usize), DecodeError> {
        self.literal(b"{\"encoding\":")?;
        let encoding = match self.number()? {
            1 => NameEncoding::Utf8,
            2 => NameEncoding::PosixBytes,
            3 => NameEncoding::WindowsUtf16Le,
            _ => return Err(DecodeError::Invalid),
        };
        self.literal(b",\"bytes\":")?;
        let start = self.position;
        let length = self.byte_array(None)?;
        if length == 0 || length as u64 > u64::from(limits.maximum_component_bytes) {
            return Err(DecodeError::Invalid);
        }
        self.literal(b"}")?;
        Ok((encoding, length, start))
    }

    fn path_shape(&mut self, limits: VolumeLimits) -> Result<usize, DecodeError> {
        self.literal(b"[")?;
        let mut first = true;
        let mut count = 0_usize;
        let mut length = 1_u64;
        while self.next(&mut first)? {
            let (_, bytes, _) = self.name(limits)?;
            count += 1;
            if count > usize::from(limits.maximum_path_depth) { return Err(DecodeError::Invalid); }
            length = length.checked_add(bytes as u64 + u64::from(count > 1)).ok_or(DecodeError::Invalid)?;
            if length > u64::from(limits.maximum_path_bytes) { return Err(DecodeError::Invalid); }
        }
        if length > u64::from(limits.maximum_path_bytes) { return Err(DecodeError::Invalid); }
        Ok(count)
    }

    fn path(&mut self, limits: VolumeLimits, ledger: &mut AllocationLedger) -> Result<NamespacePath, DecodeError> {
        let start = self.position;
        let count = self.path_shape(limits)?;
        self.position = start;
        let mut names = exact_vec::<LogicalName>(count, ledger, self.work, self.budget, self.token)?;
        self.literal(b"[")?;
        let mut first = true;
        while self.next(&mut first)? {
            let (encoding, length, bytes_start) = self.name(limits)?;
            let end = self.position;
            let mut bytes = exact_vec::<u8>(length, ledger, self.work, self.budget, self.token)?;
            self.position = bytes_start;
            self.byte_array(Some(&mut bytes))?;
            self.position = end;
            check(self.token)?;
            self.work.charge_items(length as u64, &self.budget).map_err(PathIndexError::from)?;
            let name = LogicalName::new(encoding, bytes, limits.maximum_component_bytes).map_err(|_| DecodeError::Invalid)?;
            copied(std::mem::size_of::<LogicalName>(), self.work, self.budget)?;
            names.push(name);
        }
        NamespacePath::new(names, limits).map_err(|_| DecodeError::Invalid)
    }

    fn document(
        &mut self, generation: GenerationId, file_ids: &[FileId], query: [u8; 32],
        limits: VolumeLimits, ledger: &mut AllocationLedger,
        mut output: Option<&mut Vec<(FileId, NamespacePath)>>,
    ) -> Result<usize, DecodeError> {
        self.literal(b"{\"version\":")?;
        if self.number()? != PATH_QUERY_VERSION { return Err(DecodeError::Invalid); }
        self.literal(b",\"generation\":")?;
        self.identity(generation.digest().as_bytes())?;
        self.literal(b",\"query\":")?;
        self.identity(&query)?;
        self.literal(b",\"entries\":[")?;
        let mut entry_first = true;
        let mut entries = 0_usize;
        let mut paths = 0_usize;
        while self.next(&mut entry_first)? {
            let file_id = *file_ids.get(entries).ok_or(DecodeError::Invalid)?;
            entries += 1;
            self.literal(b"{\"file_id\":")?;
            self.file_id(file_id)?;
            self.literal(b",\"paths\":[")?;
            let mut path_first = true;
            while self.next(&mut path_first)? {
                paths = paths.checked_add(1).ok_or(DecodeError::Invalid)?;
                if let Some(output) = output.as_deref_mut() {
                    let path = self.path(limits, ledger)?;
                    copied(std::mem::size_of::<(FileId, NamespacePath)>(), self.work, self.budget)?;
                    output.push((file_id, path));
                } else {
                    self.path_shape(limits)?;
                }
            }
            self.literal(b"}")?;
        }
        self.literal(b"}")?;
        self.whitespace()?;
        if self.position != self.bytes.len() || entries != file_ids.len() { return Err(DecodeError::Invalid); }
        Ok(paths)
    }
}

enum EncodeError {
    TooLarge,
    Boundary(PathIndexError),
}

impl From<PathIndexError> for EncodeError {
    fn from(error: PathIndexError) -> Self { Self::Boundary(error) }
}

/// Counting and producing use the same canonical encoder. Neither pass owns
/// intermediate paths, strings, JSON values, or serializer error buffers.
struct QueryWriter<'a> {
    output: Option<&'a mut Vec<u8>>,
    length: usize,
    work: &'a mut WorkCounters,
    budget: WorkBudget,
    token: &'a CancellationToken,
}

impl QueryWriter<'_> {
    fn append(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        check(self.token)?;
        let length = self.length.checked_add(bytes.len())
            .ok_or(PathIndexError::Work(WorkError::Overflow))?;
        if length > MAXIMUM_PATH_INDEX_VALUE_BYTES { return Err(EncodeError::TooLarge); }
        self.work.charge(&WorkCounters {
            bytes_encoded: bytes.len() as u64,
            bytes_copied: if self.output.is_some() { bytes.len() as u64 } else { 0 },
            items_examined: 1,
            ..WorkCounters::default()
        }, &self.budget).map_err(PathIndexError::from)?;
        if let Some(output) = self.output.as_deref_mut() {
            if length > output.capacity() { return Err(PathIndexError::AllocationFailed.into()); }
            output.extend_from_slice(bytes);
        }
        self.length = length;
        Ok(())
    }

    fn number(&mut self, mut value: u32) -> Result<(), EncodeError> {
        let mut buffer = [0_u8; 10];
        let mut start = buffer.len();
        loop {
            start -= 1;
            buffer[start] = b'0' + (value % 10) as u8;
            value /= 10;
            if value == 0 { break; }
        }
        self.append(&buffer[start..])
    }

    fn bytes(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.append(b"[")?;
        for (index, &byte) in bytes.iter().enumerate() {
            if index != 0 { self.append(b",")?; }
            self.number(u32::from(byte))?;
        }
        self.append(b"]")
    }

    fn document(
        &mut self, generation: GenerationId, query: [u8; 32], file_ids: &[FileId],
        records: &[(FileId, Vec<(NamespacePath, FileRecord)>)],
    ) -> Result<(), EncodeError> {
        self.append(b"{\"version\":")?;
        self.number(PATH_QUERY_VERSION)?;
        self.append(b",\"generation\":")?;
        self.bytes(generation.digest().as_bytes())?;
        self.append(b",\"query\":")?;
        self.bytes(&query)?;
        self.append(b",\"entries\":[")?;
        let mut record_index = 0_usize;
        for (index, &file_id) in file_ids.iter().enumerate() {
            if index != 0 { self.append(b",")?; }
            self.append(b"{\"file_id\":\"")?;
            let mut buffer = [0_u8; 36];
            let uuid = uuid::Uuid::from_bytes(file_id.into_bytes());
            self.append(uuid.hyphenated().encode_lower(&mut buffer).as_bytes())?;
            self.append(b"\",\"paths\":[")?;
            while records.get(record_index).is_some_and(|entry| entry.0 < file_id) {
                check(self.token)?;
                self.work.charge_items(1, &self.budget).map_err(PathIndexError::from)?;
                record_index += 1;
            }
            let paths = records.get(record_index).filter(|entry| entry.0 == file_id)
                .map_or(&[][..], |entry| entry.1.as_slice());
            for (path_index, (path, _)) in paths.iter().enumerate() {
                if path_index != 0 { self.append(b",")?; }
                self.append(b"[")?;
                for (name_index, name) in path.components().iter().enumerate() {
                    if name_index != 0 { self.append(b",")?; }
                    self.append(b"{\"encoding\":")?;
                    self.number(u32::from(name.encoding().tag()))?;
                    self.append(b",\"bytes\":")?;
                    self.bytes(name.as_bytes())?;
                    self.append(b"}")?;
                }
                self.append(b"]")?;
            }
            self.append(b"]}")?;
        }
        self.append(b"]}")
    }
}

pub(crate) fn encode_records_measured(
    generation: GenerationId, file_ids: &[FileId],
    records: &[(FileId, Vec<(NamespacePath, FileRecord)>)],
    ledger: &mut AllocationLedger, work: &mut WorkCounters, budget: WorkBudget,
    token: &CancellationToken,
) -> Result<Option<Vec<u8>>, PathIndexError> {
    let query = query_id_measured(file_ids, work, budget, token)?;
    let mut counter = QueryWriter { output: None, length: 0, work, budget, token };
    match counter.document(generation, query, file_ids, records) {
        Ok(()) => {}
        Err(EncodeError::TooLarge) => return Ok(None),
        Err(EncodeError::Boundary(error)) => return Err(error),
    }
    let length = counter.length;
    let initial_live = ledger.live_bytes();
    let mut output = exact_vec::<u8>(length, ledger, counter.work, budget, token)?;
    let mut writer = QueryWriter { output: Some(&mut output), length: 0, work: counter.work, budget, token };
    let result = writer.document(generation, query, file_ids, records);
    match result {
        Ok(()) => Ok(Some(output)),
        Err(error) => {
            ledger.release(ledger.live_bytes() - initial_live)?;
            match error {
                EncodeError::TooLarge => Ok(None),
                EncodeError::Boundary(error) => Err(error),
            }
        }
    }
}

fn hash_bytes(
    hasher: &mut blake3::Hasher, bytes: &[u8], work: &mut WorkCounters,
    budget: WorkBudget, token: &CancellationToken,
) -> Result<(), PathIndexError> {
    check(token)?;
    work.charge(&WorkCounters { bytes_hashed: bytes.len() as u64, items_examined: 1, ..WorkCounters::default() }, &budget)?;
    hasher.update(bytes);
    Ok(())
}

fn query_id_measured(
    file_ids: &[FileId], work: &mut WorkCounters, budget: WorkBudget, token: &CancellationToken,
) -> Result<[u8; 32], PathIndexError> {
    let mut hasher = blake3::Hasher::new();
    hash_bytes(&mut hasher, b"acyclic-generation-path-file-ids-v1\0", work, budget, token)?;
    for file_id in file_ids {
        hash_bytes(&mut hasher, &file_id.into_bytes(), work, budget, token)?;
    }
    Ok(*hasher.finalize().as_bytes())
}

pub(crate) fn cache_key_measured(
    generation: GenerationId, file_ids: &[FileId], work: &mut WorkCounters,
    budget: WorkBudget, token: &CancellationToken,
) -> Result<[u8; 32], PathIndexError> {
    let query = query_id_measured(file_ids, work, budget, token)?;
    let mut hasher = blake3::Hasher::new();
    hash_bytes(&mut hasher, b"acyclic-generation-path-query-v1\0", work, budget, token)?;
    hash_bytes(&mut hasher, generation.digest().as_bytes(), work, budget, token)?;
    hash_bytes(&mut hasher, &query, work, budget, token)?;
    Ok(*hasher.finalize().as_bytes())
}

/// Best-effort derived cache. I/O errors are misses; exhausted work and
/// cancellation remain visible to the containing operation.
pub(crate) trait GenerationPathIndex: Send + Sync {
    fn load_measured(
        &self, key: [u8; 32], ledger: &mut AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<Option<Vec<u8>>, PathIndexError>;

    fn store_measured(
        &self, key: [u8; 32], value: &[u8], ledger: &mut AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<(), PathIndexError>;

    fn load(&self, key: [u8; 32]) -> Option<Vec<u8>> {
        self.load_measured(key, &mut AllocationLedger::default(),
            &mut WorkCounters::default(), WorkBudget::UNBOUNDED, &CancellationToken::new())
            .ok().flatten()
    }

    fn store(&self, key: [u8; 32], value: &[u8]) {
        let _ = self.store_measured(key, value, &mut AllocationLedger::default(),
            &mut WorkCounters::default(), WorkBudget::UNBOUNDED, &CancellationToken::new());
    }
}

fn cache_check(cancellation: &CancellationToken) -> Result<(), PathIndexError> {
    cancellation.check().map_err(|_| PathIndexError::Cancelled)
}

fn cache_find<T>(
    entries: &[([u8; 32], T)], key: [u8; 32], work: &mut WorkCounters,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<Result<usize, usize>, PathIndexError> {
    let mut left = 0;
    let mut right = entries.len();
    while left < right {
        cache_check(cancellation)?;
        work.charge_items(1, &budget)?;
        let index = left + (right - left) / 2;
        let (existing, _) = entries.get(index).ok_or(PathIndexError::AllocationFailed)?;
        match existing.cmp(&key) {
            std::cmp::Ordering::Equal => return Ok(Ok(index)),
            std::cmp::Ordering::Greater => right = index,
            std::cmp::Ordering::Less => left = index + 1,
        }
    }
    Ok(Err(left))
}

fn cache_move<T>(
    count: usize, work: &mut WorkCounters, budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), PathIndexError> {
    cache_check(cancellation)?;
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)?;
    work.charge(&WorkCounters { bytes_copied: bytes as u64,
        ..WorkCounters::UNCHARGED }, &budget)?;
    Ok(())
}

fn cache_reserve<T>(
    entries: &mut Vec<T>, capacity: usize, ledger: &mut AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<(), PathIndexError> {
    if entries.capacity() >= capacity { return Ok(()); }
    cache_check(cancellation)?;
    let old_bytes = entries.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let new_bytes = ledger.claim_elements::<T>(capacity, work, budget)?;
    let result = (|| {
        let mut replacement = Vec::new();
        replacement.try_reserve_exact(capacity).map_err(|_| PathIndexError::AllocationFailed)?;
        cache_move::<T>(entries.len(), work, budget, cancellation)?;
        replacement.append(entries);
        *entries = replacement;
        Ok(())
    })();
    if result.is_err() { ledger.release(new_bytes)?; }
    else { ledger.release(old_bytes)?; }
    result
}

fn cache_copy(
    value: &[u8], ledger: &mut AllocationLedger, work: &mut WorkCounters,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<Vec<u8>, PathIndexError> {
    cache_check(cancellation)?;
    work.admit(&WorkCounters { bytes_copied: value.len() as u64,
        ..WorkCounters::UNCHARGED }, &budget)?;
    let bytes = ledger.claim_elements::<u8>(value.len(), work, budget)?;
    let mut copied = Vec::new();
    if copied.try_reserve_exact(value.len()).is_err() {
        ledger.release(bytes)?;
        return Err(PathIndexError::AllocationFailed);
    }
    if let Err(error) = cache_move::<u8>(value.len(), work, budget, cancellation) {
        ledger.release(bytes)?;
        return Err(error);
    }
    copied.extend_from_slice(value);
    Ok(copied)
}

pub(crate) struct MemoryGenerationPathIndex {
    state: Mutex<MemoryPathIndexState>,
}

#[derive(Default)]
struct MemoryPathIndexState {
    // Sorted keys preserve the cache's lexicographic eviction policy, while
    // making the actual retained container capacity measurable.
    entries: Vec<([u8; 32], Vec<u8>)>,
    retained_bytes: usize,
}

impl MemoryPathIndexState {
    fn owned_bytes(&self) -> Result<u64, PathIndexError> {
        let container = self.entries.capacity().checked_mul(
            std::mem::size_of::<([u8; 32], Vec<u8>)>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        self.entries.iter().try_fold(container, |total, (_, value)| {
            total.checked_add(value.capacity() as u64)
                .ok_or_else(|| crate::WorkError::Overflow.into())
        })
    }
}

impl Default for MemoryGenerationPathIndex {
    fn default() -> Self {
        Self { state: Mutex::new(MemoryPathIndexState::default()) }
    }
}

impl GenerationPathIndex for MemoryGenerationPathIndex {
    fn load_measured(
        &self, key: [u8; 32], ledger: &mut AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<Option<Vec<u8>>, PathIndexError> {
        cache_check(cancellation)?;
        let Ok(state) = self.state.lock() else { return Ok(None); };
        let retained = state.owned_bytes()?;
        ledger.claim_bytes(retained, 0, work, budget)?;
        let result = (|| {
            match cache_find(&state.entries, key, work, budget, cancellation)? {
                Ok(index) => cache_copy(&state.entries.get(index)
                    .ok_or(PathIndexError::AllocationFailed)?.1,
                    ledger, work, budget, cancellation).map(Some),
                Err(_) => Ok(None),
            }
        })();
        ledger.release(retained)?;
        result
    }

    fn store_measured(
        &self, key: [u8; 32], value: &[u8], ledger: &mut AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<(), PathIndexError> {
        cache_check(cancellation)?;
        if value.len() > MAXIMUM_PATH_INDEX_VALUE_BYTES { return Ok(()); }
        let Ok(mut state) = self.state.lock() else { return Ok(()); };
        ledger.claim_bytes(state.owned_bytes()?, 0, work, budget)?;
        let result = (|| {
            let found = cache_find(&state.entries, key, work, budget, cancellation)?;
            cache_reserve(&mut state.entries, MAXIMUM_PATH_INDEX_ENTRIES,
                ledger, work, budget, cancellation)?;
            let copied = cache_copy(value, ledger, work, budget, cancellation)?;
            let copied_bytes = copied.capacity() as u64;
            let mutation = (|| {
                if let Ok(index) = found {
                    cache_move::<([u8; 32], Vec<u8>)>(
                        state.entries.len() - index - 1, work, budget, cancellation)?;
                    let (_, previous) = state.entries.remove(index);
                    state.retained_bytes -= previous.len();
                    ledger.release(previous.capacity() as u64)?;
                }
                while state.entries.len() >= MAXIMUM_PATH_INDEX_ENTRIES
                    || state.retained_bytes.saturating_add(value.len())
                        > MAXIMUM_PATH_INDEX_RETAINED_BYTES
                {
                    cache_move::<([u8; 32], Vec<u8>)>(
                        state.entries.len() - 1, work, budget, cancellation)?;
                    let (_, evicted) = state.entries.remove(0);
                    state.retained_bytes -= evicted.len();
                    ledger.release(evicted.capacity() as u64)?;
                }
                let index = cache_find(&state.entries, key, work, budget, cancellation)?
                    .unwrap_or_else(|index| index);
                cache_move::<([u8; 32], Vec<u8>)>(
                    state.entries.len() - index, work, budget, cancellation)?;
                state.entries.insert(index, (key, copied));
                state.retained_bytes += value.len();
                Ok(())
            })();
            if mutation.is_err() { ledger.release(copied_bytes)?; }
            mutation
        })();
        ledger.release(state.owned_bytes()?)?;
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
    retained_bytes: usize,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl NativePathIndexState {
    fn owned_bytes(&self) -> Result<u64, PathIndexError> {
        self.entries.capacity().checked_mul(std::mem::size_of::<([u8; 32], usize)>())
            .map(|bytes| bytes as u64).ok_or_else(|| crate::WorkError::Overflow.into())
    }

    fn find_measured(
        &self, key: [u8; 32], meter: &mut CacheHostWork<'_>,
    ) -> Result<Option<usize>, PathIndexError> {
        for (index, (candidate, _)) in self.entries.iter().enumerate() {
            cache_check(meter.cancellation)?;
            meter.work.charge_items(1, &meter.budget)?;
            if *candidate == key { return Ok(Some(index)); }
        }
        Ok(None)
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
struct CacheHostWork<'a> {
    ledger: &'a mut AllocationLedger,
    work: &'a mut WorkCounters,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl crate::native_host::NativeHostWork for CacheHostWork<'_> {
    type Error = PathIndexError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        match error { PathIndexError::Io(error) => Some(error.kind()), _ => None }
    }
    fn check(&self) -> Result<(), Self::Error> { cache_check(self.cancellation) }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        cache_check(self.cancellation)?;
        self.work.admit(&delta, &self.budget)?;
        Ok(())
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.work.charge(&delta, &self.budget)?;
        Ok(())
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, Self::Error> {
        cache_check(self.cancellation)?;
        self.ledger.claim_bytes(bytes, u64::from(bytes != 0), self.work, self.budget)?;
        Ok(bytes)
    }
    fn release(&mut self, bytes: u64) -> Result<(), Self::Error> {
        self.ledger.release(bytes)?;
        Ok(())
    }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        cache_check(self.cancellation)?;
        self.work.admit(&WorkCounters {
            backend_write_operations: 1, ..WorkCounters::UNCHARGED
        }, &self.budget)?;
        let original = self.budget.backend_write_operations;
        self.budget.backend_write_operations = original.checked_sub(1)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(original)
    }
    fn cancel_cleanup_write(&mut self, original: Self::CleanupWrite) {
        self.budget.backend_write_operations = original;
    }
    fn attempt_cleanup_write<T>(
        &mut self, original: Self::CleanupWrite, operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        self.budget.backend_write_operations = original;
        let delta = WorkCounters {
            backend_write_operations: 1, ..WorkCounters::UNCHARGED
        };
        // The admitted restoration remains mandatory even if cancellation
        // arrived after the original mutation.
        let recorded = match self.work.charge(&delta, &self.budget) {
            Ok(()) => Ok(()),
            Err(error) => match self.work.try_add_assign(&delta) {
                Ok(()) => Err(error.into()),
                Err(error) => Err(error.into()),
            },
        };
        (operation(), recorded)
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn cache_io_miss(result: Result<(), PathIndexError>) -> Result<(), PathIndexError> {
    match result { Err(PathIndexError::Io(_)) => Ok(()), result => result }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn cache_host_attempt<T>(
    meter: &mut CacheHostWork<'_>, delta: WorkCounters,
    callback: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, PathIndexError> {
    use crate::native_host::NativeHostWork as _;
    meter.admit(delta)?;
    meter.record(delta)?;
    callback().map_err(Into::into)
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn cache_name(
    key: [u8; 32], temporary: bool, buffer: &mut [u8; 85],
    meter: &mut CacheHostWork<'_>,
) -> Result<usize, PathIndexError> {
    use crate::native_host::NativeHostWork as _;
    let pid = std::process::id();
    let mut digits = [0_u8; 10];
    let mut number = pid;
    let mut start = digits.len();
    loop {
        start -= 1;
        *digits.get_mut(start).ok_or(PathIndexError::AllocationFailed)? =
            b'0' + (number % 10) as u8;
        number /= 10;
        if number == 0 { break; }
    }
    let suffix = if temporary { b".next".as_slice() } else { b".json".as_slice() };
    let length = 64 + suffix.len() + if temporary { 1 + digits.len() - start } else { 0 };
    let delta = WorkCounters { bytes_encoded: length as u64, ..WorkCounters::UNCHARGED };
    meter.admit(delta)?;
    hex::encode_to_slice(key, buffer.get_mut(..64).ok_or(PathIndexError::AllocationFailed)?)
        .map_err(|_| PathIndexError::AllocationFailed)?;
    let mut cursor = 64;
    if temporary {
        *buffer.get_mut(cursor).ok_or(PathIndexError::AllocationFailed)? = b'.';
        cursor += 1;
        let decimal = digits.get(start..).ok_or(PathIndexError::AllocationFailed)?;
        buffer.get_mut(cursor..cursor + decimal.len()).ok_or(PathIndexError::AllocationFailed)?
            .copy_from_slice(decimal);
        cursor += decimal.len();
    }
    buffer.get_mut(cursor..length).ok_or(PathIndexError::AllocationFailed)?.copy_from_slice(suffix);
    meter.record(delta)?;
    Ok(length)
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn cache_leaf(buffer: &[u8]) -> Result<&std::path::Path, PathIndexError> {
    Ok(std::path::Path::new(std::str::from_utf8(buffer)
        .map_err(|_| PathIndexError::AllocationFailed)?))
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn evict_native_entries(
    state: &mut NativePathIndexState, directory: &cap_std::fs::Dir,
    meter: &mut CacheHostWork<'_>,
) -> Result<(), PathIndexError> {
    while state.entries.len() > MAXIMUM_PATH_INDEX_ENTRIES
        || state.retained_bytes > MAXIMUM_PATH_INDEX_RETAINED_BYTES
    {
        let mut oldest = None;
        for (index, &(key, _)) in state.entries.iter().enumerate() {
            cache_check(meter.cancellation)?;
            meter.work.charge_items(1, &meter.budget)?;
            if oldest.is_none_or(|(_, candidate)| key < candidate) {
                oldest = Some((index, key));
            }
        }
        let Some((index, oldest)) = oldest else { break; };
        let mut buffer = [0_u8; 85];
        let length = cache_name(oldest, false, &mut buffer, meter)?;
        let leaf = cache_leaf(buffer.get(..length).ok_or(PathIndexError::AllocationFailed)?)?;
        // A failed eviction remains a disposable disk entry, as before.
        cache_io_miss(crate::native_host::remove_leaf_with_work(directory, leaf, meter))?;
        cache_move::<([u8; 32], usize)>(
            usize::from(index + 1 != state.entries.len()),
            meter.work, meter.budget, meter.cancellation)?;
        let (_, bytes) = state.entries.swap_remove(index);
        state.retained_bytes -= bytes;
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
                    Ok(index) => {
                        if let Some(previous) = state.entries.get_mut(index) {
                            state.retained_bytes -= previous.1;
                            previous.1 = bytes;
                        }
                    }
                    Err(index) => state.entries.insert(index, (key, bytes)),
                }
                state.retained_bytes = state.retained_bytes.saturating_add(bytes);
            }
        }
        let index = Self {
            root,
            state: Mutex::new(state),
        };
        index.enforce_bounds();
        if let Ok(mut state) = index.state.lock() {
            state.entries.shrink_to_fit();
        }
        index
    }

    fn enforce_bounds(&self) {
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let token = CancellationToken::new();
        let _ = self.enforce_bounds_measured(&mut ledger, &mut work,
            WorkBudget::UNBOUNDED, &token);
    }

    fn enforce_bounds_measured(
        &self, ledger: &mut AllocationLedger, work: &mut WorkCounters,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<(), PathIndexError> {
        cache_check(cancellation)?;
        let Ok(mut state) = self.state.lock() else { return Ok(()); };
        let retained = state.owned_bytes()?.checked_add(self.root.capacity() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        ledger.claim_bytes(retained, 0, work, budget)?;
        let result = (|| {
            let mut meter = CacheHostWork { ledger, work, budget, cancellation };
            let directory = crate::native_host::open_root_directory_with_work(&self.root, &mut meter)?;
            evict_native_entries(&mut state, &directory, &mut meter)
        })();
        ledger.release(state.owned_bytes()?.checked_add(self.root.capacity() as u64)
            .ok_or(crate::WorkError::Overflow)?)?;
        cache_io_miss(result)
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl GenerationPathIndex for NativeGenerationPathIndex {
    fn load_measured(
        &self, key: [u8; 32], ledger: &mut AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<Option<Vec<u8>>, PathIndexError> {
        use crate::native_host::NativeHostWork as _;
        use std::io::Read as _;
        cache_check(cancellation)?;
        let root_bytes = self.root.capacity() as u64;
        ledger.claim_bytes(root_bytes, 0, work, budget)?;
        let result = (|| {
            let mut meter = CacheHostWork { ledger, work, budget, cancellation };
            let directory = crate::native_host::open_root_directory_with_work(&self.root, &mut meter)?;
            let mut name = [0_u8; 85];
            let length = cache_name(key, false, &mut name, &mut meter)?;
            let leaf = cache_leaf(name.get(..length).ok_or(PathIndexError::AllocationFailed)?)?;
            let mut file = crate::native_host::open_leaf_with_work(&directory, leaf,
                &crate::native_host::NativeOpenOptions::Read, &mut meter)?;
            let metadata = cache_host_attempt(&mut meter, WorkCounters {
                backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || file.metadata())?;
            let Ok(length) = usize::try_from(metadata.len()) else { return Ok(None); };
            if length > MAXIMUM_PATH_INDEX_VALUE_BYTES { return Ok(None); }
            meter.check()?;
            let owned = meter.ledger.claim_elements::<u8>(length, meter.work, meter.budget)?;
            let read = (|| {
                let mut bytes = Vec::new();
                bytes.try_reserve_exact(length).map_err(|_| PathIndexError::AllocationFailed)?;
                bytes.resize(length, 0);
                let mut consumed = 0;
                while consumed < length {
                    let remaining = bytes.get_mut(consumed..).ok_or(PathIndexError::AllocationFailed)?;
                    meter.admit(WorkCounters { backend_read_operations: 1,
                        source_bytes_read: remaining.len() as u64, ..WorkCounters::UNCHARGED })?;
                    meter.record(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
                    match file.read(remaining) {
                        Ok(0) => { bytes.truncate(consumed); break; }
                        Ok(count) => {
                            meter.record(WorkCounters { source_bytes_read: count as u64,
                                ..WorkCounters::UNCHARGED })?;
                            consumed += count;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                Ok(Some(bytes))
            })();
            if read.is_err() { meter.ledger.release(owned)?; }
            read
        })();
        ledger.release(root_bytes)?;
        match result { Err(PathIndexError::Io(_)) => Ok(None), result => result }
    }

    fn store_measured(
        &self, key: [u8; 32], value: &[u8], ledger: &mut AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<(), PathIndexError> {
        use crate::native_host::NativeHostWork as _;
        use std::io::Write as _;
        cache_check(cancellation)?;
        if value.len() > MAXIMUM_PATH_INDEX_VALUE_BYTES { return Ok(()); }
        let Ok(mut state) = self.state.lock() else { return Ok(()); };
        let root_bytes = self.root.capacity() as u64;
        ledger.claim_bytes(state.owned_bytes()?.checked_add(root_bytes)
            .ok_or(crate::WorkError::Overflow)?, 0, work, budget)?;
        let result = (|| {
            cache_reserve(&mut state.entries, MAXIMUM_PATH_INDEX_ENTRIES + 1,
                ledger, work, budget, cancellation)?;
            let mut meter = CacheHostWork { ledger, work, budget, cancellation };
            let directory = match crate::native_host::open_root_directory_with_work(&self.root, &mut meter) {
                Ok(directory) => directory,
                Err(PathIndexError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    crate::native_host::create_directories_with_work(&self.root, &mut meter)?;
                    crate::native_host::open_root_directory_with_work(&self.root, &mut meter)?
                }
                Err(error) => return Err(error),
            };
            // Complete an earlier interrupted eviction before another entry
            // can consume the reserved final slot.
            evict_native_entries(&mut state, &directory, &mut meter)?;
            let found = state.find_measured(key, &mut meter)?;
            let mut name = [0_u8; 85];
            let mut temporary = [0_u8; 85];
            let name_length = cache_name(key, false, &mut name, &mut meter)?;
            let temporary_length = cache_name(key, true, &mut temporary, &mut meter)?;
            let leaf = cache_leaf(name.get(..name_length).ok_or(PathIndexError::AllocationFailed)?)?;
            let temporary_leaf = cache_leaf(temporary.get(..temporary_length)
                .ok_or(PathIndexError::AllocationFailed)?)?;
            let stored = (|| {
                let mut file = crate::native_host::open_leaf_with_work(&directory, temporary_leaf,
                    &crate::native_host::NativeOpenOptions::ReadWriteCreate, &mut meter)?;
                cache_host_attempt(&mut meter, WorkCounters {
                    backend_write_operations: 1, ..WorkCounters::UNCHARGED }, || file.set_len(0))?;
                let mut remaining = value;
                while !remaining.is_empty() {
                    meter.admit(WorkCounters { backend_write_operations: 1,
                        output_bytes: remaining.len() as u64, ..WorkCounters::UNCHARGED })?;
                    meter.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
                    match file.write(remaining) {
                        Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
                        Ok(count) => {
                            meter.record(WorkCounters { output_bytes: count as u64,
                                ..WorkCounters::UNCHARGED })?;
                            remaining = remaining.get(count..).ok_or(PathIndexError::AllocationFailed)?;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                drop(file);
                cache_io_miss(crate::native_host::remove_leaf_with_work(&directory, leaf, &mut meter))?;
                crate::native_host::rename_no_replace_held_with_work(&directory, temporary_leaf,
                    &directory, leaf, &mut meter)
            })();
            match stored {
                Err(error @ (PathIndexError::Work(_) | PathIndexError::Cancelled
                    | PathIndexError::AllocationFailed)) => return Err(error),
                Err(PathIndexError::Io(_)) => {
                    cache_io_miss(crate::native_host::remove_leaf_with_work(
                        &directory, temporary_leaf, &mut meter))?;
                    return Ok(());
                }
                Ok(()) => {}
            }
            match found {
                Some(index) => {
                    let previous = state.entries.get_mut(index).ok_or(PathIndexError::AllocationFailed)?;
                    let previous_bytes = previous.1;
                    previous.1 = value.len();
                    state.retained_bytes -= previous_bytes;
                }
                None => state.entries.push((key, value.len())),
            }
            state.retained_bytes += value.len();
            evict_native_entries(&mut state, &directory, &mut meter)
        })();
        ledger.release(state.owned_bytes()?.checked_add(root_bytes)
            .ok_or(crate::WorkError::Overflow)?)?;
        cache_io_miss(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let bytes = encode_records_measured(generation, &file_ids, &records,
            &mut AllocationLedger::default(), &mut WorkCounters::default(),
            WorkBudget::UNBOUNDED, &CancellationToken::new()).expect("encode").expect("cacheable");
        (generation, file_ids, bytes)
    }

    #[test]
    fn measured_codec_nested_capacity_denial_releases_only_operation_owned_buffers() {
        let (generation, file_ids, bytes) = fixture_bytes();
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        ledger.claim_bytes(bytes.len() as u64, 0, &mut work, WorkBudget::UNBOUNDED).expect("input");
        assert!(matches!(GenerationPathQuery::decode_measured(&bytes, generation, &file_ids,
            VolumeLimits::default(), &mut ledger, &mut work,
            WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &token),
            Err(PathIndexError::Work(_))));
        assert_eq!(work.allocation_operations, 1);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(ledger.live_bytes(), bytes.len() as u64);
        let (generation, file_ids, records) = codec_fixture();
        let mut work = WorkCounters::default();
        let mut output_ledger = AllocationLedger::default();
        assert!(matches!(encode_records_measured(generation, &file_ids, &records,
            &mut output_ledger, &mut work,
            WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED }, &token),
            Err(PathIndexError::Work(_))));
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
        let bytes = encode_records_measured(generation, &file_ids, &records,
            &mut ledger, &mut work, WorkBudget::UNBOUNDED, &token).expect("encode").expect("cacheable");
        let wire: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
        assert_eq!(wire["version"], 1);
        assert_eq!(wire["entries"][0]["paths"][0][0]["bytes"], serde_json::json!([255, 120]));
        assert_eq!(wire["entries"][0]["paths"][0][1]["bytes"], serde_json::json!([0, 216]));
        assert_eq!(wire["entries"][1]["paths"], serde_json::json!([]));
        let encode_budget = work;
        assert!(encode_budget.has_finite_allowances());
        let mut finite_work = WorkCounters::default();
        let finite = encode_records_measured(generation, &file_ids, &records,
            &mut AllocationLedger::default(), &mut finite_work, encode_budget, &token)
            .expect("finite encode").expect("cacheable");
        assert_eq!(finite, bytes);
        assert_eq!(finite_work, encode_budget);
        let baseline = work;
        let decoded = GenerationPathQuery::decode_measured(&bytes, generation, &file_ids,
            VolumeLimits::default(), &mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)
            .expect("decode").expect("valid");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1, records[0].1[0].0);
        let retained = (decoded.capacity() * std::mem::size_of::<(FileId, NamespacePath)>()) as u64
            + decoded.iter().map(|(_, path)| path.retained_bytes()).sum::<u64>();
        assert_eq!(ledger.live_bytes(), bytes.capacity() as u64 + retained);
        let mut finite_ledger = AllocationLedger::default();
        let mut finite_work = baseline;
        finite_ledger.claim_bytes(bytes.capacity() as u64, 0, &mut finite_work, work).expect("caller bytes");
        let finite = GenerationPathQuery::decode_measured(&bytes, generation, &file_ids,
            VolumeLimits::default(), &mut finite_ledger, &mut finite_work, work, &token)
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
        assert!(matches!(GenerationPathQuery::decode_measured(&bytes, generation, &file_ids,
            VolumeLimits::default(), &mut ledger, &mut work, budget, &token), Err(PathIndexError::Work(_))));
        assert_eq!(work.allocation_operations, 0);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(ledger.live_bytes(), bytes.len() as u64);
        let (generation, file_ids, records) = codec_fixture();
        let mut work = WorkCounters::default();
        assert!(matches!(encode_records_measured(generation, &file_ids, &records,
            &mut AllocationLedger::default(), &mut work,
            WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &token),
            Err(PathIndexError::Work(_))));
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
            assert!(GenerationPathQuery::decode_measured(invalid, generation, &file_ids,
                VolumeLimits::default(), &mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)
                .expect("invalid miss").is_none());
            assert_eq!(ledger.live_bytes(), 0);
        }
        for limits in [
            VolumeLimits { maximum_path_depth: 1, ..VolumeLimits::default() },
            VolumeLimits { maximum_component_bytes: 1, ..VolumeLimits::default() },
            VolumeLimits { maximum_path_bytes: 5, ..VolumeLimits::default() },
        ] {
            assert!(GenerationPathQuery::decode_measured(&bytes, generation, &file_ids, limits,
                &mut ledger, &mut work, WorkBudget::UNBOUNDED, &token).expect("bounded miss").is_none());
            assert_eq!(ledger.live_bytes(), 0);
        }
        assert!(GenerationPathQuery::decode_measured(&bytes,
            GenerationId::new(crate::Digest::from_bytes([8; 32])), &file_ids,
            VolumeLimits::default(), &mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)
            .expect("generation miss").is_none());
        assert!(GenerationPathQuery::decode_measured(&bytes, generation, &file_ids[..1],
            VolumeLimits::default(), &mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)
            .expect("query miss").is_none());
        let mut invalid_name = bytes.clone();
        let offset = invalid_name.windows(7).position(|chunk| chunk == b"255,120").expect("name");
        invalid_name.splice(offset..offset + 7, b"0,120".iter().copied());
        assert!(GenerationPathQuery::decode_measured(&invalid_name, generation, &file_ids,
            VolumeLimits::default(), &mut ledger, &mut work, WorkBudget::UNBOUNDED, &token)
            .expect("name miss").is_none());
        assert_eq!(ledger.live_bytes(), 0);
    }

    #[test]
    fn measured_codec_cancellation_and_denial_preserve_actual_work_prefix() {
        let (generation, file_ids, bytes) = fixture_bytes();
        let token = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        assert!(matches!(GenerationPathQuery::decode_measured(&bytes, generation, &file_ids,
            VolumeLimits::default(), &mut ledger, &mut work,
            WorkBudget { items_examined: 8, ..WorkBudget::UNBOUNDED }, &token), Err(PathIndexError::Work(_))));
        assert_eq!(work.items_examined, 8);
        assert!(work.bytes_hashed > 0);
        assert_eq!(ledger.live_bytes(), 0);
        token.cancel();
        let mut work = WorkCounters::default();
        assert!(matches!(GenerationPathQuery::decode_measured(&bytes, generation, &file_ids,
            VolumeLimits::default(), &mut ledger, &mut work, WorkBudget::UNBOUNDED, &token),
            Err(PathIndexError::Cancelled)));
        assert_eq!(work, WorkCounters::default());
    }

    #[test]
    fn memory_index_has_exact_entry_and_byte_bounds() {
        let index = MemoryGenerationPathIndex::default();
        for ordinal in 0..=MAXIMUM_PATH_INDEX_ENTRIES {
            let mut key = [0_u8; 32];
            key[..8].copy_from_slice(&u64::try_from(ordinal).expect("ordinal").to_be_bytes());
            index.store(key, b"value");
        }
        let state = index.state.lock().expect("path index");
        assert_eq!(state.entries.len(), MAXIMUM_PATH_INDEX_ENTRIES);
        assert_eq!(state.retained_bytes, MAXIMUM_PATH_INDEX_ENTRIES * 5);
        assert!(!state.entries.iter().any(|(key, _)| *key == [0; 32]));
        drop(state);

        index.store([0xff; 32], &vec![0; MAXIMUM_PATH_INDEX_VALUE_BYTES + 1]);
        assert!(index.load([0xff; 32]).is_none());
    }

    #[test]
    fn measured_memory_copy_preserves_caller_and_cache_overlap() {
        let index = MemoryGenerationPathIndex::default();
        index.store([1; 32], b"cached");
        let cache_bytes = index.state.lock().expect("cache").owned_bytes().expect("owned");
        let token = CancellationToken::new();
        let mut work = WorkCounters::default();
        let mut ledger = AllocationLedger::default();
        ledger.claim_bytes(11, 0, &mut work, WorkBudget::UNBOUNDED).expect("caller");
        let budget = WorkBudget { peak_allocation_bytes: cache_bytes + 11 + 6,
            ..WorkBudget::UNBOUNDED };
        let loaded = index.load_measured([1; 32], &mut ledger, &mut work, budget, &token)
            .expect("finite load").expect("cached");
        assert_eq!(loaded, b"cached");
        assert_eq!(work.bytes_copied, 6);
        assert_eq!(work.peak_allocation_bytes, cache_bytes + 17);
        assert_eq!(ledger.live_bytes(), 17);
        ledger.release(loaded.capacity() as u64).expect("release returned bytes");
        assert_eq!(ledger.live_bytes(), 11);

        let mut denied_work = WorkCounters::default();
        let denied = WorkBudget { peak_allocation_bytes: cache_bytes + 16, ..budget };
        assert!(matches!(index.load_measured([1; 32], &mut ledger,
            &mut denied_work, denied, &token), Err(PathIndexError::Work(_))));
        assert_eq!(denied_work.bytes_copied, 0);
        assert_eq!(ledger.live_bytes(), 11);
        assert_eq!(index.load([1; 32]).expect("retained entry"), b"cached");
    }

    #[test]
    fn measured_memory_store_rejects_copy_before_retained_mutation() {
        let index = MemoryGenerationPathIndex::default();
        index.store([1; 32], b"original");
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let token = CancellationToken::new();
        let budget = WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED };
        assert!(matches!(index.store_measured([1; 32], b"replacement",
            &mut ledger, &mut work, budget, &token), Err(PathIndexError::Work(_))));
        assert_eq!(ledger.live_bytes(), 0);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(index.load([1; 32]).expect("original retained"), b"original");
        token.cancel();
        let mut cancelled_work = WorkCounters::default();
        assert!(matches!(index.load_measured([1; 32], &mut ledger,
            &mut cancelled_work, WorkBudget::UNBOUNDED, &token),
            Err(PathIndexError::Cancelled)));
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
        index.store_measured([7; 32], b"cached", &mut ledger,
            &mut stored_work, budget, &token).expect("finite native store");
        assert_eq!(stored_work.output_bytes, 6);
        assert!(stored_work.backend_write_operations > 0);
        assert!(stored_work.allocation_operations > 0);
        assert_eq!(ledger.live_bytes(), 0);
        stored_work.verify(budget).expect("finite store receipt");

        let mut measured = WorkCounters::default();
        let bytes = index.load_measured([7; 32], &mut ledger,
            &mut measured, budget, &token).expect("finite read").expect("warm cache");
        assert_eq!(bytes, b"cached");
        assert_eq!(measured.source_bytes_read, 6);
        assert!(measured.backend_read_operations > 0);
        assert_eq!(ledger.live_bytes(), bytes.capacity() as u64);
        ledger.release(bytes.capacity() as u64).expect("return ownership");
        let mut replayed = WorkCounters::default();
        let replay = index.load_measured([7; 32], &mut ledger,
            &mut replayed, measured, &token).expect("exact finite read").expect("warm replay");
        assert_eq!(replay, bytes);
        assert_eq!(replayed, measured);
        ledger.release(replay.capacity() as u64).expect("replay ownership");

        let denied = WorkBudget { source_bytes_read: 5, ..budget };
        let mut denied_work = WorkCounters::default();
        assert!(matches!(index.load_measured([7; 32], &mut ledger,
            &mut denied_work, denied, &token), Err(PathIndexError::Work(_))));
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
        assert!(index.load_measured([3; 32], &mut ledger,
            &mut work, budget, &token).expect("missing cache").is_none());
        assert!(work.backend_read_operations > 0);
        assert_eq!(ledger.live_bytes(), 0);

        let denied = WorkBudget { backend_read_operations: 0, ..budget };
        let mut work = WorkCounters::default();
        assert!(matches!(index.load_measured([3; 32], &mut ledger,
            &mut work, denied, &token), Err(PathIndexError::Work(_))));
        assert_eq!(work.backend_read_operations, 0);
        assert_eq!(ledger.live_bytes(), 0);
        token.cancel();
        let mut work = WorkCounters::default();
        assert!(matches!(index.store_measured([3; 32], b"value", &mut ledger,
            &mut work, budget, &token), Err(PathIndexError::Cancelled)));
        assert_eq!(work, WorkCounters::default());
        assert!(!directory.path().join("missing").exists());
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[test]
    fn measured_native_write_denial_preserves_previous_cache_entry() {
        let directory = tempfile::tempdir().expect("cache directory");
        let index = NativeGenerationPathIndex::new(directory.path().to_path_buf());
        index.store([8; 32], b"original");
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let token = CancellationToken::new();
        let budget = WorkBudget { output_bytes: 0, ..finite_provider_budget() };
        assert!(matches!(index.store_measured([8; 32], b"replacement", &mut ledger,
            &mut work, budget, &token), Err(PathIndexError::Work(_))));
        assert_eq!(work.output_bytes, 0);
        assert_eq!(ledger.live_bytes(), 0);
        assert_eq!(index.load([8; 32]).expect("original remains"), b"original");
        index.store([8; 32], b"replacement");
        assert_eq!(index.load([8; 32]).expect("scratch reused"), b"replacement");
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
            index.store_measured(key, b"value", &mut ledger,
                &mut work, budget, &token).expect("finite bounded insertion");
            assert_eq!(ledger.live_bytes(), 0);
            work.verify(budget).expect("finite insertion receipt");
        }
        let state = index.state.lock().expect("cache state");
        assert_eq!(state.entries.len(), MAXIMUM_PATH_INDEX_ENTRIES);
        assert_eq!(state.retained_bytes, MAXIMUM_PATH_INDEX_ENTRIES * 5);
        assert!(!state.entries.iter().any(|(key, _)| *key == [0; 32]));
        drop(state);
        assert!(index.load([0; 32]).is_none());
        assert_eq!(std::fs::read_dir(directory.path()).expect("native entries")
            .count(), MAXIMUM_PATH_INDEX_ENTRIES);
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
        let mut meter = CacheHostWork { ledger: &mut ledger, work: &mut work,
            budget, cancellation: &token };
        let cleanup = meter.reserve_cleanup_write().expect("reserve restoration");
        assert_eq!(meter.work.backend_write_operations, 0);
        assert_eq!(meter.budget.backend_write_operations, 1);
        cache_host_attempt(&mut meter, WorkCounters { backend_write_operations: 1,
            ..WorkCounters::UNCHARGED }, || file.set_len(8)).expect("actual mutation");
        assert!(matches!(meter.admit(WorkCounters { backend_write_operations: 1,
            ..WorkCounters::UNCHARGED }), Err(PathIndexError::Work(_))));
        token.cancel();
        let (restored, recorded) = meter.attempt_cleanup_write(cleanup, || file.set_len(0));
        restored.expect("mandatory physical restoration");
        recorded.expect("admitted restoration receipt");
        assert_eq!(meter.work.backend_write_operations, 2);
        assert_eq!(meter.budget.backend_write_operations, 2);
        assert_eq!(file.metadata().expect("restored file").len(), 0);
        meter.work.verify(budget).expect("exact cleanup receipt");
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
        let mut meter = CacheHostWork { ledger: &mut ledger, work: &mut work,
            budget, cancellation: &token };
        let cleanup = meter.reserve_cleanup_write().expect("reserve write");
        assert_eq!(meter.budget.backend_write_operations, 0);
        meter.cancel_cleanup_write(cleanup);
        assert_eq!(meter.work.backend_write_operations, 0);
        cache_host_attempt(&mut meter, WorkCounters { backend_write_operations: 1,
            ..WorkCounters::UNCHARGED }, || file.set_len(3)).expect("released write allowance");
        assert_eq!(meter.work.backend_write_operations, 1);
        assert_eq!(file.metadata().expect("actual write").len(), 3);
    }
}
