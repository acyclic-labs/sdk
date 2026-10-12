//! Private v1 state deltas. Bodies remain authenticated physical references.
use super::super::response;
use super::*;
use crate::LocalObjectsLimits;
use crate::body::{LocalBodyLocation};
use crate::physical::native_durability;
use fs2::FileExt;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

const MAGIC: &[u8] = b"ACYCLIC-OBJECTS-V1-LOCAL\0\x01";
pub(super) const RECORD_LIMIT: usize = 2 * 1024 * 1024;
const MINIMUM_MAINTENANCE_INLINE_BYTES: u64 = 8 * 1024 * 1024;

/// Current invocation authority and custody for temporary publication controls.
pub(crate) struct Publication {
    pub(crate) native: Option<(Arc<dyn NativeReadMeter>, ReadAllocation)>,
}
impl Publication {
    pub(crate) fn new(meter: Option<Arc<dyn NativeReadMeter>>) -> Self {
        Self { native: meter.map(|meter| {
            let retained = ReadAllocation::empty(&meter);
            (meter, retained)
        }) }
    }
    pub(crate) fn check(&self) -> Result<(), NativeObjectsError> {
        if let Some((meter, _)) = &self.native { meter.check()?; }
        Ok(())
    }
    pub(crate) fn allocate(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((meter, retained)) = &mut self.native {
            let mut claim = ReadAllocation::new(meter, bytes)?;
            retained.absorb(&mut claim)?;
        }
        Ok(())
    }
    pub(crate) fn retain(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((meter, retained)) = &mut self.native {
            let mut claim = ReadAllocation::adopt(meter, bytes)?;
            retained.absorb(&mut claim)?;
        }
        Ok(())
    }
    pub(crate) fn retain_capacity(&mut self, requested: usize, actual: usize) -> Result<(), NativeObjectsError> {
        if let Some((_, retained)) = &mut self.native {
            let total = usize::try_from(retained.bytes()).map_err(|_| Error::from(QuotaExceeded))?;
            retained.reconcile_capacity(total.checked_sub(requested).and_then(|bytes| bytes.checked_add(actual)).ok_or(Error::from(QuotaExceeded))?)?;
        }
        Ok(())
    }
    pub(crate) fn allocation_failed(&self, error: std::collections::TryReserveError) -> NativeObjectsError {
        match &self.native {
            Some((meter, _)) => NativeObjectsError::from(NativeObjectsCause::Admission(meter.allocation_failed(error))),
            None => NativeObjectsError::from(NativeObjectsCause::Service(Unavailable.into())),
        }
    }
    pub(crate) fn retire(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((_, retained)) = &mut self.native { retained.release(bytes)?; }
        Ok(())
    }
    pub(crate) fn reserve<T>(&mut self, value: &mut Vec<T>, additional: usize) -> Result<(), NativeObjectsError> {
        let needed = value.len().checked_add(additional).ok_or(Error::from(QuotaExceeded))?;
        if needed <= value.capacity() { return self.check(); }
        let width = std::mem::size_of::<T>();
        let old = value.capacity().checked_mul(width).ok_or(Error::from(QuotaExceeded))?;
        let requested = needed.checked_mul(width).ok_or(Error::from(QuotaExceeded))?;
        let mut claim = self.native.as_ref().map(|(meter, _)| ReadAllocation::new(meter, requested)).transpose()?;
        if let Err(error) = value.try_reserve_exact(additional) {
            return Err(self.allocation_failed(error));
        }
        if let (Some(claim), Some((_, retained))) = (&mut claim, &mut self.native) {
            let reconciled = claim.reconcile_capacity(value.capacity().checked_mul(width).ok_or(Error::from(QuotaExceeded))?);
            retained.resize_retained(usize::try_from(retained.bytes()).map_err(|_| Error::from(QuotaExceeded))?.saturating_sub(old))?;
            retained.absorb(claim)?;
            reconciled?;
        }
        Ok(())
    }
    pub(crate) fn push<T>(&mut self, value: &mut Vec<T>, item: T) -> Result<(), NativeObjectsError> {
        if value.len() == value.capacity() {
            self.reserve(value, value.capacity().max(1))?;
        } else {
            self.check()?;
        }
        value.push(item);
        Ok(())
    }
    pub(crate) fn codec<T>(&self, encoded: usize, copied: usize, hashed: usize, work: impl FnOnce() -> T) -> Result<T, NativeObjectsError> {
        if let Some((meter, _)) = &self.native {
            meter.admit_codec(encoded as u64, copied as u64, hashed as u64)?;
        }
        let value = work();
        if let Some((meter, _)) = &self.native {
            meter.record_codec(encoded as u64, copied as u64, hashed as u64)?;
        }
        Ok(value)
    }
    pub(crate) fn text(&mut self, value: &str) -> Result<String, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_text(value, meter, retained),
            _ => Ok(value.to_owned()),
        }
    }
    fn info(&mut self, value: &wire::ObjectInfo) -> Result<wire::ObjectInfo, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_info(value, meter, retained),
            _ => Ok(value.clone()),
        }
    }
    fn metadata(&mut self, value: &wire::ObjectMetadata) -> Result<wire::ObjectMetadata, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_metadata(value, meter, retained),
            _ => Ok(value.clone()),
        }
    }
    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<Vec<u8>, NativeObjectsError> {
        let mut output = Vec::new();
        self.reserve(&mut output, value.len())?;
        self.codec(0, value.len(), 0, || output.extend_from_slice(value))?;
        Ok(output)
    }
    pub(crate) fn encode<M: prost::Message>(&mut self, value: &M) -> Result<Vec<u8>, NativeObjectsError> {
        let length = value.encoded_len();
        let mut output = Vec::new();
        self.reserve(&mut output, length)?;
        self.codec(length, 0, 0, || value.encode(&mut output))?
            .map_err(|_| NativeObjectsError::from(NativeObjectsCause::Service(Unavailable.into())))?;
        Ok(output)
    }

    fn relocated(
        &mut self, body: &StoredBody, moves: &crate::body::LocalBodyRelocations,
    ) -> Result<Option<StoredBody>, NativeObjectsError> {
        self.check()?;
        match body {
            StoredBody::Memory(_) => Ok(None),
            StoredBody::Composite { parts, length } => {
                if !has_relocation(body, moves) { return Ok(None); }
                let mut moved = Vec::new();
                self.reserve(&mut moved, parts.value.len())?;
                for part in &parts.value {
                    let relocated = self.relocated(part, moves)?;
                    self.codec(0, std::mem::size_of::<StoredBody>(), 0, || {
                        moved.push(relocated.unwrap_or_else(|| part.clone()));
                    })?;
                }
                composite_publication(moved, *length, self).map(Some)
            }
            StoredBody::Local { root, digest, length, location } => {
                Ok(moves.get(&(location.clone(), *digest)).map(|location| StoredBody::Local {
                    root: Arc::clone(root), digest: *digest, length: *length, location: location.clone(),
                }))
            }
        }
    }
}

pub(super) fn service_error(error: NativeObjectsError) -> Error {
    match error {
        error if !error.has_payload() => match error.into_cause() {
            NativeObjectsCause::Service(error) => error,
            _ => Unavailable.into(),
        },
        _ => Unavailable.into(),
    }
}

fn open_error(error: NativeObjectsError) -> LocalOpenError {
    match error {
        error if !error.has_payload() => match error.into_cause() {
            NativeObjectsCause::Admission(error) => LocalOpenError::Io(error),
            _ => LocalOpenError::Unavailable,
        },
        _ => LocalOpenError::Unavailable,
    }
}

fn composite_publication(parts: Vec<StoredBody>, length: usize, publication: &mut Publication) -> Result<StoredBody, NativeObjectsError> {
    match &mut publication.native {
        Some((meter, retained)) => {
            let capacity = parts.capacity().checked_mul(std::mem::size_of::<StoredBody>()).ok_or(Error::from(QuotaExceeded))?;
            let allocation = retained.split(capacity)?;
            StoredBody::composite_measured(parts, length, meter, allocation)
        }
        None => Ok(StoredBody::Composite { parts: Arc::new(crate::v1::Guarded { value: parts, _allocation: None }), length }),
    }
}

fn has_relocation(body: &StoredBody, moves: &crate::body::LocalBodyRelocations) -> bool {
    match body {
        StoredBody::Memory(_) => false,
        StoredBody::Composite { parts, .. } => parts.value.iter().any(|part| has_relocation(part, moves)),
        StoredBody::Local { location, digest, .. } => moves.contains_key(&(location.clone(), *digest)),
    }
}

fn unpersisted(body: &StoredBody) -> bool {
    match body {
        StoredBody::Memory(_) => true,
        StoredBody::Composite { parts, .. } => parts.value.iter().any(unpersisted),
        StoredBody::Local { .. } => false,
    }
}

/// Failure to open or recover logical Objects storage.
#[derive(Debug, thiserror::Error)]
pub enum LocalOpenError {
    /// Invalid capacity or durability configuration.
    #[error("invalid local Objects v1 configuration")]
    Invalid,
    /// A complete durable record or referenced body is corrupt.
    #[error("corrupt local Objects v1 store")]
    Corrupt,
    /// Another owner has this exact root open.
    #[error("local Objects v1 store already has an owner")]
    AlreadyOwned,
    /// A native worker or cursor entropy source could not complete.
    #[error("local Objects v1 unavailable")]
    Unavailable,
    /// Host I/O failed.
    #[error("local Objects v1 I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, PartialEq, prost::Message)]
struct Header {
    #[prost(uint64, tag = "1")]
    object_bytes: u64,
    #[prost(uint64, tag = "2")]
    total_bytes: u64,
    #[prost(uint64, tag = "3")]
    operations: u64,
    #[prost(uint64, tag = "4")]
    journal_bytes: u64,
    #[prost(bytes = "vec", tag = "5")]
    cursor_key: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Delta {
    #[prost(uint64, tag = "1")]
    sequence: u64,
    #[prost(uint64, tag = "2")]
    ordinal: u64,
    #[prost(message, repeated, tag = "3")]
    buckets: Vec<BucketChange>,
    #[prost(message, repeated, tag = "4")]
    objects: Vec<ObjectChange>,
    #[prost(message, repeated, tag = "5")]
    uploads: Vec<UploadChange>,
    #[prost(message, repeated, tag = "6")]
    parts: Vec<PartChange>,
    #[prost(message, repeated, tag = "7")]
    receipts: Vec<ReceiptChange>,
    #[prost(bytes = "vec", tag = "8")]
    inline_data: Vec<u8>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct BucketChange {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, optional, tag = "2")]
    info: Option<wire::Bucket>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct ObjectChange {
    #[prost(string, tag = "1")]
    bucket: String,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(message, optional, tag = "3")]
    info: Option<wire::ObjectInfo>,
    #[prost(message, repeated, tag = "4")]
    bodies: Vec<BodyRecord>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct UploadChange {
    #[prost(string, tag = "1")]
    id: String,
    #[prost(bool, tag = "2")]
    deleted: bool,
    #[prost(string, tag = "3")]
    bucket: String,
    #[prost(string, tag = "4")]
    key: String,
    #[prost(message, optional, tag = "5")]
    metadata: Option<wire::ObjectMetadata>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct PartChange {
    #[prost(string, tag = "1")]
    upload: String,
    #[prost(uint32, tag = "2")]
    number: u32,
    #[prost(message, optional, tag = "3")]
    receipt: Option<wire::UploadedPart>,
    #[prost(message, repeated, tag = "4")]
    bodies: Vec<BodyRecord>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct ReceiptChange {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(bytes = "vec", tag = "2")]
    digest: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    response: Vec<u8>,
    #[prost(uint32, tag = "4")]
    kind: u32,
}
#[derive(Clone, PartialEq, prost::Message)]
struct BodyRecord {
    #[prost(bytes = "vec", tag = "1")]
    segment: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    digest: Vec<u8>,
    #[prost(uint64, tag = "3")]
    length: u64,
    #[prost(fixed64, tag = "4")]
    offset: u64,
    #[prost(bool, tag = "5")]
    journal: bool,
}

struct Tail {
    file: File,
    bytes: u64,
    operations: u64,
    inline_bytes: u64,
}
pub(super) struct Journal {
    root: Arc<crate::physical::LocalRoot>,
    tail: Mutex<Tail>,
    limits: LocalObjectsLimits,
    header: Header,
    poisoned: AtomicBool,
    _owner: File,
    _anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
    #[cfg(test)]
    fault_bytes: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    fault_sync: AtomicBool,
    #[cfg(test)]
    fault_checkpoint: std::sync::atomic::AtomicU64,
}

impl Drop for Journal {
    fn drop(&mut self) {
        // Unlock explicitly despite inherited child descriptors.
        let _ = FileExt::unlock(&self._owner);
    }
}

mod checkpoint;
mod inline;

#[cfg(test)]
mod tests;

pub(super) fn response_kind<R>() -> Result<u32, Error> {
    match std::any::type_name::<R>().rsplit("::").next() {
        Some("Bucket") => Ok(1),
        Some("ObjectInfo") => Ok(2),
        Some("DeleteBucketResponse") => Ok(3),
        Some("DeleteObjectResponse") => Ok(4),
        Some("MultipartUpload") => Ok(5),
        Some("UploadedPart") => Ok(6),
        Some("AbortMultipartResponse") => Ok(7),
        _ => Err(Unavailable.into()),
    }
}

/// An upper bound on the bytes one put adds to a durable record: its inline body, plus its object change and retry receipt.
pub(super) fn put_record_bytes(query: &wire::PutObjectHeader, body: &OwnedBytes) -> usize {
    let inline = if body.len() <= inline::LIMIT {
        body.len()
    } else {
        0
    };
    inline
        .saturating_add(query.encoded_len().saturating_mul(3))
        .saturating_add(1024)
}

fn sync(file: &File, limits: LocalObjectsLimits) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(limits.durability), None)
}
fn corrupt<T>(_: T) -> LocalOpenError {
    LocalOpenError::Corrupt
}

impl MemoryObjects {
    pub(in crate::v1) fn local_maintenance_due(&self) -> Result<bool, Error> {
        let _state = self.lock_state()?;
        let journal = self.journal.as_ref().ok_or(Error::from(Unavailable))?;
        journal.maintenance_due()
    }
    pub(in crate::v1) fn compact_local_if_due(&self, meter: Option<Arc<dyn NativeReadMeter>>) -> Result<(), NativeObjectsError> {
        obs::scoped(&obs::span!(DEBUG, "acyclic.objects.compact"), || {
            if let Some(meter) = &meter { meter.check()?; }
            let mut state = self.lock_state()?;
            let journal = self.journal.as_ref().ok_or(Error::from(Unavailable))?;
            if !journal.maintenance_due()? { return Ok(()); }
            if let Some(meter) = &meter { meter.check()?; }
            let mut next = state.clone();
            journal.materialize_inline(&mut next, meter.clone())?;
            journal.compact(&next, meter)?;
            *state = next;
            Ok(())
        })
    }
    pub(in crate::v1) fn collect_local_garbage(
        &self,
        maximum_candidates: u64,
    ) -> Result<crate::LocalObjectsGarbageCollection, LocalOpenError> {
        obs::scoped(
            &obs::span!(
                INFO,
                "acyclic.objects.collect_garbage",
                segments = obs::Empty,
                reclaimed = obs::Empty
            ),
            || {
                let mut state = self.lock_state().map_err(|_| LocalOpenError::Unavailable)?;
                let journal = self.journal.as_ref().ok_or(LocalOpenError::Unavailable)?;
                let mut next = state.clone();
                journal.materialize_inline(&mut next, None).map_err(open_error)?;
                let mut references = BTreeSet::new();
                for bucket in next.buckets.values() {
                    for object in bucket.objects.values() {
                        object.body.local_references(&mut references);
                    }
                }
                for upload in next.uploads.values() {
                    for (_, body) in upload.parts.values() {
                        body.local_references(&mut references);
                    }
                }
                let mut report = crate::physical::collect_physical_garbage(
                    &journal.root.path,
                    &references,
                    maximum_candidates,
                    journal.limits.maximum_object_bytes,
                    journal.limits.durability,
                )
                .map_err(|error| match error {
                    crate::physical::PhysicalError::Invalid(_) => LocalOpenError::Invalid,
                    crate::physical::PhysicalError::Io(error) => LocalOpenError::Io(error),
                    crate::physical::PhysicalError::Corrupt => LocalOpenError::Corrupt,
                })?;
                report.journal_bytes_reclaimed = journal.compact(&next, None).map_err(|error| match error {
                    error if !error.has_payload() && matches!(&error.cause, NativeObjectsCause::Service(error) if error.code == QuotaExceeded) => LocalOpenError::Invalid,
                    error => open_error(error),
                })?;
                *state = next;
                obs::record!(
                    "segments" = report.segments_removed,
                    "reclaimed" = report.journal_bytes_reclaimed,
                );
                Ok(report)
            },
        )
    }
}

pub(crate) fn open(
    root: PathBuf,
    limits: LocalObjectsLimits,
    anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
) -> Result<MemoryObjects, LocalOpenError> {
    let span = obs::span!(
        INFO,
        "acyclic.objects.open",
        frames = obs::Empty,
        bytes = obs::Empty
    );
    obs::scoped(&span, || {
        if limits.maximum_object_bytes == 0
            || limits.maximum_object_bytes > limits.maximum_bytes
            || limits.maximum_journal_operations == 0
            || limits.maximum_journal_bytes < 256
        {
            return Err(LocalOpenError::Invalid);
        }
        let options = MemoryOptions {
            maximum_bytes: usize::try_from(limits.maximum_bytes)
                .map_err(|_| LocalOpenError::Invalid)?,
            maximum_entries: usize::MAX,
        };
        fs::create_dir_all(root.join("segments"))?;
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("owner.lock"))?;
        owner.try_lock_exclusive().map_err(|error| {
            if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
                LocalOpenError::AlreadyOwned
            } else {
                LocalOpenError::Io(error)
            }
        })?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("mutations.log"))?;
        let header = load_header(&mut file, &root, limits, getrandom::fill)?;
        let root = Arc::new(crate::physical::LocalRoot::new(root));
        let (state, operations, inline_bytes) = replay(&mut file, &root, options, limits)?;
        let bytes = file.stream_position()?;
        obs::record!("frames" = operations, "bytes" = bytes);
        let mut references = BTreeSet::new();
        for bucket in state.buckets.values() {
            for object in bucket.objects.values() {
                object.body.local_references(&mut references);
            }
        }
        for upload in state.uploads.values() {
            for (_, body) in upload.parts.values() {
                body.local_references(&mut references);
            }
        }
        crate::physical::validate_referenced_segments(
            &root.path,
            &references,
            limits.maximum_object_bytes,
        )
        .map_err(corrupt)?;
        let key = header.cursor_key.as_slice().try_into().map_err(corrupt)?;
        Ok(MemoryObjects {
            state: Arc::new(Mutex::new(state)),
            options,
            token_key: Arc::new(Mutex::new(Some(key))),
            defer_local: false,
            journal: Some(Arc::new(Journal {
                root,
                limits,
                header,
                tail: Mutex::new(Tail {
                    file,
                    bytes,
                    operations,
                    inline_bytes,
                }),
                poisoned: AtomicBool::new(false),
                _owner: owner,
                _anchor: anchor,
                #[cfg(test)]
                fault_bytes: std::sync::atomic::AtomicU64::new(u64::MAX),
                #[cfg(test)]
                fault_sync: AtomicBool::new(false),
                #[cfg(test)]
                fault_checkpoint: std::sync::atomic::AtomicU64::new(0),
            })),
        })
    })
}

fn load_header(
    file: &mut File,
    root: &Path,
    limits: LocalObjectsLimits,
    fill: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>,
) -> Result<Header, LocalOpenError> {
    if file.metadata()?.len() == 0 {
        let key = fresh_cursor_key(fill).map_err(|_| LocalOpenError::Unavailable)?;
        let header = Header {
            object_bytes: limits.maximum_object_bytes,
            total_bytes: limits.maximum_bytes,
            operations: limits.maximum_journal_operations,
            journal_bytes: limits.maximum_journal_bytes,
            cursor_key: key.to_vec(),
        };
        let encoded = header.encode_to_vec();
        let length = u32::try_from(encoded.len()).map_err(corrupt)?;
        file.write_all(MAGIC)?;
        file.write_all(&length.to_le_bytes())?;
        file.write_all(blake3::hash(&encoded).as_bytes())?;
        file.write_all(&encoded)?;
        sync(file, limits)?;
        acyclic_native_runtime::sync_parent(root, native_durability(limits.durability))?;
        return Ok(header);
    }
    let mut magic = [0; MAGIC.len()];
    file.read_exact(&mut magic).map_err(corrupt)?;
    if magic.as_slice() != MAGIC {
        return Err(LocalOpenError::Corrupt);
    }
    let mut length = [0; 4];
    file.read_exact(&mut length).map_err(corrupt)?;
    let length = u32::from_le_bytes(length) as usize;
    if !(1..=512).contains(&length) {
        return Err(LocalOpenError::Corrupt);
    }
    let mut digest = [0; 32];
    file.read_exact(&mut digest).map_err(corrupt)?;
    let mut encoded = vec![0; length];
    file.read_exact(&mut encoded).map_err(corrupt)?;
    if *blake3::hash(&encoded).as_bytes() != digest {
        return Err(LocalOpenError::Corrupt);
    }
    let header = Header::decode(encoded.as_slice()).map_err(corrupt)?;
    if header.encode_to_vec() != encoded
        || header.cursor_key.len() != 32
        || header.object_bytes != limits.maximum_object_bytes
        || header.total_bytes != limits.maximum_bytes
        || header.operations != limits.maximum_journal_operations
        || header.journal_bytes != limits.maximum_journal_bytes
    {
        return Err(LocalOpenError::Invalid);
    }
    Ok(header)
}

fn replay(
    file: &mut File,
    root: &Arc<crate::physical::LocalRoot>,
    options: MemoryOptions,
    limits: LocalObjectsLimits,
) -> Result<(State, u64, u64), LocalOpenError> {
    obs::scoped(&obs::span!(DEBUG, "acyclic.objects.journal.replay"), || {
        let mut state = State::default();
        let mut operations = 0;
        let mut inline_bytes = 0_u64;
        let mut inline_bodies = BTreeMap::new();
        let end = file.metadata()?.len();
        if end > limits.maximum_journal_bytes {
            return Err(LocalOpenError::Corrupt);
        }
        loop {
            let start = file.stream_position()?;
            if start == end {
                return Ok((state, operations, inline_bytes));
            }
            let mut prefix = [0; 36];
            if let Err(error) = file.read_exact(&mut prefix) {
                return repair_tail(file, start, error, limits)
                    .map(|()| (state, operations, inline_bytes));
            }
            let size = u32::from_le_bytes(
                prefix
                    .get(..4)
                    .ok_or(LocalOpenError::Corrupt)?
                    .try_into()
                    .map_err(corrupt)?,
            ) as usize;
            if !(1..=RECORD_LIMIT).contains(&size) {
                return Err(LocalOpenError::Corrupt);
            }
            let mut encoded = vec![0; size];
            if let Err(error) = file.read_exact(&mut encoded) {
                return repair_tail(file, start, error, limits)
                    .map(|()| (state, operations, inline_bytes));
            }
            if blake3::hash(&encoded).as_bytes().as_slice()
                != prefix.get(4..).ok_or(LocalOpenError::Corrupt)?
            {
                return Err(LocalOpenError::Corrupt);
            }
            let delta = Delta::decode(encoded.as_slice()).map_err(corrupt)?;
            if delta.encode_to_vec() != encoded
                || delta.ordinal != operations + 1
                || operations >= limits.maximum_journal_operations
            {
                return Err(LocalOpenError::Corrupt);
            }
            inline::authenticate(&delta, start, &encoded, &mut inline_bodies)?;
            inline_bytes = inline_bytes.saturating_add(delta.inline_data.len() as u64);
            apply(&mut state, delta, root, options, limits)?;
            operations += 1;
        }
    })
}
fn repair_tail(
    file: &mut File,
    start: u64,
    error: std::io::Error,
    limits: LocalObjectsLimits,
) -> Result<(), LocalOpenError> {
    if error.kind() != std::io::ErrorKind::UnexpectedEof {
        return Err(error.into());
    }
    file.set_len(start)?;
    file.seek(SeekFrom::Start(start))?;
    sync(file, limits)?;
    Ok(())
}

impl Journal {
    fn maintenance_due(&self) -> Result<bool, Error> {
        let tail = self.tail.lock().map_err(|_| Error::from(Unavailable))?;
        Ok(tail.inline_bytes >= MINIMUM_MAINTENANCE_INLINE_BYTES
            && tail.inline_bytes >= tail.bytes.saturating_sub(tail.inline_bytes))
    }
    pub(super) fn validate_bodies(&self, state: &State) -> Result<(), Error> {
        let bodies = state
            .buckets
            .values()
            .flat_map(|bucket| bucket.objects.values())
            .map(|object| &object.body)
            .chain(
                state
                    .uploads
                    .values()
                    .flat_map(|upload| upload.parts.values())
                    .map(|(_, body)| body),
            );
        if bodies
            .into_iter()
            .any(|body| body.len() as u64 > self.limits.maximum_object_bytes)
        {
            return Err(QuotaExceeded.into());
        }
        Ok(())
    }
    pub(super) fn check(&self) -> Result<(), Error> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Unavailable.into())
        } else {
            Ok(())
        }
    }
    pub(super) fn commit(&self, before: &State, next: &mut State, meter: Option<Arc<dyn NativeReadMeter>>) -> Result<(), NativeObjectsError> {
        obs::scoped(
            &obs::span!(DEBUG, "acyclic.objects.journal.commit", bytes = obs::Empty),
            || {
                let mut publication = Publication::new(meter);
                publication.check()?;
                self.check()?;
                let mut tail = self.tail.lock().map_err(|_| Error::from(Unavailable))?;
                if tail.operations >= self.limits.maximum_journal_operations {
                    return Err(Error::from(QuotaExceeded).into());
                }
                let mut inline_data = Vec::new();
                // Only changed objects can hold unpersisted bodies.
                for (name, _) in &next.buckets.clone() {
                    let bucket = next.buckets.get_mut(name).ok_or(Error::from(Unavailable))?;
                    let keys = changed_keys(before.buckets.get(name), bucket, &mut publication)?;
                    for key in keys {
                        let object = bucket
                            .objects
                            .get_mut(&key)
                            .ok_or(Error::from(Unavailable))?;
                        if object.body.len() as u64 > self.limits.maximum_object_bytes {
                            return Err(Error::from(QuotaExceeded).into());
                        }
                        object.body = self.prepare_inline(&object.body, &mut inline_data, &mut publication)?;
                    }
                }
                for (id, _) in &next.uploads.clone() {
                    let upload = next.uploads.get_mut(id).ok_or(Error::from(Unavailable))?;
                    for (number, _) in &upload.parts.clone() {
                        let (_, body) = upload.parts.get_mut(number).ok_or(Error::from(Unavailable))?;
                        *body = self.prepare_inline(body, &mut inline_data, &mut publication)?;
                    }
                }
                let mut delta = difference(before, next, tail.operations + 1, &mut publication)?;
                delta.inline_data = inline_data;
                inline::place(&mut delta, next, tail.bytes, &mut publication)?;
                let record = publication.encode(&delta)?;
                let end = tail
                    .bytes
                    .checked_add(36 + record.len() as u64)
                    .ok_or(Error::from(QuotaExceeded))?;
                if record.len() > RECORD_LIMIT || end > self.limits.maximum_journal_bytes {
                    return Err(Error::from(QuotaExceeded).into());
                }
                let mut frame = Vec::new();
                publication.reserve(&mut frame, 36 + record.len())?;
                let length = u32::try_from(record.len()).map_err(|_| Error::from(QuotaExceeded))?;
                let digest = publication.codec(0, 0, record.len(), || *blake3::hash(&record).as_bytes())?;
                publication.codec(0, 36 + record.len(), 0, || {
                    frame.extend_from_slice(&length.to_le_bytes());
                    frame.extend_from_slice(&digest);
                    frame.extend_from_slice(&record);
                })?;
                obs::record!("bytes" = frame.len());
                let start = tail.bytes;
                if let Err(error) = self.write(&mut tail.file, start, &frame, &mut publication) {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(error);
                }
                tail.bytes = end;
                tail.operations += 1;
                tail.inline_bytes = tail
                    .inline_bytes
                    .saturating_add(delta.inline_data.len() as u64);
                Ok(())
            },
        )
    }
    fn external(&self, body: &StoredBody, publication: &mut Publication) -> Result<StoredBody, NativeObjectsError> {
        if !unpersisted(body) { return Ok(body.clone()); }
        match body {
            StoredBody::Memory(bytes) => {
                if bytes.len() as u64 > self.limits.maximum_object_bytes {
                    return Err(Error::from(QuotaExceeded).into());
                }
                let digest = publication.codec(0, 0, bytes.len(), || *blake3::hash(bytes).as_bytes())?;
                let (id, offsets) = crate::physical::persist_segment(
                    &self.root.path,
                    &[(digest, bytes.as_slice())],
                    self.limits.durability,
                    publication,
                )?;
                Ok(StoredBody::Local {
                    root: Arc::clone(&self.root),
                    digest,
                    length: bytes.len(),
                    location: LocalBodyLocation::Segment {
                        id,
                        offset: *offsets.first().ok_or(Error::from(Unavailable))?,
                    },
                })
            }
            StoredBody::Composite { parts, length } => {
                let mut output = Vec::new();
                publication.reserve(&mut output, parts.value.len())?;
                for part in parts.value.iter() {
                    output.push(self.external(part, publication)?);
                }
                composite_publication(output, *length, publication)
            }
            StoredBody::Local { .. } => Ok(body.clone()),
        }
    }
    fn write(&self, file: &mut File, offset: u64, frame: &[u8], publication: &mut Publication) -> Result<(), NativeObjectsError> {
        obs::scoped(
            &obs::span!(
                DEBUG,
                "acyclic.objects.journal.append",
                bytes = frame.len(),
                fsyncs = 1
            ),
            || {
                #[cfg(test)]
                {
                    let cut = self.fault_bytes.swap(u64::MAX, Ordering::AcqRel);
                    if cut != u64::MAX {
                        let count = usize::try_from(cut).unwrap_or(frame.len()).min(frame.len());
                        crate::physical::write_publication(
                            file, offset,
                            frame.get(..count).ok_or_else(|| std::io::Error::other("invalid fault point"))?,
                            publication,
                        )?;
                        crate::physical::sync_publication(file, self.limits.durability, publication)?;
                        return Err(std::io::Error::other("injected journal write failure").into());
                    }
                }
                crate::physical::write_publication(file, offset, frame, publication)?;
                #[cfg(test)]
                if self.fault_sync.swap(false, Ordering::AcqRel) {
                    return Err(std::io::Error::other("injected journal sync failure").into());
                }
                crate::physical::sync_publication(file, self.limits.durability, publication)
            },
        )
    }
}

fn bodies(body: &StoredBody, publication: &mut Publication) -> Result<Vec<BodyRecord>, NativeObjectsError> {
    fn ordered(body: &StoredBody, out: &mut Vec<BodyRecord>, publication: &mut Publication) -> Result<(), NativeObjectsError> {
        match body {
            StoredBody::Local { digest, length, location, .. } => {
                let (segment, offset, journal) = match location {
                    LocalBodyLocation::Segment { id, offset } => (publication.bytes(id)?, *offset, false),
                    LocalBodyLocation::Journal { offset } => (Vec::new(), *offset, true),
                };
                let digest = publication.bytes(digest)?;
                publication.push(out, BodyRecord { segment, digest, length: *length as u64, offset, journal })?;
                Ok(())
            }
            StoredBody::Composite { parts, .. } => {
                for part in parts.value.iter() { ordered(part, out, publication)?; }
                Ok(())
            }
            _ => Err(Error::from(Unavailable).into()),
        }
    }
    // Preserve leaf order and repetitions; no set or deduplication allocation.
    let mut output = Vec::new();
    ordered(body, &mut output, publication)?;
    Ok(output)
}

/// Keys of `next`'s objects that are new or differ from `before`'s, in key order.
fn changed_keys(before: Option<&Bucket>, next: &Bucket, publication: &mut Publication) -> Result<Vec<SharedInfo<String>>, NativeObjectsError> {
    let mut output = Vec::new();
    if let Some(before) = before {
        for item in before.objects.diff(&next.objects) {
            if let imbl::ordmap::DiffItem::Add(key, _) | imbl::ordmap::DiffItem::Update { new: (key, _), .. } = item {
                publication.push(&mut output, key.clone())?;
            }
        }
    } else {
        for key in next.objects.keys() { publication.push(&mut output, key.clone())?; }
    }
    Ok(output)
}

/// Bucket and object changes from `before` to `next`, into `delta`.
fn bucket_changes(before: &State, next: &State, delta: &mut Delta, publication: &mut Publication) -> Result<(), NativeObjectsError> {
    fn changed(name: &str, key: &str, value: &Stored, publication: &mut Publication) -> Result<ObjectChange, NativeObjectsError> {
        Ok(ObjectChange {
            bucket: publication.text(name)?,
            key: publication.text(key)?,
            info: Some(publication.info(&value.info)?),
            bodies: bodies(&value.body, publication)?,
        })
    }
    let mut removed = Vec::new();
    for (name, bucket) in &next.buckets {
        let old = before.buckets.get(name);
        if old.is_none_or(|old| old.info != bucket.info) {
            let info = wire::Bucket {
                bucket: bucket.info.bucket.as_ref().map(|bucket| publication.text(&bucket.name).map(|name| wire::BucketRef { name })).transpose()?,
                created_at: bucket.info.created_at,
            };
            let change = BucketChange { name: publication.text(name)?, info: Some(info) };
            publication.push(&mut delta.buckets, change)?;
        }
        let Some(old) = old else {
            for (key, value) in &bucket.objects {
                let change = changed(name, key.as_str(), value, publication)?;
                publication.push(&mut delta.objects, change)?;
            }
            continue;
        };
        for item in old.objects.diff(&bucket.objects) {
            match item {
                imbl::ordmap::DiffItem::Add(key, value) => {
                    let change = changed(name, key.as_str(), value, publication)?;
                    publication.push(&mut delta.objects, change)?;
                }
                imbl::ordmap::DiffItem::Update { old: (_, old), new: (key, value) } => {
                    let refs = bodies(&value.body, publication)?;
                    if old.info != value.info || bodies(&old.body, publication)? != refs {
                        let change = ObjectChange {
                            bucket: publication.text(name)?,
                            key: publication.text(key.as_str())?,
                            info: Some(publication.info(&value.info)?),
                            bodies: refs,
                        };
                        publication.push(&mut delta.objects, change)?;
                    }
                }
                imbl::ordmap::DiffItem::Remove(key, _) => {
                    let change = ObjectChange { bucket: publication.text(name)?, key: publication.text(key.as_str())?, ..Default::default() };
                    publication.push(&mut removed, change)?;
                }
            }
        }
    }
    publication.reserve(&mut delta.objects, removed.len())?;
    delta.objects.extend(removed);
    for name in before.buckets.keys().filter(|name| !next.buckets.contains_key(*name)) {
        let change = BucketChange { name: publication.text(name)?, info: None };
        publication.push(&mut delta.buckets, change)?;
    }
    Ok(())
}

fn difference(before: &State, next: &State, ordinal: u64, publication: &mut Publication) -> Result<Delta, NativeObjectsError> {
    let mut delta = Delta { sequence: next.sequence, ordinal, ..Default::default() };
    bucket_changes(before, next, &mut delta, publication)?;
    for (id, upload) in &next.uploads {
        let old = before.uploads.get(id);
        if old.is_none() {
            let metadata = upload.metadata.as_ref().map(|value| publication.metadata(value)).transpose()?;
            let change = UploadChange {
                id: publication.text(id)?,
                bucket: publication.text(&upload.bucket)?,
                key: publication.text(&upload.key)?,
                metadata,
                deleted: false,
            };
            publication.push(&mut delta.uploads, change)?;
        }
        for (number, (receipt, body)) in &upload.parts {
            let refs = bodies(body, publication)?;
            if old.and_then(|old| old.parts.get(number)).is_none_or(|(old, _)| old != receipt) {
                let change = PartChange {
                    upload: publication.text(id)?,
                    number: *number,
                    receipt: Some(wire::UploadedPart { etag: publication.text(&receipt.etag)?, part_number: receipt.part_number, size: receipt.size }),
                    bodies: refs,
                };
                publication.push(&mut delta.parts, change)?;
            }
        }
    }
    for id in before.uploads.keys().filter(|id| !next.uploads.contains_key(*id)) {
        let change = UploadChange { id: publication.text(id)?, deleted: true, ..Default::default() };
        publication.push(&mut delta.uploads, change)?;
    }
    for item in before.receipts.diff(&next.receipts) {
        if let imbl::ordmap::DiffItem::Add(key, receipt) = item {
            let change = ReceiptChange {
                key: publication.text(key.as_str())?,
                digest: publication.bytes(&receipt.digest)?,
                response: publication.bytes(receipt.response.as_ref())?,
                kind: receipt.kind,
            };
            publication.push(&mut delta.receipts, change)?;
        }
    }
    Ok(delta)
}

fn restore_body(
    records: Vec<BodyRecord>,
    root: &Arc<crate::physical::LocalRoot>,
    expected: u64,
) -> Result<StoredBody, LocalOpenError> {
    if records.is_empty() || records.len() > 10_000 {
        return Err(LocalOpenError::Corrupt);
    }
    let mut length = 0usize;
    let mut leaves = Vec::with_capacity(records.len());
    for value in records {
        let count = usize::try_from(value.length).map_err(corrupt)?;
        length = length.checked_add(count).ok_or(LocalOpenError::Corrupt)?;
        if value.offset < 68
            || value.journal && (count > inline::LIMIT || !value.segment.is_empty())
        {
            return Err(LocalOpenError::Corrupt);
        }
        leaves.push(StoredBody::Local {
            root: Arc::clone(root),
            digest: value.digest.as_slice().try_into().map_err(corrupt)?,
            length: count,
            location: if value.journal {
                LocalBodyLocation::Journal {
                    offset: value.offset,
                }
            } else {
                LocalBodyLocation::Segment {
                    id: value.segment.as_slice().try_into().map_err(corrupt)?,
                    offset: value.offset,
                }
            },
        });
    }
    if length as u64 != expected {
        return Err(LocalOpenError::Corrupt);
    }
    Ok(StoredBody::Composite {
        parts: Arc::new(crate::v1::Guarded { value: leaves, _allocation: None }),
        length,
    })
}

fn apply(
    state: &mut State,
    delta: Delta,
    root: &Arc<crate::physical::LocalRoot>,
    options: MemoryOptions,
    limits: LocalObjectsLimits,
) -> Result<(), LocalOpenError> {
    if delta.sequence < state.sequence {
        return Err(LocalOpenError::Corrupt);
    }
    let mut next = state.clone();
    next.sequence = delta.sequence;
    let mut seen = BTreeSet::new();
    for value in delta.buckets {
        if !seen.insert(value.name.clone()) {
            return Err(LocalOpenError::Corrupt);
        }
        if let Some(info) = value.info {
            response::bucket(
                &info,
                &wire::BucketRef {
                    name: value.name.clone(),
                },
            )
            .map_err(corrupt)?;
            if next.buckets.contains_key(value.name.as_str()) {
                return Err(LocalOpenError::Corrupt);
            }
            next.buckets.insert(
                value.name.into(),
                Bucket {
                    info: info.into(),
                    objects: OrdMap::new(),
                },
            );
        } else if next
            .buckets
            .remove(value.name.as_str())
            .is_none_or(|bucket| !bucket.objects.is_empty())
        {
            return Err(LocalOpenError::Corrupt);
        }
    }
    let mut seen = BTreeSet::new();
    for value in delta.objects {
        request::key(&value.key).map_err(corrupt)?;
        if !seen.insert((value.bucket.clone(), value.key.clone())) {
            return Err(LocalOpenError::Corrupt);
        }
        let bucket = next
            .buckets
            .get_mut(value.bucket.as_str())
            .ok_or(LocalOpenError::Corrupt)?;
        if let Some(info) = value.info {
            response::object_info(&info).map_err(corrupt)?;
            if info.size > limits.maximum_object_bytes {
                return Err(LocalOpenError::Corrupt);
            }
            let body = restore_body(value.bodies, root, info.size)?;
            bucket.objects.insert(value.key.into(), Stored { info: info.into(), body });
        } else if !value.bodies.is_empty() || bucket.objects.remove(value.key.as_str()).is_none() {
            return Err(LocalOpenError::Corrupt);
        }
    }
    apply_uploads(&mut next, delta.uploads)?;
    apply_parts(&mut next, delta.parts, root, limits)?;
    apply_receipts(&mut next, delta.receipts)?;
    validate_state(state, &mut next, options)?;
    *state = next;
    Ok(())
}

fn apply_uploads(next: &mut State, changes: Vec<UploadChange>) -> Result<(), LocalOpenError> {
    let mut seen = BTreeSet::new();
    for value in changes {
        request::upload_id(&value.id).map_err(corrupt)?;
        if !seen.insert(value.id.clone()) {
            return Err(LocalOpenError::Corrupt);
        }
        if value.deleted {
            if !value.bucket.is_empty()
                || !value.key.is_empty()
                || value.metadata.is_some()
                || next.uploads.remove(value.id.as_str()).is_none()
            {
                return Err(LocalOpenError::Corrupt);
            }
        } else {
            request::key(&value.key).map_err(corrupt)?;
            request::metadata(&value.metadata).map_err(corrupt)?;
            if !next.buckets.contains_key(value.bucket.as_str()) || next.uploads.contains_key(value.id.as_str()) {
                return Err(LocalOpenError::Corrupt);
            }
            next.uploads.insert(
                value.id.into(),
                Upload {
                    bucket: value.bucket.into(),
                    key: value.key.into(),
                    metadata: value.metadata.into(),
                    parts: OrdMap::new(),
                },
            );
        }
    }
    Ok(())
}

fn apply_parts(
    next: &mut State,
    changes: Vec<PartChange>,
    root: &Arc<crate::physical::LocalRoot>,
    limits: LocalObjectsLimits,
) -> Result<(), LocalOpenError> {
    let mut seen = BTreeSet::new();
    for value in changes {
        if !seen.insert((value.upload.clone(), value.number)) {
            return Err(LocalOpenError::Corrupt);
        }
        let upload = next
            .uploads
            .get_mut(value.upload.as_str())
            .ok_or(LocalOpenError::Corrupt)?;
        let receipt = value.receipt.ok_or(LocalOpenError::Corrupt)?;
        let query = wire::UploadPartHeader {
            bucket: Some(wire::BucketRef {
                name: upload.bucket.to_string(),
            }),
            object_key: upload.key.to_string(),
            upload_id: value.upload,
            part_number: value.number,
            mutation: None,
        };
        response::validate_binary(
            "multipart/upload-part",
            &query.encode_to_vec(),
            &receipt.encode_to_vec(),
            receipt.size,
        )
        .map_err(corrupt)?;
        if receipt.size > limits.maximum_object_bytes {
            return Err(LocalOpenError::Corrupt);
        }
        let body = restore_body(value.bodies, root, receipt.size)?;
        upload.parts.insert(value.number, (receipt.into(), body));
    }
    Ok(())
}

fn apply_receipts(next: &mut State, changes: Vec<ReceiptChange>) -> Result<(), LocalOpenError> {
    for value in changes {
        request::identity(&Some(wire::MutationIdentity {
            idempotency_key: value.key.clone(),
        }))
        .map_err(corrupt)?;
        validate_receipt(value.kind, &value.response)?;
        let put_info = if value.kind == 2 {
            Some(wire::ObjectInfo::decode(value.response.as_slice()).map_err(corrupt)?.into())
        } else {
            None
        };
        let receipt = Receipt {
            digest: value.digest.as_slice().try_into().map_err(corrupt)?,
            response: value.response.into(),
            put_info,
            kind: value.kind,
        };
        if next.receipts.insert(value.key.into(), receipt).is_some() {
            return Err(LocalOpenError::Corrupt);
        }
    }
    Ok(())
}

fn validate_state(
    before: &State,
    next: &mut State,
    options: MemoryOptions,
) -> Result<(), LocalOpenError> {
    let size = super::stored_bytes(before, next).ok_or(LocalOpenError::Corrupt)?;
    if size > options.maximum_bytes
        || next
            .uploads
            .values()
            .any(|upload| !next.buckets.contains_key(upload.bucket.as_str()))
    {
        return Err(LocalOpenError::Corrupt);
    }
    Ok(())
}

fn validate_receipt(kind: u32, bytes: &[u8]) -> Result<(), LocalOpenError> {
    macro_rules! checked {
        ($ty:ident) => {{
            let value = wire::$ty::decode(bytes).map_err(corrupt)?;
            if value.encode_to_vec() != bytes {
                return Err(LocalOpenError::Corrupt);
            }
            value
        }};
    }
    match kind {
        1 => {
            let value = checked!(Bucket);
            response::bucket(
                &value,
                value.bucket.as_ref().ok_or(LocalOpenError::Corrupt)?,
            )
            .map_err(corrupt)?;
        }
        2 => {
            response::object_info(&checked!(ObjectInfo)).map_err(corrupt)?;
        }
        3 => {
            checked!(DeleteBucketResponse);
        }
        4 => {
            checked!(DeleteObjectResponse);
        }
        5 => {
            request::upload_id(&checked!(MultipartUpload).upload_id).map_err(corrupt)?;
        }
        6 => {
            let part = checked!(UploadedPart);
            let reply = wire::ListPartsResponse {
                parts: vec![part],
                ..Default::default()
            };
            response::parts(
                &wire::ListPartsRequest {
                    page_size: 1,
                    ..Default::default()
                },
                &reply,
            )
            .map_err(corrupt)?;
        }
        7 => {
            checked!(AbortMultipartResponse);
        }
        _ => return Err(LocalOpenError::Corrupt),
    }
    Ok(())
}
