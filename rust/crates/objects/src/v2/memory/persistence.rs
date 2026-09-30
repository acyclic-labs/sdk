//! Private v2 state deltas. Bodies remain authenticated physical references.
use super::super::response;
use super::*;
use crate::body::{LocalBodyLocation, LocalBodyReference};
use crate::{LocalDurability, LocalObjectsLimits};
use fs2::FileExt;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

const MAGIC: &[u8] = b"ACYCLIC-OBJECTS-V2-LOCAL\0\x01";
const RECORD_LIMIT: usize = 2 * 1024 * 1024;

/// Failure to open or recover logical Objects storage.
#[derive(Debug, thiserror::Error)]
pub enum LocalOpenError {
    /// Invalid capacity or durability configuration.
    #[error("invalid local Objects v2 configuration")]
    Invalid,
    /// A complete durable record or referenced body is corrupt.
    #[error("corrupt local Objects v2 store")]
    Corrupt,
    /// Another owner has this exact root open.
    #[error("local Objects v2 store already has an owner")]
    AlreadyOwned,
    /// A native worker could not complete.
    #[error("local Objects v2 worker unavailable")]
    Unavailable,
    /// Host I/O failed.
    #[error("local Objects v2 I/O failed: {0}")]
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
}

struct Tail {
    file: File,
    bytes: u64,
    operations: u64,
}
pub(super) struct Journal {
    root: PathBuf,
    tail: Mutex<Tail>,
    limits: LocalObjectsLimits,
    poisoned: AtomicBool,
    _owner: File,
    _anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
    #[cfg(test)]
    fault_bytes: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    fault_sync: AtomicBool,
}

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

fn durability(value: LocalDurability) -> acyclic_native_runtime::Durability {
    match value {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}
fn sync(file: &File, limits: LocalObjectsLimits) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, durability(limits.durability))
}
fn corrupt<T>(_: T) -> LocalOpenError {
    LocalOpenError::Corrupt
}

pub(crate) fn open(
    root: PathBuf,
    limits: LocalObjectsLimits,
    anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
) -> Result<MemoryObjects, LocalOpenError> {
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
    let header = load_header(&mut file, &root, limits)?;
    let (state, operations) = replay(&mut file, &root, options, limits)?;
    let bytes = file.stream_position()?;
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
    crate::local::validate_referenced_segments(&root, &references, limits.maximum_object_bytes)
        .map_err(corrupt)?;
    let key = header.cursor_key.as_slice().try_into().map_err(corrupt)?;
    Ok(MemoryObjects {
        state: Arc::new(Mutex::new(state)),
        options,
        token_key: Arc::new(Mutex::new(Some(key))),
        journal: Some(Arc::new(Journal {
            root,
            limits,
            tail: Mutex::new(Tail {
                file,
                bytes,
                operations,
            }),
            poisoned: AtomicBool::new(false),
            _owner: owner,
            _anchor: anchor,
            #[cfg(test)]
            fault_bytes: std::sync::atomic::AtomicU64::new(u64::MAX),
            #[cfg(test)]
            fault_sync: AtomicBool::new(false),
        })),
    })
}

fn load_header(
    file: &mut File,
    root: &Path,
    limits: LocalObjectsLimits,
) -> Result<Header, LocalOpenError> {
    let mut key = [0; 32];
    if file.metadata()?.len() == 0 {
        getrandom::fill(&mut key).map_err(corrupt)?;
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
        acyclic_native_runtime::sync_parent(root, durability(limits.durability))?;
        return Ok(header);
    }
    let mut magic = vec![0; MAGIC.len()];
    file.read_exact(&mut magic).map_err(corrupt)?;
    if magic != MAGIC {
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
    root: &Path,
    options: MemoryOptions,
    limits: LocalObjectsLimits,
) -> Result<(State, u64), LocalOpenError> {
    let mut state = State::default();
    let mut operations = 0;
    let end = file.metadata()?.len();
    if end > limits.maximum_journal_bytes {
        return Err(LocalOpenError::Corrupt);
    }
    loop {
        let start = file.stream_position()?;
        if start == end {
            return Ok((state, operations));
        }
        let mut prefix = [0; 36];
        if let Err(error) = file.read_exact(&mut prefix) {
            return repair_tail(file, start, error, limits).map(|()| (state, operations));
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
            return repair_tail(file, start, error, limits).map(|()| (state, operations));
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
        apply(&mut state, delta, root, options, limits)?;
        operations += 1;
    }
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
    pub(super) fn check(&self) -> Result<(), Error> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Unavailable.into())
        } else {
            Ok(())
        }
    }
    pub(super) fn commit(&self, before: &State, next: &mut State) -> Result<(), Error> {
        self.check()?;
        let mut tail = self.tail.lock().map_err(|_| Error::from(Unavailable))?;
        if tail.operations >= self.limits.maximum_journal_operations {
            return Err(QuotaExceeded.into());
        }
        for bucket in next.buckets.values_mut() {
            for object in bucket.objects.values_mut() {
                if object.body.len() as u64 > self.limits.maximum_object_bytes {
                    return Err(QuotaExceeded.into());
                }
                object.body = self.external(&object.body)?;
            }
        }
        for upload in next.uploads.values_mut() {
            for (_, body) in upload.parts.values_mut() {
                *body = self.external(body)?;
            }
        }
        let record = difference(before, next, tail.operations + 1)?.encode_to_vec();
        let end = tail
            .bytes
            .checked_add(36 + record.len() as u64)
            .ok_or(Error::from(QuotaExceeded))?;
        if record.len() > RECORD_LIMIT || end > self.limits.maximum_journal_bytes {
            return Err(QuotaExceeded.into());
        }
        let mut frame = Vec::with_capacity(36 + record.len());
        frame.extend_from_slice(
            &u32::try_from(record.len())
                .map_err(|_| Error::from(QuotaExceeded))?
                .to_le_bytes(),
        );
        frame.extend_from_slice(blake3::hash(&record).as_bytes());
        frame.extend_from_slice(&record);
        if self.write(&mut tail.file, &frame).is_err() {
            self.poisoned.store(true, Ordering::Release);
            return Err(Unavailable.into());
        }
        tail.bytes = end;
        tail.operations += 1;
        Ok(())
    }
    fn external(&self, body: &StoredBody) -> Result<StoredBody, Error> {
        match body {
            StoredBody::Memory(bytes) => {
                if bytes.len() as u64 > self.limits.maximum_object_bytes {
                    return Err(QuotaExceeded.into());
                }
                let digest = *blake3::hash(bytes).as_bytes();
                let (id, offsets) = crate::local::persist_segment(
                    &self.root,
                    &[(digest, bytes.clone())],
                    self.limits.durability,
                )
                .map_err(|_| Error::from(Unavailable))?;
                Ok(StoredBody::Local {
                    root: Arc::new(self.root.clone()),
                    digest,
                    length: bytes.len(),
                    location: LocalBodyLocation::Segment {
                        id,
                        offset: *offsets.first().ok_or(Error::from(Unavailable))?,
                    },
                })
            }
            StoredBody::Composite { parts, length } => Ok(StoredBody::Composite {
                parts: parts
                    .iter()
                    .map(|part| self.external(part))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
                length: *length,
            }),
            StoredBody::Local { .. } => Ok(body.clone()),
        }
    }
    fn write(&self, file: &mut File, frame: &[u8]) -> std::io::Result<()> {
        #[cfg(test)]
        {
            let cut = self.fault_bytes.swap(u64::MAX, Ordering::AcqRel);
            if cut != u64::MAX {
                let count = usize::try_from(cut).unwrap_or(frame.len()).min(frame.len());
                file.write_all(
                    frame
                        .get(..count)
                        .ok_or_else(|| std::io::Error::other("invalid fault point"))?,
                )?;
                sync(file, self.limits)?;
                return Err(std::io::Error::other("injected journal write failure"));
            }
        }
        file.write_all(frame)?;
        #[cfg(test)]
        if self.fault_sync.swap(false, Ordering::AcqRel) {
            return Err(std::io::Error::other("injected journal sync failure"));
        }
        sync(file, self.limits)
    }
}

fn bodies(body: &StoredBody) -> Result<Vec<BodyRecord>, Error> {
    let mut refs = BTreeSet::<LocalBodyReference>::new();
    // Preserve leaf order and repeated leaves; a set would change concatenation.
    fn ordered(body: &StoredBody, out: &mut Vec<BodyRecord>) -> Result<(), Error> {
        match body {
            StoredBody::Local {
                digest,
                length,
                location: LocalBodyLocation::Segment { id, offset },
                ..
            } => {
                out.push(BodyRecord {
                    segment: id.to_vec(),
                    digest: digest.to_vec(),
                    length: *length as u64,
                    offset: *offset,
                });
                Ok(())
            }
            StoredBody::Composite { parts, .. } => {
                for part in parts.iter() {
                    ordered(part, out)?;
                }
                Ok(())
            }
            _ => Err(Unavailable.into()),
        }
    }
    body.local_references(&mut refs);
    let mut out = Vec::with_capacity(refs.len());
    ordered(body, &mut out)?;
    Ok(out)
}

fn difference(before: &State, next: &State, ordinal: u64) -> Result<Delta, Error> {
    let mut delta = Delta {
        sequence: next.sequence,
        ordinal,
        ..Default::default()
    };
    for (name, bucket) in &next.buckets {
        let old = before.buckets.get(name);
        if old.is_none_or(|old| old.info != bucket.info) {
            delta.buckets.push(BucketChange {
                name: name.clone(),
                info: Some(bucket.info.clone()),
            });
        }
        for (key, value) in &bucket.objects {
            let old = old.and_then(|old| old.objects.get(key));
            let refs = bodies(&value.body)?;
            if old.is_none_or(|old| old.info != value.info)
                || old
                    .map(|old| bodies(&old.body))
                    .transpose()?
                    .is_some_and(|old| old != refs)
            {
                delta.objects.push(ObjectChange {
                    bucket: name.clone(),
                    key: key.clone(),
                    info: Some(value.info.clone()),
                    bodies: refs,
                });
            }
        }
    }
    for (name, bucket) in &before.buckets {
        if let Some(new) = next.buckets.get(name) {
            for key in bucket
                .objects
                .keys()
                .filter(|key| !new.objects.contains_key(*key))
            {
                delta.objects.push(ObjectChange {
                    bucket: name.clone(),
                    key: key.clone(),
                    ..Default::default()
                });
            }
        } else {
            delta.buckets.push(BucketChange {
                name: name.clone(),
                info: None,
            });
        }
    }
    for (id, upload) in &next.uploads {
        let old = before.uploads.get(id);
        if old.is_none() {
            delta.uploads.push(UploadChange {
                id: id.clone(),
                bucket: upload.bucket.clone(),
                key: upload.key.clone(),
                metadata: upload.metadata.clone(),
                deleted: false,
            });
        }
        for (number, (receipt, body)) in &upload.parts {
            let refs = bodies(body)?;
            if old
                .and_then(|old| old.parts.get(number))
                .is_none_or(|(old, _)| old != receipt)
            {
                delta.parts.push(PartChange {
                    upload: id.clone(),
                    number: *number,
                    receipt: Some(receipt.clone()),
                    bodies: refs,
                });
            }
        }
    }
    for id in before
        .uploads
        .keys()
        .filter(|id| !next.uploads.contains_key(*id))
    {
        delta.uploads.push(UploadChange {
            id: id.clone(),
            deleted: true,
            ..Default::default()
        });
    }
    for (key, receipt) in &next.receipts {
        if !before.receipts.contains_key(key) {
            delta.receipts.push(ReceiptChange {
                key: key.clone(),
                digest: receipt.digest.to_vec(),
                response: receipt.response.clone(),
                kind: receipt.kind,
            });
        }
    }
    Ok(delta)
}

fn restore_body(
    records: Vec<BodyRecord>,
    root: &Path,
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
        if value.offset < 68 {
            return Err(LocalOpenError::Corrupt);
        }
        leaves.push(StoredBody::Local {
            root: Arc::new(root.to_path_buf()),
            digest: value.digest.as_slice().try_into().map_err(corrupt)?,
            length: count,
            location: LocalBodyLocation::Segment {
                id: value.segment.as_slice().try_into().map_err(corrupt)?,
                offset: value.offset,
            },
        });
    }
    if length as u64 != expected {
        return Err(LocalOpenError::Corrupt);
    }
    Ok(StoredBody::Composite {
        parts: leaves.into(),
        length,
    })
}

fn apply(
    state: &mut State,
    delta: Delta,
    root: &Path,
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
            if next.buckets.contains_key(&value.name) {
                return Err(LocalOpenError::Corrupt);
            }
            next.buckets.insert(
                value.name,
                Bucket {
                    info,
                    objects: BTreeMap::new(),
                },
            );
        } else if next
            .buckets
            .remove(&value.name)
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
            .get_mut(&value.bucket)
            .ok_or(LocalOpenError::Corrupt)?;
        if let Some(info) = value.info {
            response::object_info(&info).map_err(corrupt)?;
            if info.size > limits.maximum_object_bytes {
                return Err(LocalOpenError::Corrupt);
            }
            let body = restore_body(value.bodies, root, info.size)?;
            bucket.objects.insert(value.key, Stored { info, body });
        } else if !value.bodies.is_empty() || bucket.objects.remove(&value.key).is_none() {
            return Err(LocalOpenError::Corrupt);
        }
    }
    apply_uploads(&mut next, delta.uploads)?;
    apply_parts(&mut next, delta.parts, root, limits)?;
    apply_receipts(&mut next, delta.receipts)?;
    validate_state(&next, options)?;
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
                || next.uploads.remove(&value.id).is_none()
            {
                return Err(LocalOpenError::Corrupt);
            }
        } else {
            request::key(&value.key).map_err(corrupt)?;
            request::metadata(&value.metadata).map_err(corrupt)?;
            if !next.buckets.contains_key(&value.bucket) || next.uploads.contains_key(&value.id) {
                return Err(LocalOpenError::Corrupt);
            }
            next.uploads.insert(
                value.id,
                Upload {
                    bucket: value.bucket,
                    key: value.key,
                    metadata: value.metadata,
                    parts: BTreeMap::new(),
                },
            );
        }
    }
    Ok(())
}

fn apply_parts(
    next: &mut State,
    changes: Vec<PartChange>,
    root: &Path,
    limits: LocalObjectsLimits,
) -> Result<(), LocalOpenError> {
    let mut seen = BTreeSet::new();
    for value in changes {
        if !seen.insert((value.upload.clone(), value.number)) {
            return Err(LocalOpenError::Corrupt);
        }
        let upload = next
            .uploads
            .get_mut(&value.upload)
            .ok_or(LocalOpenError::Corrupt)?;
        let receipt = value.receipt.ok_or(LocalOpenError::Corrupt)?;
        let query = wire::UploadPartHeader {
            bucket: Some(wire::BucketRef {
                name: upload.bucket.clone(),
            }),
            object_key: upload.key.clone(),
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
        upload.parts.insert(value.number, (receipt, body));
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
        let receipt = Receipt {
            digest: value.digest.as_slice().try_into().map_err(corrupt)?,
            response: value.response,
            kind: value.kind,
        };
        if next.receipts.insert(value.key, receipt).is_some() {
            return Err(LocalOpenError::Corrupt);
        }
    }
    Ok(())
}

fn validate_state(next: &State, options: MemoryOptions) -> Result<(), LocalOpenError> {
    let size = next
        .buckets
        .values()
        .flat_map(|bucket| bucket.objects.values())
        .map(|object| object.body.len())
        .chain(
            next.uploads
                .values()
                .flat_map(|upload| upload.parts.values())
                .map(|(_, body)| body.len()),
        )
        .try_fold(0usize, usize::checked_add)
        .ok_or(LocalOpenError::Corrupt)?;
    if size > options.maximum_bytes
        || next
            .uploads
            .values()
            .any(|upload| !next.buckets.contains_key(&upload.bucket))
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
