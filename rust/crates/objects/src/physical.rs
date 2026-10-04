//! Private authenticated immutable segments and journal body reads.
use crate::body::{BodyError, LocalBodyLocation, LocalBodyReference};
use crate::{LocalDurability, LocalObjectsGarbageCollection};
#[cfg(feature = "test-support")]
use std::io;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
const JOURNAL_FILE: &str = "mutations.log";
const MAXIMUM_INLINE_BODY_BYTES: usize = 64 * 1_024;
const SEGMENT_MAGIC: &[u8; 24] = b"ACYCLIC-OBJECT-SEGMENT\0\x02";
const SEGMENT_HEADER_BYTES: usize = SEGMENT_MAGIC.len() + 4;
const SEGMENT_RECORD_BYTES: usize = 32 + 8;
const MAXIMUM_SEGMENT_BODIES: usize = 1_024;
const MAXIMUM_SEGMENT_BYTES: usize = 4 * 1024 * 1024;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[derive(Debug, thiserror::Error)]
pub(crate) enum PhysicalError {
    #[error("invalid private Objects storage: {0}")]
    Invalid(&'static str),
    #[error("corrupt private Objects storage")]
    Corrupt,
    #[error("Objects storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
}
#[allow(clippy::too_many_lines)]
pub(crate) fn persist_segment(
    root: &Path,
    bodies: &[([u8; 32], bytes::Bytes)],
    durability: LocalDurability,
) -> Result<([u8; 32], Vec<u64>), PhysicalError> {
    if bodies.is_empty() {
        return Err(PhysicalError::Invalid("object segment is empty"));
    }
    if bodies.len() > MAXIMUM_SEGMENT_BODIES {
        return Err(PhysicalError::Invalid("object segment has too many bodies"));
    }
    if bodies.len() > 1
        && bodies
            .iter()
            .map(|(_, body)| body.len())
            .try_fold(0_usize, usize::checked_add)
            .is_none_or(|bytes| bytes > MAXIMUM_SEGMENT_BYTES)
    {
        return Err(PhysicalError::Invalid(
            "multi-body object segment is too large",
        ));
    }
    let count = u32::try_from(bodies.len())
        .map_err(|_| PhysicalError::Invalid("object segment has too many bodies"))?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(bodies.len())
        .map_err(|_| PhysicalError::Invalid("object segment offset allocation failed"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(SEGMENT_MAGIC);
    hasher.update(&count.to_le_bytes());
    let table_bytes = bodies
        .len()
        .checked_mul(SEGMENT_RECORD_BYTES)
        .ok_or(PhysicalError::Invalid("object segment length overflowed"))?;
    let mut position = u64::try_from(
        SEGMENT_HEADER_BYTES
            .checked_add(table_bytes)
            .ok_or(PhysicalError::Invalid("object segment length overflowed"))?,
    )
    .map_err(|_| PhysicalError::Invalid("object segment length overflowed"))?;
    for (digest, body) in bodies {
        let length = u64::try_from(body.len())
            .map_err(|_| PhysicalError::Invalid("object segment body is too large"))?;
        hasher.update(digest);
        hasher.update(&length.to_le_bytes());
        offsets.push(position);
        position = position
            .checked_add(length)
            .ok_or(PhysicalError::Invalid("object segment length overflowed"))?;
    }
    for (_, body) in bodies {
        hasher.update(body);
    }
    let id = *hasher.finalize().as_bytes();
    let parent = root.join("segments");
    let destination = segment_path(root, &id);
    if destination.exists() {
        validate_segment_file(&destination, &id, position)?;
        // Another put may have published it and not yet synchronized its
        // directory entry; this put's acknowledgement depends on that entry.
        sync_parent(&parent, durability)?;
        return Ok((id, offsets));
    }
    let identity = hex(&id);
    let (temporary, mut file) = (0..1_024)
        .find_map(|_| {
            let nonce = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(".{identity}.{}-{nonce}.tmp", std::process::id()));
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => Some(Ok((path, file))),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
                Err(error) => Some(Err(error)),
            }
        })
        .transpose()?
        .ok_or(PhysicalError::Corrupt)?;
    let write_result = (|| -> std::io::Result<()> {
        let metadata_capacity = SEGMENT_HEADER_BYTES
            .checked_add(table_bytes)
            .ok_or_else(|| std::io::Error::other("object segment metadata is too large"))?;
        let mut metadata = Vec::with_capacity(metadata_capacity);
        metadata.extend_from_slice(SEGMENT_MAGIC);
        metadata.extend_from_slice(&count.to_le_bytes());
        for (digest, body) in bodies {
            metadata.extend_from_slice(digest);
            let length = u64::try_from(body.len())
                .map_err(|_| std::io::Error::other("object segment body is too large"))?;
            metadata.extend_from_slice(&length.to_le_bytes());
        }
        file.write_all(&metadata)?;
        for (_, body) in bodies {
            file.write_all(body)?;
        }
        sync_file(&file, durability)
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    match publish_new(&temporary, &destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_segment_file(&destination, &id, position)?;
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
    }
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    sync_parent(&parent, durability)?;
    Ok((id, offsets))
}

fn validate_segment_file(
    path: &Path,
    expected_id: &[u8; 32],
    expected_length: u64,
) -> Result<(), PhysicalError> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != expected_length {
        return Err(PhysicalError::Corrupt);
    }
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(buffer.get(..read).ok_or(PhysicalError::Corrupt)?);
    }
    if hasher.finalize().as_bytes() != expected_id {
        return Err(PhysicalError::Corrupt);
    }
    Ok(())
}

/// Authenticated body lengths of one segment, keyed by offset and digest: an empty body
/// shares its offset with the body that follows it.
type ValidatedSegmentRecords = BTreeMap<(u64, [u8; 32]), u64>;

/// Authenticates every segment a live body references and returns their identities.
///
/// Journal-resident bodies were authenticated when replay decoded or an append wrote them.
/// A live reclaimed body means compaction dropped bytes that were still reachable.
pub(crate) fn validate_referenced_segments(
    root: &Path,
    bodies: &BTreeSet<LocalBodyReference>,
    maximum_object_bytes: u64,
) -> Result<BTreeSet<[u8; 32]>, PhysicalError> {
    let mut validated_segments = BTreeMap::new();
    for body in bodies {
        let (id, offset) = match &body.location {
            LocalBodyLocation::Segment { id, offset } => (id, offset),
            LocalBodyLocation::Journal { .. } => continue,
        };
        if !validated_segments.contains_key(id) {
            let records =
                validate_segment_records(&segment_path(root, id), id, maximum_object_bytes)?;
            validated_segments.insert(*id, records);
        }
        let expected_length = u64::try_from(body.length).map_err(|_| PhysicalError::Corrupt)?;
        if validated_segments
            .get(id)
            .and_then(|records| records.get(&(*offset, body.digest)))
            != Some(&expected_length)
        {
            return Err(PhysicalError::Corrupt);
        }
    }
    Ok(validated_segments.into_keys().collect())
}

#[allow(clippy::too_many_lines)]
fn validate_segment_records(
    path: &Path,
    expected_id: &[u8; 32],
    maximum_object_bytes: u64,
) -> Result<ValidatedSegmentRecords, PhysicalError> {
    let mut file = File::open(path).map_err(|_| PhysicalError::Corrupt)?;
    let actual_length = file.metadata().map_err(|_| PhysicalError::Corrupt)?.len();
    let mut hasher = blake3::Hasher::new();
    let mut magic = [0_u8; SEGMENT_MAGIC.len()];
    file.read_exact(&mut magic)
        .map_err(|_| PhysicalError::Corrupt)?;
    if magic != *SEGMENT_MAGIC {
        return Err(PhysicalError::Corrupt);
    }
    hasher.update(&magic);
    let mut count = [0_u8; 4];
    file.read_exact(&mut count)
        .map_err(|_| PhysicalError::Corrupt)?;
    hasher.update(&count);
    let count = usize::try_from(u32::from_le_bytes(count)).map_err(|_| PhysicalError::Corrupt)?;
    if count == 0 || count > MAXIMUM_SEGMENT_BODIES {
        return Err(PhysicalError::Corrupt);
    }
    let table_bytes = count
        .checked_mul(SEGMENT_RECORD_BYTES)
        .ok_or(PhysicalError::Corrupt)?;
    let body_start = SEGMENT_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(PhysicalError::Corrupt)?;
    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(count)
        .map_err(|_| PhysicalError::Corrupt)?;
    for _ in 0..count {
        let mut digest = [0_u8; 32];
        file.read_exact(&mut digest)
            .map_err(|_| PhysicalError::Corrupt)?;
        hasher.update(&digest);
        let mut length = [0_u8; 8];
        file.read_exact(&mut length)
            .map_err(|_| PhysicalError::Corrupt)?;
        hasher.update(&length);
        metadata.push((digest, u64::from_le_bytes(length)));
    }
    if file.stream_position().map_err(|_| PhysicalError::Corrupt)?
        != u64::try_from(body_start).map_err(|_| PhysicalError::Corrupt)?
    {
        return Err(PhysicalError::Corrupt);
    }
    let mut records = BTreeMap::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut body_bytes = 0_u64;
    for (digest, length) in metadata {
        let offset = file.stream_position().map_err(|_| PhysicalError::Corrupt)?;
        if records.insert((offset, digest), length).is_some() {
            return Err(PhysicalError::Corrupt);
        }
        body_bytes = body_bytes
            .checked_add(length)
            .ok_or(PhysicalError::Corrupt)?;
        let maximum_body_bytes = if count == 1 {
            maximum_object_bytes
        } else {
            MAXIMUM_SEGMENT_BYTES as u64
        };
        if body_bytes > maximum_body_bytes {
            return Err(PhysicalError::Corrupt);
        }
        let mut body_hasher = blake3::Hasher::new();
        let mut remaining = length;
        while remaining != 0 {
            let selected = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| PhysicalError::Corrupt)?;
            let bytes = buffer.get_mut(..selected).ok_or(PhysicalError::Corrupt)?;
            file.read_exact(bytes).map_err(|_| PhysicalError::Corrupt)?;
            hasher.update(bytes);
            body_hasher.update(bytes);
            remaining -= u64::try_from(selected).map_err(|_| PhysicalError::Corrupt)?;
        }
        if body_hasher.finalize().as_bytes() != &digest {
            return Err(PhysicalError::Corrupt);
        }
    }
    if file.stream_position().map_err(|_| PhysicalError::Corrupt)? != actual_length
        || hasher.finalize().as_bytes() != expected_id
    {
        return Err(PhysicalError::Corrupt);
    }
    Ok(records)
}

pub(crate) fn collect_physical_garbage(
    root: &Path,
    live_bodies: &BTreeSet<LocalBodyReference>,
    maximum_candidates: u64,
    maximum_object_bytes: u64,
    durability: LocalDurability,
) -> Result<LocalObjectsGarbageCollection, PhysicalError> {
    let mut candidates = 0_u64;
    let mut temporary = Vec::new();
    let segments = scan_segments(root, maximum_candidates, &mut candidates, &mut temporary)?;
    let live_segments = validate_referenced_segments(root, live_bodies, maximum_object_bytes)?;
    let mut report = LocalObjectsGarbageCollection {
        segments_examined: u64::try_from(segments.len()).unwrap_or(u64::MAX),
        ..LocalObjectsGarbageCollection::default()
    };
    let mut changed_parents = BTreeSet::new();
    for (id, path) in segments {
        if !live_segments.contains(&id) {
            fs::remove_file(&path)?;
            report.segments_removed = report.segments_removed.saturating_add(1);
            changed_parents.insert(root.join("segments"));
        }
    }
    for path in temporary {
        fs::remove_file(&path)?;
        report.temporary_files_removed = report.temporary_files_removed.saturating_add(1);
        if let Some(parent) = path.parent() {
            changed_parents.insert(parent.to_path_buf());
        }
    }
    for parent in changed_parents {
        sync_parent(&parent, durability)?;
    }
    Ok(report)
}

fn scan_segments(
    root: &Path,
    maximum_candidates: u64,
    candidates: &mut u64,
    temporary: &mut Vec<PathBuf>,
) -> Result<Vec<([u8; 32], PathBuf)>, PhysicalError> {
    let mut entries = fs::read_dir(root.join("segments"))?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    let mut files = Vec::new();
    for entry in entries {
        if !entry.file_type()?.is_file() {
            return Err(PhysicalError::Corrupt);
        }
        *candidates = candidates.checked_add(1).ok_or(PhysicalError::Invalid(
            "garbage-collection count overflowed",
        ))?;
        if *candidates > maximum_candidates {
            return Err(PhysicalError::Invalid(
                "garbage-collection candidate bound exceeded",
            ));
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or(PhysicalError::Corrupt)?;
        if name.starts_with('.') && name.ends_with(".tmp") {
            temporary.push(entry.path());
            continue;
        }
        let encoded = name
            .strip_suffix(".segment")
            .ok_or(PhysicalError::Corrupt)?;
        if encoded.len() != 64 || !encoded.bytes().all(is_lower_hex) {
            return Err(PhysicalError::Corrupt);
        }
        let mut id = [0_u8; 32];
        decode_lower_hex(encoded, &mut id)?;
        files.push((id, entry.path()));
    }
    Ok(files)
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn decode_lower_hex(encoded: &str, output: &mut [u8]) -> Result<(), PhysicalError> {
    if encoded.len() != output.len().saturating_mul(2) {
        return Err(PhysicalError::Corrupt);
    }
    for (index, &[high, low]) in encoded.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let value = (hex_nibble(high)? << 4) | hex_nibble(low)?;
        *output.get_mut(index).ok_or(PhysicalError::Corrupt)? = value;
    }
    Ok(())
}

fn hex_nibble(byte: u8) -> Result<u8, PhysicalError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(PhysicalError::Corrupt),
    }
}

pub(crate) async fn read_body_at_async(
    root: &Path,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
) -> Result<bytes::Bytes, BodyError> {
    if start > end || end > expected_length {
        return Err(BodyError::Unavailable);
    }
    let (id, offset) = match location {
        LocalBodyLocation::Segment { id, offset } => (id, offset),
        LocalBodyLocation::Journal { offset } => {
            let (journal, offset, digest) = (root.join(JOURNAL_FILE), *offset, *expected_digest);
            let body = acyclic_native_runtime::run_blocking_io(move || {
                let journal = File::open(journal).map_err(|_| BodyError::Unavailable)?;
                read_journal_body(&journal, offset, &digest, expected_length)
            })
            .await
            .map_err(|_| BodyError::Unavailable)??;
            return Ok(body.slice(start..end));
        }
    };
    let segment = segment_path(root, id);
    let (file, file_length) = acyclic_native_runtime::run_blocking_io(move || {
        let file = File::open(segment)?;
        let length = file.metadata()?.len();
        Ok::<_, std::io::Error>((file, length))
    })
    .await
    .map_err(|_| BodyError::Unavailable)?
    .map_err(|_| BodyError::Unavailable)?;
    let selected_length = end.saturating_sub(start);
    let selected_offset = offset
        .checked_add(u64::try_from(start).map_err(|_| BodyError::Unavailable)?)
        .ok_or(BodyError::Unavailable)?;
    let maximum_prefix = SEGMENT_HEADER_BYTES
        .checked_add(MAXIMUM_SEGMENT_BODIES * SEGMENT_RECORD_BYTES)
        .ok_or(BodyError::Unavailable)?;
    let reads = acyclic_native_runtime::read_batch_async(
        file,
        vec![
            acyclic_native_runtime::OwnedRead {
                offset: 0,
                length: maximum_prefix,
            },
            acyclic_native_runtime::OwnedRead {
                offset: selected_offset,
                length: selected_length,
            },
        ],
    )
    .await
    .map_err(|_| BodyError::Unavailable)?;
    let mut reads = reads.into_iter();
    let prefix = reads.next().ok_or(BodyError::Unavailable)?;
    let selected = reads.next().ok_or(BodyError::Unavailable)?;
    if prefix.len() < SEGMENT_HEADER_BYTES
        || prefix.get(..SEGMENT_MAGIC.len()) != Some(SEGMENT_MAGIC)
    {
        return Err(BodyError::Unavailable);
    }
    let count = usize::try_from(u32::from_le_bytes(
        prefix
            .get(SEGMENT_MAGIC.len()..SEGMENT_HEADER_BYTES)
            .ok_or(BodyError::Unavailable)?
            .try_into()
            .map_err(|_| BodyError::Unavailable)?,
    ))
    .map_err(|_| BodyError::Unavailable)?;
    if count == 0 || count > MAXIMUM_SEGMENT_BODIES {
        return Err(BodyError::Unavailable);
    }
    let table_bytes = count
        .checked_mul(SEGMENT_RECORD_BYTES)
        .ok_or(BodyError::Unavailable)?;
    let table_end = SEGMENT_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(BodyError::Unavailable)?;
    let table = prefix
        .get(SEGMENT_HEADER_BYTES..table_end)
        .ok_or(BodyError::Unavailable)?;
    if selected.len() != selected_length {
        return Err(BodyError::Unavailable);
    }
    if !segment_table_matches(
        table,
        file_length,
        *offset,
        expected_digest,
        u64::try_from(expected_length).map_err(|_| BodyError::Unavailable)?,
    )? {
        return Err(BodyError::Unavailable);
    }
    Ok(selected)
}

fn segment_table_matches(
    table: &[u8],
    file_length: u64,
    expected_offset: u64,
    expected_digest: &[u8; 32],
    expected_length: u64,
) -> Result<bool, BodyError> {
    let mut offset = u64::try_from(
        SEGMENT_HEADER_BYTES
            .checked_add(table.len())
            .ok_or(BodyError::Unavailable)?,
    )
    .map_err(|_| BodyError::Unavailable)?;
    for record in table.as_chunks::<SEGMENT_RECORD_BYTES>().0 {
        let digest: [u8; 32] = record
            .get(..32)
            .ok_or(BodyError::Unavailable)?
            .try_into()
            .map_err(|_| BodyError::Unavailable)?;
        let length = u64::from_le_bytes(
            record
                .get(32..SEGMENT_RECORD_BYTES)
                .ok_or(BodyError::Unavailable)?
                .try_into()
                .map_err(|_| BodyError::Unavailable)?,
        );
        let end = offset.checked_add(length).ok_or(BodyError::Unavailable)?;
        if end > file_length {
            return Err(BodyError::Unavailable);
        }
        if offset == expected_offset && digest == *expected_digest && length == expected_length {
            return Ok(true);
        }
        offset = end;
    }
    Ok(false)
}

/// Reads one whole journal-resident body and proves it matches its digest. Inline bodies are
/// small, so every read of one, ranged or not, is authenticated.
pub(crate) fn read_journal_body(
    journal: &File,
    offset: u64,
    expected_digest: &[u8; 32],
    expected_length: usize,
) -> Result<bytes::Bytes, BodyError> {
    if expected_length > MAXIMUM_INLINE_BODY_BYTES {
        return Err(BodyError::Unavailable);
    }
    let mut body = vec![0_u8; expected_length];
    read_exact_at_unsequenced(journal, offset, &mut body)?;
    if blake3::hash(&body).as_bytes() != expected_digest {
        return Err(BodyError::Unavailable);
    }
    Ok(body.into())
}

/// Positional read outside the native per-file operation sequencer. An inline body never
/// changes once its frame is appended, so its read must not queue behind the flush of a
/// later append to the same journal.
fn read_exact_at_unsequenced(file: &File, offset: u64, bytes: &mut [u8]) -> Result<(), BodyError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt as _;
        file.read_exact_at(bytes, offset)
            .map_err(|_| BodyError::Unavailable)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt as _;
        let mut offset = offset;
        let mut bytes = bytes;
        while !bytes.is_empty() {
            let count = file
                .seek_read(bytes, offset)
                .map_err(|_| BodyError::Unavailable)?;
            if count == 0 {
                return Err(BodyError::Unavailable);
            }
            offset = offset
                .checked_add(count as u64)
                .ok_or(BodyError::Unavailable)?;
            bytes = bytes.get_mut(count..).ok_or(BodyError::Unavailable)?;
        }
        Ok(())
    }
}

fn segment_path(root: &Path, id: &[u8; 32]) -> PathBuf {
    root.join("segments").join(format!("{}.segment", hex(id)))
}

/// Opaque location of one authenticated immutable body record.
///
/// This is exposed only through the opt-in `test-support` feature so storage
/// conformance suites can inject faults into the exact body selected by an
/// object identity without duplicating the private segment format parser.
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[derive(Debug)]
pub struct SegmentBody {
    path: PathBuf,
    offset: u64,
    length: usize,
    digest: [u8; 32],
}

/// Resolve the unique authenticated segment record for body digest and bytes.
///
/// This probes physical content only; it does not traverse a logical object's
/// generation graph. Callers that need a logical identity must first resolve
/// that identity and provide its authenticated body digest. Ambiguous physical
/// locations are rejected rather than selecting an arbitrary copy.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn locate_segment_body_for_test(
    root: &Path,
    expected_digest: &[u8; 32],
    expected_bytes: &[u8],
) -> io::Result<SegmentBody> {
    if blake3::hash(expected_bytes).as_bytes() != expected_digest {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected bytes do not match the requested object digest",
        ));
    }
    let mut matches = Vec::new();
    let mut candidates_seen = 0_u64;
    let mut ignored_temporary = Vec::new();
    let segments = scan_segments(root, u64::MAX, &mut candidates_seen, &mut ignored_temporary)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    for (id, path) in segments {
        let records = validate_segment_records(&path, &id, u64::MAX)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        for ((offset, digest), length) in records {
            let length = usize::try_from(length).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment body length is too large",
                )
            })?;
            if digest == *expected_digest
                && length == expected_bytes.len()
                && read_segment_body(&path, offset, length)? == expected_bytes
            {
                matches.push(SegmentBody {
                    path: path.clone(),
                    offset,
                    length,
                    digest,
                });
            }
        }
    }
    match matches.len() {
        1 => Ok(matches.pop().expect("one segment body candidate")),
        0 => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "requested object body is not segment resident",
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "requested object body has multiple segment locations",
        )),
    }
}

#[cfg(feature = "test-support")]
fn read_segment_body(path: &Path, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(std::io::SeekFrom::Start(offset))?;
    let mut body = vec![0_u8; length];
    file.read_exact(&mut body)?;
    Ok(body)
}

/// Replace one already-resolved body with same-length bytes for fault injection.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn corrupt_segment_body_for_test(body: &SegmentBody, replacement: &[u8]) -> io::Result<()> {
    if replacement.len() != body.length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "replacement changes the object body length",
        ));
    }
    let mut physical = fs::read(&body.path)?;
    let start = usize::try_from(body.offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "body offset is too large"))?;
    let end = start
        .checked_add(body.length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "body range overflowed"))?;
    let current = physical
        .get(start..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "body disappeared"))?;
    if blake3::hash(current).as_bytes() != &body.digest {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "resolved body changed before corruption",
        ));
    }
    physical[start..end].copy_from_slice(replacement);
    fs::write(&body.path, physical)
}

/// Delete the entire segment containing one already-resolved body for fault
/// injection. Any co-resident body records are deleted with that segment.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn delete_segment_for_test(body: SegmentBody) -> io::Result<()> {
    fs::remove_file(body.path)
}

#[allow(
    clippy::indexing_slicing,
    reason = "`byte >> 4` and `byte & 0x0f` are both bit operations on a u8 bounded to 0..16, always in range for the 16-entry DIGITS table"
)]
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[(byte >> 4) as usize]));
        output.push(char::from(DIGITS[(byte & 0x0f) as usize]));
    }
    output
}

/// Splits a hex-encoded digest `identity` into its two-character storage-directory
/// prefix and the remaining suffix used as the file stem.
///
/// Every caller passes an `identity` produced by [`hex`] applied to a `[u8; 32]`
/// digest, which always yields exactly 64 lowercase ASCII hex digits. Because the
/// string is provably pure ASCII, splitting at the fixed byte offset `2` can never
/// land inside a multi-byte character, so `split_at` (unlike byte-offset string
/// indexing) is both panic-free here and exempt from `clippy::string_slice`.
fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(durability))
}

#[cfg(not(windows))]
fn publish_new(temporary: &Path, destination: &Path) -> std::io::Result<()> {
    fs::hard_link(temporary, destination)
}

#[cfg(windows)]
fn publish_new(temporary: &Path, destination: &Path) -> std::io::Result<()> {
    acyclic_native_runtime::durable_rename(
        temporary,
        destination,
        acyclic_native_runtime::RenameMode::NoReplace,
    )
}

fn native_durability(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

fn sync_parent(path: &Path, durability: LocalDurability) -> Result<(), PhysicalError> {
    #[cfg(test)]
    tests::PARENT_SYNCS.with(|syncs| syncs.set(syncs.get() + 1));
    acyclic_native_runtime::sync_parent(path, native_durability(durability))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    thread_local! {
        /// Directory synchronizations issued on this thread.
        pub(super) static PARENT_SYNCS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    /// A put that finds its segment already published still makes the
    /// segment's directory entry durable before it is acknowledged: the put
    /// that published it may not have synchronized the directory yet.
    #[test]
    fn a_segment_found_already_published_is_made_durable_again() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        fs::create_dir_all(root.path().join("segments")).unwrap_or_else(|_| unreachable!());
        let body = bytes::Bytes::from(vec![7_u8; 1_024]);
        let digest = *blake3::hash(&body).as_bytes();
        let bodies = [(digest, body)];
        let first = persist_segment(root.path(), &bodies, LocalDurability::FullFlush)
            .unwrap_or_else(|_| unreachable!());
        let before = PARENT_SYNCS.with(std::cell::Cell::get);
        let again = persist_segment(root.path(), &bodies, LocalDurability::FullFlush)
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(again, first);
        assert_eq!(PARENT_SYNCS.with(std::cell::Cell::get), before + 1);
    }

    #[cfg(feature = "test-support")]
    #[test]
    fn test_support_rejects_ambiguous_body_locations() {
        let root = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        fs::create_dir_all(root.path().join("segments")).unwrap_or_else(|_| unreachable!());
        let body = bytes::Bytes::from(vec![7_u8; 1_024]);
        let digest = *blake3::hash(&body).as_bytes();
        let first_other = bytes::Bytes::from(vec![8_u8; 1_024]);
        let second_other = bytes::Bytes::from(vec![9_u8; 1_024]);
        persist_segment(
            root.path(),
            &[
                (digest, body.clone()),
                (*blake3::hash(&first_other).as_bytes(), first_other),
            ],
            LocalDurability::FullFlush,
        )
        .unwrap_or_else(|_| unreachable!());
        persist_segment(
            root.path(),
            &[
                (digest, body.clone()),
                (*blake3::hash(&second_other).as_bytes(), second_other),
            ],
            LocalDurability::FullFlush,
        )
        .unwrap_or_else(|_| unreachable!());

        let error = locate_segment_body_for_test(root.path(), &digest, &body)
            .expect_err("two valid segment locations must remain ambiguous");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
