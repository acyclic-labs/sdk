//! Private authenticated immutable segments and journal body reads.
use crate::body::{LocalBodyLocation, LocalBodyReference};
use crate::v1::{Error, wire::ErrorCode::Unavailable};
use crate::{LocalDurability, LocalObjectsGarbageCollection};
use crate::v1::{NativeObjectsError, Publication};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};
const JOURNAL_FILE: &str = "mutations.log";
const MAXIMUM_INLINE_BODY_BYTES: usize = 64 * 1_024;
const SEGMENT_MAGIC: &[u8; 24] = b"ACYCLIC-OBJECT-SEGMENT\0\x02";
const SEGMENT_HEADER_BYTES: usize = SEGMENT_MAGIC.len() + 4;
const SEGMENT_RECORD_BYTES: usize = 32 + 8;
pub(crate) const MAXIMUM_SEGMENT_BODIES: usize = 1_024;
pub(crate) const MAXIMUM_SEGMENT_BYTES: usize = 4 * 1024 * 1024;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
/// One store's directory and a shared read handle on its journal, so a read of a journal-resident body does not reopen the journal (a costly open on Windows).
pub(crate) struct LocalRoot {
    pub(crate) path: PathBuf,
    journal: RwLock<Option<File>>,

    #[cfg(test)]
    read_gate: Option<JournalReadGate>,
}

/// One test's worker admission gate; ordinary roots never install one.
#[cfg(test)]
struct JournalReadGate {
    admitted: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    completed: std::sync::mpsc::Sender<Result<acyclic_stream::OwnedBytes, Error>>,
}

/// Roots are equal when they name the same directory; the handle is a cache.
impl PartialEq for LocalRoot {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl LocalRoot {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            journal: RwLock::new(None),

            #[cfg(test)]
            read_gate: None,
        }
    }

    /// Runs `replace` with the journal read handle closed and no reader able to reopen it, so Windows may replace the file and no reader keeps a handle on the replaced one.
    pub(crate) fn replace_journal(
        &self,
        replace: impl FnOnce() -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let mut journal = self
            .journal
            .write()
            .map_err(|_| std::io::Error::other("journal reader poisoned"))?;
        *journal = None;
        replace()
    }

    pub(crate) fn read_journal_body(
        &self,
        offset: u64,
        expected_digest: &[u8; 32],
        expected_length: usize,
    ) -> Result<acyclic_stream::OwnedBytes, Error> {
        #[cfg(test)]
        if let Some(gate) = &self.read_gate {
            gate.admitted.send(()).map_err(|_| Error::from(Unavailable))?;
            gate.release
                .lock()
                .map_err(|_| Error::from(Unavailable))?
                .recv()
                .map_err(|_| Error::from(Unavailable))?;
            let result = self.read_journal_body_inner(offset, expected_digest, expected_length);
            let _ = gate.completed.send(result.clone());
            return result;
        }
        self.read_journal_body_inner(offset, expected_digest, expected_length)
    }

    fn read_journal_body_inner(
        &self,
        offset: u64,
        expected_digest: &[u8; 32],
        expected_length: usize,
    ) -> Result<acyclic_stream::OwnedBytes, Error> {
        if let Some(journal) = self
            .journal
            .read()
            .map_err(|_| Error::from(Unavailable))?
            .as_ref()
        {
            return read_journal_body(journal, offset, expected_digest, expected_length);
        }
        let mut journal = self.journal.write().map_err(|_| Error::from(Unavailable))?;
        if journal.is_none() {

            *journal =
                Some(File::open(self.path.join(JOURNAL_FILE)).map_err(|_| Error::from(Unavailable))?);
        }
        read_journal_body(
            journal.as_ref().ok_or(Error::from(Unavailable))?,
            offset,
            expected_digest,
            expected_length,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PhysicalError {
    #[error("invalid private Objects storage: {0}")]
    Invalid(&'static str),
    #[error("corrupt private Objects storage")]
    Corrupt,
    #[error("Objects storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
}
/// The caller retains `publication` until the returned offsets have been freed.
pub(crate) fn persist_segment<B: AsRef<[u8]>>(
    root: &Path,
    bodies: &[([u8; 32], B)],
    durability: LocalDurability,
    publication: &mut Publication,
) -> Result<([u8; 32], Vec<u64>), NativeObjectsError> {
    let published = write_segment(root, bodies, durability, publication)?;
    sync_segment_directory(root, durability, publication)?;
    Ok(published)
}

pub(crate) fn sync_segment_directory(
    root: &Path,
    durability: LocalDurability,
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    let parent = publication_path(root, "segments", publication)?;
    if let Some(meter) = &publication.meter {
        meter.check()?;
        meter.admit_read(0)?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            options.write(true).share_mode(7).custom_flags(0x0200_0000);
        }
        let directory = options.open(&parent)?;
        acyclic_native_runtime::sync_file(
            &directory, native_durability(durability), Some(meter.as_ref()),
        )?;

        Ok(())
    } else {
        sync_parent(&parent, durability).map_err(native_write_error)
    }
}

pub(crate) fn sync_publication(
    file: &File,
    durability: LocalDurability,
    publication: &Publication,
) -> Result<(), NativeObjectsError> {
    match &publication.meter {
        Some(meter) => acyclic_native_runtime::sync_file(
            file, native_durability(durability), Some(meter.as_ref()),
        )?,
        None => sync_file(file, durability)?,
    }
    Ok(())
}

/// Advances the ordinary append cursor as well as the physical positional write, so a later unmetered append observes the same journal tail.
pub(crate) fn write_publication(
    file: &mut File,
    offset: u64,
    bytes: &[u8],
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    if let Some(meter) = &publication.meter {
        acyclic_native_runtime::write_all_at(file, offset, bytes, Some((meter.as_ref(), false)))?;
        let end = offset.checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("publication offset overflow"))?;
        loop {
            meter.check()?;
            meter.admit_write(0)?;
            match file.seek(std::io::SeekFrom::Start(end)) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => { result?; break; }
            }
        }
        meter.check()?;
    } else {
        file.write_all(bytes)?;
    }
    Ok(())
}

pub(crate) fn open_publication(
    path: &Path,
    publication: &Publication,
) -> Result<File, NativeObjectsError> {
    publication_namespace(publication, || {
        OpenOptions::new().create(true).truncate(true).read(true).write(true).open(path)
    })
}

/// Uses the same publication engine as ordinary writes.
pub(crate) fn write_segment<B: AsRef<[u8]>>(
    root: &Path,
    bodies: &[([u8; 32], B)],
    durability: LocalDurability,
    publication: &mut Publication,
) -> Result<([u8; 32], Vec<u64>), NativeObjectsError> {
    let span = crate::obs::span!(TRACE, "acyclic.objects.segment.write", items = bodies.len());
    crate::obs::scoped(&span, || write_segment_inner(root, bodies, durability, publication))
}

fn native_write_error(error: PhysicalError) -> NativeObjectsError {
    match error {
        PhysicalError::Io(error) => NativeObjectsError::Admission(error),
        PhysicalError::Invalid(_) | PhysicalError::Corrupt =>
            NativeObjectsError::Service(crate::v1::wire::ErrorCode::Unavailable.into()),
    }
}

fn publication_path(
    parent: &Path,
    name: &str,
    publication: &mut Publication,
) -> Result<PathBuf, NativeObjectsError> {
    let separator = std::path::MAIN_SEPARATOR_STR;
    let capacity = parent.as_os_str().len().checked_add(separator.len())
        .and_then(|length| length.checked_add(name.len()))
        .ok_or_else(|| native_write_error(PhysicalError::Invalid("publication path overflow")))?;
    publication.allocate(capacity)?;
    let mut path = std::ffi::OsString::new();
    path.try_reserve_exact(capacity).map_err(|error| publication.allocation_failed(error))?;
    publication.retain_capacity(capacity, path.capacity())?;
    publication.codec(0, capacity, 0, || {
        path.push(parent);
        path.push(separator);
        path.push(name);
        PathBuf::from(path)
    })
}

fn publication_string(
    capacity: usize,
    publication: &mut Publication,
) -> Result<String, NativeObjectsError> {
    publication.allocate(capacity)?;
    let mut value = String::new();
    value.try_reserve_exact(capacity).map_err(|error| publication.allocation_failed(error))?;
    publication.retain_capacity(capacity, value.capacity())?;
    Ok(value)
}

fn publication_namespace<T>(
    publication: &Publication,
    work: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, NativeObjectsError> {
    if let Some(meter) = &publication.meter {
        meter.check()?;
        meter.admit_write(0)?;
    }
    let result = work()?;
    publication.check()?;
    Ok(result)
}

fn publication_probe<T>(
    publication: &Publication,
    work: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, NativeObjectsError> {
    if let Some(meter) = &publication.meter {
        meter.check()?;
        meter.admit_read(0)?;
    }
    let result = work()?;
    publication.check()?;
    Ok(result)
}

#[allow(clippy::too_many_lines, reason = "ordered immutable segment publication")]
fn write_segment_inner<B: AsRef<[u8]>>(
    root: &Path,
    bodies: &[([u8; 32], B)],
    durability: LocalDurability,
    publication: &mut Publication,
) -> Result<([u8; 32], Vec<u64>), NativeObjectsError> {
    let invalid = || native_write_error(PhysicalError::Invalid("invalid object segment"));
    publication.check()?;
    if bodies.is_empty() || bodies.len() > MAXIMUM_SEGMENT_BODIES {
        return Err(invalid());
    }
    if bodies.len() > 1 && bodies.iter().map(|(_, body)| body.as_ref().len())
        .try_fold(0_usize, usize::checked_add)
        .is_none_or(|bytes| bytes > MAXIMUM_SEGMENT_BYTES) {
        return Err(invalid());
    }
    let count = u32::try_from(bodies.len()).map_err(|_| invalid())?;
    let table_bytes = bodies.len().checked_mul(SEGMENT_RECORD_BYTES).ok_or_else(invalid)?;
    let metadata_capacity = SEGMENT_HEADER_BYTES.checked_add(table_bytes).ok_or_else(invalid)?;
    let mut offsets = Vec::new();
    publication.reserve(&mut offsets, bodies.len())?;
    let mut metadata = Vec::new();
    publication.reserve(&mut metadata, metadata_capacity)?;
    publication.codec(metadata_capacity, metadata_capacity, 0, || {
        metadata.extend_from_slice(SEGMENT_MAGIC);
        metadata.extend_from_slice(&count.to_le_bytes());
        for (digest, body) in bodies {
            metadata.extend_from_slice(digest);
            metadata.extend_from_slice(&(body.as_ref().len() as u64).to_le_bytes());
        }
    })?;
    let mut position = metadata_capacity as u64;
    for (_, body) in bodies {
        offsets.push(position);
        position = position.checked_add(body.as_ref().len() as u64).ok_or_else(invalid)?;
    }
    let mut hasher = blake3::Hasher::new();
    publication.codec(0, 0, metadata.len(), || { hasher.update(&metadata); })?;
    for (_, body) in bodies {
        publication.codec(0, 0, body.as_ref().len(), || { hasher.update(body.as_ref()); })?;
    }
    let id = *hasher.finalize().as_bytes();
    let parent = publication_path(root, "segments", publication)?;
    let mut identity = publication_string(64, publication)?;
    publication.codec(64, 0, 0, || {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        for byte in id {
            identity.push(char::from(DIGITS[usize::from(byte >> 4)]));
            identity.push(char::from(DIGITS[usize::from(byte & 15)]));
        }
    })?;
    let mut name = publication_string(72, publication)?;
    publication.codec(72, 72, 0, || {
        name.push_str(&identity);
        name.push_str(".segment");
    })?;
    let destination = publication_path(&parent, &name, publication)?;
    let existing = publication_probe(publication, || fs::metadata(&destination));
    match existing {
        Ok(_) => {
            validate_segment_publication(&destination, &id, position, publication)?;
            return Ok((id, offsets));
        }
        Err(NativeObjectsError::Admission(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut opened = None;
    for _ in 0..1_024 {
        use std::fmt::Write as _;
        let nonce = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let name_length = 1 + 64 + 1 + (pid.max(1).ilog10() as usize + 1)
            + 1 + (nonce.max(1).ilog10() as usize + 1) + 4;
        let mut temporary_name = publication_string(name_length, publication)?;
        publication.codec(name_length, name_length, 0, || {
            let _ = write!(temporary_name, ".{identity}.{pid}-{nonce}.tmp");
        })?;
        let temporary = publication_path(&parent, &temporary_name, publication)?;
        let result = publication_namespace(publication, || {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(windows)]
            if publication.meter.is_some() {
                use std::os::windows::fs::OpenOptionsExt as _;
                options.custom_flags(0x4000_0000);
            }
            options.open(&temporary)
        });
        match result {
            Ok(file) => { opened = Some((temporary, file)); break; }
            Err(NativeObjectsError::Admission(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    let (temporary, mut file) = opened.ok_or_else(invalid)?;
    let write_result = (|| -> Result<(), NativeObjectsError> {
        if let Some(meter) = &publication.meter {
            acyclic_native_runtime::write_all_at(&file, 0, &metadata, Some((meter.as_ref(), true)))?;
            for ((_, body), offset) in bodies.iter().zip(&offsets) {
                acyclic_native_runtime::write_all_at(
                    &file, *offset, body.as_ref(), Some((meter.as_ref(), true)),
                )?;
            }
            acyclic_native_runtime::sync_file(
                &file, native_durability(durability), Some(meter.as_ref()),
            )?;
        } else {
            file.write_all(&metadata)?;
            for (_, body) in bodies { file.write_all(body.as_ref())?; }
            sync_file(&file, durability)?;
        }
        Ok(())
    })();
    drop(file);
    if let Err(error) = write_result {
        let _ = publication_namespace(publication, || fs::remove_file(&temporary));
        return Err(error);
    }
    // Use the same no-replace engine with original pathname custody.
    let published = match &publication.meter {
        Some(meter) => acyclic_native_runtime::publish_new(
            &temporary, &destination, Some(meter),
        ),
        None => acyclic_native_runtime::publish_new(&temporary, &destination, None),
    }.map_err(NativeObjectsError::Admission);
    match published {
        Ok(()) => {}
        Err(NativeObjectsError::Admission(error)) if error.kind() == std::io::ErrorKind::AlreadyExists =>
            validate_segment_publication(&destination, &id, position, publication)?,
        Err(error) => {
            let _ = publication_namespace(publication, || fs::remove_file(&temporary));
            return Err(error);
        }
    }
    match publication_namespace(publication, || fs::remove_file(&temporary)) {
        Ok(()) => {}
        Err(NativeObjectsError::Admission(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok((id, offsets))
}

fn validate_segment_publication(
    path: &Path,
    expected_id: &[u8; 32],
    expected_length: u64,
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    let mut file = publication_probe(publication, || File::open(path))?;
    let length = publication_probe(publication, || file.metadata())?.len();
    if length != expected_length { return Err(native_write_error(PhysicalError::Corrupt)); }
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if let Some(meter) = &publication.meter {
            meter.check()?;
            meter.admit_read(buffer.len())?;
        }
        let read = file.read(&mut buffer)?;
        if let Some(meter) = &publication.meter { meter.record_read(read)?; }
        if read == 0 { break; }
        publication.codec(0, 0, read, || { hasher.update(&buffer[..read]); })?;
    }
    if hasher.finalize().as_bytes() != expected_id {
        return Err(native_write_error(PhysicalError::Corrupt));
    }
    Ok(())
}

/// Authenticated body lengths of one segment, keyed by offset and digest: an empty body shares its offset with the body that follows it.
type ValidatedSegmentRecords = BTreeMap<(u64, [u8; 32]), u64>;

/// Authenticates every segment a live body references and returns their identities.
pub(crate) fn validate_referenced_segments(
    root: &Path,
    bodies: &BTreeSet<LocalBodyReference>,
    maximum_object_bytes: u64,
) -> Result<BTreeSet<[u8; 32]>, PhysicalError> {
    let span = crate::obs::span!(
        TRACE,
        "acyclic.objects.segment.validate",
        items = bodies.len(),
    );
    crate::obs::scoped(&span, || {
        validate_referenced_segments_inner(root, bodies, maximum_object_bytes)
    })
}

fn validate_referenced_segments_inner(
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
    root: &Arc<LocalRoot>,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
) -> Result<acyclic_stream::OwnedBytes, Error> {
    let backend = match location {
        LocalBodyLocation::Segment { .. } => "segment",
        LocalBodyLocation::Journal { .. } => "journal",
    };
    let span = crate::obs::span!(
        TRACE,
        "acyclic.objects.physical.read",
        backend,
        bytes = end.saturating_sub(start),
        body_bytes = expected_length,
    );
    crate::obs::traced(
        span,
        read_body_at_inner_async(root, expected_digest, expected_length, location, start, end),
    )
    .await
}

async fn read_body_at_inner_async(
    root: &Arc<LocalRoot>,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
) -> Result<acyclic_stream::OwnedBytes, Error> {
    if start > end || end > expected_length {
        return Err(Error::from(Unavailable));
    }
    let (id, offset) = match location {
        LocalBodyLocation::Segment { id, offset } => (id, offset),
        LocalBodyLocation::Journal { offset } => {
            let (root, offset, digest) = (Arc::clone(root), *offset, *expected_digest);
            let body = acyclic_native_runtime::run_blocking_io(move || {
                root.read_journal_body(offset, &digest, expected_length)
            }, None).expect("unmetered task construction cannot fail")
            .await
            .map_err(|_| Error::from(Unavailable))??;
            return Ok(body.slice(start..end));
        }
    };
    let segment = segment_path(&root.path, id);
    let (file, file_length) = acyclic_native_runtime::run_blocking_io(move || {
        let file = File::open(segment)?;
        let length = file.metadata()?.len();
        Ok::<_, std::io::Error>((file, length))
    }, None).expect("unmetered task construction cannot fail")
    .await
    .map_err(|_| Error::from(Unavailable))?
    .map_err(|_| Error::from(Unavailable))?;
    let selected_length = end.saturating_sub(start);
    let selected_offset = offset
        .checked_add(u64::try_from(start).map_err(|_| Error::from(Unavailable))?)
        .ok_or(Error::from(Unavailable))?;
    let maximum_prefix = SEGMENT_HEADER_BYTES
        .checked_add(MAXIMUM_SEGMENT_BODIES * SEGMENT_RECORD_BYTES)
        .ok_or(Error::from(Unavailable))?;
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
    .map_err(|_| Error::from(Unavailable))?;
    let mut reads = reads.into_iter();
    let prefix = reads.next().ok_or(Error::from(Unavailable))?;
    let selected = reads.next().ok_or(Error::from(Unavailable))?;
    let table = segment_table(&prefix)?;
    if selected.len() != selected_length {
        return Err(Error::from(Unavailable));
    }
    if !segment_table_matches(
        table,
        file_length,
        *offset,
        expected_digest,
        u64::try_from(expected_length).map_err(|_| Error::from(Unavailable))?, &mut 0,
    )? {
        return Err(Error::from(Unavailable));
    }
    // Runtime batch buffers are opaque here; preserve honest unknown capacity.
    Ok(selected.into())
}

pub(crate) struct LocalPath {
    pub(crate) path: PathBuf,
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}
pub(crate) fn local_path(
    root: &Path,
    suffix: &str,
    meter: Option<&Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
) -> Result<LocalPath, NativeObjectsError> {
    let maximum = root.as_os_str().len().checked_add(suffix.len()).and_then(|length| length.checked_add(1))
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    let mut allocation = meter.map(|meter| acyclic_native_runtime::ReadAllocation::new(meter, maximum)).transpose()?;
    let mut path = PathBuf::new();
    path.try_reserve_exact(maximum).map_err(|error| match meter {
        Some(meter) => NativeObjectsError::Admission(meter.allocation_failed(error)),
        None => NativeObjectsError::Service(crate::v1::wire::ErrorCode::Unavailable.into()),
    })?;
    if let Some(allocation) = &mut allocation { allocation.reconcile_capacity(path.capacity())?; }
    if let Some(meter) = meter { meter.admit_codec(0, maximum as u64, 0)?; }
    path.push(root);
    path.push(suffix);
    if let Some(meter) = meter { meter.record_codec(0, path.as_os_str().len() as u64, 0)?; }
    Ok(LocalPath { path, _allocation: allocation })
}

pub(crate) async fn read_body_at_native(
    root: &Arc<LocalRoot>,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
    meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<acyclic_stream::OwnedBytes, crate::v1::NativeObjectsError> {
    let backend = match location {
        LocalBodyLocation::Segment { .. } => "segment",
        LocalBodyLocation::Journal { .. } => "journal",
    };
    let span = crate::obs::span!(
        TRACE, "acyclic.objects.physical.read",
        backend, bytes = end.saturating_sub(start), body_bytes = expected_length,
    );
    crate::obs::traced(span, read_body_at_native_inner(
        root, expected_digest, expected_length, location, start, end, meter,
    )).await
}

/// The same journal authentication and segment-table selection as ordinary reads, with original-meter buffers and shared controls prepared before physical IO.
async fn read_body_at_native_inner(
    root: &Arc<LocalRoot>,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
    meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<acyclic_stream::OwnedBytes, crate::v1::NativeObjectsError> {
    use crate::v1::{Error, NativeObjectsError, wire::ErrorCode::Unavailable};
    let unavailable = || NativeObjectsError::from(Error::from(Unavailable));
    meter.check()?;
    if start > end || end > expected_length { return Err(unavailable()); }
    if matches!(location, LocalBodyLocation::Journal { .. }) && expected_length > MAXIMUM_INLINE_BODY_BYTES {
        return Err(unavailable());
    }
    let length = match location {
        LocalBodyLocation::Journal { .. } => expected_length,
        LocalBodyLocation::Segment { .. } => end - start,
    };
    let storage = crate::body::prepare_native_bytes(length, &meter)?;
    let maximum_prefix = SEGMENT_HEADER_BYTES + MAXIMUM_SEGMENT_BODIES * SEGMENT_RECORD_BYTES;
    let prefix = match location {
        LocalBodyLocation::Segment { .. } => Some(crate::body::prepare_native_bytes(maximum_prefix, &meter)?),
        LocalBodyLocation::Journal { .. } => None,
    };
    let (root, location) = (Arc::clone(root), location.clone());
    let open_meter = Arc::clone(&meter);
    let opened = acyclic_native_runtime::run_blocking_io(move || {
        open_meter.check()?;
        let file = match &location {
            LocalBodyLocation::Journal { .. } => {
                let mut journal = root.journal.write()
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
                if journal.is_none() {
                    let path = local_path(&root.path, JOURNAL_FILE, Some(&open_meter))?;
                    open_meter.admit_read(0)?;
                    *journal = Some(File::open(&path.path)?);

                    drop(path);
                }
                open_meter.admit_read(0)?;
                journal.as_ref().ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?.try_clone()?
            }
            LocalBodyLocation::Segment { id, .. } => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let mut name = [0u8; 72];
                open_meter.admit_codec(64, 0, 0)?;
                for (index, byte) in id.iter().copied().enumerate() {
                    name[index * 2] = HEX[(byte >> 4) as usize];
                    name[index * 2 + 1] = HEX[(byte & 15) as usize];
                }
                name[64..].copy_from_slice(b".segment");
                open_meter.record_codec(64, 0, 0)?;
                let name = std::str::from_utf8(&name)
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
                let path = local_path(&root.path, "segments", Some(&open_meter))?;
                let file_path = local_path(&path.path, name, Some(&open_meter))?;
                open_meter.admit_read(0)?;
                let file = File::open(&file_path.path)?;
                drop(path);
                file
            }
        };
        open_meter.admit_read(0)?;
        let file_length = file.metadata()?.len();
        let file = acyclic_native_runtime::NativeFile::from_file(file, Some(acyclic_native_runtime::NativeFileMeter::Owned(open_meter)))
            .map_err(|(error, file)| { drop(file); error })?;
        Ok::<_, NativeObjectsError>((file, file_length, location))
    }, Some((Arc::clone(&meter), false)))?;
    let (file, file_length, location) = opened.await.map_err(|_| unavailable())??;
    match location {
        LocalBodyLocation::Journal { offset } => {
            let (storage, actual) = file.read_prepared_bounded_async(
                offset, storage, expected_length, true, Arc::clone(&meter),
            )?.await?;
            let body = storage.freeze(actual).map_err(|_| unavailable())?;
            meter.admit_codec(0, 0, actual as u64)?;
            let digest = blake3::hash(body.as_slice());
            meter.record_codec(0, 0, actual as u64)?;
            if actual != expected_length || digest.as_bytes() != expected_digest { return Err(unavailable()); }
            Ok(body.slice(start..end))
        }
        LocalBodyLocation::Segment { offset, .. } => {
            let prefix = prefix.ok_or_else(unavailable)?;
            let (prefix, actual) = file.read_prepared_bounded_async(
                0, prefix, maximum_prefix, false, Arc::clone(&meter),
            )?.await?;
            let prefix = prefix.freeze(actual).map_err(|_| unavailable())?;
            let table = segment_table(&prefix)?;
            let count = table.len() / SEGMENT_RECORD_BYTES;
            meter.admit_examination(count as u64, 0)?;
            let mut examined = 0;
            let matches = segment_table_matches(
                table, file_length, offset, expected_digest, expected_length as u64, &mut examined,
            );
            meter.record_examination(examined, 0)?;
            let matches = matches.map_err(|_| unavailable())?;
            if !matches { return Err(unavailable()); }
            let selected_offset = offset.checked_add(start as u64).ok_or_else(unavailable)?;
            let (storage, actual) = file.read_prepared_bounded_async(
                selected_offset, storage, length, true, meter,
            )?.await?;
            storage.freeze(actual).map_err(|_| unavailable())
        }
    }
}

fn segment_table(prefix: &[u8]) -> Result<&[u8], Error> {
    if prefix.get(..SEGMENT_MAGIC.len()) != Some(SEGMENT_MAGIC) { return Err(Error::from(Unavailable)); }
    let count = u32::from_le_bytes(prefix.get(SEGMENT_MAGIC.len()..SEGMENT_HEADER_BYTES)
        .ok_or(Error::from(Unavailable))?.try_into().map_err(|_| Error::from(Unavailable))?) as usize;
    if count == 0 || count > MAXIMUM_SEGMENT_BODIES { return Err(Error::from(Unavailable)); }
    prefix.get(SEGMENT_HEADER_BYTES..SEGMENT_HEADER_BYTES + count * SEGMENT_RECORD_BYTES)
        .ok_or(Error::from(Unavailable))
}

fn segment_table_matches(
    table: &[u8],
    file_length: u64,
    expected_offset: u64,
    expected_digest: &[u8; 32],
    expected_length: u64,
    examined: &mut u64,
) -> Result<bool, Error> {
    let mut offset = u64::try_from(
        SEGMENT_HEADER_BYTES
            .checked_add(table.len())
            .ok_or(Error::from(Unavailable))?,
    )
    .map_err(|_| Error::from(Unavailable))?;
    for record in table.as_chunks::<SEGMENT_RECORD_BYTES>().0 {
        *examined += 1;
        let digest: [u8; 32] = record
            .get(..32)
            .ok_or(Error::from(Unavailable))?
            .try_into()
            .map_err(|_| Error::from(Unavailable))?;
        let length = u64::from_le_bytes(
            record
                .get(32..SEGMENT_RECORD_BYTES)
                .ok_or(Error::from(Unavailable))?
                .try_into()
                .map_err(|_| Error::from(Unavailable))?,
        );
        let end = offset.checked_add(length).ok_or(Error::from(Unavailable))?;
        if end > file_length {
            return Err(Error::from(Unavailable));
        }
        if offset == expected_offset && digest == *expected_digest && length == expected_length {
            return Ok(true);
        }
        offset = end;
    }
    Ok(false)
}

/// Reads and authenticates one whole journal-resident body.
fn read_journal_body(
    journal: &File,
    offset: u64,
    expected_digest: &[u8; 32],
    expected_length: usize,
) -> Result<acyclic_stream::OwnedBytes, Error> {
    if expected_length > MAXIMUM_INLINE_BODY_BYTES {
        return Err(Error::from(Unavailable));
    }
    let mut body = vec![0_u8; expected_length];
    read_exact_at_unsequenced(journal, offset, &mut body)?;
    if blake3::hash(&body).as_bytes() != expected_digest {
        return Err(Error::from(Unavailable));
    }
    Ok(body.into())
}

/// Positional read outside the native per-file operation sequencer.
fn read_exact_at_unsequenced(file: &File, offset: u64, bytes: &mut [u8]) -> Result<(), Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt as _;
        file.read_exact_at(bytes, offset)
            .map_err(|_| Error::from(Unavailable))
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt as _;
        let mut offset = offset;
        let mut bytes = bytes;
        while !bytes.is_empty() {
            let count = file
                .seek_read(bytes, offset)
                .map_err(|_| Error::from(Unavailable))?;
            if count == 0 {
                return Err(Error::from(Unavailable));
            }
            offset = offset
                .checked_add(count as u64)
                .ok_or(Error::from(Unavailable))?;
            bytes = bytes.get_mut(count..).ok_or(Error::from(Unavailable))?;
        }
        Ok(())
    }
}

fn segment_path(root: &Path, id: &[u8; 32]) -> PathBuf {
    root.join("segments").join(format!("{}.segment", hex(id)))
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

/// Splits a hex-encoded digest `identity` into its two-character storage-directory prefix and the remaining suffix used as the file stem.
fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(durability), None)
}


pub(crate) fn native_durability(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

fn sync_parent(path: &Path, durability: LocalDurability) -> Result<(), PhysicalError> {

    acyclic_native_runtime::sync_parent(path, native_durability(durability))?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one admission/cancellation/release scenario keeps its owned worker cleanup explicit"
    )]
    async fn admitted_read_drop_retains_root_until_worker_finishes() {
        use crate::body::StoredBody;
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        // Release the blocked worker during unwinding too.
        struct ReleaseOnDrop(mpsc::Sender<()>);
        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }

        {
            let directory = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
            let bytes = acyclic_stream::OwnedBytes::from_static(b"pending private journal body");
            fs::write(directory.path().join(JOURNAL_FILE), &bytes)
                .unwrap_or_else(|_| unreachable!());
            let (admitted_tx, admitted_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (completed_tx, completed_rx) = mpsc::channel();
            let release = ReleaseOnDrop(release_tx);
            let mut root = LocalRoot::new(directory.path().to_path_buf());
            root.read_gate = Some(JournalReadGate {
                admitted: admitted_tx,
                release: std::sync::Mutex::new(release_rx),
                completed: completed_tx,
            });
            let root = Arc::new(root);
            let weak = Arc::downgrade(&root);
            let body = StoredBody::Local {
                root: Arc::clone(&root),
                digest: *blake3::hash(&bytes).as_bytes(),
                length: bytes.len(),
                location: LocalBodyLocation::Journal { offset: 0 },
            };
            let mut read = Box::pin(body.read(0, bytes.len(), crate::body::BodyReadMode::Ordinary));
            let waker = futures::task::noop_waker();
            let mut context = std::task::Context::from_waker(&waker);
            assert!(read.as_mut().poll(&mut context).is_pending());
            admitted_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|_| unreachable!());
            drop(read);
            drop(body);
            drop(root);
            let retained_while_blocked = weak.upgrade().is_some();
            drop(release);
            let completed = completed_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|_| unreachable!())
                .unwrap_or_else(|_| unreachable!());
            assert_eq!(completed, bytes, "the detached read still finishes safely");
            assert_eq!(completed.retained_capacity(), Some(bytes.len()));
            assert!(retained_while_blocked, "the admitted worker owns its root");
            let deadline = Instant::now() + Duration::from_secs(5);
            while weak.strong_count() != 0
                && Instant::now() < deadline
            {
                std::thread::yield_now();
            }
            assert_eq!(
                weak.strong_count(),
                0,
                "the completed worker releases its root"
            );
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one ordered scenario keeps each fault next to the outcome it checks"
    )]
    async fn physical_reads_validate_ranges_and_corruption() {
        {
            let directory = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
            fs::create_dir_all(directory.path().join("segments"))
                .unwrap_or_else(|_| unreachable!());
            let body = acyclic_stream::OwnedBytes::from_static(b"private body bytes");
            let digest = *blake3::hash(&body).as_bytes();
            let (id, offsets) = persist_segment(
                directory.path(),
                &[(digest, body.clone())],
                LocalDurability::FullFlush,
                &mut Publication::new(None),
            )
            .unwrap_or_else(|_| unreachable!());
            assert!(write_segment::<acyclic_stream::OwnedBytes>(directory.path(), &[], LocalDurability::FullFlush, &mut Publication::new(None)).is_err());
            let location = LocalBodyLocation::Segment {
                id,
                offset: *offsets.first().unwrap_or_else(|| unreachable!()),
            };
            let root = Arc::new(LocalRoot::new(directory.path().to_path_buf()));
            let read = |location: &LocalBodyLocation, start, end| {
                futures::executor::block_on(read_body_at_async(
                    &root,
                    &digest,
                    body.len(),
                    location,
                    start,
                    end,
                ))
            };
            let selected = read(&location, 1, 5).unwrap_or_else(|_| unreachable!());
            assert_eq!(selected, body.slice(1..5));
            assert!(read(&location, 5, 1).is_err());
            let references = [LocalBodyReference {
                digest,
                length: body.len(),
                location: location.clone(),
            }]
            .into_iter()
            .collect();
            assert!(validate_referenced_segments(directory.path(), &references, 1024).is_ok());
            let segment = segment_path(directory.path(), &id);
            OpenOptions::new()
                .write(true)
                .open(&segment)
                .unwrap_or_else(|_| unreachable!())
                .set_len(SEGMENT_HEADER_BYTES as u64)
                .unwrap_or_else(|_| unreachable!());
            assert!(read(&location, 0, body.len()).is_err());
            assert!(validate_referenced_segments(directory.path(), &references, 1024).is_err());
            fs::write(directory.path().join(JOURNAL_FILE), &body)
                .unwrap_or_else(|_| unreachable!());
            let journal = LocalBodyLocation::Journal { offset: 0 };
            assert!(read(&journal, 0, body.len()).is_ok());
            fs::write(directory.path().join(JOURNAL_FILE), vec![0; body.len()])
                .unwrap_or_else(|_| unreachable!());
            assert!(read(&journal, 0, body.len()).is_err());
        }

    }
}
