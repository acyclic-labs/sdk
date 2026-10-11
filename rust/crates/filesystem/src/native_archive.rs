//! Source-bound native directory capture and bounded canonical GNU tar/gzip.
//!
//! Archive transport never substitutes for the original native root receipt.
//! Captures retain the held capability and journal the original generation,
//! policy and physical preimages through the existing native state store.

use crate::kernel::{AsyncBlobSource, ByteRange, FileKind, FileMetadata, LogicalName, MetadataField, NameEncoding, NamespacePath};
use crate::materializer::{capture_native_preimages, MaterializationPreimage};
use crate::native_capture::{capture_baseline_from_root, ensure_current_host_node, host_path_to_namespace, CaptureOptions, CapturePolicy, CaptureReceipt, HostSnapshot};
use crate::native_host::HostRoot;
use crate::path::PortablePath;
use crate::record_store::Revisioned;
use crate::{AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, Checkout, CheckoutCommitOutcome, FileId, GenerationId, LocalCoreStateStore, NativeRootIdentity, OperationId, OperationReceipt, PinnedReader, PublicationPermit, ResolvedFile, StagedContent, VolumeConfig, VolumeId, WorkBudget, WorkCounters};
use bytes::{Bytes, BytesMut};
use flate2::{bufread::GzDecoder, Compression, GzBuilder};
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeSeq};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, BufRead, BufReader, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tar::{EntryType, Header};
use thiserror::Error;

/// The existing original-upload body admission bound, also enforced expanded.
pub const MAXIMUM_NATIVE_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
/// The existing original-upload ordered raw-body chunk size.
pub const NATIVE_ARCHIVE_CHUNK_BYTES: u32 = 1024 * 1024;
const CAPTURE_FAMILY: &str = "native-directory-captures";
const BLOCK_BYTES: u64 = 512;
const GZIP_HEADER: [u8; 10] = [31, 139, 8, 0, 0, 0, 0, 0, 0, 255];

/// Exact compressed archive transport facts, not a descriptor of host authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeArchiveDescription {
    /// Exact compressed transport length, bounded to 64 MiB.
    pub total_bytes: u64,
    /// Lowercase SHA256 of the exact retained compressed bytes.
    pub sha256: String,
}

/// Immutable compressed bytes retained in upload-sized owned chunks.
pub struct NativeDirectoryArchive {
    description: NativeArchiveDescription,
    chunks: Vec<Bytes>,
    regular_digests: Arc<BTreeMap<FileId, [u8; 32]>>,
}

impl NativeDirectoryArchive {
    /// Returns transport facts of these immutable bytes.
    #[must_use]
    pub fn description(&self) -> &NativeArchiveDescription { &self.description }

    /// Retains received upload-sized native chunks only after exact body
    /// length and SHA256 authentication against the original upload facts.
    ///
    /// # Errors
    /// Rejects empty/oversized bodies, noncanonical chunk lengths and digests.
    pub fn from_chunks(chunks: Vec<Bytes>, expected: NativeArchiveDescription) -> Result<Self, NativeArchiveError> {
        if expected.total_bytes == 0 || expected.total_bytes > MAXIMUM_NATIVE_ARCHIVE_BYTES
            || expected.sha256.len() != 64 || !expected.sha256.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return Err(NativeArchiveError::InvalidBounds);
        }
        let mut total = 0_u64;
        let mut digest = Sha256::new();
        for (index, chunk) in chunks.iter().enumerate() {
            if chunk.is_empty() || chunk.len() > NATIVE_ARCHIVE_CHUNK_BYTES as usize
                || (index + 1 != chunks.len() && chunk.len() != NATIVE_ARCHIVE_CHUNK_BYTES as usize) {
                return Err(NativeArchiveError::InvalidBounds);
            }
            total = total.checked_add(chunk.len() as u64).ok_or(NativeArchiveError::InvalidBounds)?;
            if total > expected.total_bytes { return Err(NativeArchiveError::InvalidBounds); }
            digest.update(chunk);
        }
        if total != expected.total_bytes || hex::encode(digest.finalize()) != expected.sha256 {
            return Err(NativeArchiveError::InvalidArchive);
        }
        Ok(Self { description: expected, chunks, regular_digests: Arc::new(BTreeMap::new()) })
    }

    /// Reads at most one upload body; aligned upload reads share immutable bytes.
    ///
    /// # Errors
    /// Rejects out-of-range offsets and zero or oversized reads.
    pub fn read_chunk(&self, offset: u64, maximum: u32) -> Result<Bytes, NativeArchiveError> {
        if maximum == 0 || maximum > NATIVE_ARCHIVE_CHUNK_BYTES || offset > self.description.total_bytes {
            return Err(NativeArchiveError::InvalidBounds);
        }
        let length = u64::from(maximum).min(self.description.total_bytes - offset);
        if length == 0 { return Ok(Bytes::new()); }
        let index = usize::try_from(offset / u64::from(NATIVE_ARCHIVE_CHUNK_BYTES)).map_err(|_| NativeArchiveError::InvalidBounds)?;
        let start = usize::try_from(offset % u64::from(NATIVE_ARCHIVE_CHUNK_BYTES)).map_err(|_| NativeArchiveError::InvalidBounds)?;
        let length = usize::try_from(length).map_err(|_| NativeArchiveError::InvalidBounds)?;
        let first = &self.chunks[index];
        if start + length <= first.len() { return Ok(first.slice(start..start + length)); }
        let mut result = BytesMut::with_capacity(length);
        result.extend_from_slice(&first[start..]);
        result.extend_from_slice(&self.chunks[index + 1][..length - result.len()]);
        Ok(result.freeze())
    }

    /// Borrows the immutable chunks as a native streaming reader.
    #[must_use]
    pub fn reader(&self) -> NativeArchiveReader<'_> { NativeArchiveReader { archive: self, position: 0 } }
}

/// A borrowing stream over the exact retained compressed chunks.
pub struct NativeArchiveReader<'a> {
    archive: &'a NativeDirectoryArchive,
    position: u64,
}

impl Read for NativeArchiveReader<'_> {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        if destination.is_empty() || self.position == self.archive.description.total_bytes { return Ok(0); }
        let index = usize::try_from(self.position / u64::from(NATIVE_ARCHIVE_CHUNK_BYTES)).map_err(|_| io::Error::other("archive offset overflow"))?;
        let start = usize::try_from(self.position % u64::from(NATIVE_ARCHIVE_CHUNK_BYTES)).map_err(|_| io::Error::other("archive offset overflow"))?;
        let chunk = &self.archive.chunks[index];
        let count = destination.len().min(chunk.len() - start);
        destination[..count].copy_from_slice(&chunk[start..start + count]);
        self.position += u64::try_from(count).map_err(|_| io::Error::other("archive read overflow"))?;
        Ok(count)
    }
}

/// The single opaque original-root receipt shared with conditional native apply.
#[derive(Clone)]
pub struct NativeDirectoryCapture {
    root: Arc<HostRoot>,
    record: Arc<NativeCaptureRecord>,
}

impl NativeDirectoryCapture {
    /// Returns the identity of the retained original root handle.
    #[must_use]
    pub fn root_identity(&self) -> NativeRootIdentity { self.root.identity() }
    /// Returns the original absolute path binding, not another authority.
    #[must_use]
    pub fn root_path(&self) -> &Path { &self.record.source_root }
    /// Returns the committed immutable generation captured from this root.
    #[must_use]
    pub fn original_generation(&self) -> GenerationId { self.record.original_generation }
    /// Returns the original canonical SDK volume.
    #[must_use]
    pub fn original_volume(&self) -> VolumeId { self.record.volume_id }
    /// Returns the original volume's name profile and limits.
    #[must_use]
    pub fn volume_config(&self) -> VolumeConfig { self.record.volume_config }
    /// Returns the durable opaque capture locator.
    #[must_use]
    pub fn operation_id(&self) -> OperationId { self.record.operation_id }
    /// Returns the original exclusion policy.
    #[must_use]
    pub fn capture_policy(&self) -> &CapturePolicy { &self.record.policy }
    /// Returns original physical preimages, never freshly adopted witnesses.
    #[must_use]
    pub fn original_preimages(&self) -> &BTreeMap<NamespacePath, MaterializationPreimage> { &self.record.preimages }
    #[must_use]
    pub(crate) fn retained_preimages(&self) -> Arc<BTreeMap<NamespacePath, MaterializationPreimage>> { Arc::clone(&self.record.preimages) }
    #[must_use]
    pub(crate) fn held_root(&self) -> &Arc<HostRoot> { &self.root }
    #[must_use]
    /// Returns actual work and path counts of the original native capture.
    pub fn capture_receipt(&self) -> CaptureReceipt {
        CaptureReceipt { examined_paths: self.record.capture_counts[0], changed_paths: self.record.capture_counts[1], staged_file_bytes: self.record.capture_counts[2], work: self.record.capture_work }
    }
    /// Returns the original exact compressed archive description.
    #[must_use]
    pub fn archive_description(&self) -> &NativeArchiveDescription { &self.record.archive }

    /// Revalidates the original root binding, without capturing another tree.
    ///
    /// # Errors
    /// Rejects a removed, replaced, linked or otherwise inaccessible root.
    pub fn verify_current_binding(&self) -> Result<(), NativeArchiveError> {
        verify_root_binding(&self.root, &self.record.source_root)
    }
}

/// A real archive and its independently retained original-root receipt.
pub struct NativeDirectoryArchiveCapture {
    /// Original held-root and durable conditional-apply receipt.
    pub capture: NativeDirectoryCapture,
    /// Exact immutable compressed bytes, independently releasable.
    pub archive: NativeDirectoryArchive,
    /// Measured canonical-engine work incurred by the capture.
    pub work: WorkCounters,
}

/// Facts of a successfully authenticated private checkout import.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeArchiveImport {
    /// Number of validated filesystem entries, excluding the root.
    pub entries: u64,
    /// Sum of streamed regular entry lengths, excluding hard-link aliases.
    pub regular_file_bytes: u64,
}

/// Fail-closed archive, source movement, and original-receipt failures.
#[derive(Debug, Error)]
pub enum NativeArchiveError {
    /// A transport, entry, path-count or expanded-size bound was exceeded.
    #[error("native archive bounds are invalid or exceeded")]
    InvalidBounds,
    /// A path or link cannot be represented safely within the root.
    #[error("native archive path or link is not exactly portable and root-relative")]
    InvalidPath,
    /// An entry kind or permission bit is unsupported.
    #[error("native archive kind or permission bits are unsupported")]
    UnsupportedEntry,
    /// The observed source moved during its native capture.
    #[error("native directory changed during capture")]
    SourceChanged,
    /// The absolute path no longer names the original held root.
    #[error("native capture root binding changed")]
    RootChanged,
    /// The checkout is not the required clean original or empty baseline.
    #[error("native archive requires its exact clean original or an empty checkout")]
    InvalidCheckout,
    /// The canonical authority rejected generation publication.
    #[error("native capture generation publication was rejected")]
    PublicationRejected,
    /// The original durable locator is occupied, missing or malformed.
    #[error("native capture locator is already bound or its original record is invalid")]
    InvalidReceipt,
    /// The byte stream violates the canonical archive contract.
    #[error("native archive is not a complete canonical GNU tar/gzip stream")]
    InvalidArchive,
    /// A canonical SDK engine operation failed.
    #[error("native archive filesystem operation failed: {0}")]
    Engine(String),
    /// Native I/O failed.
    #[error("native archive I/O failed: {0}")]
    Io(#[from] io::Error),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeCaptureRecord {
    version: u32,
    revision: u64,
    operation_id: OperationId,
    source_root: PathBuf,
    root_identity: [u8; 16],
    volume_id: VolumeId,
    volume_config: VolumeConfig,
    original_generation: GenerationId,
    #[serde(serialize_with = "serialize_policy", deserialize_with = "deserialize_policy")]
    policy: Arc<CapturePolicy>,
    #[serde(serialize_with = "serialize_preimages", deserialize_with = "deserialize_preimages")]
    preimages: Arc<BTreeMap<NamespacePath, MaterializationPreimage>>,
    #[serde(serialize_with = "serialize_regular_digests", deserialize_with = "deserialize_regular_digests")]
    regular_digests: Arc<BTreeMap<FileId, [u8; 32]>>,
    capture_counts: [u64; 3],
    capture_work: WorkCounters,
    archive: NativeArchiveDescription,
}

impl Revisioned for NativeCaptureRecord { fn revision(&self) -> u64 { self.revision } }

struct SavedNamespace<'a>(&'a NamespacePath);
impl Serialize for SavedNamespace<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.components().len()))?;
        for name in self.0.components() { sequence.serialize_element(&(name.encoding().tag(), name.as_bytes()))?; }
        sequence.end()
    }
}

type SavedNamespaceNames = Vec<(u8, Vec<u8>)>;
fn restore_namespace<E: serde::de::Error>(names: SavedNamespaceNames) -> Result<NamespacePath, E> {
    // The complete record is subsequently checked against its actual volume
    // limits; decoding must not substitute default limits for a custom volume.
    let limits = crate::model::VolumeLimits { maximum_component_bytes: u32::MAX, maximum_path_bytes: u32::MAX, maximum_path_depth: u16::MAX, ..crate::model::VolumeLimits::default() };
    let names = names.into_iter().map(|(tag, bytes)| {
        let encoding = NameEncoding::from_tag(tag).map_err(E::custom)?;
        LogicalName::new(encoding, bytes, limits.maximum_component_bytes).map_err(E::custom)
    }).collect::<Result<Vec<_>, E>>()?;
    NamespacePath::new(names, limits).map_err(E::custom)
}

fn serialize_policy<S: Serializer>(policy: &Arc<CapturePolicy>, serializer: S) -> Result<S::Ok, S::Error> {
    let mut sequence = serializer.serialize_seq(Some(policy.excluded_prefixes().len()))?;
    for path in policy.excluded_prefixes() { sequence.serialize_element(&SavedNamespace(path))?; }
    sequence.end()
}
fn deserialize_policy<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Arc<CapturePolicy>, D::Error> {
    let paths = Vec::<SavedNamespaceNames>::deserialize(deserializer)?.into_iter().map(restore_namespace::<D::Error>).collect::<Result<Vec<_>, _>>()?;
    CapturePolicy::excluding(paths).map(Arc::new).map_err(serde::de::Error::custom)
}
fn serialize_preimages<S: Serializer>(preimages: &Arc<BTreeMap<NamespacePath, MaterializationPreimage>>, serializer: S) -> Result<S::Ok, S::Error> {
    let mut sequence = serializer.serialize_seq(Some(preimages.len()))?;
    for (path, image) in preimages.iter() { sequence.serialize_element(&(SavedNamespace(path), image))?; }
    sequence.end()
}
fn deserialize_preimages<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Arc<BTreeMap<NamespacePath, MaterializationPreimage>>, D::Error> {
    let entries = Vec::<(SavedNamespaceNames, MaterializationPreimage)>::deserialize(deserializer)?;
    let mut result = BTreeMap::new();
    for (path, image) in entries {
        if result.insert(restore_namespace::<D::Error>(path)?, image).is_some() { return Err(serde::de::Error::custom("duplicate native capture preimage")); }
    }
    Ok(Arc::new(result))
}

fn serialize_regular_digests<S: Serializer>(digests: &Arc<BTreeMap<FileId, [u8; 32]>>, serializer: S) -> Result<S::Ok, S::Error> {
    let mut sequence = serializer.serialize_seq(Some(digests.len()))?;
    for entry in digests.iter() { sequence.serialize_element(&entry)?; }
    sequence.end()
}

fn deserialize_regular_digests<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Arc<BTreeMap<FileId, [u8; 32]>>, D::Error> {
    let entries = Vec::<(FileId, [u8; 32])>::deserialize(deserializer)?;
    let mut result = BTreeMap::new();
    for (file_id, digest) in entries {
        if result.insert(file_id, digest).is_some() { return Err(serde::de::Error::custom("duplicate native capture content digest")); }
    }
    Ok(Arc::new(result))
}

fn verify_root_binding(root: &HostRoot, path: &Path) -> Result<(), NativeArchiveError> {
    let current = HostRoot::open(path).map_err(|_| NativeArchiveError::RootChanged)?;
    if current.identity() != root.identity() { return Err(NativeArchiveError::RootChanged); }
    Ok(())
}

/// Restores the original native receipt through the canonical journaled family.
/// No new host tree is captured and no caller-supplied identity is trusted.
///
/// # Errors
/// Rejects missing or malformed records, unsupported paths, and changed roots.
pub async fn restore_native_directory_capture(store: &LocalCoreStateStore, operation_id: OperationId) -> Result<NativeDirectoryCapture, NativeArchiveError> {
    let record: NativeCaptureRecord = store.load_record(CAPTURE_FAMILY, operation_id.into_bytes()).await.map_err(|error| NativeArchiveError::Engine(error.to_string()))?.ok_or(NativeArchiveError::InvalidReceipt)?;
    if record.version != 1 || record.revision != 1 || record.operation_id != operation_id || !record.source_root.is_absolute()
        || record.archive.total_bytes == 0 || record.archive.total_bytes > MAXIMUM_NATIVE_ARCHIVE_BYTES
        || record.archive.sha256.len() != 64 || !record.archive.sha256.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !record.preimages.keys().any(NamespacePath::is_root)
        || record.preimages.len().saturating_sub(1) > usize::try_from(record.volume_config.limits.maximum_paths_per_batch).map_err(|_| NativeArchiveError::InvalidReceipt)? {
        return Err(NativeArchiveError::InvalidReceipt);
    }
    for path in record.preimages.keys() {
        if record.policy.excludes(path) { return Err(NativeArchiveError::InvalidReceipt); }
        let restored = NamespacePath::new(path.components().to_vec(), record.volume_config.limits).map_err(|_| NativeArchiveError::InvalidReceipt)?;
        if restored != *path { return Err(NativeArchiveError::InvalidReceipt); }
        if !path.is_root() { portable_archive_path(path, record.volume_config)?; }
    }
    let root = Arc::new(HostRoot::open(&record.source_root).map_err(|_| NativeArchiveError::RootChanged)?);
    if root.identity().to_bytes() != record.root_identity { return Err(NativeArchiveError::RootChanged); }
    Ok(NativeDirectoryCapture { root, record: Arc::new(record) })
}

struct SourceObservation { relative: PathBuf, snapshot: HostSnapshot }

fn scan_source(root: &HostRoot, config: VolumeConfig, options: &CaptureOptions, policy: &CapturePolicy, cancellation: &CancellationToken) -> Result<BTreeMap<NamespacePath, SourceObservation>, NativeArchiveError> {
    let root_path = NamespacePath::new(Vec::new(), config.limits).map_err(|_| NativeArchiveError::InvalidBounds)?;
    let mut result = BTreeMap::new();
    result.insert(root_path, SourceObservation { relative: PathBuf::new(), snapshot: HostSnapshot::from_metadata(&root.symlink_metadata_held(Path::new(""))?).map_err(|_| NativeArchiveError::SourceChanged)? });
    let mut pending = vec![PathBuf::new()];
    let mut regular_identities = BTreeSet::new();
    let mut expanded_lower_bound = 2 * BLOCK_BYTES;
    while let Some(parent) = pending.pop() {
        if cancellation.is_cancelled() { return Err(NativeArchiveError::Engine("native archive capture cancelled".to_owned())); }
        let directory = root.open_dir_held(&parent)?;
        for entry in HostRoot::scan_held_dir(&directory)? {
            let entry = entry?;
            let relative = parent.join(entry.name);
            let path = host_path_to_namespace(&relative, config.profile, config.limits).map_err(|_| NativeArchiveError::InvalidPath)?;
            if policy.excludes(&path) { continue; }
            let archive_path = portable_archive_path(&path, config)?;
            expanded_lower_bound = expanded_lower_bound.checked_add(BLOCK_BYTES).ok_or(NativeArchiveError::InvalidBounds)?;
            if archive_path.len() > 100 {
                let name_bytes = u64::try_from(archive_path.len()).map_err(|_| NativeArchiveError::InvalidBounds)? + 1;
                let extension_bytes = BLOCK_BYTES + name_bytes.div_ceil(BLOCK_BYTES) * BLOCK_BYTES;
                expanded_lower_bound = expanded_lower_bound.checked_add(extension_bytes).ok_or(NativeArchiveError::InvalidBounds)?;
            }
            let metadata = root.symlink_metadata_held(&relative)?;
            let kind = metadata.file_type();
            if !kind.is_dir() && !kind.is_file() && !kind.is_symlink() { return Err(NativeArchiveError::UnsupportedEntry); }
            if kind.is_file() {
                let identity = NativeRootIdentity::from_metadata(&metadata)?.to_bytes();
                if regular_identities.insert(identity) {
                    let padded_bytes = metadata.len().checked_add(BLOCK_BYTES - 1).ok_or(NativeArchiveError::InvalidBounds)? / BLOCK_BYTES * BLOCK_BYTES;
                    expanded_lower_bound = expanded_lower_bound.checked_add(padded_bytes).ok_or(NativeArchiveError::InvalidBounds)?;
                }
            }
            if expanded_lower_bound > MAXIMUM_NATIVE_ARCHIVE_BYTES { return Err(NativeArchiveError::InvalidBounds); }
            let snapshot = HostSnapshot::from_metadata(&metadata).map_err(|_| NativeArchiveError::SourceChanged)?;
            if kind.is_dir() { pending.push(relative.clone()); }
            result.insert(path, SourceObservation { relative, snapshot });
            if result.len() - 1 > usize::try_from(options.maximum_paths).map_err(|_| NativeArchiveError::InvalidBounds)? { return Err(NativeArchiveError::InvalidBounds); }
        }
    }
    Ok(result)
}

fn verify_source(root: &HostRoot, expected: &BTreeMap<NamespacePath, SourceObservation>, config: VolumeConfig, options: &CaptureOptions, policy: &CapturePolicy, cancellation: &CancellationToken) -> Result<(), NativeArchiveError> {
    let current = scan_source(root, config, options, policy, cancellation)?;
    if !expected.keys().eq(current.keys()) { return Err(NativeArchiveError::SourceChanged); }
    for observation in expected.values() {
        ensure_current_host_node(root, &observation.relative, &observation.snapshot).map_err(|_| NativeArchiveError::SourceChanged)?;
    }
    Ok(())
}

async fn require_empty_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &Checkout<A, O>, budget: WorkBudget, cancellation: &CancellationToken) -> Result<(), NativeArchiveError> {
    if checkout.has_pending_mutations() { return Err(NativeArchiveError::InvalidCheckout); }
    let path = NamespacePath::new(Vec::new(), checkout.volume_config().limits).map_err(|_| NativeArchiveError::InvalidBounds)?;
    let page = checkout.snapshot_reader().resolve_directory_page(&path, None, 1, budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?;
    if !page.value.entries.is_empty() { return Err(NativeArchiveError::InvalidCheckout); }
    Ok(())
}

/// Captures the actual held directory, produces its archive, and durably binds
/// the original published generation and physical preimages before returning.
///
/// Whole-directory capture is not an atomic host filesystem snapshot. Callers
/// must quiesce source writers. Held-root path/metadata checks and repeated
/// canonical physical preimages reject observed movement without adopting it.
///
/// # Errors
/// Rejects source changes, inadmissible entries, archive bounds, publication
/// conflicts, reused locators, and native state-store failures.
#[allow(clippy::too_many_arguments, reason = "capture keeps the original capability, policy, operation, publication permission and state owner explicit")]
pub async fn capture_native_directory_archive<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>, root: Arc<HostRoot>, options: &CaptureOptions, policy: &CapturePolicy, operation_id: OperationId, permit: PublicationPermit, store: &LocalCoreStateStore, budget: WorkBudget, cancellation: &CancellationToken) -> Result<NativeDirectoryArchiveCapture, NativeArchiveError> {
    if !options.source_root.is_absolute() || options.expected_root_identity != root.identity() || options.maximum_paths == 0 || options.maximum_extent_spans == 0
        || options.maximum_paths > checkout.volume_config().limits.maximum_paths_per_batch { return Err(NativeArchiveError::InvalidBounds); }
    verify_root_binding(&root, &options.source_root)?;
    let existing: Option<NativeCaptureRecord> = store.load_record(CAPTURE_FAMILY, operation_id.into_bytes()).await.map_err(|error| NativeArchiveError::Engine(error.to_string()))?;
    if existing.is_some() { return Err(NativeArchiveError::InvalidReceipt); }
    require_empty_checkout(checkout, budget, cancellation).await?;
    let config = checkout.volume_config();
    let observed = scan_source(&root, config, options, policy, cancellation)?;
    let paths = Arc::new(observed.keys().cloned().collect::<Vec<_>>());
    let retained_root = Arc::clone(&root);
    let initial_paths = Arc::clone(&paths);
    let preimages = Arc::new(acyclic_native_runtime::run_blocking_io(move || capture_native_preimages(&retained_root, &initial_paths)).await?.map_err(|error| NativeArchiveError::Engine(error.to_string()))?);
    let mut candidate = checkout.private_candidate();
    let captured = capture_baseline_from_root(&mut candidate, &root, options, policy, budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?;
    verify_source(&root, &observed, config, options, policy, cancellation)?;
    verify_root_binding(&root, &options.source_root)?;
    let archive = export_archive(&candidate.snapshot_reader(), config, options.maximum_paths, true, budget, cancellation).await?;
    let retained_root = Arc::clone(&root);
    let final_preimages = acyclic_native_runtime::run_blocking_io(move || capture_native_preimages(&retained_root, &paths)).await?.map_err(|error| NativeArchiveError::Engine(error.to_string()))?;
    if preimages.as_ref() != &final_preimages { return Err(NativeArchiveError::SourceChanged); }
    verify_source(&root, &observed, config, options, policy, cancellation)?;
    verify_root_binding(&root, &options.source_root)?;
    let mut work = captured.work;
    if candidate.has_pending_mutations() {
        let committed = candidate.commit_with_permit(operation_id, permit, budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?;
        work = work.checked_add(committed.work).map_err(|_| NativeArchiveError::InvalidBounds)?;
        match committed.value {
            CheckoutCommitOutcome::Committed { .. } | CheckoutCommitOutcome::AlreadyCommitted { .. } => {},
            _ => return Err(NativeArchiveError::PublicationRejected),
        }
    }
    let receipt = captured.value;
    let record = NativeCaptureRecord {
        version: 1, revision: 1, operation_id, source_root: options.source_root.clone(), root_identity: root.identity().to_bytes(), volume_id: candidate.volume_id(), volume_config: config,
        original_generation: candidate.generation_id(), policy: Arc::new(policy.clone()), preimages,
        regular_digests: Arc::clone(&archive.regular_digests),
        capture_counts: [receipt.examined_paths, receipt.changed_paths, receipt.staged_file_bytes], capture_work: receipt.work, archive: archive.description.clone(),
    };
    if !store.compare_and_swap_record(CAPTURE_FAMILY, operation_id.into_bytes(), 0, record.clone()).await.map_err(|error| NativeArchiveError::Engine(error.to_string()))? { return Err(NativeArchiveError::InvalidReceipt); }
    *checkout = candidate;
    Ok(NativeDirectoryArchiveCapture { capture: NativeDirectoryCapture { root, record: Arc::new(record) }, archive, work })
}

fn portable_archive_path(path: &NamespacePath, config: VolumeConfig) -> Result<String, NativeArchiveError> {
    if path.is_root() { return Err(NativeArchiveError::InvalidPath); }
    let mut result = String::new();
    for name in path.components() {
        let name = name.unicode_text().ok_or(NativeArchiveError::InvalidPath)?;
        if name.contains('\\') || name.contains(':') { return Err(NativeArchiveError::InvalidPath); }
        if !result.is_empty() { result.push('/'); }
        result.push_str(&name);
    }
    PortablePath::parse(&format!("/{result}"), config.limits).map_err(|_| NativeArchiveError::InvalidPath)?;
    Ok(result)
}

async fn snapshot_entries<A: AsyncAuthorityStore, O: AsyncObjectStore>(reader: &PinnedReader<A, O>, config: VolumeConfig, maximum_paths: u32, budget: WorkBudget, cancellation: &CancellationToken) -> Result<Vec<(String, ResolvedFile<A, O>)>, NativeArchiveError> {
    let mut entries = Vec::new();
    let mut pending = vec![NamespacePath::new(Vec::new(), config.limits).map_err(|_| NativeArchiveError::InvalidBounds)?];
    while let Some(parent) = pending.pop() {
        let mut after = None;
        loop {
            let page = reader.resolve_directory_page(&parent, after.as_ref(), config.limits.maximum_directory_page_entries, budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?.value;
            if page.has_more && page.entries.is_empty() { return Err(NativeArchiveError::InvalidArchive); }
            for entry in page.entries {
                after = Some(entry.name.clone());
                let mut components = parent.components().to_vec();
                components.push(entry.name);
                let path = NamespacePath::new(components, config.limits).map_err(|_| NativeArchiveError::InvalidPath)?;
                let relative = portable_archive_path(&path, config)?;
                if entry.file.description().kind == FileKind::Directory { pending.push(path); }
                entries.push((relative, entry.file));
                if entries.len() > usize::try_from(maximum_paths).map_err(|_| NativeArchiveError::InvalidBounds)? { return Err(NativeArchiveError::InvalidBounds); }
            }
            if !page.has_more { break; }
        }
    }
    entries.sort_unstable_by(|left, right| compare_archive_paths(&left.0, &right.0));
    Ok(entries)
}

fn compare_archive_paths(left: &str, right: &str) -> std::cmp::Ordering {
    left.split('/').cmp(right.split('/'))
}

struct ChunkSink {
    complete: Vec<Bytes>,
    current: BytesMut,
    bytes: u64,
    digest: Sha256,
}
impl ChunkSink {
    fn new() -> Self { Self { complete: Vec::new(), current: BytesMut::new(), bytes: 0, digest: Sha256::new() } }
    fn finish(mut self) -> NativeDirectoryArchive {
        if !self.current.is_empty() { self.complete.push(self.current.freeze()); }
        NativeDirectoryArchive { description: NativeArchiveDescription { total_bytes: self.bytes, sha256: hex::encode(self.digest.finalize()) }, chunks: self.complete, regular_digests: Arc::new(BTreeMap::new()) }
    }
}
impl Write for ChunkSink {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let count = bytes.len();
        let total = self.bytes.checked_add(u64::try_from(count).map_err(|_| io::Error::other("archive byte overflow"))?).ok_or_else(|| io::Error::other("archive byte overflow"))?;
        if total > MAXIMUM_NATIVE_ARCHIVE_BYTES { return Err(io::Error::other("compressed native archive exceeds 64 MiB")); }
        self.digest.update(bytes);
        self.bytes = total;
        while !bytes.is_empty() {
            if self.current.capacity() == 0 { self.current = BytesMut::with_capacity(NATIVE_ARCHIVE_CHUNK_BYTES as usize); }
            let count = bytes.len().min(NATIVE_ARCHIVE_CHUNK_BYTES as usize - self.current.len());
            self.current.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.current.len() == NATIVE_ARCHIVE_CHUNK_BYTES as usize { self.complete.push(std::mem::take(&mut self.current).freeze()); }
        }
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

struct ExpandedWriter<W> { inner: W, bytes: u64 }
impl<W: Write> Write for ExpandedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self.bytes.checked_add(u64::try_from(bytes.len()).map_err(|_| io::Error::other("archive byte overflow"))?).ok_or_else(|| io::Error::other("archive byte overflow"))?;
        if next > MAXIMUM_NATIVE_ARCHIVE_BYTES { return Err(io::Error::other("expanded native archive exceeds 64 MiB")); }
        self.inner.write_all(bytes)?;
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> { self.inner.flush() }
}

fn header(kind: EntryType, bytes: u64, mode: u32) -> Header {
    let mut header = Header::new_gnu();
    header.set_entry_type(kind); header.set_size(bytes); header.set_mode(mode); header.set_uid(0); header.set_gid(0); header.set_mtime(0);
    header
}
fn short_text(value: &str) -> &str {
    let mut end = value.len().min(100);
    while !value.is_char_boundary(end) { end -= 1; }
    &value[..end]
}
fn write_padding<W: Write>(writer: &mut W, length: u64) -> io::Result<()> {
    let padding = (BLOCK_BYTES - length % BLOCK_BYTES) % BLOCK_BYTES;
    writer.write_all(&[0; 512][..padding as usize])
}
fn write_long_text<W: Write>(writer: &mut W, value: &str, kind: EntryType) -> io::Result<()> {
    let length = u64::try_from(value.len()).map_err(|_| io::Error::other("archive name overflow"))? + 1;
    let mut extension = header(kind, length, 0);
    extension.set_path("././@LongLink")?; extension.set_cksum();
    writer.write_all(extension.as_bytes())?; writer.write_all(value.as_bytes())?; writer.write_all(&[0])?;
    write_padding(writer, length)
}
fn write_header<W: Write>(writer: &mut W, path: &str, link: Option<&str>, kind: EntryType, bytes: u64, mode: u32) -> io::Result<()> {
    if path.len() > 100 { write_long_text(writer, path, EntryType::GNULongName)?; }
    if let Some(link) = link { if link.len() > 100 { write_long_text(writer, link, EntryType::GNULongLink)?; } }
    let mut entry = header(kind, bytes, mode); entry.set_path(short_text(path))?;
    if let Some(link) = link { entry.set_link_name_literal(short_text(link))?; }
    entry.set_cksum(); writer.write_all(entry.as_bytes())
}
fn ordinary_mode(metadata: FileMetadata, kind: FileKind) -> Result<u32, NativeArchiveError> {
    let mode = match metadata.posix_mode { MetadataField::Value(value) => value & 0o7777, _ if kind == FileKind::Directory => 0o755, _ if kind == FileKind::SymbolicLink => 0o777, _ => 0o644 };
    if mode & !0o777 != 0 { return Err(NativeArchiveError::UnsupportedEntry); }
    Ok(mode)
}
fn validate_link(path: &str, target: &str, limits: crate::model::VolumeLimits) -> Result<(), NativeArchiveError> {
    if target.is_empty() || target.starts_with('/') || target.contains('\\') || target.contains(':') || target.as_bytes().contains(&0)
        || target.len() > limits.maximum_path_bytes as usize { return Err(NativeArchiveError::InvalidPath); }
    let mut depth = path.split('/').count() - 1;
    for component in target.split('/') {
        match component {
            "" => return Err(NativeArchiveError::InvalidPath),
            "." => {},
            ".." => { depth = depth.checked_sub(1).ok_or(NativeArchiveError::InvalidPath)?; },
            name => { if name.len() > limits.maximum_component_bytes as usize { return Err(NativeArchiveError::InvalidPath); } depth += 1; },
        }
        if depth > usize::from(limits.maximum_path_depth) { return Err(NativeArchiveError::InvalidPath); }
    }
    Ok(())
}
fn native_link_text(bytes: &[u8]) -> Result<String, NativeArchiveError> {
    #[cfg(windows)]
    {
        if bytes.len() % 2 != 0 { return Err(NativeArchiveError::InvalidPath); }
        let units = bytes.chunks_exact(2).map(|part| u16::from_le_bytes([part[0], part[1]]));
        let text = char::decode_utf16(units).collect::<Result<String, _>>().map_err(|_| NativeArchiveError::InvalidPath)?;
        Ok(text.replace('\\', "/"))
    }
    #[cfg(not(windows))]
    { std::str::from_utf8(bytes).map(str::to_owned).map_err(|_| NativeArchiveError::InvalidPath) }
}

fn native_link_bytes(target: String) -> Bytes {
    #[cfg(windows)]
    { Bytes::from(target.encode_utf16().flat_map(u16::to_le_bytes).collect::<Vec<_>>()) }
    #[cfg(not(windows))]
    { Bytes::from(target) }
}

/// Encodes exact immutable SDK bytes, without rereading ambient host files.
///
/// # Errors
/// Rejects unsupported names, modes, kinds and links, exceeded transport or
/// expanded bounds, and canonical immutable-object read failures.
pub async fn export_native_directory_archive<A: AsyncAuthorityStore, O: AsyncObjectStore>(reader: &PinnedReader<A, O>, config: VolumeConfig, maximum_paths: u32, budget: WorkBudget, cancellation: &CancellationToken) -> Result<NativeDirectoryArchive, NativeArchiveError> {
    export_archive(reader, config, maximum_paths, false, budget, cancellation).await
}

async fn export_archive<A: AsyncAuthorityStore, O: AsyncObjectStore>(reader: &PinnedReader<A, O>, config: VolumeConfig, maximum_paths: u32, retain_regular_digests: bool, budget: WorkBudget, cancellation: &CancellationToken) -> Result<NativeDirectoryArchive, NativeArchiveError> {
    if maximum_paths == 0 || maximum_paths > config.limits.maximum_paths_per_batch { return Err(NativeArchiveError::InvalidBounds); }
    let entries = snapshot_entries(reader, config, maximum_paths, budget, cancellation).await?;
    let gzip = GzBuilder::new().mtime(0).operating_system(255).write(ChunkSink::new(), Compression::new(6));
    let mut writer = ExpandedWriter { inner: gzip, bytes: 0 };
    let mut regular = HashMap::<crate::FileId, String>::new();
    let mut regular_digests = BTreeMap::new();
    for (path, file) in entries {
        if cancellation.is_cancelled() { return Err(NativeArchiveError::Engine("native archive export cancelled".to_owned())); }
        let description = file.description();
        let mode = ordinary_mode(description.metadata, description.kind)?;
        match description.kind {
            FileKind::Directory => write_header(&mut writer, &path, None, EntryType::Directory, 0, mode)?,
            FileKind::Regular => {
                if let Some(first) = regular.get(&file.file_id()) { write_header(&mut writer, &path, Some(first), EntryType::Link, 0, mode)?; continue; }
                regular.insert(file.file_id(), path.clone());
                write_header(&mut writer, &path, None, EntryType::Regular, description.logical_bytes, mode)?;
                let mut offset = 0_u64;
                let mut digest = retain_regular_digests.then(Sha256::new);
                while offset < description.logical_bytes {
                    let wanted = u64::from(NATIVE_ARCHIVE_CHUNK_BYTES).min(config.limits.maximum_read_bytes).min(description.logical_bytes - offset);
                    if wanted == 0 { return Err(NativeArchiveError::InvalidBounds); }
                    let read = file.read_range(ByteRange { offset, length: wanted }, budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?.value;
                    if read.bytes.len() as u64 != wanted { return Err(NativeArchiveError::InvalidArchive); }
                    if let Some(digest) = &mut digest { digest.update(&read.bytes); }
                    writer.write_all(&read.bytes)?; offset += wanted;
                }
                if let Some(digest) = digest { regular_digests.insert(file.file_id(), digest.finalize().into()); }
                write_padding(&mut writer, description.logical_bytes)?;
            },
            FileKind::SymbolicLink => {
                let target = file.read_symbolic_link(budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?.value;
                let target = native_link_text(&target)?; validate_link(&path, &target, config.limits)?;
                write_header(&mut writer, &path, Some(&target), EntryType::Symlink, 0, mode)?;
            },
            _ => return Err(NativeArchiveError::UnsupportedEntry),
        }
    }
    writer.write_all(&[0; 1024])?;
    let mut archive = writer.inner.finish()?.finish();
    archive.regular_digests = Arc::new(regular_digests);
    Ok(archive)
}

struct LimitedReader<R> { inner: R, bytes: u64 }
impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        if destination.is_empty() { return Ok(0); }
        let permitted = usize::try_from(MAXIMUM_NATIVE_ARCHIVE_BYTES.saturating_sub(self.bytes)).unwrap_or(usize::MAX).min(destination.len());
        if permitted == 0 {
            let mut byte = [0];
            if self.inner.read(&mut byte)? == 0 { return Ok(0); }
            return Err(io::Error::other("native archive exceeds 64 MiB"));
        }
        let count = self.inner.read(&mut destination[..permitted])?;
        self.bytes += count as u64;
        Ok(count)
    }
}

struct EntrySource<'a, R> { reader: &'a mut R, remaining: u64, digest: Option<Sha256> }
impl<R: Read + Send> AsyncBlobSource for EntrySource<'_, R> {
    async fn read<'a>(&'a mut self, destination: &'a mut [u8], cancellation: &'a CancellationToken) -> io::Result<usize> {
        if cancellation.is_cancelled() { return Err(io::Error::new(io::ErrorKind::Interrupted, "native archive import cancelled")); }
        let maximum = usize::try_from(self.remaining).unwrap_or(usize::MAX).min(destination.len()).min(NATIVE_ARCHIVE_CHUNK_BYTES as usize);
        if maximum == 0 { return Ok(0); }
        let count = self.reader.read(&mut destination[..maximum])?;
        if count == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "native archive entry is truncated")); }
        self.remaining -= count as u64;
        if let Some(digest) = &mut self.digest { digest.update(&destination[..count]); }
        Ok(count)
    }
}

struct ImportedRegular {
    content: StagedContent,
    digest: [u8; 32],
}

type ImportedRegulars = BTreeMap<NamespacePath, Arc<ImportedRegular>>;

fn read_padding<R: Read>(reader: &mut R, length: u64) -> Result<(), NativeArchiveError> {
    let padding = ((BLOCK_BYTES - length % BLOCK_BYTES) % BLOCK_BYTES) as usize;
    let mut bytes = [0; 512]; reader.read_exact(&mut bytes[..padding])?;
    if bytes[..padding].iter().any(|byte| *byte != 0) { return Err(NativeArchiveError::InvalidArchive); }
    Ok(())
}
fn valid_header(header: &Header) -> Result<(), NativeArchiveError> {
    let actual = header.as_bytes().iter().enumerate().map(|(index, byte)| if (148..156).contains(&index) { 32_u32 } else { u32::from(*byte) }).sum::<u32>();
    if header.as_gnu().is_none() || header.cksum()? != actual || header.uid()? != 0 || header.gid()? != 0 || header.mtime()? != 0
        || header.mode()? & !0o777 != 0 || header.username_bytes().is_some_and(|value| !value.is_empty()) || header.groupname_bytes().is_some_and(|value| !value.is_empty()) { return Err(NativeArchiveError::InvalidArchive); }
    Ok(())
}
fn read_long_text<R: Read>(reader: &mut R, size: u64, maximum: u32) -> Result<String, NativeArchiveError> {
    if size < 2 || size > u64::from(maximum) + 1 { return Err(NativeArchiveError::InvalidBounds); }
    let mut bytes = vec![0; usize::try_from(size).map_err(|_| NativeArchiveError::InvalidBounds)?]; reader.read_exact(&mut bytes)?;
    if bytes.pop() != Some(0) || bytes.contains(&0) { return Err(NativeArchiveError::InvalidArchive); }
    read_padding(reader, size)?;
    String::from_utf8(bytes).map_err(|_| NativeArchiveError::InvalidPath)
}
fn imported_path(path: &str, config: VolumeConfig) -> Result<NamespacePath, NativeArchiveError> {
    if path.is_empty() || path.starts_with('/') || path.contains('\\') || path.contains(':') { return Err(NativeArchiveError::InvalidPath); }
    let path = PortablePath::parse(&format!("/{path}"), config.limits).map_err(|_| NativeArchiveError::InvalidPath)?;
    NamespacePath::from_portable_in_profile(&path, config.profile, config.limits).map_err(|_| NativeArchiveError::InvalidPath)
}

/// Validates the single canonical archive format and stages exact entry bytes
/// into an atomic private SDK checkout, never an ambient filesystem extraction.
///
/// # Errors
/// Rejects path/link escapes, symlink ancestors, duplicate or unordered names,
/// invalid modes/kinds/headers, truncation, trailing members, compressed and
/// expanded bounds, canonical-engine failures, and cancellation. The caller's
/// checkout remains unchanged unless the entire stream validates.
pub async fn import_native_directory_archive<A: AsyncAuthorityStore, O: AsyncObjectStore, R: Read + Send>(checkout: &mut Checkout<A, O>, source: R, budget: WorkBudget, cancellation: &CancellationToken) -> Result<OperationReceipt<NativeArchiveImport>, NativeArchiveError> {
    require_empty_checkout(checkout, budget, cancellation).await?;
    import_archive_into_empty_candidate(checkout, source, None, None, budget, cancellation).await
}

/// Imports a complete desired tree as a child of the retained original volume.
///
/// The caller must supply the exact clean captured generation. Original root
/// metadata and unchanged file identities are retained; excluded paths cannot
/// be introduced. Unavailable archive metadata never clears original facts.
/// Publication remains the caller's canonical operation and permit decision.
///
/// # Errors
/// Rejects foreign volumes, another baseline, pending edits, a replaced original
/// root, excluded paths, invalid archives and bounded engine failures. No caller
/// checkout changes survive any rejected archive.
pub async fn replace_captured_native_directory_archive<A: AsyncAuthorityStore, O: AsyncObjectStore, R: Read + Send>(checkout: &mut Checkout<A, O>, capture: &NativeDirectoryCapture, source: R, budget: WorkBudget, cancellation: &CancellationToken) -> Result<OperationReceipt<NativeArchiveImport>, NativeArchiveError> {
    if checkout.has_pending_mutations() || checkout.volume_id() != capture.original_volume()
        || checkout.generation_id() != capture.original_generation() || checkout.volume_config() != capture.volume_config() {
        return Err(NativeArchiveError::InvalidCheckout);
    }
    capture.verify_current_binding()?;
    let config = checkout.volume_config();
    let original = snapshot_entries(&checkout.snapshot_reader(), config, config.limits.maximum_paths_per_batch, budget, cancellation).await?.into_iter().collect::<BTreeMap<_, _>>();
    let mut parsed = checkout.private_candidate();
    let mut work = WorkCounters::default();
    for (name, file) in original.iter().rev() {
        let remaining = work.remaining(budget).map_err(|_| NativeArchiveError::InvalidBounds)?;
        let removed = parsed.remove(imported_path(name, config)?, Some(file.file_id()), remaining, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?;
        work = work.checked_add(removed.work).map_err(|_| NativeArchiveError::InvalidBounds)?;
    }
    let mut retained = ImportedRegulars::new();
    let remaining = work.remaining(budget).map_err(|_| NativeArchiveError::InvalidBounds)?;
    let imported = import_archive_into_empty_candidate(&mut parsed, source, Some(capture.capture_policy()), Some(&mut retained), remaining, cancellation).await?;
    work = work.checked_add(imported.work).map_err(|_| NativeArchiveError::InvalidBounds)?;
    let desired = snapshot_entries(&parsed.snapshot_reader(), config, config.limits.maximum_paths_per_batch, budget, cancellation).await?.into_iter().collect::<BTreeMap<_, _>>();
    let groups = desired_alias_groups(&original, &desired, &retained, capture, config, budget, cancellation).await?;
    let mut candidate = checkout.private_candidate();
    reconcile_desired_tree(&mut candidate, &original, &desired, &groups, &retained, capture, &mut work, budget, cancellation).await?;
    work.verify(budget).map_err(|_| NativeArchiveError::InvalidBounds)?;
    *checkout = candidate;
    Ok(OperationReceipt { value: imported.value, work })
}

struct DesiredAliasGroup {
    paths: Vec<String>,
    file_id: FileId,
    metadata: FileMetadata,
    content_matches: bool,
    target: Option<Bytes>,
    candidates: Vec<(FileId, usize, bool)>,
}

fn overlay_archive_mode(original: FileMetadata, desired: FileMetadata) -> FileMetadata {
    let mut merged = original;
    if let (MetadataField::Value(old), MetadataField::Value(new)) = (original.posix_mode, desired.posix_mode) {
        merged.posix_mode = MetadataField::Value((old & !0o777) | (new & 0o777));
    }
    merged
}

#[allow(clippy::too_many_arguments, reason = "all inputs are retained original authority or parser-authenticated desired facts")]
async fn desired_alias_groups<A: AsyncAuthorityStore, O: AsyncObjectStore>(original: &BTreeMap<String, ResolvedFile<A, O>>, desired: &BTreeMap<String, ResolvedFile<A, O>>, retained: &ImportedRegulars, capture: &NativeDirectoryCapture, config: VolumeConfig, budget: WorkBudget, cancellation: &CancellationToken) -> Result<Vec<DesiredAliasGroup>, NativeArchiveError> {
    let mut aliases = BTreeMap::<FileId, Vec<String>>::new();
    for (name, file) in desired {
        if file.description().kind != FileKind::Directory { aliases.entry(file.file_id()).or_default().push(name.clone()); }
    }
    let mut groups = Vec::with_capacity(aliases.len());
    for (file_id, paths) in aliases {
        let first = paths.first().ok_or(NativeArchiveError::InvalidArchive)?;
        let file = desired.get(first).ok_or(NativeArchiveError::InvalidArchive)?;
        let target = if file.description().kind == FileKind::SymbolicLink {
            Some(file.read_symbolic_link(budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?.value)
        } else { None };
        let digest = if file.description().kind == FileKind::Regular {
            Some(retained.get(&imported_path(first, config)?).ok_or(NativeArchiveError::InvalidArchive)?.digest)
        } else { None };
        let mut candidates = BTreeMap::<FileId, (usize, bool)>::new();
        for name in &paths {
            let Some(before) = original.get(name).filter(|before| before.description().kind == file.description().kind) else { continue; };
            let matches = if let Some(digest) = digest {
                capture.record.regular_digests.get(&before.file_id()).ok_or(NativeArchiveError::InvalidReceipt)? == &digest
            } else {
                let before_target = before.read_symbolic_link(budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?.value;
                target.as_ref() == Some(&before_target)
            };
            let entry = candidates.entry(before.file_id()).or_insert((0, matches));
            entry.0 += 1;
        }
        groups.push(DesiredAliasGroup { paths, file_id, metadata: file.description().metadata, content_matches: false, target, candidates: candidates.into_iter().map(|(id, (count, matches))| (id, count, matches)).collect() });
    }
    groups.sort_unstable_by(|left, right| {
        (!left.candidates.iter().any(|entry| entry.2)).cmp(&(!right.candidates.iter().any(|entry| entry.2)))
            .then_with(|| compare_archive_paths(&left.paths[0], &right.paths[0]))
    });
    let mut inherited = BTreeSet::new();
    for group in &mut groups {
        let kind = desired.get(group.paths.first().ok_or(NativeArchiveError::InvalidArchive)?).ok_or(NativeArchiveError::InvalidArchive)?.description().kind;
        group.candidates.sort_unstable_by_key(|(id, count, matches)| (!*matches, std::cmp::Reverse(*count), *id));
        if let Some((id, _, matches)) = group.candidates.iter().find(|(id, _, _)| !inherited.contains(id)).copied() {
            inherited.insert(id);
            group.file_id = id;
            group.content_matches = matches;
            let before = group.paths.iter().filter_map(|name| original.get(name)).find(|file| file.file_id() == id).ok_or(NativeArchiveError::InvalidReceipt)?;
            group.metadata = overlay_archive_mode(before.description().metadata, group.metadata);
        }
        else if let Some(before) = group.paths.iter().filter_map(|name| original.get(name)).find(|file| file.description().kind == kind) {
            group.metadata = overlay_archive_mode(before.description().metadata, group.metadata);
        }
    }
    Ok(groups)
}

#[allow(clippy::too_many_arguments, reason = "reconciliation retains original scope, parsed proofs and one measured work accumulator")]
async fn reconcile_desired_tree<A: AsyncAuthorityStore, O: AsyncObjectStore>(candidate: &mut Checkout<A, O>, original: &BTreeMap<String, ResolvedFile<A, O>>, desired: &BTreeMap<String, ResolvedFile<A, O>>, groups: &[DesiredAliasGroup], retained: &ImportedRegulars, capture: &NativeDirectoryCapture, work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken) -> Result<(), NativeArchiveError> {
    let config = candidate.volume_config();
    let mut desired_groups = BTreeMap::new();
    for (index, group) in groups.iter().enumerate() {
        for path in &group.paths { desired_groups.insert(path.as_str(), index); }
    }
    let mut mutations = Vec::new();
    let mut kept = BTreeSet::new();
    for (name, before) in original.iter().rev() {
        let preserve = match desired.get(name) {
            Some(after) if before.description().kind == FileKind::Directory && after.description().kind == FileKind::Directory => true,
            Some(after) if before.description().kind == after.description().kind => {
                let group = &groups[*desired_groups.get(name.as_str()).ok_or(NativeArchiveError::InvalidArchive)?];
                group.file_id == before.file_id() && (before.description().kind == FileKind::Regular || group.content_matches)
            },
            _ => false,
        };
        if preserve { kept.insert(name.as_str()); }
        else { mutations.push(AuthoredMutation::Remove { path: imported_path(name, config)?, expected_file_id: Some(before.file_id()) }); }
    }
    apply_reconciliation_mutations(candidate, mutations, work, budget, cancellation).await?;
    for (name, after) in desired {
        if after.description().kind != FileKind::Directory { continue; }
        let path = imported_path(name, config)?;
        let mutation = if kept.contains(name.as_str()) {
            let before = original.get(name).ok_or(NativeArchiveError::InvalidReceipt)?;
            let metadata = overlay_archive_mode(before.description().metadata, after.description().metadata);
            (metadata != before.description().metadata).then_some(AuthoredMutation::SetMetadata { path, metadata })
        } else { Some(AuthoredMutation::CreateDirectory { path, metadata: after.description().metadata }) };
        if let Some(mutation) = mutation { apply_reconciliation_mutations(candidate, vec![mutation], work, budget, cancellation).await?; }
    }
    for group in groups {
        let anchor = group.paths.iter().find(|name| kept.contains(name.as_str())).unwrap_or(&group.paths[0]);
        let path = imported_path(anchor, config)?;
        let after = desired.get(anchor).ok_or(NativeArchiveError::InvalidArchive)?;
        let mut mutations = Vec::new();
        if after.description().kind == FileKind::Regular {
            let content = retained.get(&path).ok_or(NativeArchiveError::InvalidArchive)?;
            if kept.contains(anchor.as_str()) {
                if !group.content_matches {
                    mutations.push(AuthoredMutation::Resize { path: path.clone(), logical_bytes: 0 });
                    mutations.push(AuthoredMutation::WriteFromContent { path: path.clone(), offset: 0, content: content.content.clone() });
                }
                let before = original.get(anchor).ok_or(NativeArchiveError::InvalidReceipt)?;
                if !group.content_matches || before.description().metadata != group.metadata { mutations.push(AuthoredMutation::SetMetadata { path: path.clone(), metadata: group.metadata }); }
            } else {
                mutations.push(AuthoredMutation::CreateFileFromContent { path: path.clone(), content: content.content.clone(), metadata: group.metadata, file_id: Some(group.file_id) });
            }
        } else {
            if !kept.contains(anchor.as_str()) {
                mutations.push(AuthoredMutation::CreateSymbolicLink { path: path.clone(), target: group.target.clone().ok_or(NativeArchiveError::InvalidArchive)?, metadata: group.metadata });
                mutations.push(AuthoredMutation::Reidentify { path: path.clone(), file_id: group.file_id });
            } else {
                let before = original.get(anchor).ok_or(NativeArchiveError::InvalidReceipt)?;
                if before.description().metadata != group.metadata { mutations.push(AuthoredMutation::SetMetadata { path: path.clone(), metadata: group.metadata }); }
            }
        }
        for name in &group.paths {
            if name != anchor && !kept.contains(name.as_str()) {
                mutations.push(AuthoredMutation::HardLink { source: path.clone(), destination: imported_path(name, config)? });
            }
        }
        apply_reconciliation_mutations(candidate, mutations, work, budget, cancellation).await?;
    }
    capture.verify_current_binding()?;
    Ok(())
}

async fn apply_reconciliation_mutations<A: AsyncAuthorityStore, O: AsyncObjectStore>(candidate: &mut Checkout<A, O>, mutations: Vec<AuthoredMutation>, work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken) -> Result<(), NativeArchiveError> {
    if mutations.is_empty() { return Ok(()); }
    let remaining = work.remaining(budget).map_err(|_| NativeArchiveError::InvalidBounds)?;
    let applied = candidate.apply_authored_bulk_transaction(mutations, remaining, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?;
    *work = work.checked_add(applied.work).map_err(|_| NativeArchiveError::InvalidBounds)?;
    Ok(())
}

async fn import_archive_into_empty_candidate<A: AsyncAuthorityStore, O: AsyncObjectStore, R: Read + Send>(checkout: &mut Checkout<A, O>, source: R, policy: Option<&CapturePolicy>, mut retained_content: Option<&mut ImportedRegulars>, budget: WorkBudget, cancellation: &CancellationToken) -> Result<OperationReceipt<NativeArchiveImport>, NativeArchiveError> {
    let mut compressed = LimitedReader { inner: source, bytes: 0 };
    let mut first = [0; 10]; compressed.read_exact(&mut first)?;
    if first != GZIP_HEADER { return Err(NativeArchiveError::InvalidArchive); }
    let source = Cursor::new(first).chain(compressed);
    let decoder = GzDecoder::new(BufReader::new(source));
    let mut expanded = LimitedReader { inner: decoder, bytes: 0 };
    let config = checkout.volume_config();
    let mut candidate = checkout.private_candidate();
    let mut imported = NativeArchiveImport::default();
    let mut work = WorkCounters::default();
    let mut paths = BTreeMap::<NamespacePath, (FileKind, u32)>::new();
    let mut previous = None;
    let mut long_path = None;
    let mut long_link = None;
    loop {
        if cancellation.is_cancelled() { return Err(NativeArchiveError::Engine("native archive import cancelled".to_owned())); }
        let mut entry = Header::new_gnu(); expanded.read_exact(entry.as_mut_bytes())?;
        if entry.as_bytes().iter().all(|byte| *byte == 0) {
            let mut second = [0; 512]; expanded.read_exact(&mut second)?;
            if second.iter().any(|byte| *byte != 0) || long_path.is_some() || long_link.is_some() { return Err(NativeArchiveError::InvalidArchive); }
            let mut extra = [0]; if expanded.read(&mut extra)? != 0 { return Err(NativeArchiveError::InvalidArchive); }
            let mut compressed = expanded.inner.into_inner();
            if !compressed.fill_buf()?.is_empty() { return Err(NativeArchiveError::InvalidArchive); }
            break;
        }
        valid_header(&entry)?;
        let size = entry.size()?;
        let kind = entry.entry_type();
        if kind == EntryType::GNULongName || kind == EntryType::GNULongLink {
            let value = read_long_text(&mut expanded, size, config.limits.maximum_path_bytes)?;
            let slot = if kind == EntryType::GNULongName { &mut long_path } else { &mut long_link };
            if slot.replace(value).is_some() { return Err(NativeArchiveError::InvalidArchive); }
            continue;
        }
        let name = match long_path.take() { Some(value) => value, None => std::str::from_utf8(&entry.path_bytes()).map_err(|_| NativeArchiveError::InvalidPath)?.to_owned() };
        let path = imported_path(&name, config)?;
        if policy.is_some_and(|policy| policy.excludes(&path)) { return Err(NativeArchiveError::InvalidPath); }
        if previous.as_ref().is_some_and(|last: &String| compare_archive_paths(last, &name).is_ge()) { return Err(NativeArchiveError::InvalidArchive); }
        previous = Some(name.clone());
        if let Some(parent) = path.parent() {
            if !parent.is_root() && paths.get(&parent).map(|entry| entry.0) != Some(FileKind::Directory) { return Err(NativeArchiveError::InvalidPath); }
        }
        imported.entries = imported.entries.checked_add(1).ok_or(NativeArchiveError::InvalidBounds)?;
        if imported.entries > u64::from(config.limits.maximum_paths_per_batch) { return Err(NativeArchiveError::InvalidBounds); }
        let mode = entry.mode()?;
        let link = match long_link.take() { Some(value) => Some(value), None => entry.link_name_bytes().map(|bytes| String::from_utf8(bytes.into_owned())).transpose().map_err(|_| NativeArchiveError::InvalidPath)? };
        let mut metadata = FileMetadata::default();
        metadata.posix_mode = MetadataField::Value(mode | match kind {
            EntryType::Directory => 0o040000,
            EntryType::Symlink => 0o120000,
            _ => 0o100000,
        });
        let mut mutations = Vec::new();
        let semantic_kind;
        match kind {
            EntryType::Regular => {
                if link.is_some() || size > MAXIMUM_NATIVE_ARCHIVE_BYTES { return Err(NativeArchiveError::InvalidArchive); }
                imported.regular_file_bytes = imported.regular_file_bytes.checked_add(size).ok_or(NativeArchiveError::InvalidBounds)?;
                if size == 0 && retained_content.is_none() {
                    mutations.push(AuthoredMutation::CreateFile { path: path.clone(), bytes: Bytes::new(), metadata });
                } else {
                    let mut source = EntrySource { reader: &mut expanded, remaining: size, digest: retained_content.is_some().then(Sha256::new) };
                    let staged = candidate.content_stager().stage(&mut source, size.max(1), budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?;
                    if staged.value.logical_bytes() != size || source.remaining != 0 { return Err(NativeArchiveError::InvalidArchive); }
                    work = work.checked_add(staged.work).map_err(|_| NativeArchiveError::InvalidBounds)?;
                    if let (Some(retained), Some(digest)) = (retained_content.as_deref_mut(), source.digest) {
                        retained.insert(path.clone(), Arc::new(ImportedRegular { content: staged.value.clone(), digest: digest.finalize().into() }));
                    }
                    mutations.push(AuthoredMutation::CreateFileFromContent { path: path.clone(), content: staged.value, metadata, file_id: None });
                }
                semantic_kind = FileKind::Regular;
            },
            EntryType::Directory => {
                if size != 0 || link.is_some() { return Err(NativeArchiveError::InvalidArchive); }
                mutations.push(AuthoredMutation::CreateDirectory { path: path.clone(), metadata });
                semantic_kind = FileKind::Directory;
            },
            EntryType::Symlink => {
                if size != 0 { return Err(NativeArchiveError::InvalidArchive); }
                let target = link.ok_or(NativeArchiveError::InvalidArchive)?; validate_link(&name, &target, config.limits)?;
                mutations.push(AuthoredMutation::CreateSymbolicLink { path: path.clone(), target: native_link_bytes(target), metadata });
                semantic_kind = FileKind::SymbolicLink;
            },
            EntryType::Link => {
                if size != 0 { return Err(NativeArchiveError::InvalidArchive); }
                let source = imported_path(&link.ok_or(NativeArchiveError::InvalidArchive)?, config)?;
                if paths.get(&source) != Some(&(FileKind::Regular, mode)) { return Err(NativeArchiveError::InvalidPath); }
                if let Some(retained) = retained_content.as_deref_mut() {
                    let original = Arc::clone(retained.get(&source).ok_or(NativeArchiveError::InvalidArchive)?);
                    retained.insert(path.clone(), original);
                }
                mutations.push(AuthoredMutation::HardLink { source, destination: path.clone() });
                semantic_kind = FileKind::Regular;
            },
            _ => return Err(NativeArchiveError::UnsupportedEntry),
        }
        read_padding(&mut expanded, size)?;
        let applied = candidate.apply_authored_bulk_transaction(mutations, budget, cancellation).await.map_err(|failure| NativeArchiveError::Engine(failure.to_string()))?;
        work = work.checked_add(applied.work).map_err(|_| NativeArchiveError::InvalidBounds)?;
        work.verify(budget).map_err(|_| NativeArchiveError::InvalidBounds)?;
        paths.insert(path, (semantic_kind, mode));
    }
    *checkout = candidate;
    Ok(OperationReceipt { value: imported, work })
}

#[cfg(all(test, feature = "local", unix))]
mod tests {
    use super::*;
    use crate::model::{CheckoutMode, GenerationSelector};
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    fn path(value: &str, config: VolumeConfig) -> Result<NamespacePath, NativeArchiveError> {
        imported_path(value, config)
    }

    struct ArchiveFixtureEntry<'a> {
        name: &'a [u8],
        kind: EntryType,
        link: Option<&'a str>,
        mode: u32,
        declared_size: u64,
        body: &'a [u8],
    }

    fn archive_fixture(entries: &[ArchiveFixtureEntry<'_>]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut writer = GzBuilder::new().mtime(0).operating_system(255).write(Vec::new(), Compression::new(6));
        for fixture in entries {
            let mut entry = header(fixture.kind, fixture.declared_size, fixture.mode);
            entry.as_mut_bytes()[..fixture.name.len()].copy_from_slice(fixture.name);
            if let Some(link) = fixture.link { entry.set_link_name_literal(link)?; }
            entry.set_cksum();
            writer.write_all(entry.as_bytes())?;
            writer.write_all(fixture.body)?;
            write_padding(&mut writer, fixture.body.len() as u64)?;
        }
        writer.write_all(&[0; 1024])?;
        Ok(writer.finish()?)
    }

    fn malicious_archive(name: &[u8], kind: EntryType, link: Option<&str>, declared_size: u64, body: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        archive_fixture(&[ArchiveFixtureEntry { name, kind, link, mode: 0o644, declared_size, body }])
    }

    #[tokio::test]
    async fn real_directory_archive_import_retains_modes_links_bytes_and_original_receipt() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::create_dir(&source)?;
        std::fs::create_dir(source.join("pkg"))?;
        std::fs::create_dir(source.join(".git"))?;
        std::fs::write(source.join(".git/history"), b"private history must not be captured")?;
        std::fs::write(source.join(".env"), b"PRIVATE_TOKEN=not-an-archive-entry")?;
        std::fs::write(source.join("pkg/entry.sh"), b"#!/bin/sh\nprintf actual-native-archive\n")?;
        std::fs::set_permissions(source.join("pkg/entry.sh"), std::fs::Permissions::from_mode(0o755))?;
        std::fs::write(source.join("pkg/\u{100}.txt"), b"portable unicode bytes")?;
        std::fs::write(source.join("empty"), [])?;
        std::fs::hard_link(source.join("pkg/entry.sh"), source.join("pkg/second.sh"))?;
        symlink("entry.sh", source.join("pkg/link"))?;
        let long_directory = "a".repeat(120);
        let long_filename = "b".repeat(120);
        let long_path = format!("{long_directory}/{long_filename}");
        std::fs::create_dir(source.join(&long_directory))?;
        std::fs::write(source.join(&long_path), b"actual long native bytes")?;
        std::fs::hard_link(source.join(&long_path), source.join("pkg/long-second"))?;
        symlink(format!("../{long_path}"), source.join("pkg/long-link"))?;
        let fs = crate::Fs::local(crate::LocalOptions::new(directory.path().join("sdk-store"))).await?;
        let workspace = fs.create_workspace("archive-original").await?;
        let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let original_empty = checkout.generation_id();
        let root = Arc::new(HostRoot::open(&source)?);
        let options = CaptureOptions { source_root: source.clone(), expected_root_identity: root.identity(), maximum_paths: 32, maximum_extent_spans: 32 };
        let config = checkout.volume_config();
        let policy = CapturePolicy::excluding(vec![path(".git", config)?, path(".env", config)?])?;
        let store = LocalCoreStateStore::new(directory.path().join("native-state"));
        let operation = OperationId::new();
        let cancellation = CancellationToken::new();
        let captured = capture_native_directory_archive(&mut checkout, Arc::clone(&root), &options, &policy, operation, PublicationPermit::Unrestricted, &store, WorkBudget::UNBOUNDED, &cancellation).await?;
        assert_eq!(captured.capture.original_generation(), checkout.generation_id());
        assert_ne!(captured.capture.original_generation(), original_empty);
        assert!(Arc::ptr_eq(captured.capture.held_root(), &root));
        let recoded = export_native_directory_archive(&checkout.snapshot_reader(), config, options.maximum_paths, WorkBudget::UNBOUNDED, &cancellation).await?;
        assert_eq!(recoded.description(), captured.archive.description());
        let mut chunks = Vec::new();
        let mut offset = 0;
        let mut digest = Sha256::new();
        while offset < captured.archive.description().total_bytes {
            let chunk = captured.archive.read_chunk(offset, NATIVE_ARCHIVE_CHUNK_BYTES)?;
            digest.update(&chunk);
            offset += chunk.len() as u64;
            chunks.push(chunk);
        }
        assert_eq!(offset, captured.archive.description().total_bytes);
        assert_eq!(hex::encode(digest.finalize()), captured.archive.description().sha256);
        let received = NativeDirectoryArchive::from_chunks(chunks, captured.archive.description().clone())?;
        let target = fs.create_workspace("archive-imported").await?;
        let mut imported = target.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let result = import_native_directory_archive(&mut imported, received.reader(), WorkBudget::UNBOUNDED, &cancellation).await?;
        assert_eq!(result.value.entries, 10);
        let reader = imported.snapshot_reader();
        let content = reader.read_file_range(&path("pkg/entry.sh", config)?, ByteRange { offset: 0, length: b"#!/bin/sh\nprintf actual-native-archive\n".len() as u64 }, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert_eq!(content.bytes.as_ref(), b"#!/bin/sh\nprintf actual-native-archive\n");
        let metadata = reader.read_metadata(&path("pkg/entry.sh", config)?, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert!(matches!(metadata.posix_mode, MetadataField::Value(value) if value & 0o777 == 0o755));
        let link = reader.read_symbolic_link(&path("pkg/link", config)?, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert_eq!(link.as_ref(), b"entry.sh");
        let listing = reader.resolve_directory_page(&path("pkg", config)?, None, 16, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let first = listing.entries.iter().find(|entry| entry.name.unicode_text().as_deref() == Some("entry.sh")).ok_or("missing original file")?;
        let second = listing.entries.iter().find(|entry| entry.name.unicode_text().as_deref() == Some("second.sh")).ok_or("missing hardlink")?;
        assert_eq!(first.file.file_id(), second.file.file_id());
        let long_link = reader.read_symbolic_link(&path("pkg/long-link", config)?, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert_eq!(long_link.as_ref(), format!("../{long_path}").as_bytes());
        let long_listing = reader.resolve_directory_page(&path(&long_directory, config)?, None, 16, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let long_file = long_listing.entries.first().ok_or("missing long GNU-name file")?;
        let long_alias = listing.entries.iter().find(|entry| entry.name.unicode_text().as_deref() == Some("long-second")).ok_or("missing long GNU-link alias")?;
        assert_eq!(long_file.file.file_id(), long_alias.file.file_id());
        assert!(!captured.capture.original_preimages().contains_key(&path(".env", config)?));
        assert!(!captured.capture.original_preimages().contains_key(&path(".git", config)?));
        drop(captured.capture);
        let restored = restore_native_directory_capture(&store, operation).await?;
        assert_eq!(restored.original_generation(), checkout.generation_id());
        assert_eq!(restored.root_identity(), root.identity());
        assert_eq!(restored.capture_policy().fingerprint(), policy.fingerprint());
        assert!(restored.original_preimages().contains_key(&path("pkg/entry.sh", config)?));
        std::fs::rename(&source, directory.path().join("original-source"))?;
        std::fs::create_dir(&source)?;
        assert!(matches!(restored.verify_current_binding(), Err(NativeArchiveError::RootChanged)));
        assert!(matches!(restore_native_directory_capture(&store, operation).await, Err(NativeArchiveError::RootChanged)));
        Ok(())
    }

    #[tokio::test]
    async fn desired_archive_requires_original_volume_and_preserves_original_baseline_on_failure() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::create_dir(&source)?;
        std::fs::write(source.join("before.txt"), b"original physical bytes")?;
        let fs = crate::Fs::local(crate::LocalOptions::new(directory.path().join("sdk-store"))).await?;
        let workspace = fs.create_workspace("archive-original-authority").await?;
        let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let config = checkout.volume_config();
        let root = Arc::new(HostRoot::open(&source)?);
        let options = CaptureOptions { source_root: source.clone(), expected_root_identity: root.identity(), maximum_paths: 16, maximum_extent_spans: 16 };
        let policy = CapturePolicy::excluding(vec![path(".env", config)?])?;
        let store = LocalCoreStateStore::new(directory.path().join("native-state"));
        let cancellation = CancellationToken::new();
        let captured = capture_native_directory_archive(&mut checkout, root, &options, &policy, OperationId::new(), PublicationPermit::Unrestricted, &store, WorkBudget::UNBOUNDED, &cancellation).await?;
        let original_generation = checkout.generation_id();
        let desired = malicious_archive(b"desired.txt", EntryType::Regular, None, 6, b"actual")?;
        let foreign = fs.create_workspace("archive-foreign-authority").await?;
        let mut foreign_checkout = foreign.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        assert!(matches!(replace_captured_native_directory_archive(&mut foreign_checkout, &captured.capture, Cursor::new(&desired), WorkBudget::UNBOUNDED, &cancellation).await, Err(NativeArchiveError::InvalidCheckout)));
        assert!(!foreign_checkout.has_pending_mutations());
        let excluded = malicious_archive(b".env", EntryType::Regular, None, 1, b"x")?;
        assert!(matches!(replace_captured_native_directory_archive(&mut checkout, &captured.capture, Cursor::new(excluded), WorkBudget::UNBOUNDED, &cancellation).await, Err(NativeArchiveError::InvalidPath)));
        assert_eq!(checkout.generation_id(), original_generation);
        assert!(!checkout.has_pending_mutations());
        let before = checkout.snapshot_reader().read_file_range(&path("before.txt", config)?, ByteRange { offset: 0, length: 23 }, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert_eq!(before.bytes.as_ref(), b"original physical bytes");
        let imported = replace_captured_native_directory_archive(&mut checkout, &captured.capture, Cursor::new(&desired), WorkBudget::UNBOUNDED, &cancellation).await?;
        assert_eq!(imported.value.entries, 1);
        let reader = checkout.snapshot_reader();
        assert!(reader.read_file_range(&path("before.txt", config)?, ByteRange { offset: 0, length: 1 }, WorkBudget::UNBOUNDED, &cancellation).await.is_err());
        let after = reader.read_file_range(&path("desired.txt", config)?, ByteRange { offset: 0, length: 6 }, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert_eq!(after.bytes.as_ref(), b"actual");
        assert_eq!(std::fs::read(source.join("before.txt"))?, b"original physical bytes");
        let published = checkout.commit_with_permit(OperationId::new(), PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation).await?;
        assert!(matches!(published.value, CheckoutCommitOutcome::Committed { .. }));
        assert_eq!(checkout.volume_id(), captured.capture.original_volume());
        assert_ne!(checkout.generation_id(), original_generation);
        assert_eq!(captured.capture.original_generation(), original_generation);
        assert!(matches!(replace_captured_native_directory_archive(&mut checkout, &captured.capture, Cursor::new(desired), WorkBudget::UNBOUNDED, &cancellation).await, Err(NativeArchiveError::InvalidCheckout)));
        Ok(())
    }

    #[tokio::test]
    async fn unchanged_desired_archive_retains_ids_metadata_and_unrelated_physical_writer() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::create_dir(&source)?;
        std::fs::create_dir(source.join("pkg"))?;
        std::fs::write(source.join("pkg/shared"), b"immutable original bytes")?;
        std::fs::hard_link(source.join("pkg/shared"), source.join("pkg/alias"))?;
        std::fs::write(source.join("unrelated"), b"captured before later writer")?;
        symlink("shared", source.join("pkg/link"))?;
        let workspace = crate::Fs::memory().create_workspace("archive-retains-original").await?;
        let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let root = Arc::new(HostRoot::open(&source)?);
        let options = CaptureOptions { source_root: source.clone(), expected_root_identity: root.identity(), maximum_paths: 16, maximum_extent_spans: 16 };
        let policy = CapturePolicy::allow_all();
        let store = LocalCoreStateStore::new(directory.path().join("native-state"));
        let cancellation = CancellationToken::new();
        let captured = capture_native_directory_archive(&mut checkout, root, &options, &policy, OperationId::new(), PublicationPermit::Unrestricted, &store, WorkBudget::UNBOUNDED, &cancellation).await?;
        let config = checkout.volume_config();
        let before = snapshot_entries(&checkout.snapshot_reader(), config, 16, WorkBudget::UNBOUNDED, &cancellation).await?.into_iter().map(|(path, file)| (path, (file.file_id(), file.description().metadata))).collect::<BTreeMap<_, _>>();
        std::fs::write(source.join("unrelated"), b"actual unrelated later writer")?;
        let restored = restore_native_directory_capture(&store, captured.capture.operation_id()).await?;
        replace_captured_native_directory_archive(&mut checkout, &restored, captured.archive.reader(), WorkBudget::UNBOUNDED, &cancellation).await?;
        let after = snapshot_entries(&checkout.snapshot_reader(), config, 16, WorkBudget::UNBOUNDED, &cancellation).await?.into_iter().map(|(path, file)| (path, (file.file_id(), file.description().metadata))).collect::<BTreeMap<_, _>>();
        assert_eq!(before, after);
        assert!(!checkout.has_pending_mutations());
        assert_eq!(checkout.generation_id(), restored.original_generation());
        assert_eq!(std::fs::read(source.join("unrelated"))?, b"actual unrelated later writer");
        Ok(())
    }

    #[tokio::test]
    async fn split_original_hardlink_keeps_unchanged_alias_identity_and_complete_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::create_dir(&source)?;
        std::fs::write(source.join("x"), b"base")?;
        std::fs::set_permissions(source.join("x"), std::fs::Permissions::from_mode(0o644))?;
        std::fs::hard_link(source.join("x"), source.join("y"))?;
        let workspace = crate::Fs::memory().create_workspace("archive-split-original-alias").await?;
        let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let root = Arc::new(HostRoot::open(&source)?);
        let options = CaptureOptions { source_root: source.clone(), expected_root_identity: root.identity(), maximum_paths: 8, maximum_extent_spans: 8 };
        let policy = CapturePolicy::allow_all();
        let store = LocalCoreStateStore::new(directory.path().join("native-state"));
        let cancellation = CancellationToken::new();
        let captured = capture_native_directory_archive(&mut checkout, root, &options, &policy, OperationId::new(), PublicationPermit::Unrestricted, &store, WorkBudget::UNBOUNDED, &cancellation).await?;
        let config = checkout.volume_config();
        let before = checkout.snapshot_reader().resolve_directory_page(&NamespacePath::new(Vec::new(), config.limits)?, None, 8, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let original = before.entries.first().ok_or("missing original aliases")?;
        let original_id = original.file.file_id();
        let original_metadata = original.file.description().metadata;
        let desired = archive_fixture(&[
            ArchiveFixtureEntry { name: b"x", kind: EntryType::Regular, link: None, mode: 0o644, declared_size: 7, body: b"changed" },
            ArchiveFixtureEntry { name: b"y", kind: EntryType::Regular, link: None, mode: 0o644, declared_size: 4, body: b"base" },
        ])?;
        replace_captured_native_directory_archive(&mut checkout, &captured.capture, Cursor::new(desired), WorkBudget::UNBOUNDED, &cancellation).await?;
        let after = checkout.snapshot_reader().resolve_directory_page(&NamespacePath::new(Vec::new(), config.limits)?, None, 8, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let x = after.entries.iter().find(|entry| entry.name.unicode_text().as_deref() == Some("x")).ok_or("missing changed split alias")?;
        let y = after.entries.iter().find(|entry| entry.name.unicode_text().as_deref() == Some("y")).ok_or("missing unchanged split alias")?;
        assert_eq!(y.file.file_id(), original_id);
        assert_ne!(x.file.file_id(), original_id);
        assert_eq!(x.file.description().metadata, original_metadata);
        assert_eq!(y.file.description().metadata, original_metadata);
        let unchanged = y.file.read_range(ByteRange { offset: 0, length: 4 }, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert_eq!(unchanged.bytes.as_ref(), b"base");
        assert_eq!(std::fs::read(source.join("x"))?, b"base");
        assert_eq!(std::fs::read(source.join("y"))?, b"base");
        Ok(())
    }

    #[tokio::test]
    async fn changed_directory_and_replaced_root_fail_without_adopting_new_source() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::create_dir(&source)?;
        std::fs::write(source.join("before.txt"), b"before")?;
        let root = Arc::new(HostRoot::open(&source)?);
        let workspace = crate::Fs::memory().create_workspace("archive-race").await?;
        let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let options = CaptureOptions { source_root: source.clone(), expected_root_identity: root.identity(), maximum_paths: 8, maximum_extent_spans: 8 };
        let policy = CapturePolicy::allow_all();
        let cancellation = CancellationToken::new();
        let original = scan_source(&root, checkout.volume_config(), &options, &policy, &cancellation)?;
        std::fs::write(source.join("before.txt"), b"actual later writer")?;
        assert!(matches!(verify_source(&root, &original, checkout.volume_config(), &options, &policy, &cancellation), Err(NativeArchiveError::SourceChanged)));
        let store = LocalCoreStateStore::new(directory.path().join("native-state"));
        std::fs::rename(&source, directory.path().join("retained-original"))?;
        std::fs::create_dir(&source)?;
        std::fs::write(source.join("foreign.txt"), b"must not be adopted")?;
        let result = capture_native_directory_archive(&mut checkout, root, &options, &policy, OperationId::new(), PublicationPermit::Unrestricted, &store, WorkBudget::UNBOUNDED, &cancellation).await;
        assert!(matches!(result, Err(NativeArchiveError::RootChanged)));
        assert!(!checkout.has_pending_mutations());
        Ok(())
    }

    #[tokio::test]
    async fn malformed_foreign_paths_links_modes_and_truncation_do_not_mutate_checkout() -> Result<(), Box<dyn std::error::Error>> {
        let fs = crate::Fs::memory();
        let cancellation = CancellationToken::new();
        let valid = malicious_archive(b"safe", EntryType::Regular, None, 1, b"x")?;
        let mut wrong_crc = valid.clone();
        let crc_offset = wrong_crc.len() - 8;
        wrong_crc[crc_offset] ^= 1;
        let mut another_member = valid.clone();
        another_member.extend_from_slice(&valid);
        let cases = [
            malicious_archive(b"../outside", EntryType::Regular, None, 1, b"x")?,
            malicious_archive(b"/absolute", EntryType::Regular, None, 1, b"x")?,
            malicious_archive(b"windows\\foreign", EntryType::Regular, None, 1, b"x")?,
            malicious_archive(b"\xffforeign", EntryType::Regular, None, 1, b"x")?,
            malicious_archive(b"link", EntryType::Symlink, Some("../outside"), 0, b"")?,
            malicious_archive(b"hard", EntryType::Link, Some("not-present"), 0, b"")?,
            malicious_archive(b"truncated", EntryType::Regular, None, 2048, b"short")?,
            malicious_archive(b"too-large", EntryType::Regular, None, MAXIMUM_NATIVE_ARCHIVE_BYTES + 1, b"")?,
            archive_fixture(&[ArchiveFixtureEntry { name: b"special-mode", kind: EntryType::Regular, link: None, mode: 0o4755, declared_size: 1, body: b"x" }])?,
            archive_fixture(&[
                ArchiveFixtureEntry { name: b"alias", kind: EntryType::Symlink, link: Some("other"), mode: 0o777, declared_size: 0, body: b"" },
                ArchiveFixtureEntry { name: b"alias/child", kind: EntryType::Regular, link: None, mode: 0o644, declared_size: 1, body: b"x" },
            ])?,
            wrong_crc,
            another_member,
        ];
        for (index, archive) in cases.into_iter().enumerate() {
            let workspace = fs.create_workspace(format!("archive-rejected-{index}")).await?;
            let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
            let before = checkout.generation_id();
            assert!(import_native_directory_archive(&mut checkout, Cursor::new(archive), WorkBudget::UNBOUNDED, &cancellation).await.is_err());
            assert_eq!(checkout.generation_id(), before);
            assert!(!checkout.has_pending_mutations());
        }
        Ok(())
    }

    #[tokio::test]
    async fn exact_expanded_boundary_captures_and_imports_real_sparse_directory_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::create_dir(&source)?;
        let length = MAXIMUM_NATIVE_ARCHIVE_BYTES - 1536;
        std::fs::File::create(source.join("boundary.bin"))?.set_len(length)?;
        let fs = crate::Fs::local(crate::LocalOptions::new(directory.path().join("sdk-store"))).await?;
        let workspace = fs.create_workspace("archive-boundary").await?;
        let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let root = Arc::new(HostRoot::open(&source)?);
        let options = CaptureOptions { source_root: source.clone(), expected_root_identity: root.identity(), maximum_paths: 4, maximum_extent_spans: 4 };
        let store = LocalCoreStateStore::new(directory.path().join("native-state"));
        let cancellation = CancellationToken::new();
        let captured = capture_native_directory_archive(&mut checkout, root, &options, &CapturePolicy::allow_all(), OperationId::new(), PublicationPermit::Unrestricted, &store, WorkBudget::UNBOUNDED, &cancellation).await?;
        let workspace = fs.create_workspace("archive-boundary-import").await?;
        let mut imported = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let result = import_native_directory_archive(&mut imported, captured.archive.reader(), WorkBudget::UNBOUNDED, &cancellation).await?;
        assert_eq!(result.value.regular_file_bytes, length);
        let bytes = imported.snapshot_reader().read_file_range(&path("boundary.bin", imported.volume_config())?, ByteRange { offset: length - 16, length: 16 }, WorkBudget::UNBOUNDED, &cancellation).await?.value;
        assert_eq!(bytes.bytes.as_ref(), &[0; 16]);
        std::fs::File::options().write(true).open(source.join("boundary.bin"))?.set_len(length + 1)?;
        let workspace = fs.create_workspace("archive-over-boundary").await?;
        let mut rejected = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
        let root = Arc::new(HostRoot::open(&source)?);
        let options = CaptureOptions { expected_root_identity: root.identity(), ..options };
        assert!(capture_native_directory_archive(&mut rejected, root, &options, &CapturePolicy::allow_all(), OperationId::new(), PublicationPermit::Unrestricted, &store, WorkBudget::UNBOUNDED, &cancellation).await.is_err());
        assert!(!rejected.has_pending_mutations());
        Ok(())
    }

    #[test]
    fn received_native_chunks_reject_wrong_original_digest_length_and_ordered_shape() -> Result<(), Box<dyn std::error::Error>> {
        let body = Bytes::from_static(b"actual received body bytes");
        let expected = NativeArchiveDescription { total_bytes: body.len() as u64, sha256: hex::encode(Sha256::digest(&body)) };
        let received = NativeDirectoryArchive::from_chunks(vec![body.clone()], expected.clone())?;
        assert_eq!(received.read_chunk(0, NATIVE_ARCHIVE_CHUNK_BYTES)?, body);
        assert!(NativeDirectoryArchive::from_chunks(vec![Bytes::from_static(b"substituted body bytes")], expected.clone()).is_err());
        assert!(NativeDirectoryArchive::from_chunks(vec![body.clone()], NativeArchiveDescription { total_bytes: expected.total_bytes + 1, ..expected.clone() }).is_err());
        assert!(NativeDirectoryArchive::from_chunks(vec![body.slice(..3), body.slice(3..)], expected).is_err());
        assert!(received.read_chunk(0, NATIVE_ARCHIVE_CHUNK_BYTES + 1).is_err());
        Ok(())
    }
}
