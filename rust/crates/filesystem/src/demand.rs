//! Bounded, versioned access to unresolved filesystem sources.
//!
//! A source reference identifies a provider capability. It never contains a
//! host path or provider credential. Resolving one request does not publish a
//! workspace generation or make the result part of recorded history.

use crate::cancellation::CancellationToken;
use crate::kernel::{NamespacePath, NamespacePathError};
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Opaque identity and invalidation epoch of an attached source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReference {
    /// Provider-scoped opaque identity; no local path is encoded here.
    pub identity: [u8; 16],
    /// Incremented when source knowledge is invalidated.
    pub epoch: u64,
}

/// Evidence for the exact node observed by a source request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SourceVersion(pub [u8; 32]);

/// The kind of an observed source node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SourceNodeKind {
    /// Directory, whose children remain unresolved until requested.
    Directory,
    /// Regular file, whose content ranges remain unresolved until requested.
    RegularFile,
    /// Symbolic link, never followed by the source provider.
    SymbolicLink,
    /// POSIX named pipe.
    Fifo,
    /// POSIX local socket node.
    Socket,
    /// Native character device.
    CharacterDevice,
    /// Native block device.
    BlockDevice,
    /// Another native node kind which cannot be projected losslessly.
    Unsupported,
}

/// Scalar host metadata. `None` means the provider did not make that fact
/// available; it does not mean a numeric zero.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceMetadata {
    /// POSIX permission and kind bits, when available.
    pub posix_mode: Option<u32>,
    /// POSIX numeric owner, when available.
    pub posix_uid: Option<u32>,
    /// POSIX numeric group, when available.
    pub posix_gid: Option<u32>,
    /// POSIX inode flags, when available.
    pub posix_flags: Option<u64>,
    /// Windows file attributes, when available.
    pub windows_attributes: Option<u32>,
    /// Creation/birth time in signed Unix nanoseconds.
    pub created_ns: Option<i128>,
    /// Last content modification time in signed Unix nanoseconds.
    pub modified_ns: Option<i128>,
    /// Last access time in signed Unix nanoseconds.
    pub accessed_ns: Option<i128>,
    /// Last metadata-change time in signed Unix nanoseconds.
    pub changed_ns: Option<i128>,
}

/// Metadata returned by an exact path lookup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceNode {
    /// Source node kind.
    pub kind: SourceNodeKind,
    /// Stable path-independent source identity. Hard-link aliases share it.
    pub file_identity: [u8; 32],
    /// Exact number of source namespace bindings when the provider exposes it.
    pub link_count: Option<u64>,
    /// Native device identity for character and block devices.
    pub device: Option<(u32, u32)>,
    /// Logical length for a regular file; absent for other kinds.
    pub logical_bytes: Option<u64>,
    /// Version that must match a later read of this node.
    pub version: SourceVersion,
    /// Explicitly available scalar metadata.
    pub metadata: SourceMetadata,
}

/// One directory entry in native enumeration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDirectoryEntry {
    /// Lossless host component encoding.
    pub name: crate::kernel::LogicalName,
    /// The entry's node, exactly as a lookup of its path would report it.
    pub node: SourceNode,
}

/// Cursor bound to one directory and one source epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    source: SourceReference,
    directory: NamespacePath,
    directory_version: SourceVersion,
    token: u64,
}

impl SourceCursor {
    /// Actual owned query-name backing; scalar token evidence is inline.
    #[must_use]
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.directory.retained_bytes()
    }

    pub(crate) fn cursor_encoded_len(&self) -> Result<usize, DemandError> {
        self.directory.cursor_encoded_len().map_err(Self::cursor_path_error)?
            .checked_add(16 + 8 + 32 + 8).ok_or(DemandError::InvalidRequest)
    }

    /// Preserves the provider's original opaque resume evidence. The containing
    /// cursor admits the one output allocation before any field is appended.
    pub(crate) fn encode_cursor_into(
        &self,
        encoder: &mut crate::kernel::Encoder,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), DemandError> {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        if encoder.remaining_capacity() < self.cursor_encoded_len()? {
            return Err(DemandError::InvalidRequest);
        }
        work.charge(&WorkCounters { bytes_encoded: 16, bytes_copied: 16, items_examined: 1,
            ..WorkCounters::default() }, &budget)?;
        encoder.fixed(&self.source.identity);
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        work.charge(&WorkCounters { bytes_encoded: 8, items_examined: 1,
            ..WorkCounters::default() }, &budget)?;
        encoder.u64(self.source.epoch);
        self.directory.encode_cursor_into(encoder, work, budget, cancellation)
            .map_err(Self::cursor_path_error)?;
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        work.charge(&WorkCounters { bytes_encoded: 32, bytes_copied: 32, items_examined: 1,
            ..WorkCounters::default() }, &budget)?;
        encoder.fixed(&self.directory_version.0);
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        work.charge(&WorkCounters { bytes_encoded: 8, items_examined: 1,
            ..WorkCounters::default() }, &budget)?;
        encoder.u64(self.token);
        Ok(())
    }

    /// Decoding does not authorize a resume: the source's existing epoch,
    /// version and cursor-registry checks remain authoritative at use time.
    pub(crate) fn decode_cursor_with_ledger(
        decoder: &mut crate::kernel::Decoder<'_>,
        expected_source: SourceReference,
        expected_directory: &NamespacePath,
        profile: crate::FilesystemProfile,
        limits: crate::VolumeLimits,
        ledger: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, DemandError> {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        let initial_live = ledger.live_bytes();
        let result = (|| {
            let identity_bytes = decoder.take_exact(16).map_err(|_| DemandError::InvalidRequest)?;
            work.charge(&WorkCounters { bytes_copied: 16, items_examined: 1,
                ..WorkCounters::default() }, &budget)?;
            let identity = <[u8; 16]>::try_from(identity_bytes).map_err(|_| DemandError::InvalidRequest)?;
            work.charge_items(1, &budget)?;
            let epoch = decoder.u64().map_err(|_| DemandError::InvalidRequest)?;
            let source = SourceReference { identity, epoch };
            if source != expected_source {
                return Err(DemandError::StaleCursor);
            }
            let directory = NamespacePath::decode_cursor_with_ledger(
                decoder, profile, limits, ledger, work, budget, cancellation,
            ).map_err(Self::cursor_path_error)?;
            work.charge_items(1, &budget)?;
            if directory.depth() != expected_directory.depth() {
                return Err(DemandError::StaleCursor);
            }
            for (actual, expected) in directory.components().iter().zip(expected_directory.components()) {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                work.charge_items(1, &budget)?;
                if actual.encoding() != expected.encoding() || actual.as_bytes().len() != expected.as_bytes().len() {
                    return Err(DemandError::StaleCursor);
                }
                for (actual, expected) in actual.as_bytes().iter().zip(expected.as_bytes()) {
                    work.charge_items(1, &budget)?;
                    if actual != expected {
                        return Err(DemandError::StaleCursor);
                    }
                }
            }
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            let version_bytes = decoder.take_exact(32).map_err(|_| DemandError::InvalidRequest)?;
            work.charge(&WorkCounters { bytes_copied: 32, items_examined: 1,
                ..WorkCounters::default() }, &budget)?;
            let directory_version = SourceVersion(<[u8; 32]>::try_from(version_bytes)
                .map_err(|_| DemandError::InvalidRequest)?);
            work.charge_items(1, &budget)?;
            let token = decoder.u64().map_err(|_| DemandError::InvalidRequest)?;
            Ok(Self { source, directory, directory_version, token })
        })();
        if result.is_err() {
            ledger.release(ledger.live_bytes() - initial_live).map_err(|error| match error {
                crate::kernel::allocation::AllocationError::Work(error) => DemandError::Work(error),
                _ => DemandError::InvalidRequest,
            })?;
        }
        result
    }

    fn cursor_path_error(error: NamespacePathError) -> DemandError {
        match error {
            NamespacePathError::Work(error) => DemandError::Work(error),
            NamespacePathError::Cancelled => DemandError::Cancelled,
            _ => DemandError::InvalidRequest,
        }
    }
}

#[cfg(test)]
mod source_cursor_codec_tests {
    use super::*;
    use crate::kernel::allocation::AllocationLedger;
    use crate::kernel::{Decoder, Encoder, LogicalName, NameEncoding};

    const DOMAIN: &[u8] = b"source-cursor-control";

    fn cursor(token: u64) -> Result<SourceCursor, NamespacePathError> {
        let limits = crate::VolumeLimits::default();
        Ok(SourceCursor {
            source: SourceReference { identity: [4; 16], epoch: 7 },
            directory: NamespacePath::new(vec![LogicalName::new(
                NameEncoding::WindowsUtf16Le, vec![0, 0xd8], limits.maximum_component_bytes,
            ).map_err(NamespacePathError::Name)?], limits)?,
            directory_version: SourceVersion([9; 32]),
            token,
        })
    }

    fn encoded(cursor: &SourceCursor) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let capacity = DOMAIN.len() + 2 + cursor.cursor_encoded_len()?;
        let mut encoder = Encoder::with_exact_capacity(DOMAIN, 1, capacity)?;
        let mut work = WorkCounters::default();
        cursor.encode_cursor_into(&mut encoder, &mut work, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
        assert_eq!(work.bytes_encoded, cursor.cursor_encoded_len()? as u64);
        Ok(encoder.finish())
    }

    #[test]
    fn opaque_tokens_and_original_version_roundtrip_without_new_authorization_rules()
    -> Result<(), Box<dyn std::error::Error>> {
        for token in [0, u64::MAX] {
            let original = cursor(token)?;
            let bytes = encoded(&original)?;
            let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
            let mut ledger = AllocationLedger::default();
            let mut work = WorkCounters::default();
            let prior_live = original.directory.retained_bytes() + bytes.capacity() as u64;
            ledger.claim_bytes(prior_live, 0, &mut work, WorkBudget::UNBOUNDED)?;
            let decoded = SourceCursor::decode_cursor_with_ledger(
                &mut decoder, original.source, &original.directory,
                crate::FilesystemProfile::Windows, crate::VolumeLimits::default(),
                &mut ledger, &mut work, WorkBudget::UNBOUNDED, &CancellationToken::new(),
            )?;
            decoder.finish()?;
            assert_eq!(decoded, original);
            assert_eq!(work.bytes_copied, 16 + 2 + 32);
            assert_eq!(work.allocation_operations, 2);
            assert_eq!(ledger.live_bytes(), prior_live + decoded.retained_bytes());
            assert_eq!(work.peak_allocation_bytes, ledger.live_bytes());
            ledger.release(decoded.retained_bytes())?;
            assert_eq!(ledger.live_bytes(), prior_live);
        }
        Ok(())
    }

    #[test]
    fn foreign_bindings_fail_with_actual_prefix_and_retire_only_decoded_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = cursor(0)?;
        let bytes = encoded(&original)?;
        let foreign_directory = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::WindowsUtf16Le, vec![b'b', 0], 255,
        )?], crate::VolumeLimits::default())?;
        for foreign_source in [true, false] {
            let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
            let mut ledger = AllocationLedger::default();
            let mut work = WorkCounters::default();
            let prior_live = original.directory.retained_bytes()
                + foreign_directory.retained_bytes() + bytes.capacity() as u64;
            ledger.claim_bytes(prior_live, 0, &mut work, WorkBudget::UNBOUNDED)?;
            let expected_source = if foreign_source {
                SourceReference { epoch: original.source.epoch + 1, ..original.source }
            } else { original.source };
            let expected_directory = if foreign_source { &original.directory } else { &foreign_directory };
            let error = SourceCursor::decode_cursor_with_ledger(
                &mut decoder, expected_source, expected_directory,
                crate::FilesystemProfile::Windows, crate::VolumeLimits::default(),
                &mut ledger, &mut work, WorkBudget::UNBOUNDED, &CancellationToken::new(),
            ).expect_err("foreign cursor binding must fail");
            assert!(matches!(error, DemandError::StaleCursor));
            assert_eq!(work.bytes_copied, if foreign_source { 16 } else { 18 });
            assert_eq!(work.allocation_operations, if foreign_source { 0 } else { 2 });
            assert_eq!(ledger.live_bytes(), prior_live);
        }
        Ok(())
    }

    #[test]
    fn copy_denial_and_cancellation_preserve_actual_prefix_and_caller_claims()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = cursor(u64::MAX)?;
        let bytes = encoded(&original)?;
        for (copy_budget, copied, allocations) in [(0, 0, 0), (17, 16, 1), (18, 18, 2)] {
            let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
            let mut ledger = AllocationLedger::default();
            let mut work = WorkCounters::default();
            let prior_live = original.directory.retained_bytes() + bytes.capacity() as u64;
            ledger.claim_bytes(prior_live, 0, &mut work, WorkBudget::UNBOUNDED)?;
            let error = SourceCursor::decode_cursor_with_ledger(
                &mut decoder, original.source, &original.directory,
                crate::FilesystemProfile::Windows, crate::VolumeLimits::default(),
                &mut ledger, &mut work,
                WorkBudget { bytes_copied: copy_budget, ..WorkBudget::UNBOUNDED }, &CancellationToken::new(),
            ).expect_err("copy allowance must deny before the next copy/allocation");
            assert!(matches!(error, DemandError::Work(_)));
            assert_eq!(work.bytes_copied, copied);
            assert_eq!(work.allocation_operations, allocations);
            assert_eq!(ledger.live_bytes(), prior_live);
        }
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let error = SourceCursor::decode_cursor_with_ledger(
            &mut decoder, original.source, &original.directory,
            crate::FilesystemProfile::Windows, crate::VolumeLimits::default(),
            &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        ).expect_err("cancelled decode must not allocate");
        assert!(matches!(error, DemandError::Cancelled));
        assert_eq!(work, WorkCounters::default());
        assert_eq!(ledger.live_bytes(), 0);
        Ok(())
    }
}

/// Immutable entries backed by the producer's original page, never a retry copy.
#[derive(Debug)]
pub struct SourceDirectoryEntries {
    storage: SourceEntryStorage,
    range: std::ops::Range<usize>,
    indices: Option<Vec<usize>>,
    backing_bytes: u64,
}

#[derive(Debug)]
enum SourceEntryStorage {
    Owned(Vec<SourceDirectoryEntry>),
    Shared(std::sync::Arc<Vec<SourceDirectoryEntry>>),
}

impl SourceDirectoryEntries {
    fn buffer_bytes(entries: &Vec<SourceDirectoryEntry>) -> u64 {
        (entries.capacity() * std::mem::size_of::<SourceDirectoryEntry>()) as u64
            + entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>()
    }

    fn owned(entries: Vec<SourceDirectoryEntry>) -> Self {
        let end = entries.len();
        let backing_bytes = Self::buffer_bytes(&entries);
        Self { storage: SourceEntryStorage::Owned(entries), range: 0..end, indices: None, backing_bytes }
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    fn shared(
        entries: std::sync::Arc<Vec<SourceDirectoryEntry>>, range: std::ops::Range<usize>, backing_bytes: u64,
    ) -> Self {
        Self { storage: SourceEntryStorage::Shared(entries), range, indices: None, backing_bytes }
    }

    fn backing(&self) -> &[SourceDirectoryEntry] {
        match &self.storage {
            SourceEntryStorage::Owned(entries) => entries,
            SourceEntryStorage::Shared(entries) => entries,
        }
    }

    fn backing_index(&self, index: usize) -> Option<usize> {
        if let Some(indices) = &self.indices { indices.get(index).copied() }
        else if index < self.range.len() { Some(self.range.start + index) } else { None }
    }

    /// Number of selected entries.
    #[must_use]
    pub fn len(&self) -> usize { self.indices.as_ref().map_or(self.range.len(), Vec::len) }

    /// Whether this view selects no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Borrows one selected entry without transferring its replay backing.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&SourceDirectoryEntry> {
        self.backing_index(index).and_then(|index| self.backing().get(index))
    }

    /// Traverses only the selected entries, preserving source order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SourceDirectoryEntry> {
        (0..self.len()).map(|index| self.get(index).expect("bounded source page index"))
    }

    /// Actual unique page and selector buffers kept alive by this view.
    #[must_use]
    pub fn retained_bytes(&self) -> u64 {
        self.backing_bytes + self.indices.as_ref().map_or(0, |indices| (indices.capacity() * std::mem::size_of::<usize>()) as u64)
    }
}

impl std::ops::Index<usize> for SourceDirectoryEntries {
    type Output = SourceDirectoryEntry;
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("bounded source page index")
    }
}

impl<'a> IntoIterator for &'a SourceDirectoryEntries {
    type Item = &'a SourceDirectoryEntry;
    type IntoIter = SourceDirectoryEntriesIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        SourceDirectoryEntriesIter { entries: self, index: 0 }
    }
}

/// An allocation-free borrowed traversal of a published or staged page.
pub struct SourceDirectoryEntriesIter<'a> {
    entries: &'a SourceDirectoryEntries,
    index: usize,
}

impl<'a> Iterator for SourceDirectoryEntriesIter<'a> {
    type Item = &'a SourceDirectoryEntry;
    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.entries.get(self.index)?;
        self.index += 1;
        Some(entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.entries.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for SourceDirectoryEntriesIter<'_> {}

/// A producer's actual publication transaction, not durable filesystem authority.
pub trait SourceDirectoryPublication: Send + Sync {
    /// Maximum infallible commit/abort cleanup still reserved by the producer.
    fn cleanup_budget(&self) -> WorkCounters;
    /// Actual private held backing, excluding the public entry view's backing.
    fn retained_bytes(&self) -> u64;
    /// Publishes cursor progress only after every fallible consumer projection.
    fn accept(&mut self, budget: WorkBudget, cancellation: &CancellationToken) -> std::future::Ready<DemandResult<()>>;
    /// Restores the original position, even when cancellation has already won.
    fn abort(&mut self, budget: WorkBudget) -> DemandResult<()>;
}

/// Owns one publication transaction. Dropping an unaccepted native guard restores
/// its original held page and cursor without allocating or duplicating entries.
pub struct SourcePagePublication {
    guard: Option<Box<dyn SourceDirectoryPublication>>,
}

impl std::fmt::Debug for SourcePagePublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SourcePagePublication").field("pending", &self.guard.is_some()).finish()
    }
}

impl SourcePagePublication {
    /// Explicit stateless publication: no mutable producer cursor exists.
    #[must_use]
    pub const fn stateless() -> Self { Self { guard: None } }

    /// Takes an already-admitted producer guard.
    #[must_use]
    pub fn guarded(guard: Box<dyn SourceDirectoryPublication>) -> Self { Self { guard: Some(guard) } }

    /// Work that must remain reserved until acceptance or abortion.
    #[must_use]
    pub fn cleanup_budget(&self) -> WorkCounters {
        self.guard.as_ref().map_or(WorkCounters::UNCHARGED, |guard| guard.cleanup_budget())
    }

    /// Private held memory not already counted by the public entry view.
    #[must_use]
    pub fn retained_bytes(&self) -> u64 { self.guard.as_ref().map_or(0, |guard| guard.retained_bytes()) }

    /// Completes the producer's pre-admitted publication transaction.
    pub async fn accept(&mut self, budget: WorkBudget, cancellation: &CancellationToken) -> DemandResult<()> {
        let answer = if let Some(guard) = self.guard.as_mut() { guard.accept(budget, cancellation).await }
        else {
            cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
            Ok(OperationReceipt { value: (), work: WorkCounters::UNCHARGED })
        };
        if answer.is_ok() { self.guard.take(); }
        answer
    }

    /// Aborts without a cancellation check; cleanup cannot be skipped.
    pub fn abort(&mut self, budget: WorkBudget) -> DemandResult<()> {
        let answer = self.guard.as_mut().map_or(
            Ok(OperationReceipt { value: (), work: WorkCounters::UNCHARGED }),
            |guard| guard.abort(budget),
        );
        if answer.is_ok() { self.guard.take(); }
        answer
    }
}

/// One unpublished bounded page, in native enumeration order.
#[derive(Debug)]
pub struct SourceDirectoryPage {
    /// Immutable entries owned by this page's original producer.
    pub entries: SourceDirectoryEntries,
    /// Continuation becomes usable only after accepting publication.
    pub next: Option<SourceCursor>,
    /// Directory metadata evidence for this page.
    pub version: SourceVersion,
    /// Required acceptance/abortion of producer progress.
    pub publication: SourcePagePublication,
}

impl SourceDirectoryPage {
    /// Constructs an actual stateless provider page without extra allocation.
    #[must_use]
    pub fn stateless(entries: Vec<SourceDirectoryEntry>, next: Option<SourceCursor>, version: SourceVersion) -> Self {
        Self { entries: SourceDirectoryEntries::owned(entries), next, version, publication: SourcePagePublication::stateless() }
    }

    /// Actual unique SDK-owned backing held throughout consumer projection.
    #[must_use]
    pub fn retained_bytes(&self) -> u64 {
        self.entries.retained_bytes() + self.next.as_ref().map_or(0, SourceCursor::retained_bytes)
            + self.publication.retained_bytes()
    }
}

/// Typed failures from a source dependency.
#[derive(Debug, Error)]
pub enum DemandError {
    /// The reference is for another source or an invalidated epoch.
    #[error("source reference is stale")]
    StaleSource,
    /// The node changed while a request was being answered.
    #[error("source node changed during the request")]
    StaleVersion,
    /// A directory cursor is expired or belongs to another query.
    #[error("source directory cursor is expired or mismatched")]
    StaleCursor,
    /// Another request is currently advancing this cursor.
    #[error("source directory cursor is in use")]
    CursorBusy,
    /// Source root disappeared or was replaced.
    #[error("source root is unavailable")]
    SourceUnavailable,
    /// Exact requested path is absent.
    #[error("source path is absent")]
    Absent,
    /// Request requires a regular file.
    #[error("source node is not a regular file")]
    NotRegularFile,
    /// Request requires a directory.
    #[error("source node is not a directory")]
    NotDirectory,
    /// Bounds or path are invalid.
    #[error("source request is invalid")]
    InvalidRequest,
    /// Request was cancelled.
    #[error("source request was cancelled")]
    Cancelled,
    /// The bounded native worker could not complete the request.
    #[error("native source worker failed")]
    WorkerUnavailable,
    /// Explicit native preparation allowance was exhausted.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Native or remote source failed.
    #[error("source failed: {0}")]
    Io(std::io::Error),
    /// Demand-driven native observation could not be admitted exactly.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error(transparent)]
    Observation(#[from] crate::watch::NativeWatchError),
    /// Canonical mounted-source delivery failed with its original typed cause.
    #[cfg(feature = "native-mount")]
    #[error(transparent)]
    Mount(#[from] crate::native_mount::MountSourceError),
    /// A held Linux metadata mutation failed without losing its typed cause.
    #[cfg(all(feature = "native-watch", target_os = "linux", any(feature = "native-mount", test)))]
    #[error(transparent)]
    LinuxMetadata(#[from] crate::native_host::LinuxMetadataError),
    /// A held macOS metadata mutation failed without losing its typed cause.
    #[cfg(all(feature = "native-watch", target_os = "macos", any(feature = "native-mount", test)))]
    #[error(transparent)]
    MacMetadata(#[from] crate::native_host::MacMetadataError),
    /// A held Windows metadata mutation failed without losing its typed cause.
    #[cfg(all(feature = "native-watch", windows, any(feature = "native-mount", test)))]
    #[error(transparent)]
    WindowsMetadata(#[from] crate::native_host::WindowsMetadataError),
}

impl From<std::io::Error> for DemandError {
    fn from(error: std::io::Error) -> Self {
        // A file that gave way to a rename or delete changed under the read.
        #[cfg(all(windows, feature = "native-watch"))]
        if error
            .get_ref()
            .is_some_and(|inner| inner.is::<crate::native_host::Yielded>())
        {
            return Self::StaleVersion;
        }
        Self::Io(error)
    }
}

impl From<NamespacePathError> for DemandError {
    fn from(error: NamespacePathError) -> Self {
        match error {
            NamespacePathError::Work(error) => Self::Work(error),
            NamespacePathError::Cancelled => Self::Cancelled,
            NamespacePathError::AllocationFailed => Self::WorkerUnavailable,
            _ => Self::InvalidRequest,
        }
    }
}

/// Demand result with source-work counters on both success and failure.
pub type DemandResult<T> = Result<OperationReceipt<T>, OperationFailure<DemandError>>;

/// One change a source reports having happened to it outside every view of
/// it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceChange {
    /// This name may bind another entry, or none, and so may everything
    /// reached through it.
    Name(NamespacePath),
    /// The entry this name binds may have changed in place: its content or
    /// attributes. The name, and everything reached through it, still binds
    /// what it did.
    Entry(NamespacePath),
    /// The node with this [`SourceNode::file_identity`] may have changed,
    /// under every name bound to it.
    Node([u8; 32]),
    /// Changes may have gone unreported: anything may have changed.
    Everything,
    /// Changes may not all have been reported yet, though none is known to
    /// have been lost: every fact read before is verified against the
    /// source at its next use, and kept where it still holds.
    Unconfirmed,
}

/// Receives the changes one watched source reports.
pub trait SourceChangeSink: Send + Sync {
    /// One batch of changes, in the order the host reported them. Called on
    /// the source's notification thread, which holds no lock a source
    /// request waits on; the sink must not wait on a later notification.
    fn source_changed(
        &self, changes: &[SourceChange], budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<()>;
}

/// Live observation of every change made to one source outside every view
/// of it. Dropping it stops the notifications.
pub trait SourceWatch: Send + Sync {
    /// Returns once every change that completed before the call has reached
    /// the sink.
    ///
    /// # Errors
    ///
    /// Fails only when the host cannot be read; the watch is then inexact.
    fn fence(&self) -> Result<(), DemandError>;

    /// Whether every change still reaches the sink. False once the host
    /// stopped reporting changes exactly, after the sink last received
    /// [`SourceChange::Everything`]; from then on a view reads the source
    /// afresh.
    fn is_exact(&self) -> bool;
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
fn cancellation_error(error: crate::cancellation::CancellationWaitError) -> DemandError {
    match error {
        crate::cancellation::CancellationWaitError::Work(error) => error.into(),
        crate::cancellation::CancellationWaitError::AllocationFailed => {
            std::io::Error::from(std::io::ErrorKind::OutOfMemory).into()
        }
    }
}


#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
struct DemandCancellationRoot {
    token: CancellationToken,
    admission: crate::cancellation::CancellationWaitAdmission,
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
struct DemandObserverCustody {
    token: std::sync::Arc<crate::kernel::ProofDataCustody>,
    adopted_bytes: u64,
}

/// Registry ownership travels with the one operation ledger, including
/// original cancellation roots and measured mount-view gate backing.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[derive(Default)]
pub(crate) struct DemandCancellationRoots {
    inline: [Option<DemandCancellationRoot>; 3],
    overflow: Vec<DemandCancellationRoot>,
    observers: [Option<DemandObserverCustody>; 3],
    observer_overflow: Vec<DemandObserverCustody>,
    observer_capacity: crate::kernel::allocation::LogicalVecCapacity,
    #[cfg(feature = "native-mount")]
    view_gates: crate::native_mount::ViewGateReadAdmissions,
    #[cfg(feature = "native-mount")]
    view_observers: crate::native_mount::ViewObserverAdmissions,
    #[cfg(feature = "native-mount")]
    native_mount_admissions: crate::native_mount::NativeMountAllocationAdmissions,
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
impl DemandCancellationRoots {
    fn reconcile(&mut self, allocations: &mut crate::kernel::allocation::AllocationLedger) -> Result<(), crate::WorkError> {
        for root in self.inline.iter_mut().flatten().chain(self.overflow.iter_mut()) {
            root.admission.reconcile_registry(&root.token, allocations)?;
        }
        for custody in self.observers.iter_mut().flatten().chain(self.observer_overflow.iter_mut()) {
            let current = custody.token.retained_bytes();
            if current < custody.adopted_bytes {
                allocations.release(custody.adopted_bytes - current)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                custody.adopted_bytes = current;
            }
        }
        #[cfg(feature = "native-mount")]
        self.view_gates.reconcile(allocations)?;
        #[cfg(feature = "native-mount")]
        self.view_observers.reconcile(allocations)?;
        #[cfg(feature = "native-mount")]
        self.native_mount_admissions.reconcile(allocations)?;
        Ok(())
    }

    fn freed_bytes(&self) -> u64 {
        let freed = self.inline.iter().flatten().chain(self.overflow.iter())
            .map(|root| root.admission.freed_registry_bytes(&root.token))
            .sum::<u64>();
        #[cfg(feature = "native-mount")]
        let freed = freed.checked_add(self.view_gates.freed_bytes())
            .expect("retained registry proofs cannot exceed the ledger's owned byte range");
        #[cfg(feature = "native-mount")]
        let freed = freed.checked_add(self.view_observers.freed_bytes())
            .expect("retained observer registry proofs belong to the original ledger");
        #[cfg(feature = "native-mount")]
        let freed = freed.checked_add(self.native_mount_admissions.freed_bytes())
            .expect("native lifecycle admissions belong to the original ledger");
        self.observers.iter().flatten().chain(self.observer_overflow.iter())
            .try_fold(freed, |sum, custody| sum.checked_add(
                custody.adopted_bytes.saturating_sub(custody.token.retained_bytes()),
            ))
            .expect("owned observer and registry data cannot exceed original ledger custody")
    }

    fn unrecorded_observer_bytes(&self) -> u64 {
        self.observers.iter().flatten().chain(self.observer_overflow.iter())
            .try_fold(0_u64, |sum, custody| sum.checked_add(
                custody.token.retained_bytes().saturating_sub(custody.adopted_bytes),
            ))
            .expect("completed observer data remains representable in its original ledger")
    }

    fn retain_observer(
        &mut self, token: std::sync::Arc<crate::kernel::ProofDataCustody>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<(), DemandError> {
        for custody in self.observers.iter().flatten().chain(self.observer_overflow.iter()) {
            work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
            if std::sync::Arc::ptr_eq(&custody.token, &token) { return Ok(()); }
        }
        let custody = DemandObserverCustody { token, adopted_bytes: 0 };
        if let Some(index) = self.observers.iter().position(Option::is_none) {
            self.observers[index] = Some(custody);
            return Ok(());
        }
        let prior_peak = work.peak_allocation_bytes;
        if let Err(error) = self.observer_capacity.ensure_for_push(
            &mut self.observer_overflow, usize::MAX, allocations, work, budget,
        ) {
            work.peak_allocation_bytes = prior_peak;
            return Err(error.into_work_or(DemandError::InvalidRequest));
        }
        self.observer_overflow.push(custody);
        Ok(())
    }

    fn record_observer_growth(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<(), crate::WorkError> {
        let mut failure = None;
        for custody in self.observers.iter_mut().flatten().chain(self.observer_overflow.iter_mut()) {
            let current = custody.token.retained_bytes();
            if current > custody.adopted_bytes {
                let delta = current - custody.adopted_bytes;
                let recorded = record_completed_retention(allocations, work, budget, delta);
                custody.adopted_bytes = current;
                if let Err(error) = recorded { failure.get_or_insert(error); }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn find(
        &mut self, token: &CancellationToken, allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<&mut crate::cancellation::CancellationWaitAdmission, crate::cancellation::CancellationWaitError> {
        let mut found = None;
        for (index, root) in self.inline.iter().enumerate() {
            if let Some(root) = root {
                work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)
                    .map_err(crate::cancellation::CancellationWaitError::Work)?;
                if root.token.same_state(token) { found = Some(index); break; }
            }
        }
        if let Some(index) = found {
            return Ok(&mut self.inline[index].as_mut().expect("selected cancellation root").admission);
        }
        let mut found = None;
        for (index, root) in self.overflow.iter().enumerate() {
            work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)
                .map_err(crate::cancellation::CancellationWaitError::Work)?;
            if root.token.same_state(token) { found = Some(index); break; }
        }
        if let Some(index) = found { return Ok(&mut self.overflow[index].admission); }
        let root = DemandCancellationRoot { token: token.clone(), admission: Default::default() };
        if let Some(index) = self.inline.iter().position(Option::is_none) {
            self.inline[index] = Some(root);
            return Ok(&mut self.inline[index].as_mut().expect("inserted cancellation root").admission);
        }
        let old_bytes = (self.overflow.capacity() as u64)
            .checked_mul(std::mem::size_of::<DemandCancellationRoot>() as u64)
            .ok_or(crate::cancellation::CancellationWaitError::Work(crate::WorkError::Overflow))?;
        if self.overflow.len() == self.overflow.capacity() {
            let next = self.overflow.len().checked_add(1)
                .ok_or(crate::cancellation::CancellationWaitError::Work(crate::WorkError::Overflow))?;
            let bytes = (next as u64).checked_mul(std::mem::size_of::<DemandCancellationRoot>() as u64)
                .ok_or(crate::cancellation::CancellationWaitError::Work(crate::WorkError::Overflow))?;
            let copy = WorkCounters { bytes_copied: (self.overflow.len() * std::mem::size_of::<DemandCancellationRoot>()) as u64, ..WorkCounters::default() };
            work.admit(&copy, &budget).map_err(crate::cancellation::CancellationWaitError::Work)?;
            let prior_peak = work.peak_allocation_bytes;
            allocations.claim_bytes(bytes, 1, work, budget)
                .map_err(|error| crate::cancellation::CancellationWaitError::Work(error.into_work_or(crate::WorkError::Overflow)))?;
            let mut replacement = Vec::new();
            if replacement.try_reserve_exact(next).is_err() {
                allocations.release(bytes).map_err(|error| crate::cancellation::CancellationWaitError::Work(error.into_work_or(crate::WorkError::Overflow)))?;
                work.peak_allocation_bytes = prior_peak;
                return Err(crate::cancellation::CancellationWaitError::AllocationFailed);
            }
            replacement.append(&mut self.overflow);
            work.charge(&copy, &budget).map_err(crate::cancellation::CancellationWaitError::Work)?;
            self.overflow = replacement;
            allocations.release(old_bytes).map_err(|error| crate::cancellation::CancellationWaitError::Work(error.into_work_or(crate::WorkError::Overflow)))?;
        }
        self.overflow.push(root);
        Ok(&mut self.overflow.last_mut().expect("inserted overflow cancellation root").admission)
    }

    fn for_prepared(&mut self, prepared: &crate::cancellation::PreparedCancelled) -> Option<&mut crate::cancellation::CancellationWaitAdmission> {
        self.inline.iter_mut().flatten().chain(self.overflow.iter_mut())
            .find(|root| prepared.same_token(&root.token))
            .map(|root| &mut root.admission)
    }
}

/// Accounting for data whose producer already finished; never an IO or
/// allocation admission. A violated ceiling retains the actual prefix.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
fn record_completed_retention(
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget, bytes: u64,
) -> Result<(), crate::WorkError> {
    if let Err(error) = allocations.claim_bytes(bytes, 0, work, budget) {
        let failure = error.into_work_or(crate::WorkError::Overflow);
        allocations.claim_bytes(bytes, 0, work, WorkBudget::UNBOUNDED)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        return Err(failure);
    }
    Ok(())
}

/// One native attempt's admission and allocation state. Helpers charge at the
/// actual boundary, before starting work; denied work never enters the receipt.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) struct DemandWork {
    pub(crate) work: WorkCounters,
    pub(crate) budget: WorkBudget,
    allocations: crate::kernel::allocation::AllocationLedger,
    cancellation: CancellationToken,
    parent_cancellation: Option<CancellationToken>,
    stop: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    cancellation_wait: DemandCancellationRoots,
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
impl DemandWork {
    pub(crate) fn new(budget: WorkBudget, cancellation: &CancellationToken) -> Self {
        Self {
            work: WorkCounters::default(),
            budget,
            allocations: crate::kernel::allocation::AllocationLedger::default(),
            cancellation: cancellation.clone(),
            parent_cancellation: None,
            stop: None,
            cancellation_wait: DemandCancellationRoots::default(),
        }
    }

    /// Moves the complete original invocation into the existing native queue.
    /// The exclusive caller must restore it on every terminal path before
    /// using this inert shell; no operation may execute against the shell.
    pub(crate) fn take_for_native_queue(&mut self) -> Self {
        let cancellation = self.cancellation.clone();
        std::mem::replace(self, Self::new(WorkBudget::default(), &cancellation))
    }

    /// Recovers original custody under the invocation's full budget, not its
    /// remaining allowance. Pending cleanup reservations are reconstructed
    /// from the original retained proofs.
    pub(crate) fn from_current(
        budget: WorkBudget, cancellation: &CancellationToken, work: WorkCounters,
        allocations: crate::kernel::allocation::AllocationLedger,
        cancellation_wait: DemandCancellationRoots,
    ) -> Self {
        #[cfg(feature = "native-mount")]
        let cancellation_wait = {
            let mut roots = cancellation_wait;
            roots.view_gates.budget_reconstructed();
            roots
        };
        Self {
            work, budget, allocations, cancellation: cancellation.clone(),
            parent_cancellation: None, stop: None, cancellation_wait,
        }
    }

    /// Restores an original full budget without losing retained quota proofs.
    pub(crate) fn restore_budget(&mut self, budget: WorkBudget) {
        self.budget = budget;
        #[cfg(feature = "native-mount")]
        self.cancellation_wait.view_gates.budget_reconstructed();
    }

    pub(crate) fn into_current(mut self) -> (
        WorkCounters, crate::kernel::allocation::AllocationLedger,
        DemandCancellationRoots,
    ) {
        self.reconcile_cancellation_registry()
            .expect("original reserved registry cleanup remains accountable across ownership transfer");
        (self.work, self.allocations, self.cancellation_wait)
    }

    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub(crate) fn check(&self) -> Result<(), DemandError> {
        self.cancellation.check().map_err(|_| DemandError::Cancelled)?;
        if self.parent_cancellation.as_ref().is_some_and(CancellationToken::is_cancelled) {
            return Err(DemandError::Cancelled);
        }
        if self.stop.as_ref().is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
            return Err(DemandError::Cancelled);
        }
        Ok(())
    }

    /// Splits the existing ledger and retained gate proofs without replacing
    /// either. Admission callers check cancellation; terminal cleanup must
    /// still recover these original parts after cancellation.
    #[cfg(feature = "native-mount")]
    pub(crate) fn view_gate_admission_parts(&mut self) -> Result<(
        &mut crate::native_mount::ViewGateReadAdmissions,
        &mut crate::kernel::allocation::AllocationLedger,
        &mut WorkCounters,
        WorkBudget,
    ), DemandError> {
        self.reconcile_cancellation_registry()?;
        Ok((&mut self.cancellation_wait.view_gates, &mut self.allocations, &mut self.work, self.budget))
    }

    /// Borrows the original native view-observer registry custody and ledger.
    #[cfg(feature = "native-mount")]
    pub(crate) fn view_observer_admission_parts(&mut self) -> Result<(
        &mut crate::native_mount::ViewObserverAdmissions,
        &mut crate::kernel::allocation::AllocationLedger,
        &mut WorkCounters,
        WorkBudget,
    ), DemandError> {
        self.reconcile_cancellation_registry()?;
        Ok((&mut self.cancellation_wait.view_observers, &mut self.allocations, &mut self.work, self.budget))
    }

    /// Borrows only this invocation's physical native-lifecycle admissions.
    #[cfg(feature = "native-mount")]
    pub(crate) fn native_mount_admission_parts(&mut self) -> Result<(
        &mut crate::native_mount::NativeMountAllocationAdmissions,
        &mut crate::kernel::allocation::AllocationLedger,
        &mut WorkCounters,
        WorkBudget,
    ), DemandError> {
        self.reconcile_cancellation_registry()?;
        Ok((&mut self.cancellation_wait.native_mount_admissions, &mut self.allocations, &mut self.work, self.budget))
    }

    pub(crate) fn prepare_cancelled(
        &mut self, token: &CancellationToken,
    ) -> Result<crate::cancellation::PreparedCancelled, crate::cancellation::CancellationWaitError> {
        self.reconcile_cancellation_registry().map_err(crate::cancellation::CancellationWaitError::Work)?;
        if token.is_cancelled() {
            return token.prepare_cancelled_measured(
                &mut self.allocations, &mut self.work, self.budget, &mut Default::default(),
            );
        }
        let admission = self.cancellation_wait.find(token, &mut self.allocations, &mut self.work, self.budget)?;
        token.prepare_cancelled_measured(&mut self.allocations, &mut self.work, self.budget, admission)
    }

    pub(crate) fn finish_cancelled(
        &mut self, prepared: crate::cancellation::PreparedCancelled,
    ) -> Result<(), crate::cancellation::CancellationWaitError> {
        self.reconcile_cancellation_registry().map_err(crate::cancellation::CancellationWaitError::Work)?;
        if let Some(admission) = self.cancellation_wait.for_prepared(&prepared) {
            prepared.finish(&mut self.allocations, &mut self.work, self.budget, admission)
        } else {
            assert_eq!(prepared.retained_waiter_bytes(), 0, "prepared cancellation retains its original root proof");
            prepared.finish(&mut self.allocations, &mut self.work, self.budget, &mut Default::default())
        }
    }

    pub(crate) fn rebind_cancellation(&mut self, token: &CancellationToken) {
        self.cancellation = token.clone();
    }

    pub(crate) fn new_cancellation(&mut self) -> Result<CancellationToken, crate::WorkError> {
        self.reconcile_cancellation_registry()?;
        CancellationToken::new_measured(&mut self.allocations, &mut self.work, self.budget)
    }

    fn reconcile_cancellation_registry(&mut self) -> Result<(), crate::WorkError> {
        self.cancellation_wait.reconcile(&mut self.allocations)?;
        #[cfg(feature = "native-mount")]
        self.cancellation_wait.view_gates.reconcile_work(&mut self.work, &mut self.budget)?;
        self.cancellation_wait.record_observer_growth(&mut self.allocations, &mut self.work, self.budget)?;
        Ok(())
    }

    pub(crate) fn live_allocation_bytes(&self) -> u64 {
        self.allocations.live_bytes()
            .checked_sub(self.cancellation_wait.freed_bytes())
            .and_then(|live| live.checked_add(self.cancellation_wait.unrecorded_observer_bytes()))
            .expect("retained observer and registry data belong to this allocation ledger")
    }

    /// Retains only this observer's new-data ownership token. Its constructor
    /// must already have accounted the token payload, which now remains live
    /// in this original carrier rather than ending with the observer handle.
    pub(crate) fn retain_observer_custody(
        &mut self, token: std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.cancellation_wait.retain_observer(
            token, &mut self.allocations, &mut self.work, self.budget,
        )
    }

    pub(crate) fn charge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.work.charge(&delta, &self.budget)?;
        Ok(())
    }

    /// Records admitted work that already completed, even if cancellation
    /// arrived during its native call.
    pub(crate) fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.work.charge(&delta, &self.budget)?;
        Ok(())
    }

    pub(crate) fn allocate(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.allocations
            .claim_bytes(bytes, u64::from(bytes != 0), &mut self.work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))
    }


    fn allocate_for(&mut self, bytes: u64, delta: WorkCounters) -> Result<(), DemandError> {
        self.work.admit(&delta, &self.budget)?;
        self.allocate(bytes)?;
        self.record(delta)
    }

    pub(crate) fn adopt(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.allocations.claim_bytes(bytes, 0, &mut self.work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))
    }

    /// Records known, already-completed ownership even after cancellation.
    /// This is not admission for creating or allocating new data.
    pub(crate) fn record_adoption(&mut self, bytes: u64) -> Result<(), DemandError> {
        let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
        let recorded = record_completed_retention(&mut self.allocations, &mut self.work, self.budget, bytes)
            .map_err(DemandError::from);
        reconciled.and(recorded)
    }

    pub(crate) fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.allocations
            .release(bytes)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))
    }

    pub(crate) fn remaining(&self) -> Result<WorkBudget, DemandError> {
        let mut budget = self.work.remaining(self.budget)?;
        budget.peak_allocation_bytes = budget.peak_allocation_bytes
            .checked_sub(self.live_allocation_bytes())
            .ok_or(crate::WorkError::Overflow)?;
        Ok(budget)
    }

    pub(crate) fn merge(&mut self, work: WorkCounters) -> Result<(), DemandError> {
        // The child receipt already includes its newly retained observer data.
        // Release original deaths first; adopt new data only after composing
        // the child peak, so the same payload is not counted twice.
        self.cancellation_wait.reconcile(&mut self.allocations)?;
        #[cfg(feature = "native-mount")]
        let cleanup = self.cancellation_wait.view_gates.reconcile_work(&mut self.work, &mut self.budget);
        #[cfg(not(feature = "native-mount"))]
        let cleanup: Result<(), crate::WorkError> = Ok(());
        self.work = self.work.with_backend(work, self.allocations.live_bytes())?;
        let growth = self.cancellation_wait.record_observer_growth(
            &mut self.allocations, &mut self.work, self.budget,
        );
        cleanup?;
        growth?;
        self.work.verify(self.budget)?;
        Ok(())
    }

    pub(crate) fn finish<T>(mut self, result: Result<T, DemandError>) -> DemandResult<T> {
        let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
        reconciled.and(result).map(|value| OperationReceipt { value, work: self.work })
            .map_err(|error| OperationFailure::new(error, self.work))
    }
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
fn namespace_owned_bytes(path: &NamespacePath) -> Result<u64, DemandError> {
    let names = (path.depth() as u64)
        .checked_mul(std::mem::size_of::<crate::kernel::LogicalName>() as u64)
        .ok_or(crate::WorkError::Overflow)?;
    path.components().iter().try_fold(names, |bytes, name| {
        bytes.checked_add(name.as_bytes().len() as u64)
            .ok_or(crate::WorkError::Overflow.into())
    })
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
fn clone_namespace(path: &NamespacePath, work: &mut DemandWork) -> Result<NamespacePath, DemandError> {
    work.check()?;
    work.work.admit(&WorkCounters {
        allocation_operations: path.depth() as u64 + u64::from(path.depth() != 0),
        peak_allocation_bytes: work.allocations.live_bytes()
            .checked_add(namespace_owned_bytes(path)?).ok_or(crate::WorkError::Overflow)?,
        bytes_copied: path.components().iter().map(|name| name.as_bytes().len() as u64).sum(),
        ..WorkCounters::default()
    }, &work.budget)?;
    work.allocate((path.depth() * std::mem::size_of::<crate::kernel::LogicalName>()) as u64)?;
    let mut components = Vec::with_capacity(path.depth());
    for name in path.components() {
        work.allocate(name.as_bytes().len() as u64)?;
        work.record(WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::default() })?;
        components.push(name.clone());
    }
    // Reconstruct this already-valid path without narrowing the limits under
    // which the caller constructed it. Provider limits are checked separately.
    NamespacePath::new(components, crate::model::VolumeLimits {
        maximum_component_bytes: u32::MAX,
        maximum_path_depth: u16::MAX,
        maximum_path_bytes: u32::MAX,
        ..crate::model::VolumeLimits::default()
    }).map_err(DemandError::from)
}

#[cfg(all(feature = "native-watch", unix))]
fn host_component(name: &crate::kernel::LogicalName, work: &mut DemandWork) -> Result<std::ffi::OsString, DemandError> {
    use crate::kernel::NameEncoding;
    use std::os::unix::ffi::OsStringExt;
    if name.encoding() != NameEncoding::WindowsUtf16Le {
        work.allocate_for(name.as_bytes().len() as u64, WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::default() })?;
        return Ok(std::ffi::OsString::from_vec(name.as_bytes().to_vec()));
    }
    let units = || name.as_bytes().chunks_exact(2).map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]));
    let length = char::decode_utf16(units()).try_fold(0usize, |length, value| {
        value.map(|value| length + value.len_utf8()).map_err(|_| DemandError::InvalidRequest)
    })?;
    work.allocate_for(length as u64, WorkCounters { bytes_encoded: length as u64, ..WorkCounters::default() })?;
    let mut text = String::with_capacity(length);
    for value in char::decode_utf16(units()) {
        text.push(value.map_err(|_| DemandError::InvalidRequest)?);
    }
    Ok(text.into())
}

#[cfg(all(feature = "native-watch", windows))]
pub(crate) fn native_wide_path(
    path: &NamespacePath, limits: crate::model::VolumeLimits, work: &mut DemandWork,
) -> Result<Vec<u16>, DemandError> {
    use crate::kernel::NameEncoding;
    if path.depth() > usize::from(limits.maximum_path_depth) || path.encoded_bytes() > limits.maximum_path_bytes {
        return Err(DemandError::InvalidRequest);
    }
    let mut units = path.depth().saturating_sub(1);
    for name in path.components() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
        if name.as_bytes().len() > limits.maximum_component_bytes as usize { return Err(DemandError::InvalidRequest); }
        let length = if name.encoding() == NameEncoding::WindowsUtf16Le {
            name.as_bytes().len() / 2
        } else {
            let text = std::str::from_utf8(name.as_bytes()).map_err(|_| DemandError::InvalidRequest)?;
            if text.contains(['/', '\\']) { return Err(DemandError::InvalidRequest); }
            text.encode_utf16().count()
        };
        units = units.checked_add(length).ok_or(crate::WorkError::Overflow)?;
    }
    let bytes = units.checked_mul(std::mem::size_of::<u16>()).ok_or(crate::WorkError::Overflow)?;
    if bytes > isize::MAX as usize { return Err(crate::WorkError::Overflow.into()); }
    work.allocate(bytes as u64)?;
    let mut relative = Vec::with_capacity(units);
    for name in path.components() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
        if !relative.is_empty() {
            work.charge(WorkCounters { bytes_encoded: 2, ..WorkCounters::default() })?;
            relative.push(u16::from(b'\\'));
        }
        if name.encoding() == NameEncoding::WindowsUtf16Le {
            work.charge(WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::default() })?;
            relative.extend(name.as_bytes().chunks_exact(2).map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]])));
        } else {
            let text = std::str::from_utf8(name.as_bytes()).map_err(|_| DemandError::InvalidRequest)?;
            work.charge(WorkCounters { bytes_encoded: (text.encode_utf16().count() * 2) as u64, ..WorkCounters::default() })?;
            relative.extend(text.encode_utf16());
        }
    }
    Ok(relative)
}


/// Provider contract for exact lookup, bounded pages, ranges, and metadata.
/// Implementations must validate the reference and version for each request.
/// Every provider must admit each backend call, consumed byte, item, and owned
/// buffer before spending the supplied budget, and retain actual spent work on
/// errors and cooperative cancellation. No unbounded-provider fallback is used.
/// Async methods do not by themselves guarantee kernel completion I/O; each
/// provider must document its execution mechanism separately.
pub trait DemandSource: Send + Sync {
    /// Current opaque source reference.
    fn reference(&self) -> SourceReference;

    /// Resolve only the requested path. `None` is known absence.
    fn lookup(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = DemandResult<Option<SourceNode>>> + Send;

    /// Continue one directory enumeration by at most `maximum_entries`.
    fn list_page(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = DemandResult<SourceDirectoryPage>> + Send;

    /// Read at most `length` bytes at `offset`, using exact version evidence.
    fn read_range(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = DemandResult<crate::kernel::OwnedSourceChunk>> + Send;

    /// Opens one regular file at exactly `expected` for repeated range reads.
    fn open_file(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = DemandResult<Box<dyn DemandFile>>> + Send;

    /// Reads one symbolic link's exact opaque target without following it.
    fn read_link(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = DemandResult<crate::kernel::OwnedSourceChunk>> + Send;

    /// Starts reporting every change made to this source outside every view
    /// of it to `sink`, when the host reports such changes exactly: `None`
    /// when it cannot, and every view of the source then reads it afresh.
    /// The answer holds for the source, not per request.
    ///
    /// # Errors
    ///
    /// Fails when the host refuses a watch it would otherwise provide.
    fn watch(
        &self,
        sink: std::sync::Arc<dyn SourceChangeSink>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<Box<dyn SourceWatch>>> {
        let _ = (sink, budget);
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        Ok(OperationReceipt { value: None, work: WorkCounters::default() })
    }

    /// Read metadata without reading a file body.
    fn read_metadata(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: Option<SourceVersion>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = DemandResult<Option<SourceNode>>> + Send {
        async move {
            let mut answer = self.lookup(source, path, budget, cancellation).await?;
            cancellation.check().map_err(|_| OperationFailure::new(DemandError::Cancelled, answer.work))?;
            if let Some(expected) = expected {
                answer.work.charge(
                    &WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, &budget,
                ).map_err(|error| OperationFailure::new(DemandError::Work(error), answer.work))?;
                match answer.value {
                    Some(node) if node.version == expected => {}
                    _ => {
                        return Err(OperationFailure::new(
                            DemandError::StaleVersion,
                            answer.work,
                        ));
                    }
                }
            }
            Ok(answer)
        }
    }
}

/// A regular source file opened at one exact version.
///
/// Reads block the calling thread like a native file read, so they belong on
/// threads that may block, such as native mount callback threads. Every read
/// proves that the opened version is still current before returning bytes.
pub trait DemandFile: Send + Sync {
    /// SDK-owned payload retained by this opened file, including its box;
    /// shared source authority and private allocator headers are not payload.
    fn logical_owned_bytes(&self) -> u64;

    /// Reads at most `length` bytes at `offset`; a range past the end of the
    /// file is shortened, never padded.
    ///
    /// # Errors
    ///
    /// Fails with the same typed evidence as [`DemandSource::read_range`]
    /// once the file, its path, or its source is no longer the opened one.
    fn read_range(
        &self,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<crate::kernel::OwnedSourceChunk>;
}

/// Synchronous admission hook for directories reached by a native lazy source.
///
/// Implementations normally install one non-recursive native notification
/// subscription. The hook runs before the corresponding filesystem read, so a
/// change cannot race between first observation and watcher admission. It must
/// inspect only the named directory and its ancestors; recursive enumeration
/// would violate the lazy-source contract.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub trait DemandDirectoryObserver: Send + Sync {
    /// Admits one exact directory for observation.
    ///
    /// # Errors
    ///
    /// Fails closed when the directory cannot be observed exactly.
    fn observe_directory(
        &self,
        directory: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<()>;
}

/// A composable source view that hides exact subtrees before they enter lazy
/// observation state. This is the SDK boundary used for host-private entries
/// such as a physical checkout's `.git` directory.
#[derive(Clone)]
pub struct FilteredDemandSource<D> {
    inner: D,
    excluded: std::sync::Arc<Vec<NamespacePath>>,
}

impl<D> FilteredDemandSource<D> {
    /// Wraps a source with canonical excluded subtree roots.
    #[must_use]
    pub fn new(inner: D, excluded: Vec<NamespacePath>) -> Self {
        Self {
            inner,
            excluded: std::sync::Arc::new(excluded),
        }
    }

    /// Returns the wrapped source capability.
    #[must_use]
    pub const fn inner(&self) -> &D {
        &self.inner
    }

    fn excludes(
        &self, path: &NamespacePath, work: &mut WorkCounters,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<bool, DemandError> {
        for root in self.excluded.iter() {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
            if path.is_within(root) { return Ok(true); }
        }
        Ok(false)
    }

    fn excludes_child(
        &self, directory: &NamespacePath, name: &crate::kernel::LogicalName,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<bool, DemandError> {
        for root in self.excluded.iter() {
            work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
            if root.components().len() == directory.components().len().saturating_add(1)
                && root.components().starts_with(directory.components())
                && root.components().last() == Some(name) { return Ok(true); }
        }
        Ok(false)
    }
}

/// Forwards changes outside a filtered source's excluded subtrees, which no
/// view of it can hold facts about.
struct FilteredChangeSink {
    excluded: std::sync::Arc<Vec<NamespacePath>>,
    sink: std::sync::Arc<dyn SourceChangeSink>,
}

fn clone_source_change_with_ledger(
    change: &SourceChange, ledger: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<SourceChange, DemandError> {
    cancellation.check().map_err(|_| DemandError::Cancelled)?;
    Ok(match change {
        SourceChange::Name(path) => SourceChange::Name(path.clone_with_ledger(ledger, work, budget, cancellation)?),
        SourceChange::Entry(path) => SourceChange::Entry(path.clone_with_ledger(ledger, work, budget, cancellation)?),
        SourceChange::Node(identity) => {
            work.charge(&WorkCounters { bytes_copied: identity.len() as u64, ..WorkCounters::default() }, &budget)?;
            SourceChange::Node(*identity)
        }
        SourceChange::Everything => SourceChange::Everything,
        SourceChange::Unconfirmed => SourceChange::Unconfirmed,
    })
}

impl SourceChangeSink for FilteredChangeSink {
    fn source_changed(
        &self, changes: &[SourceChange], budget: WorkBudget, cancellation: &CancellationToken,
    ) -> DemandResult<()> {
        let mut work = WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = (|| {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            if changes.is_empty() { return Ok(()); }
            let bytes = (changes.len() as u64).checked_mul(std::mem::size_of::<SourceChange>() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            allocations.claim_bytes(bytes, 1, &mut work, budget)
                .map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
            let mut visible = Vec::new();
            let prior_peak = work.peak_allocation_bytes.saturating_sub(bytes);
            if visible.try_reserve_exact(changes.len()).is_err() {
                allocations.release(bytes).map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
                work.peak_allocation_bytes = prior_peak;
                return Err(DemandError::WorkerUnavailable);
            }
            for change in changes {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
                let mut excluded = false;
                if let SourceChange::Name(path) | SourceChange::Entry(path) = change {
                    for root in self.excluded.iter() {
                        cancellation.check().map_err(|_| DemandError::Cancelled)?;
                        work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
                        if path.is_within(root) { excluded = true; break; }
                    }
                }
                if !excluded {
                    visible.push(clone_source_change_with_ledger(change, &mut allocations, &mut work, budget, cancellation)?);
                }
            }
            if visible.is_empty() { return Ok(()); }
            let mut remaining = work.remaining(budget)?;
            remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                .checked_sub(allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
            match self.sink.source_changed(&visible, remaining, cancellation) {
                Ok(receipt) => {
                    work = work.with_backend(receipt.work, allocations.live_bytes())?;
                    work.verify(budget)?;
                    Ok(())
                }
                Err(failure) => {
                    work = work.with_backend(*failure.work, allocations.live_bytes())?;
                    Err(failure.error)
                }
            }
        })();
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }
}

fn demand_with_prior<T>(answer: DemandResult<T>, prior: WorkCounters) -> DemandResult<T> {
    match answer {
        Ok(receipt) => {
            let work = prior.checked_add(receipt.work)
                .map_err(|error| OperationFailure::new(error.into(), prior))?;
            Ok(OperationReceipt { value: receipt.value, work })
        }
        Err(failure) => Err(failure.map_with_prior_work(prior, std::convert::identity)),
    }
}

impl<D: DemandSource> DemandSource for FilteredDemandSource<D> {
    fn reference(&self) -> SourceReference {
        self.inner.reference()
    }

    fn watch(
        &self,
        sink: std::sync::Arc<dyn SourceChangeSink>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<Box<dyn SourceWatch>>> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        let bytes = std::mem::size_of::<FilteredChangeSink>() as u64;
        let work = WorkCounters {
            allocation_operations: 1, peak_allocation_bytes: bytes, ..WorkCounters::default()
        };
        work.verify(budget).map_err(|error| OperationFailure::before_work(error.into()))?;
        let mut remaining = work.remaining(budget).map_err(|error| OperationFailure::new(error.into(), work))?;
        remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(bytes)
            .ok_or_else(|| OperationFailure::new(DemandError::Work(WorkError::Overflow), work))?;
        let answer = self.inner.watch(std::sync::Arc::new(FilteredChangeSink {
            excluded: std::sync::Arc::clone(&self.excluded),
            sink,
        }), remaining, cancellation);
        match answer {
            Ok(receipt) => {
                let total = work.with_backend(receipt.work, bytes)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                Ok(OperationReceipt { value: receipt.value, work: total })
            }
            Err(failure) => {
                let total = work.with_backend(*failure.work, bytes)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                Err(OperationFailure::new(failure.error, total))
            }
        }
    }

    async fn lookup(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<SourceNode>> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        let mut work = WorkCounters::default();
        if self.excludes(path, &mut work, budget, cancellation)
            .map_err(|error| OperationFailure::new(error, work))? {
            return Ok(OperationReceipt { value: None, work });
        }
        let remaining = work.remaining(budget).map_err(|error| OperationFailure::new(error.into(), work))?;
        demand_with_prior(self.inner.lookup(source, path, remaining, cancellation).await, work)
    }

    async fn list_page(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<SourceDirectoryPage> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        let mut work = WorkCounters::default();
        if self.excludes(directory, &mut work, budget, cancellation)
            .map_err(|error| OperationFailure::new(error, work))? {
            return Err(OperationFailure::new(DemandError::Absent, work));
        }
        let filter_items = u64::from(maximum_entries).checked_mul(self.excluded.len() as u64)
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), work))?;
        let selector_bytes = u64::from(maximum_entries).checked_mul(std::mem::size_of::<usize>() as u64)
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), work))?;
        let selector_operations = u64::from(maximum_entries != 0 && !self.excluded.is_empty());
        work.admit(&WorkCounters {
            items_examined: filter_items, allocation_operations: selector_operations,
            peak_allocation_bytes: if self.excluded.is_empty() { 0 } else { selector_bytes },
            ..WorkCounters::UNCHARGED
        }, &budget).map_err(|error| OperationFailure::new(error.into(), work))?;
        let mut remaining = work.remaining(budget).map_err(|error| OperationFailure::new(error.into(), work))?;
        remaining.items_examined -= filter_items;
        remaining.allocation_operations -= selector_operations;
        if !self.excluded.is_empty() { remaining.peak_allocation_bytes -= selector_bytes; }
        let mut page = demand_with_prior(
            self.inner.list_page(source, directory, cursor, maximum_entries, remaining, cancellation).await,
            work,
        )?;
        if self.excluded.is_empty() { return Ok(page); }
        let projection = (|| {
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            allocations.claim_bytes(page.value.retained_bytes(), 0, &mut page.work, budget)
                .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
            let count = page.value.entries.len();
            let bytes = (count * std::mem::size_of::<usize>()) as u64;
            let peak = page.work.peak_allocation_bytes;
            allocations.claim_bytes(bytes, u64::from(count != 0), &mut page.work, budget)
                .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
            let mut selected = Vec::new();
            if selected.try_reserve_exact(count).is_err() {
                allocations.release(bytes).map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                page.work.peak_allocation_bytes = peak;
                return Err(DemandError::Io(std::io::Error::from(std::io::ErrorKind::OutOfMemory)));
            }
            for index in 0..count {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                let entry = &page.value.entries[index];
                if !self.excludes_child(directory, &entry.name, &mut page.work, budget)? {
                    selected.push(page.value.entries.backing_index(index).ok_or(DemandError::InvalidRequest)?);
                }
            }
            page.value.entries.indices = Some(selected);
            Ok(())
        })();
        if let Err(error) = projection {
            let remaining = page.work.remaining(budget).map_err(|error| OperationFailure::new(error.into(), page.work))?;
            let cleanup = page.value.publication.abort(remaining);
            return match cleanup {
                Ok(receipt) => {
                    let work = page.work.checked_add(receipt.work)
                        .map_err(|error| OperationFailure::new(error.into(), page.work))?;
                    Err(OperationFailure::new(error, work))
                }
                Err(failure) => Err(failure.map_with_prior_work(page.work, std::convert::identity)),
            };
        }
        Ok(page)
    }

    async fn read_range(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        let mut work = WorkCounters::default();
        if self.excludes(path, &mut work, budget, cancellation)
            .map_err(|error| OperationFailure::new(error, work))? {
            return Err(OperationFailure::new(DemandError::Absent, work));
        }
        let remaining = work.remaining(budget).map_err(|error| OperationFailure::new(error.into(), work))?;
        demand_with_prior(self.inner.read_range(source, path, expected, offset, length, remaining, cancellation).await, work)
    }

    async fn open_file(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<Box<dyn DemandFile>> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        let mut work = WorkCounters::default();
        if self.excludes(path, &mut work, budget, cancellation)
            .map_err(|error| OperationFailure::new(error, work))? {
            return Err(OperationFailure::new(DemandError::Absent, work));
        }
        let remaining = work.remaining(budget).map_err(|error| OperationFailure::new(error.into(), work))?;
        demand_with_prior(self.inner.open_file(source, path, expected, remaining, cancellation).await, work)
    }

    async fn read_link(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        let mut work = WorkCounters::default();
        if self.excludes(path, &mut work, budget, cancellation)
            .map_err(|error| OperationFailure::new(error, work))? {
            return Err(OperationFailure::new(DemandError::Absent, work));
        }
        let remaining = work.remaining(budget).map_err(|error| OperationFailure::new(error.into(), work))?;
        demand_with_prior(self.inner.read_link(source, path, expected, remaining, cancellation).await, work)
    }
}

/// Native provider backed by a held, no-follow directory capability.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native {
    use super::*;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use crate::native_host::{HostRoot, HostStat, HostStatReader};
    #[cfg(unix)]
    use crate::native_host::HostListedEntry;
    #[cfg(windows)]
    use crate::native_host::HostListedWideEntry as HostListedEntry;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Semaphore;

    const MAXIMUM_NATIVE_REQUESTS: usize = 64;

    struct RemoteReadPermit;

    impl Drop for RemoteReadPermit {
        fn drop(&mut self) {
            BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
        }
    }

    #[cfg(unix)]
    type DemandPath = PathBuf;
    #[cfg(windows)]
    type DemandPath = Vec<u16>;

    #[cfg(unix)]
    fn path_owned_bytes(path: &DemandPath) -> u64 {
        path.as_os_str().as_encoded_bytes().len() as u64
    }

    #[cfg(windows)]
    fn path_owned_bytes(path: &DemandPath) -> u64 {
        (path.len() * std::mem::size_of::<u16>()) as u64
    }

    struct DirectoryState {
        source: SourceReference,
        directory: NamespacePath,
        version: SourceVersion,
        entries: Option<HostStatReader>,
        pending: Option<HostListedEntry>,
        replay: Option<Arc<Vec<SourceDirectoryEntry>>>,
        replay_bytes: u64,
        replay_offset: usize,
        eof: bool,
        in_use: bool,
        unpublished_start: bool,
    }

    impl DirectoryState {
        fn private_bytes(&self) -> u64 {
            std::mem::size_of::<Mutex<Self>>() as u64
                + self.directory.retained_bytes()
                + self.entries.as_ref().map_or(0, HostStatReader::logical_owned_bytes)
                + self.pending.as_ref().map_or(0, HostListedEntry::logical_owned_bytes)
        }
    }

    struct CursorState {
        directory: Arc<Mutex<DirectoryState>>,
    }

    type CursorSlots = Vec<Option<(u64, CursorState)>>;

    fn cursor_index(token: u64) -> usize {
        (token.saturating_sub(1) % 1024) as usize
    }

    fn cursor_slot(active: &mut CursorSlots, token: u64) -> Option<&mut CursorState> {
        active.get_mut(cursor_index(token))?.as_mut()
            .filter(|(held, _)| *held == token).map(|(_, slot)| slot)
    }

    pub(super) struct CursorLease {
        source: NativeDemandSource,
        reference: SourceReference,
        version: SourceVersion,
        token: u64,
        state: Arc<Mutex<DirectoryState>>,
        active: bool,
    }

    impl CursorLease {
        fn state(&self) -> Result<std::sync::MutexGuard<'_, DirectoryState>, DemandError> {
            self.state.lock().map_err(|_| DemandError::StaleCursor)
        }

        fn restore(&mut self) {
            if !self.active { return; }
            // No table lookup or new backing is needed: this is the same leased
            // state, including an unpublished initial request.
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.in_use = false;
            self.active = false;
        }
    }

    impl Drop for CursorLease {
        fn drop(&mut self) {
            self.restore();
        }
    }

    struct NativePagePublication {
        lease: Option<CursorLease>,
        end: usize,
        private_bytes: u64,
    }

    fn publication_cleanup() -> WorkCounters {
        WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }
    }

    impl SourceDirectoryPublication for NativePagePublication {
        fn cleanup_budget(&self) -> WorkCounters {
            if self.lease.is_some() { publication_cleanup() } else { WorkCounters::UNCHARGED }
        }

        fn retained_bytes(&self) -> u64 {
            let table_bytes = self.lease.as_ref().map_or(0, |lease| {
                let active = lease.source.inner.cursors.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                (active.capacity() * std::mem::size_of::<Option<(u64, CursorState)>>()) as u64
            });
            self.private_bytes + table_bytes
        }

        fn accept(&mut self, budget: WorkBudget, cancellation: &CancellationToken) -> std::future::Ready<DemandResult<()>> {
            let mut work = WorkCounters::UNCHARGED;
            let answer = (|| {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                let Some(lease) = self.lease.as_mut() else { return Ok(()) };
                let mut active = lease.source.inner.cursors.lock().map_err(|_| DemandError::StaleCursor)?;
                if lease.source.reference() != lease.reference { return Err(DemandError::StaleSource); }
                let slot = cursor_slot(&mut active, lease.token).ok_or(DemandError::StaleCursor)?;
                if !Arc::ptr_eq(&slot.directory, &lease.state) { return Err(DemandError::StaleCursor); }
                let mut state = lease.state.lock().map_err(|_| DemandError::StaleCursor)?;
                if !state.in_use || state.source != lease.reference || state.version != lease.version
                    || self.end < state.replay_offset
                    || self.end > state.replay.as_ref().map_or(0, |entries| entries.len())
                {
                    return Err(DemandError::StaleCursor);
                }
                // Every fallible check precedes the final admitted cleanup and
                // progress change; failed acceptance keeps the abort permit.
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                work.charge(&publication_cleanup(), &budget)?;
                state.replay_offset = self.end;
                state.unpublished_start = false;
                if state.replay.as_ref().is_some_and(|entries| state.replay_offset == entries.len()) {
                    state.replay.take();
                    state.replay_bytes = 0;
                    state.replay_offset = 0;
                }
                let complete = state.eof && state.replay.is_none() && state.pending.is_none();
                state.in_use = false;
                lease.active = false;
                drop(state);
                if complete { active[cursor_index(lease.token)] = None; }
                Ok(())
            })();
            if answer.is_ok() { self.lease.take(); }
            std::future::ready(answer.map(|()| OperationReceipt { value: (), work })
                .map_err(|error| OperationFailure::new(error, work)))
        }

        fn abort(&mut self, budget: WorkBudget) -> DemandResult<()> {
            let mut work = WorkCounters::UNCHARGED;
            if self.lease.is_some() {
                work.charge(&publication_cleanup(), &budget)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                if let Some(mut lease) = self.lease.take() { lease.restore(); }
            }
            Ok(OperationReceipt { value: (), work })
        }
    }

    /// One native root. Opening it does not enumerate any descendants.
    /// Nonlocal calls use the existing native I/O queue; locality permits the
    /// same measured engine to run directly. Cancellation bounds cooperative
    /// work and waits for native calls to quiesce, not a hard wall-clock limit.
    #[derive(Clone)]
    pub struct NativeDemandSource {
        inner: Arc<NativeDemandInner>,
        /// Whether a test let this source be watched, so tests cover the path
        /// taken by roots the host cannot watch.
        #[cfg(test)]
        watchable: bool,
    }

    struct NativeDemandInner {
        path: DemandPath,
        root: HostRoot,
        /// Whether local host I/O can run on the native callback thread.
        /// Cancellation and deadlines are cooperative triggers: completion
        /// receipts require the started OS operation to return and quiesce.
        inline: bool,
        identity: [u8; 16],
        epoch: AtomicU64,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        cursors: Mutex<CursorSlots>,
        next_cursor: AtomicU64,
        requests: Arc<Semaphore>,
        observer: Option<Arc<dyn DemandDirectoryObserver>>,
        /// Live change watches: on Linux, inotify admits directories to them
        /// one by one; on Windows, each holds the root open (see
        /// [`Self::root_named_while_watched`]).
        #[cfg(any(target_os = "linux", windows))]
        watches: std::sync::RwLock<Vec<std::sync::Weak<crate::source_watch::NativeSourceWatch>>>,
    }

    struct CancelWorkerOnDrop(CancellationToken);

    impl Drop for CancelWorkerOnDrop {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }

    impl NativeDemandSource {
        /// Returns the stable identity of the already-open native root.
        #[must_use]
        pub fn root_identity(&self) -> crate::NativeRootIdentity {
            self.inner.root.identity()
        }

        #[cfg(test)]
        pub(super) fn read_with_deadline(
            &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
            budget: WorkBudget, cancellation: &CancellationToken, deadline: std::time::Duration,
        ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
            let mut work = DemandWork::new(budget, cancellation);
            let file = match NativeDemandFile::open(self.clone(), source, path, expected, &mut work) {
                Ok(file) => file,
                Err(error) => return work.finish(Err(error)),
            };
            file.read_remote(ReadRange { offset: 0, length: 6 }, work, deadline)
        }

        #[cfg(test)]
        pub(super) async fn occupy_all_requests(
            &self,
        ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
            self.inner.requests.clone().acquire_many_owned(64).await
        }

        #[cfg(test)]
        pub(super) async fn wait_for_cancelled_worker(
            &self,
            started: tokio::sync::oneshot::Sender<()>,
            stopped: tokio::sync::oneshot::Sender<()>,
        ) {
            let cancellation = CancellationToken::new();
            let _ = self
                .run_blocking(DemandWork::new(WorkBudget::UNBOUNDED, &cancellation), move |_, work| {
                    let _ = started.send(());
                    while work.check().is_ok() {
                        std::thread::yield_now();
                    }
                    let _ = stopped.send(());
                    Ok(())
                })
                .await;
        }

        #[cfg(test)]
        pub(super) fn cancel_during_page(
            &self,
            source: SourceReference,
            directory: &NamespacePath,
            cursor: SourceCursor,
            cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            let mut work = DemandWork::new(WorkBudget::UNBOUNDED, cancellation);
            let result = (|| {
                let metadata = self.metadata(directory, &mut work)?.ok_or(DemandError::Absent)?;
                let lease =
                    self.cursor_lease(source, directory, version(&metadata), Some(cursor), 4096, &mut work)?;
                let mut visited = 0;
                let result = self.page_entries(&mut lease.state()?, 4096, &mut work, || {
                    visited += 1;
                    if visited == 2 {
                        cancellation.cancel();
                    }
                });
                work.record(publication_cleanup())?;
                drop(lease);
                match result {
                    Err(DemandError::Cancelled) => Ok(()),
                    Err(error) => Err(error),
                    Ok(_) => Err(DemandError::InvalidRequest),
                }
            })();
            work.finish(result)
        }

        /// Opens and identifies only the root directory off the async executor.
        pub async fn open(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
        ) -> Result<Self, DemandError> {
            Self::open_configured(
                path,
                profile,
                limits,
                SourceReference {
                    identity: *uuid::Uuid::new_v4().as_bytes(),
                    epoch: 0,
                },
                None,
            )
            .await
        }

        /// Opens a lazy native source whose demanded directories are admitted
        /// to an external observer before any corresponding filesystem read.
        ///
        /// # Errors
        ///
        /// Returns the same bounded native admission failures as [`Self::open`].
        pub async fn open_with_observer(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            observer: Arc<dyn DemandDirectoryObserver>,
        ) -> Result<Self, DemandError> {
            Self::open_with_reference_and_observer(
                path,
                profile,
                limits,
                SourceReference {
                    identity: *uuid::Uuid::new_v4().as_bytes(),
                    epoch: 0,
                },
                observer,
            )
            .await
        }

        /// Reopens only the root directory using a durable provider identity.
        ///
        /// Adapters persist this opaque reference beside the lazy workspace;
        /// reopening never enumerates descendants or reads file contents.
        pub async fn open_with_reference(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
        ) -> Result<Self, DemandError> {
            Self::open_configured(path, profile, limits, reference, None).await
        }

        /// Opens only the original native root under an admitted finite budget.
        /// The optional existing observer is retained, never duplicated. Each
        /// capability/locality request is admitted immediately before execution,
        /// and worker failure retains only requests that actually began.
        /// Native I/O services must already be initialized. Ordinary opening
        /// performs that initialization outside its explicit unbounded call.
        pub async fn open_with_reference_measured(
            path: PathBuf,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Option<Arc<dyn DemandDirectoryObserver>>,
            budget: crate::WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Self> {
            cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
            WorkCounters { backend_read_operations: 1, ..WorkCounters::default() }
                .verify(budget)
                .map_err(|error| OperationFailure::before_work(DemandError::Work(error)))?;
            let mut work = DemandWork::new(budget, cancellation);
            if let Err(error) = work.adopt(path.capacity() as u64)
                .and_then(|()| work.allocate(std::mem::size_of::<NativeDemandReadMeter>() as u64)) {
                return work.finish(Err(error));
            }
            let shared = Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(work)));
            let mut prepared = {
                let mut work = shared.lock();
                match work.prepare_cancelled(cancellation) {
                    Ok(prepared) => {
                        work.budget.items_examined -= prepared.cleanup_items();
                        prepared
                    }
                    Err(error) => {
                        drop(work);
                        let mut work = Arc::try_unwrap(shared).ok().expect("admission has no queue aliases")
                            .0.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
                        work.release(std::mem::size_of::<NativeDemandReadMeter>() as u64)
                            .expect("source admission adapter allocation remains owned");
                        return work.finish(Err(cancellation_error(error)));
                    }
                }
            };
            let worker = shared.clone();
            let task_meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = shared.clone();
            let task = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(crate::obs::caller_context(), move || {
                    Self::open_blocking(path, profile, limits, reference, observer, &mut worker.lock())
                }),
                task_meter,
            );
            let result = match task {
                Ok(mut task) => tokio::select! {
                    result = &mut task => result.map_err(native_demand_error).and_then(std::convert::identity),
                    () = &mut prepared => task.await.map_err(native_demand_error).and_then(std::convert::identity),
                },
                Err(error) => Err(native_demand_error(error)),
            };
            prepared.retire_waker();
            while Arc::strong_count(&shared) != 1 {
                tokio::task::yield_now().await;
            }
            let mut work = Arc::try_unwrap(shared).ok().expect("terminal source admission retires every meter alias")
                .0.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
            work.release(std::mem::size_of::<NativeDemandReadMeter>() as u64)
                .expect("source admission adapter allocation remains owned until retirement");
            work.restore_budget(budget);
            let cleanup = work.finish_cancelled(prepared).map_err(cancellation_error);
            let result = cleanup.and(result).and_then(|value| work.check().map(|()| value));
            work.finish(result)
        }

        /// Reopens a durably identified lazy source with demand-driven
        /// directory observation.
        ///
        /// # Errors
        ///
        /// Returns the same bounded native admission failures as
        /// [`Self::open_with_reference`].
        pub async fn open_with_reference_and_observer(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Arc<dyn DemandDirectoryObserver>,
        ) -> Result<Self, DemandError> {
            Self::open_configured(path, profile, limits, reference, Some(observer)).await
        }

        async fn open_configured(
            path: impl AsRef<Path>,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Option<Arc<dyn DemandDirectoryObserver>>,
        ) -> Result<Self, DemandError> {
            acyclic_native_runtime::initialize_native_io_services()?;
            Self::open_with_reference_measured(
                path.as_ref().to_path_buf(), profile, limits, reference, observer,
                crate::WorkBudget::UNBOUNDED, &CancellationToken::new(),
            ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
        }

        fn open_blocking(
            path: PathBuf,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Option<Arc<dyn DemandDirectoryObserver>>,
            work: &mut DemandWork,
        ) -> Result<Self, DemandError> {
            work.check()?;
            let input_bytes = path.capacity() as u64;
            #[cfg(unix)]
            let path = if path.is_absolute() {
                path
            } else {
                let normalized = crate::native_host::normalize_root_path_measured(&path, work)?;
                drop(path);
                work.release(input_bytes)?;
                normalized
            };
            #[cfg(windows)]
            let path = {
                let normalized = crate::native_host::normalize_root_wide_measured(&path, work)?;
                drop(path);
                work.release(input_bytes)?;
                normalized
            };
            #[cfg(unix)]
            let root = HostRoot::open_measured(&path, work)?;
            #[cfg(windows)]
            let root = HostRoot::open_root_wide_measured(&path, work)?;
            let inline = root.is_local_measured(work)?;
            work.allocate(std::mem::size_of::<Semaphore>() as u64)?;
            let requests = Arc::new(Semaphore::new(MAXIMUM_NATIVE_REQUESTS));
            work.allocate(std::mem::size_of::<NativeDemandInner>() as u64)?;
            Ok(Self {
                inner: Arc::new(NativeDemandInner {
                    path,
                    inline,
                    root,
                    identity: reference.identity,
                    epoch: AtomicU64::new(reference.epoch),
                    profile,
                    limits,
                    cursors: Mutex::new(Vec::new()),
                    next_cursor: AtomicU64::new(0),
                    requests,
                    observer,
                    #[cfg(any(target_os = "linux", windows))]
                    watches: std::sync::RwLock::new(Vec::new()),
                }),
                #[cfg(test)]
                watchable: true,
            })
        }

        /// This source, reporting no changes as a root the host cannot watch
        /// would. The live tests that run both ways mount through FUSE or NFS.
        #[cfg(all(test, unix, feature = "native-mount"))]
        pub(crate) fn unwatched(self) -> Self {
            Self {
                watchable: false,
                ..self
            }
        }

        /// This source as if its root were not local, so tests cover the
        /// path host I/O beneath a remote root takes.
        #[cfg(test)]
        pub(crate) fn remote(mut self) -> Self {
            if let Some(inner) = Arc::get_mut(&mut self.inner) {
                inner.inline = false;
            }
            self
        }

        fn observe_directory(&self, directory: &NamespacePath, work: &mut DemandWork) -> Result<(), DemandError> {
            self.admit_to_watches(directory, work)?;
            if let Some(observer) = &self.inner.observer {
                let answer = observer.observe_directory(directory, work.remaining()?, &work.cancellation);
                match answer {
                    Ok(receipt) => work.merge(receipt.work)?,
                    Err(failure) => {
                        work.merge(*failure.work)?;
                        return Err(failure.error);
                    }
                }
            }
            Ok(())
        }

        /// Watches `directory`, and every directory on the way to it, before
        /// anything beneath it is read; returns whether a watch began
        /// watching `directory` itself only now, after which a fact about it
        /// read before must be read again.
        #[cfg(target_os = "linux")]
        fn admit_to_watches(&self, directory: &NamespacePath, work: &mut DemandWork) -> Result<bool, DemandError> {
            let watches = self.inner.watches.read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut count = 0usize;
            for watch in watches.iter() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                count += usize::from(watch.strong_count() != 0);
            }
            if count == 0 { return Ok(false); }
            let bytes = count * std::mem::size_of::<Arc<crate::source_watch::NativeSourceWatch>>();
            work.allocate(bytes as u64)?;
            let mut live = Vec::with_capacity(count);
            for watch in watches.iter() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if let Some(watch) = watch.upgrade() {
                    work.charge(WorkCounters {
                        bytes_copied: std::mem::size_of::<Arc<crate::source_watch::NativeSourceWatch>>() as u64,
                        ..WorkCounters::default()
                    })?;
                    live.push(watch);
                }
            }
            // A notification may reenter the source; never hold the registry
            // lock while admitting or abandoning a native watch.
            drop(watches);
            let relative = self.relative(directory, work)?;
            let mut added = false;
            for watch in &live { added |= watch.admit_measured(&relative, work)?; }
            work.release(path_owned_bytes(&relative))?;
            drop(live);
            work.release(bytes as u64)?;
            Ok(added)
        }

        /// One watch covers the whole root on every other host.
        #[cfg(not(target_os = "linux"))]
        #[allow(
            clippy::unused_self,
            clippy::unnecessary_wraps,
            reason = "the Linux variant admits directories to inotify watches and can fail; this one keeps its signature"
        )]
        fn admit_to_watches(&self, _directory: &NamespacePath, work: &mut DemandWork) -> Result<bool, DemandError> {
            work.check()?;
            Ok(false)
        }

        fn observe_parent(&self, path: &NamespacePath, work: &mut DemandWork) -> Result<(), DemandError> {
            let components = path.components();
            let depth = components.len().saturating_sub(1);
            work.allocate((depth * std::mem::size_of::<crate::kernel::LogicalName>()) as u64)?;
            let mut prefix = Vec::with_capacity(depth);
            for name in components.iter().take(depth) {
                work.allocate_for(name.as_bytes().len() as u64, WorkCounters {
                    bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::default()
                })?;
                prefix.push(name.clone());
            }
            let parent = NamespacePath::new(prefix, self.inner.limits)?;
            self.observe_directory(&parent, work)?;
            work.release(namespace_owned_bytes(&parent)?)?;
            Ok(())
        }

        async fn run_request<T: Send + 'static>(
            &self,
            path: &NamespacePath,
            budget: WorkBudget,
            cancellation: &CancellationToken,
            job: impl FnOnce(Self, NamespacePath, &mut DemandWork) -> Result<T, DemandError> + Send + 'static,
        ) -> DemandResult<T> {
            let mut work = DemandWork::new(budget, cancellation);
            let path = match work.charge(WorkCounters { source_path_components: path.depth() as u64, ..WorkCounters::default() })
                .and_then(|()| clone_namespace(path, &mut work)) {
                Ok(path) => path,
                Err(error) => return work.finish(Err(error)),
            };
            self.run_blocking(work, move |provider, work| job(provider, path, work)).await
        }
        async fn run_blocking<T: Send + 'static>(
            &self,
            mut work: DemandWork,
            job: impl FnOnce(Self, &mut DemandWork) -> Result<T, DemandError> + Send + 'static,
        ) -> DemandResult<T> {
            if let Err(error) = work.check() { return work.finish(Err(error)); }
            // Native callback threads already permit blocking. Other callers
            // retain the same admitted meter through queueing and worker exit.
            if self.inner.inline && acyclic_native_runtime::inline_blocking_allowed() {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(self.clone(), &mut work)))
                    .unwrap_or(Err(DemandError::WorkerUnavailable));
                return work.finish(result);
            }
            let caller_cancellation = work.cancellation.clone();
            let mut prepared = match work.prepare_cancelled(&caller_cancellation) {
                Ok(prepared) => prepared,
                Err(error) => return work.finish(Err(cancellation_error(error))),
            };
            let budget = work.budget;
            work.budget.items_examined -= prepared.cleanup_items();
            let permit = tokio::select! {
                acquired = self.inner.requests.clone().acquire_owned() =>
                    acquired.map_err(|_| DemandError::WorkerUnavailable),
                () = &mut prepared => Err(DemandError::Cancelled),
            };
            let permit = match permit.and_then(|permit| work.check().map(|()| permit)) {
                Ok(permit) => permit,
                Err(error) => {
                    work.restore_budget(budget);
                    let cleanup = work.finish_cancelled(prepared);
                    return work.finish(cleanup.map_err(cancellation_error).and(Err(error)));
                }
            };
            let worker_cancellation = match work.new_cancellation() {
                Ok(token) => token,
                Err(error) => {
                    work.restore_budget(budget);
                    let cleanup = work.finish_cancelled(prepared);
                    return work.finish(cleanup.map_err(cancellation_error).and(Err(error.into())));
                }
            };
            let cancel_on_drop = CancelWorkerOnDrop(worker_cancellation.clone());
            work.parent_cancellation = Some(caller_cancellation);
            work.rebind_cancellation(&worker_cancellation);
            let bytes = std::mem::size_of::<NativeDemandReadMeter>() as u64;
            if let Err(error) = work.allocate(bytes) {
                work.restore_budget(budget);
                let cleanup = work.finish_cancelled(prepared);
                return work.finish(cleanup.map_err(cancellation_error).and(Err(error)));
            }
            // The sole ledger remains here even if runtime/context unwinding
            // happens outside the native job's panic boundary.
            let shared = Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(work)));
            let worker_work = Arc::clone(&shared);
            let task_meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = shared.clone();
            let source = self.clone();
            let context = crate::obs::caller_context();
            let spawned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                acyclic_native_runtime::run_blocking_io_measured(crate::obs::in_context(context, move || {
                    let _permit = permit;
                    let mut held = worker_work.lock();
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(source, &mut held)))
                        .unwrap_or(Err(DemandError::WorkerUnavailable))
                }), task_meter)
            }));
            let result = match spawned {
                Ok(Ok(mut worker)) => tokio::select! {
                    result = &mut worker => result.map_err(native_demand_error).and_then(std::convert::identity),
                    () = &mut prepared => {
                        cancel_on_drop.0.cancel();
                        worker.await.map_err(native_demand_error).and_then(std::convert::identity)
                    },
                },
                Ok(Err(error)) => Err(native_demand_error(error)),
                Err(_) => Err(DemandError::WorkerUnavailable),
            };
            prepared.retire_waker();
            // The task and every completion/capacity-wake alias retire before
            // releasing this invocation's sole adapter and allocation ledger.
            while Arc::strong_count(&shared) != 1 {
                tokio::task::yield_now().await;
            }
            let mut work = Arc::try_unwrap(shared).ok().expect("terminal source task retires every meter alias")
                .0.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
            work.release(bytes).expect("source adapter allocation remains owned until retirement");
            work.restore_budget(budget);
            let cleanup = work.finish_cancelled(prepared);
            work.finish(cleanup.map_err(cancellation_error).and(result))
        }

        /// Invalidates prior source references and directory cursors without
        /// reading any descendant. Watch overflow and broad hints use this.
        pub fn invalidate(&self) -> SourceReference {
            let epoch = self.inner.epoch.fetch_add(1, Ordering::AcqRel) + 1;
            if let Ok(mut cursors) = self.inner.cursors.lock() {
                cursors.clear();
            }
            SourceReference {
                identity: self.inner.identity,
                epoch,
            }
        }

        fn check(&self, source: SourceReference, work: &mut DemandWork) -> Result<(), DemandError> {
            if source != self.reference() {
                return Err(DemandError::StaleSource);
            }
            work.check()?;
            if self.root_named_while_watched(work)? {
                return Ok(());
            }
            #[cfg(unix)]
            let identity = crate::native_host::root_path_identity_measured(&self.inner.path, work);
            #[cfg(windows)]
            let identity = crate::native_host::root_path_identity_wide_measured(&self.inner.path, work);
            match identity {
                Ok(identity) if identity == self.inner.root.identity() => Ok(()),
                Err(error @ (DemandError::Work(_) | DemandError::Cancelled)) => Err(error),
                _ => Err(DemandError::SourceUnavailable),
            }
        }

        /// Whether a live watch proves the root's path still names the held
        /// root. A Windows watch holds the root open without sharing its
        /// deletion, so while it lives neither the root nor any ancestor can
        /// be renamed or removed.
        #[cfg(windows)]
        fn root_named_while_watched(&self, work: &mut DemandWork) -> Result<bool, DemandError> {
            let watches = self.inner.watches.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            for watch in watches.iter() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if watch.strong_count() != 0 { return Ok(true); }
            }
            Ok(false)
        }

        /// Watches elsewhere report a moved root rather than prevent it.
        #[cfg(not(windows))]
        #[allow(
            clippy::unused_self,
            reason = "the Windows variant reads the live watch set; this one keeps its signature"
        )]
        fn root_named_while_watched(&self, work: &mut DemandWork) -> Result<bool, DemandError> {
            work.check()?;
            Ok(false)
        }

        #[cfg(unix)]
        fn relative(&self, path: &NamespacePath, work: &mut DemandWork) -> Result<DemandPath, DemandError> {
            let limits = self.inner.limits;
            if path.depth() > usize::from(limits.maximum_path_depth)
                || path.encoded_bytes() > limits.maximum_path_bytes
            {
                return Err(DemandError::InvalidRequest);
            }
            let mut length = path.depth().saturating_sub(1);
            for component in path.components() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if component.as_bytes().len() > limits.maximum_component_bytes as usize {
                    return Err(DemandError::InvalidRequest);
                }
                let bytes = if component.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le {
                    char::decode_utf16(component.as_bytes().chunks_exact(2)
                        .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]])))
                        .try_fold(0usize, |length, value| {
                            let value = value.map_err(|_| DemandError::InvalidRequest)?.len_utf8();
                            Ok::<_, DemandError>(length + value)
                        })?
                } else { component.as_bytes().len() };
                length = length.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
            }
            work.allocate(length as u64)?;
            let mut result = PathBuf::with_capacity(length);
            for component in path.components() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let name = host_component(component, work)?;
                work.charge(WorkCounters { bytes_copied: name.as_encoded_bytes().len() as u64, ..WorkCounters::default() })?;
                result.push(&name);
                work.release(name.as_encoded_bytes().len() as u64)?;
            }
            Ok(result)
        }

        #[cfg(windows)]
        fn relative(&self, path: &NamespacePath, work: &mut DemandWork) -> Result<DemandPath, DemandError> {
            native_wide_path(path, self.inner.limits, work)
        }

        fn stat_relative(&self, relative: &DemandPath, work: &mut DemandWork) -> Result<HostStat, DemandError> {
            #[cfg(unix)]
            { self.inner.root.stat_measured(relative, work) }
            #[cfg(windows)]
            { self.inner.root.stat_wide_measured(relative, work) }
        }

        fn metadata(&self, path: &NamespacePath, work: &mut DemandWork) -> Result<Option<HostStat>, DemandError> {
            let relative = self.relative(path, work)?;
            let result = self.stat_relative(&relative, work);
            work.release(path_owned_bytes(&relative))?;
            match result {
                Ok(metadata) => Ok(Some(metadata)),
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error),
            }
        }

        fn node(metadata: &HostStat) -> SourceNode {
            let kind = node_kind(metadata.file_type());
            SourceNode {
                kind,
                file_identity: file_identity(metadata),
                link_count: link_count(metadata),
                device: device_identity(metadata, kind),
                logical_bytes: (kind == SourceNodeKind::RegularFile
                    || cfg!(unix) && kind == SourceNodeKind::SymbolicLink)
                    .then_some(metadata.len()),
                version: version(metadata),
                metadata: source_metadata(metadata),
            }
        }

        fn node_measured(metadata: &HostStat, work: &mut DemandWork) -> Result<SourceNode, DemandError> {
            work.charge(WorkCounters {
                bytes_hashed: version_hash_bytes() + identity_hash_bytes(metadata),
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            Ok(Self::node(metadata))
        }

        pub(super) fn cursor_lease(
            &self,
            source: SourceReference,
            directory: &NamespacePath,
            observed: SourceVersion,
            cursor: Option<SourceCursor>,
            maximum_entries: u32,
            work: &mut DemandWork,
        ) -> Result<CursorLease, DemandError> {
            let mut active = self.inner.cursors.lock().map_err(|_| DemandError::StaleCursor)?;
            let slot_bytes = std::mem::size_of::<Option<(u64, CursorState)>>();
            work.adopt((active.capacity() * slot_bytes) as u64)?;
            let held = if let Some(cursor) = cursor {
                if cursor.source != source || cursor.directory != *directory || cursor.directory_version != observed {
                    return Err(DemandError::StaleCursor);
                }
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                let slot = cursor_slot(&mut active, cursor.token).ok_or(DemandError::StaleCursor)?;
                Some((cursor.token, Arc::clone(&slot.directory)))
            } else {
                let mut held = None;
                for slot in active.iter_mut() {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    let Some((token, cursor)) = slot else { continue };
                    let state = cursor.directory.lock().map_err(|_| DemandError::StaleCursor)?;
                    if state.source != source || state.directory != *directory { continue; }
                    if state.in_use { return Err(DemandError::CursorBusy); }
                    if state.unpublished_start && state.version == observed {
                        held = Some((*token, Arc::clone(&cursor.directory)));
                        break;
                    }
                }
                held
            };
            if let Some((token, state)) = held {
                let mut directory_state = state.lock().map_err(|_| DemandError::StaleCursor)?;
                if directory_state.in_use { return Err(DemandError::CursorBusy); }
                if directory_state.source != source || directory_state.directory != *directory
                    || directory_state.version != observed
                {
                    return Err(DemandError::StaleCursor);
                }
                work.adopt(directory_state.private_bytes().checked_add(directory_state.replay_bytes)
                    .ok_or(crate::WorkError::Overflow)?)?;
                directory_state.in_use = true;
                drop(directory_state);
                return Ok(CursorLease {
                    source: self.clone(), reference: source, version: observed, token, state,
                    active: true,
                });
            }
            let token = self.inner.next_cursor.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            }).map_err(|_| DemandError::StaleCursor)? + 1;
            let index = cursor_index(token);
            if let Some(Some((_, slot))) = active.get(index) {
                if slot.directory.lock().map_err(|_| DemandError::StaleCursor)?.in_use {
                    return Err(DemandError::CursorBusy);
                }
            }
            if index >= active.capacity() {
                let old = active.capacity() * slot_bytes;
                let capacity = active.capacity().saturating_mul(2).max(index + 1).min(1024);
                let copied = active.len() * slot_bytes;
                work.work.admit(&WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED }, &work.budget)?;
                work.allocate((capacity * slot_bytes) as u64)?;
                let mut grown = Vec::with_capacity(capacity);
                grown.append(&mut active);
                work.record(WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED })?;
                *active = grown;
                work.release(old as u64)?;
            }
            if index >= active.len() {
                work.charge(WorkCounters { items_examined: (index + 1 - active.len()) as u64, ..WorkCounters::UNCHARGED })?;
                active.resize_with(index + 1, || None);
            }
            let directory = clone_namespace(directory, work)?;
            let (replay, replay_bytes) = Self::page_buffer(maximum_entries, work)?;
            work.allocate(std::mem::size_of::<Mutex<DirectoryState>>() as u64)?;
            let state = Arc::new(Mutex::new(DirectoryState {
                source, directory, version: observed, entries: None, pending: None,
                replay: Some(replay), replay_bytes, replay_offset: 0,
                eof: false, in_use: true, unpublished_start: true,
            }));
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            active[index] = Some((token, CursorState { directory: Arc::clone(&state) }));
            Ok(CursorLease {
                source: self.clone(), reference: source, version: observed, token, state,
                active: true,
            })
        }

        fn page_buffer(maximum_entries: u32, work: &mut DemandWork) -> Result<(Arc<Vec<SourceDirectoryEntry>>, u64), DemandError> {
            let capacity = maximum_entries as usize;
            let buffer_bytes = capacity.checked_mul(std::mem::size_of::<SourceDirectoryEntry>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            let arc_bytes = std::mem::size_of::<Vec<SourceDirectoryEntry>>() as u64;
            work.allocate(buffer_bytes)?;
            work.allocate(arc_bytes)?;
            Ok((Arc::new(Vec::with_capacity(capacity)), buffer_bytes + arc_bytes))
        }

        fn open_directory_state(&self, state: &mut DirectoryState, work: &mut DemandWork) -> Result<(), DemandError> {
            if state.entries.is_some() { return Ok(()); }
            let relative = self.relative(&state.directory, work)?;
            #[cfg(unix)]
            let result = self.inner.root.read_dir_stats_measured(&relative, work);
            #[cfg(windows)]
            let result = self.inner.root.read_dir_stats_wide_measured(&relative, work);
            work.release(path_owned_bytes(&relative))?;
            state.entries = Some(result?);
            Ok(())
        }

        /// Converts one enumerated entry into the node a lookup reports,
        /// stat'ing the name only where enumeration could not report it.
        /// `None` means the entry vanished after enumeration.
        #[cfg(unix)]
        fn next_entry(
            &self,
            directory: &DemandPath,
            entry: &HostListedEntry,
            work: &mut DemandWork,
        ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
            let file_name = &entry.name;
            let (encoding, bytes) = crate::native_name::host_name_bytes_measured(
                file_name, self.inner.profile, self.inner.limits.maximum_component_bytes, work,
            )?;
            let name = crate::kernel::LogicalName::new(
                encoding,
                bytes,
                self.inner.limits.maximum_component_bytes,
            )
            .map_err(|_| DemandError::InvalidRequest)?;
            let node = match &entry.stat {
                Some(stat) => Self::node_measured(stat, work)?,
                None => {
                    let length = directory.as_os_str().as_encoded_bytes().len()
                        + usize::from(!directory.as_os_str().is_empty()) + file_name.as_encoded_bytes().len();
                    work.allocate_for(length as u64, WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() })?;
                    let mut joined = PathBuf::with_capacity(length);
                    joined.push(directory);
                    joined.push(file_name);
                    let result = self.inner.root.stat_measured(&joined, work);
                    work.release(length as u64)?;
                    match result {
                        Ok(stat) => Self::node_measured(&stat, work)?,
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                            work.release(name.as_bytes().len() as u64)?;
                            return Ok(None);
                        }
                        Err(error) => return Err(error),
                    }
                }
            };
            Ok(Some(SourceDirectoryEntry { name, node }))
        }

        #[cfg(windows)]
        fn next_entry(
            &self, directory: &DemandPath, entry: &HostListedEntry, work: &mut DemandWork,
        ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
            let (encoding, bytes) = crate::native_name::host_wide_name_bytes_measured(
                &entry.name, self.inner.profile, self.inner.limits.maximum_component_bytes, work,
            )?;
            let name = crate::kernel::LogicalName::new(encoding, bytes, self.inner.limits.maximum_component_bytes)
                .map_err(|_| DemandError::InvalidRequest)?;
            let node = match &entry.stat {
                Some(stat) => Self::node_measured(stat, work)?,
                None => {
                    let units = directory.len().checked_add(usize::from(!directory.is_empty()))
                        .and_then(|length| length.checked_add(entry.name.len())).ok_or(crate::WorkError::Overflow)?;
                    let copied = (directory.len() + entry.name.len()) * 2;
                    work.allocate_for((units * 2) as u64, WorkCounters { bytes_copied: copied as u64, ..WorkCounters::default() })?;
                    let mut joined = Vec::with_capacity(units);
                    joined.extend_from_slice(directory);
                    if !joined.is_empty() { joined.push(u16::from(b'\\')); }
                    joined.extend_from_slice(&entry.name);
                    let result = self.stat_relative(&joined, work);
                    work.release(path_owned_bytes(&joined))?;
                    match result {
                        Ok(stat) => Self::node_measured(&stat, work)?,
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                            work.release(name.as_bytes().len() as u64)?;
                            return Ok(None);
                        }
                        Err(error) => return Err(error),
                    }
                }
            };
            Ok(Some(SourceDirectoryEntry { name, node }))
        }

        fn read_entry(reader: &mut HostStatReader, work: &mut DemandWork) -> Result<Option<HostListedEntry>, DemandError> {
            #[cfg(unix)]
            { reader.next_measured(work) }
            #[cfg(windows)]
            { reader.next_wide_measured(work) }
        }

        fn page_entries(
            &self,
            state: &mut DirectoryState,
            maximum_entries: u32,
            work: &mut DemandWork,
            mut on_entry: impl FnMut(),
        ) -> Result<std::ops::Range<usize>, DemandError> {
            if let Some(entries) = &state.replay
                && state.replay_offset < entries.len()
            {
                let end = state.replay_offset.saturating_add(maximum_entries as usize).min(entries.len());
                for index in state.replay_offset..end {
                    work.work.admit(&WorkCounters { items_returned: (index - state.replay_offset + 1) as u64, ..WorkCounters::UNCHARGED }, &work.budget)?;
                    work.charge(WorkCounters { source_entries_visited: 1, items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    on_entry();
                }
                work.check()?;
                return Ok(state.replay_offset..end);
            }
            if state.replay.is_none() {
                let (entries, bytes) = Self::page_buffer(maximum_entries, work)?;
                state.replay = Some(entries);
                state.replay_bytes = bytes;
                state.replay_offset = 0;
            }
            // The same admitted buffer survives partial fills and every error.
            // Published replay backing is immutable; it is never extended.
            let limit = (maximum_entries as usize).min(state.replay.as_ref().map_or(0, |entries| entries.capacity()));
            self.open_directory_state(state, work)?;
            let directory = self.relative(&state.directory, work)?;
            let answer = (|| {
                while state.replay.as_ref().map_or(0, |entries| entries.len()) < limit && !state.eof {
                    work.check()?;
                    if state.pending.is_none() {
                        state.pending = Self::read_entry(state.entries.as_mut().ok_or(DemandError::StaleCursor)?, work)?;
                    }
                    let Some(entry) = state.pending.as_ref() else { state.eof = true; break };
                    let count = state.replay.as_ref().map_or(0, |entries| entries.len());
                    work.work.admit(&WorkCounters {
                        source_entries_visited: 1, items_returned: count as u64 + 1,
                        ..WorkCounters::UNCHARGED
                    }, &work.budget)?;
                    work.charge(WorkCounters { source_entries_visited: 1, items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    let converted = self.next_entry(&directory, entry, work)?;
                    let raw_bytes = entry.logical_owned_bytes();
                    if let Some(converted) = converted {
                        let name_bytes = converted.name.retained_bytes() as u64;
                        let bytes = state.replay_bytes.checked_add(name_bytes).ok_or(crate::WorkError::Overflow)?;
                        let entries = Arc::get_mut(state.replay.as_mut().ok_or(DemandError::StaleCursor)?)
                            .ok_or(DemandError::CursorBusy)?;
                        entries.push(converted);
                        state.replay_bytes = bytes;
                    }
                    state.pending.take();
                    work.release(raw_bytes)?;
                    on_entry();
                }
                if !state.eof && state.pending.is_none() {
                    state.pending = Self::read_entry(state.entries.as_mut().ok_or(DemandError::StaleCursor)?, work)?;
                    if state.pending.is_none() { state.eof = true; }
                }
                work.check()?;
                Ok(0..state.replay.as_ref().map_or(0, |entries| entries.len()))
            })();
            work.release(path_owned_bytes(&directory))?;
            answer
        }
    }

    /// A byte range admitted for one native read: at most one MiB long and
    /// addressable without overflow.
    #[derive(Clone, Copy)]
    struct ReadRange {
        offset: u64,
        length: u64,
    }

    impl ReadRange {
        fn new(offset: u64, length: u64) -> Result<Self, DemandError> {
            if length > 1024 * 1024 || offset.checked_add(length).is_none() {
                return Err(DemandError::InvalidRequest);
            }
            Ok(Self { offset, length })
        }
    }

    /// A regular source file held open after its version was proven against
    /// the opened file itself. The type cannot exist without that proof.
    ///
    /// Reads use the held file and then prove, exactly as a path-based read does, that
    /// the file is unmodified, that its source path still names it, and that
    /// the source root and reference are still current.
    struct NativeDemandFile {
        provider: NativeDemandSource,
        source: SourceReference,
        relative: DemandPath,
        expected: SourceVersion,
        logical_bytes: u64,
        #[cfg(windows)]
        file: Arc<std::sync::RwLock<Arc<SourceFile>>>,
        #[cfg(not(windows))]
        file: Arc<SourceFile>,
    }

    /// A source file held for reading. On Windows it gives way to whoever
    /// renames or deletes it, as an editor saving over it does, instead of
    /// making that fail while it is read.
    #[cfg(windows)]
    type SourceFile = crate::native_host::YieldingFile;
    #[cfg(not(windows))]
    type SourceFile = std::fs::File;

    /// Deadline for requesting cancellation of a remote read. Its receipt
    /// waits for in-flight native I/O to quiesce: finite work is guaranteed,
    /// but OS completion liveness, not a hard wall-clock return bound, is assumed.
    const REMOTE_READ_DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);
    /// Maximum remote reads admitted to the existing native queue. A hung
    /// share retains its queue worker until the host completes the operation;
    /// further reads beyond this limit fail before allocating a job.
    const MAXIMUM_BLOCKED_REMOTE_READS: usize = 16;
    static BLOCKED_REMOTE_READS: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    impl NativeDemandFile {
        fn open(
            provider: NativeDemandSource,
            source: SourceReference,
            path: &NamespacePath,
            expected: SourceVersion,
            work: &mut DemandWork,
        ) -> Result<Self, DemandError> {
            provider.check(source, work)?;
            provider.observe_parent(path, work)?;
            let relative = provider.relative(path, work)?;
            #[cfg(windows)]
            let file = provider.inner.root.open_file_yielding_wide_measured(&relative, work);
            #[cfg(not(windows))]
            let file = provider
                .inner
                .root
                .open_file_measured(&relative, work)
                .map(cap_std::fs::File::into_std);
            let file = match file {
                Ok(file) => file,
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(DemandError::Absent);
                }
                Err(error) => return Err(error),
            };
            let opened = stat_held(&provider, &file, work)?;
            if !opened.file_type().is_file() {
                return Err(DemandError::NotRegularFile);
            }
            if version_measured(&opened, work)? != expected {
                return Err(DemandError::StaleVersion);
            }
            work.allocate(std::mem::size_of::<SourceFile>() as u64)?;
            #[cfg(windows)]
            work.allocate(std::mem::size_of::<std::sync::RwLock<Arc<SourceFile>>>() as u64)?;
            Ok(Self {
                provider,
                source,
                relative,
                expected,
                logical_bytes: opened.len(),
                #[cfg(windows)]
                file: Arc::new(std::sync::RwLock::new(Arc::new(file))),
                #[cfg(not(windows))]
                file: Arc::new(file),
            })
        }

        fn read(
            &self,
            range: ReadRange,
            work: &mut DemandWork,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            #[cfg(windows)]
            let file = self
                .file
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            #[cfg(not(windows))]
            let file = Arc::clone(&self.file);
            let result = self.read_once(&file, range, work);
            #[cfg(windows)]
            if matches!(result, Err(DemandError::StaleVersion)) && file.was_yielded() {
                // A benign opener can break a handle oplock too. Reopen only
                // at the exact expected version; a save that replaced the
                // file still reports stale to the caller.
                work.allocate(std::mem::size_of::<SourceFile>() as u64)?;
                let reopened = Arc::new(self.reopen(work)?);
                let current = {
                    let mut held = self
                        .file
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if Arc::ptr_eq(&*held, &file) {
                        *held = reopened;
                    }
                    Arc::clone(&*held)
                };
                return self.read_once(&current, range, work);
            }
            result
        }

        #[cfg(windows)]
        fn reopen(&self, work: &mut DemandWork) -> Result<SourceFile, DemandError> {
            self.provider.check(self.source, work)?;
            let file = match self.provider.inner.root.open_file_yielding_wide_measured(&self.relative, work) {
                Ok(file) => file,
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(DemandError::StaleVersion);
                }
                Err(error) => return Err(error),
            };
            let opened = stat_held(&self.provider, &file, work)?;
            if version_measured(&opened, work)? != self.expected {
                return Err(DemandError::StaleVersion);
            }
            Ok(file)
        }

        fn read_once(
            &self,
            file: &SourceFile,
            ReadRange { offset, length }: ReadRange,
            work: &mut DemandWork,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            work.check()?;
            let length = length.min(self.logical_bytes.saturating_sub(offset));
            work.work.admit(&WorkCounters { output_bytes: length, ..WorkCounters::default() }, &work.budget)?;
            work.allocate(length)?;
            let mut bytes = vec![0; usize::try_from(length).map_err(|_| DemandError::InvalidRequest)?];
            let result = (|| {
                let mut filled = 0;
                while let Some(unfilled) = bytes.get_mut(filled..).filter(|rest| !rest.is_empty()) {
                    work.check()?;
                    #[cfg(unix)]
                    let read = {
                        work.work.admit(&WorkCounters { source_bytes_read: unfilled.len() as u64, ..WorkCounters::default() }, &work.budget)?;
                        work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
                        read_at(file, offset + filled as u64, unfilled).map_err(DemandError::from)
                    };
                    #[cfg(windows)]
                    let read = file.read_at_measured(offset + filled as u64, unfilled, work);
                    match read {
                        Ok(0) => break,
                        Ok(count) => {
                            #[cfg(unix)]
                            work.record(WorkCounters { source_bytes_read: count as u64, ..WorkCounters::default() })?;
                            filled += count;
                        }
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error),
                    }
                }
                self.prove_current(file, work)?;
                work.charge(WorkCounters { output_bytes: filled as u64, ..WorkCounters::default() })?;
                Ok(filled)
            })();
            match result {
                Ok(filled) => {
                    // Match the canonical native-runtime output engine:
                    // Bytes needs one private ownership header when the
                    // completed prefix does not consume the Vec capacity.
                    if filled != bytes.capacity()
                        && let Err(error) = work.charge(WorkCounters {
                            allocation_operations: 1, ..WorkCounters::UNCHARGED
                        })
                    {
                        work.release(length)?;
                        return Err(error);
                    }
                    bytes.truncate(filled);
                    let logical_owned_bytes = bytes.capacity() as u64;
                    Ok(crate::kernel::OwnedSourceChunk { bytes: Bytes::from(bytes), logical_owned_bytes })
                }
                Err(error) => {
                    work.release(length)?;
                    Err(error)
                }
            }
        }

        /// What the file's source name holds now, through its held root.
        fn named_stat(&self, work: &mut DemandWork) -> Result<HostStat, DemandError> {
            self.provider.stat_relative(&self.relative, work)
        }

        /// Proves the bytes just read belong to the opened version: the held
        /// file is unmodified (in-place writes), the source path still names
        /// it (replacement by rename), and the root and reference are current.
        fn prove_current(
            &self,
            file: &SourceFile,
            work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            let held = stat_held(&self.provider, file, work)?;
            let named = self.named_stat(work);
            if matches!(named, Err(DemandError::Work(_) | DemandError::Cancelled)) {
                return named.map(|_| ());
            }
            let held_version = version_measured(&held, work)?;
            let named_version = if held_version == self.expected {
                named.as_ref().ok().map(|metadata| version_measured(metadata, work)).transpose()?
            } else { None };
            if held_version != self.expected || named_version != Some(self.expected)
            {
                // A replaced root renames every file under it: report that.
                self.provider.check(self.source, work)?;
                return Err(DemandError::StaleVersion);
            }
            self.provider.check(self.source, work)
        }
    }

    impl DemandFile for NativeDemandFile {
        fn logical_owned_bytes(&self) -> u64 {
            let bytes = (std::mem::size_of::<Self>() + std::mem::size_of::<SourceFile>()) as u64
                + path_owned_bytes(&self.relative);
            #[cfg(windows)]
            let bytes = bytes
                + std::mem::size_of::<std::sync::RwLock<Arc<SourceFile>>>() as u64
                + self.file.read().unwrap_or_else(std::sync::PoisonError::into_inner).logical_owned_bytes();
            bytes
        }

        fn read_range(
            &self,
            offset: u64,
            length: u64,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
            let range = ReadRange::new(offset, length).map_err(OperationFailure::before_work)?;
            let mut work = DemandWork::new(budget, cancellation);
            if let Err(error) = work.check() { return work.finish(Err(error)); }
            if self.provider.inner.inline {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.read(range, &mut work)))
                    .unwrap_or(Err(DemandError::WorkerUnavailable));
                return work.finish(result);
            }
            self.read_remote(range, work, REMOTE_READ_DEADLINE)
        }
    }

    impl NativeDemandFile {
        fn read_remote(
            &self,
            range: ReadRange,
            mut work: DemandWork,
            deadline: std::time::Duration,
        ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
            if BLOCKED_REMOTE_READS.fetch_add(1, Ordering::AcqRel) >= MAXIMUM_BLOCKED_REMOTE_READS {
                BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
                return work.finish(Err(DemandError::SourceUnavailable));
            }
            let _permit = RemoteReadPermit;
            #[cfg(unix)]
            let relative = {
                use std::os::unix::ffi::{OsStrExt, OsStringExt};
                let bytes = self.relative.as_os_str().as_bytes();
                let length = bytes.len() as u64;
                if let Err(error) = work.allocate_for(length, WorkCounters {
                    bytes_copied: length, ..WorkCounters::UNCHARGED
                }) { return work.finish(Err(error)); }
                PathBuf::from(std::ffi::OsString::from_vec(bytes.to_vec()))
            };
            #[cfg(windows)]
            let relative = {
                let length = self.relative.len() as u64 * 2;
                if let Err(error) = work.allocate_for(length, WorkCounters {
                    bytes_copied: length, ..WorkCounters::UNCHARGED
                }) { return work.finish(Err(error)); }
                self.relative.to_vec()
            };
            let file = Self {
                provider: self.provider.clone(), source: self.source, relative,
                expected: self.expected, logical_bytes: self.logical_bytes,
                file: Arc::clone(&self.file),
            };
            if let Err(error) = work.allocate(std::mem::size_of::<std::sync::atomic::AtomicBool>() as u64) {
                return work.finish(Err(error));
            }
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            work.stop = Some(Arc::clone(&stop));
            let adapter_bytes = std::mem::size_of::<NativeDemandReadMeter>() as u64;
            if let Err(error) = work.allocate(adapter_bytes) {
                return work.finish(Err(error));
            }
            let budget = work.budget;
            let caller_cancellation = work.cancellation.clone();
            let shared = Arc::new(NativeDemandReadMeter(Mutex::new(work)));
            let mut prepared = {
                let mut work = shared.lock();
                match work.prepare_cancelled(&caller_cancellation) {
                    Ok(prepared) => {
                        work.budget.items_examined -= prepared.cleanup_items();
                        prepared
                    }
                    Err(error) => {
                        drop(work);
                        let mut work = Arc::try_unwrap(shared).ok().expect("remote admission has no queue aliases")
                            .0.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
                        work.release(adapter_bytes).expect("remote read adapter remains owned");
                        return work.finish(Err(cancellation_error(error)));
                    }
                }
            };
            let worker_work = Arc::clone(&shared);
            let task_meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = shared.clone();
            let task = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(crate::obs::caller_context(), move || {
                    let mut work = worker_work.lock();
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| file.read(range, &mut work)))
                        .unwrap_or(Err(DemandError::WorkerUnavailable))
                }),
                task_meter,
            );
            let mut expired = false;
            let result = match task {
                Ok(task) => task.wait_measured(std::time::Instant::now().checked_add(deadline), &mut prepared, || {
                    expired = true;
                    stop.store(true, Ordering::Release);
                }).map_err(native_demand_error).and_then(std::convert::identity),
                Err(error) => Err(native_demand_error(error)),
            };
            // A registered cancellation node can retain the measured wake's
            // meter alias. Retire that waker outside the work mutex first.
            prepared.retire_waker();
            // Completion is not enough: runtime allocation guards and all
            // meter aliases must retire before the exact receipt is published.
            while Arc::strong_count(&shared) != 1 { std::thread::yield_now(); }
            let mut work = Arc::try_unwrap(shared).ok().expect("terminal remote read retires every meter alias")
                .0.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
            work.release(adapter_bytes).expect("remote read adapter remains owned until retirement");
            work.restore_budget(budget);
            let cleanup = work.finish_cancelled(prepared).map_err(cancellation_error);
            let result = if expired {
                Err(DemandError::SourceUnavailable)
            } else {
                result.and_then(|value| work.check().map(|()| value))
            };
            work.finish(cleanup.and(result))
        }
    }

    #[cfg(unix)]
    fn read_at(
        file: &std::fs::File,
        offset: u64,
        destination: &mut [u8],
    ) -> std::io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(file, destination, offset)
    }


    /// Stats the held `file` as its source root stats a name.
    #[cfg(windows)]
    fn stat_held(
        provider: &NativeDemandSource,
        file: &SourceFile,
        work: &mut DemandWork,
    ) -> Result<HostStat, DemandError> {
        file.with_measured(|file| provider.inner.root.stat_file_measured(file, work))
    }

    /// Stats the held `file` as its source root stats a name.
    #[cfg(not(windows))]
    fn stat_held(
        provider: &NativeDemandSource,
        file: &SourceFile,
        work: &mut DemandWork,
    ) -> Result<HostStat, DemandError> {
        provider.inner.root.stat_file_measured(file, work)
    }

    fn node_kind(file_type: crate::native_host::HostFileType) -> SourceNodeKind {
        if file_type.is_dir() {
            SourceNodeKind::Directory
        } else if file_type.is_file() {
            SourceNodeKind::RegularFile
        } else if file_type.is_symlink() {
            SourceNodeKind::SymbolicLink
        } else {
            special_node_kind(&file_type)
        }
    }

    #[cfg(unix)]
    fn special_node_kind(file_type: &crate::native_host::HostFileType) -> SourceNodeKind {
        if file_type.is_fifo() {
            SourceNodeKind::Fifo
        } else if file_type.is_socket() {
            SourceNodeKind::Socket
        } else if file_type.is_char_device() {
            SourceNodeKind::CharacterDevice
        } else if file_type.is_block_device() {
            SourceNodeKind::BlockDevice
        } else {
            SourceNodeKind::Unsupported
        }
    }

    #[cfg(not(unix))]
    fn special_node_kind(_file_type: &crate::native_host::HostFileType) -> SourceNodeKind {
        SourceNodeKind::Unsupported
    }

    impl DemandSource for NativeDemandSource {
        fn reference(&self) -> SourceReference {
            SourceReference {
                identity: self.inner.identity,
                epoch: self.inner.epoch.load(Ordering::Acquire),
            }
        }

        /// Watches a root on a local file system, whose changes all pass
        /// through this kernel and so are all reported; a network or
        /// user-space file system changes out of its sight.
        #[allow(unsafe_code, reason = "initializes preadmitted native watch ownership slots")]
        fn watch(
            &self,
            sink: Arc<dyn SourceChangeSink>,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Option<Box<dyn SourceWatch>>> {
            let mut work = DemandWork::new(budget, cancellation);
            let mut claimed = 0_u64;
            let result = (|| {
                work.check()?;
                if !self.inner.inline {
                    return Ok(None);
                }
                #[cfg(test)]
                if !self.watchable {
                    return Ok(None);
                }
                #[cfg(any(target_os = "linux", windows))]
                let mut watches = self.inner.watches.write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                #[cfg(any(target_os = "linux", windows))]
                {
                    let width = std::mem::size_of::<std::sync::Weak<crate::source_watch::NativeSourceWatch>>();
                    let mut at = 0;
                    while at < watches.len() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if watches[at].strong_count() == 0 {
                            let shifted = (watches.len() - at - 1).checked_mul(width).ok_or(WorkError::Overflow)?;
                            work.charge(WorkCounters { bytes_copied: shifted as u64, ..WorkCounters::default() })?;
                            watches.remove(at);
                        } else {
                            at += 1;
                        }
                    }
                    if watches.len() == watches.capacity() {
                        let old_bytes = watches.capacity().checked_mul(width).ok_or(WorkError::Overflow)? as u64;
                        work.adopt(old_bytes)?;
                        claimed += old_bytes;
                        let capacity = watches.len().checked_add(1).ok_or(WorkError::Overflow)?;
                        let bytes = capacity.checked_mul(width).ok_or(WorkError::Overflow)? as u64;
                        let delta = WorkCounters {
                            bytes_copied: watches.len().checked_mul(width).ok_or(WorkError::Overflow)? as u64,
                            ..WorkCounters::default()
                        };
                        work.work.admit(&delta, &work.budget)?;
                        work.allocate(bytes)?;
                        claimed += bytes;
                        work.record(delta)?;
                        let mut replacement = Vec::with_capacity(capacity);
                        replacement.append(&mut watches);
                        *watches = replacement;
                        work.release(old_bytes)?;
                        claimed -= old_bytes;
                    }
                }
                let bytes = std::mem::size_of::<NativeChanges>() as u64;
                work.allocate(bytes)?;
                claimed += bytes;
                let changes = Arc::new(NativeChanges {
                    inner: Arc::downgrade(&self.inner), sink,
                });
                // Allocate the actual result owners before native startup. A
                // later result allocation cannot hide an unmetered shutdown.
                let bytes = std::mem::size_of::<crate::source_watch::NativeSourceWatch>() as u64;
                work.allocate(bytes)?;
                claimed += bytes;
                let mut watch = Arc::<crate::source_watch::NativeSourceWatch>::new_uninit();
                let bytes = std::mem::size_of::<NativeWatch>() as u64;
                work.allocate(bytes)?;
                claimed += bytes;
                let mut lease = Box::<NativeWatch>::new_uninit();
                let output = WorkCounters { items_returned: 1, ..WorkCounters::default() };
                work.work.admit(&output, &work.budget)?;
                let producer = crate::source_watch::NativeSourceWatch::start_measured(
                    &self.inner.root, changes, &mut work,
                )?;
                Arc::get_mut(&mut watch).expect("private uninitialized watch owner").write(producer);
                // SAFETY: the unique Arc payload was initialized above.
                let watch = unsafe { watch.assume_init() };
                lease.write(NativeWatch(Arc::clone(&watch)));
                #[cfg(any(target_os = "linux", windows))]
                watches.push(Arc::downgrade(&watch));
                work.record(output)?;
                // SAFETY: the actual lease payload was initialized above.
                Ok(Some(unsafe { lease.assume_init() } as Box<dyn SourceWatch>))
            })();
            let released = work.release(claimed);
            let result = result.and_then(|value| released.map(|()| value));
            result.map(|value| OperationReceipt { value, work: work.work })
                .map_err(|error| OperationFailure::new(error, work.work))
        }

        async fn lookup(
            &self,
            source: SourceReference,
            path: &NamespacePath,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Option<SourceNode>> {
            self.run_request(path, budget, cancellation, move |provider, path, work| {
                provider.check(source, work)?;
                provider.observe_parent(&path, work)?;
                let mut node = provider.metadata(&path, work)?.as_ref()
                    .map(|metadata| Self::node_measured(metadata, work)).transpose()?;
                // A directory's own facts become observable after admitting
                // that directory, so re-read when admission newly began.
                if node.is_some_and(|node| node.kind == SourceNodeKind::Directory)
                    && provider.admit_to_watches(&path, work)?
                {
                    node = provider.metadata(&path, work)?.as_ref()
                        .map(|metadata| Self::node_measured(metadata, work)).transpose()?;
                }
                provider.check(source, work)?;
                Ok(node)
            }).await
        }

        async fn list_page(
            &self,
            source: SourceReference,
            directory: &NamespacePath,
            cursor: Option<SourceCursor>,
            maximum_entries: u32,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<SourceDirectoryPage> {
            if maximum_entries == 0 || maximum_entries > 4096 {
                return Err(OperationFailure::before_work(DemandError::InvalidRequest));
            }
            self.run_request(directory, budget, cancellation, move |provider, directory, work| {
                let original_budget = work.budget;
                // Keep one cleanup permit unspent until the guard accepts,
                // explicitly aborts, or restores the original lease on drop.
                work.budget = publication_cleanup().remaining(original_budget)?;
                work.allocate(std::mem::size_of::<NativePagePublication>() as u64)?;
                let mut publication = Box::new(NativePagePublication { lease: None, end: 0, private_bytes: 0 });
                let answer = (|| {
                    provider.check(source, work)?;
                    provider.observe_directory(&directory, work)?;
                    let metadata = provider.metadata(&directory, work)?.ok_or(DemandError::Absent)?;
                    if !metadata.is_dir() { return Err(DemandError::NotDirectory); }
                    let observed = version_measured(&metadata, work)?;
                    publication.lease = Some(provider.cursor_lease(source, &directory, observed, cursor, maximum_entries, work)?);
                    let lease = publication.lease.as_ref().ok_or(DemandError::StaleCursor)?;
                    let mut state = lease.state()?;
                    let range = provider.page_entries(&mut state, maximum_entries, work, || {})?;
                    let backing = state.replay.as_ref().ok_or(DemandError::StaleCursor)?;
                    let has_next = !state.eof || state.pending.is_some() || range.end < backing.len();
                    let entries = SourceDirectoryEntries::shared(Arc::clone(backing), range.clone(), state.replay_bytes);
                    let private_bytes = state.private_bytes()
                        .checked_add(std::mem::size_of::<NativePagePublication>() as u64)
                        .ok_or(crate::WorkError::Overflow)?;
                    drop(state);
                    provider.check(source, work)?;
                    let current = provider.metadata(&directory, work)?;
                    if current.as_ref().map(|metadata| version_measured(metadata, work)).transpose()? != Some(observed) {
                        return Err(DemandError::StaleVersion);
                    }
                    let next = if has_next {
                        Some(SourceCursor {
                            source, directory: clone_namespace(&directory, work)?,
                            directory_version: observed, token: lease.token,
                        })
                    } else { None };
                    work.check()?;
                    work.record(WorkCounters { items_returned: range.len() as u64, ..WorkCounters::UNCHARGED })?;
                    publication.end = range.end;
                    publication.private_bytes = private_bytes;
                    Ok((entries, next, observed))
                })();
                work.restore_budget(original_budget);
                match answer {
                    Ok((entries, next, version)) => Ok(SourceDirectoryPage {
                        entries, next, version, publication: SourcePagePublication::guarded(publication),
                    }),
                    Err(error) => {
                        if publication.lease.is_some() {
                            work.record(publication_cleanup())?;
                            publication.lease.take();
                        }
                        Err(error)
                    }
                }
            }).await
        }

        async fn read_range(
            &self,
            source: SourceReference,
            path: &NamespacePath,
            expected: SourceVersion,
            offset: u64,
            length: u64,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
            let range = ReadRange::new(offset, length).map_err(OperationFailure::before_work)?;
            self.run_request(path, budget, cancellation, move |provider, path, work| {
                NativeDemandFile::open(provider, source, &path, expected, work)?.read(range, work)
            }).await
        }

        async fn open_file(
            &self,
            source: SourceReference,
            path: &NamespacePath,
            expected: SourceVersion,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Box<dyn DemandFile>> {
            self.run_request(path, budget, cancellation, move |provider, path, work| {
                let file = NativeDemandFile::open(provider, source, &path, expected, work)?;
                work.allocate(std::mem::size_of::<NativeDemandFile>() as u64)?;
                Ok(Box::new(file) as Box<dyn DemandFile>)
            }).await
        }

        async fn read_link(
            &self,
            source: SourceReference,
            path: &NamespacePath,
            expected: SourceVersion,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
            self.run_request(path, budget, cancellation, move |provider, path, work| {
                provider.check(source, work)?;
                provider.observe_parent(&path, work)?;
                let before = provider.metadata(&path, work)?.ok_or(DemandError::Absent)?;
                if !before.file_type().is_symlink() { return Err(DemandError::InvalidRequest); }
                if version_measured(&before, work)? != expected { return Err(DemandError::StaleVersion); }
                #[cfg(unix)]
                work.work.admit(&WorkCounters { output_bytes: before.len(), ..WorkCounters::default() }, &work.budget)?;
                let relative = provider.relative(&path, work)?;
                let remaining = work.remaining()?;
                let maximum = remaining.output_bytes.min(remaining.source_bytes_read);
                #[cfg(unix)]
                let target = provider.inner.root.read_link_measured(&relative, maximum, work)?;
                #[cfg(windows)]
                let target = provider.inner.root.read_link_wide_measured(&relative, maximum, work)?;
                work.release(path_owned_bytes(&relative))?;
                let length = path_owned_bytes(&target) as usize;
                work.work.admit(&WorkCounters { output_bytes: length as u64, ..WorkCounters::default() }, &work.budget)?;
                #[cfg(unix)]
                work.allocate_for(length as u64, WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() })?;
                #[cfg(windows)]
                work.allocate_for(length as u64, WorkCounters { bytes_encoded: length as u64, ..WorkCounters::default() })?;
                #[cfg(unix)]
                let bytes = os_string_bytes(target.as_os_str());
                #[cfg(windows)]
                let bytes = {
                    let mut bytes = Vec::with_capacity(length);
                    for unit in &target { bytes.extend_from_slice(&unit.to_le_bytes()); }
                    bytes
                };
                work.release(path_owned_bytes(&target))?;
                let after = provider.metadata(&path, work)?.ok_or(DemandError::Absent)?;
                if version_measured(&after, work)? != expected { return Err(DemandError::StaleVersion); }
                provider.check(source, work)?;
                work.charge(WorkCounters { output_bytes: length as u64, ..WorkCounters::default() })?;
                let logical_owned_bytes = bytes.capacity() as u64;
                Ok(crate::kernel::OwnedSourceChunk { bytes: Bytes::from(bytes), logical_owned_bytes })
            }).await
        }
    }

    /// The live watch of one native source.
    struct NativeWatch(Arc<crate::source_watch::NativeSourceWatch>);

    impl SourceWatch for NativeWatch {
        fn fence(&self) -> Result<(), DemandError> {
            self.0.fence().map_err(DemandError::Io)
        }

        fn is_exact(&self) -> bool {
            self.0.is_exact()
        }
    }

    /// Turns host reports into source changes: each path into its name in
    /// the source's namespace and, where the host still names a node there,
    /// that node. It refers to the source weakly, as the sink does to its
    /// view: nothing a host keeps after the watch stops may hold the source.
    struct NativeChanges {
        inner: std::sync::Weak<NativeDemandInner>,
        sink: Arc<dyn SourceChangeSink>,
    }

    #[derive(Clone, Copy)]
    enum NativeChangedName<'a> {
        Os(&'a std::ffi::OsStr),
        Invalid,
        #[cfg(windows)]
        Wide(&'a [u16]),
    }

    impl NativeDemandInner {
        /// Converts the longest representable prefix once, moving its owned
        /// names rather than repeatedly cloning shorter candidates.
        fn names_measured<'a>(
            &self, parts: impl Iterator<Item = NativeChangedName<'a>> + Clone,
            work: &mut DemandWork,
        ) -> Result<(NamespacePath, bool), DemandError> {
            let mut count = 0_usize;
            for _ in parts.clone() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
            }
            let capacity = count.min(usize::from(self.limits.maximum_path_depth));
            let vector_bytes = (capacity as u64)
                .checked_mul(std::mem::size_of::<crate::kernel::LogicalName>() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            let prior_peak = work.work.peak_allocation_bytes;
            work.allocate(vector_bytes)?;
            let mut names = Vec::new();
            if names.try_reserve_exact(capacity).is_err() {
                work.release(vector_bytes)?;
                work.work.peak_allocation_bytes = prior_peak;
                return Err(DemandError::WorkerUnavailable);
            }
            let mut exact = true;
            let mut encoded_bytes = 1_u32;
            for part in parts {
                work.charge(WorkCounters {
                    items_examined: 1, source_path_components: 1,
                    ..WorkCounters::default()
                })?;
                if names.len() == capacity { exact = false; break; }
                let encoded = match part {
                    NativeChangedName::Os(name) => crate::native_name::host_name_bytes_measured(
                        name, self.profile, self.limits.maximum_component_bytes, work,
                    ),
                    #[cfg(windows)]
                    NativeChangedName::Wide(name) => crate::native_name::host_wide_name_bytes_measured(
                        name, self.profile, self.limits.maximum_component_bytes, work,
                    ),
                    NativeChangedName::Invalid => { exact = false; break; }
                };
                let (encoding, bytes) = match encoded {
                    Ok(encoded) => encoded,
                    Err(DemandError::InvalidRequest) => { exact = false; break; }
                    Err(error) => return Err(error),
                };
                let retained = bytes.capacity() as u64;
                let next_bytes = encoded_bytes.checked_add(u32::from(!names.is_empty()))
                    .and_then(|length| length.checked_add(u32::try_from(bytes.len()).ok()?));
                if next_bytes.is_none_or(|length| length > self.limits.maximum_path_bytes) {
                    drop(bytes);
                    work.release(retained)?;
                    exact = false;
                    break;
                }
                let name = match crate::kernel::LogicalName::new(
                    encoding, bytes, self.limits.maximum_component_bytes,
                ) {
                    Ok(name) => name,
                    Err(_) => {
                        work.release(retained)?;
                        exact = false;
                        break;
                    }
                };
                encoded_bytes = next_bytes.expect("admitted encoded path length");
                names.push(name);
            }
            work.charge(WorkCounters { items_examined: names.len() as u64, ..WorkCounters::default() })?;
            Ok((NamespacePath::new(names, self.limits)?, exact))
        }

        fn name_measured(&self, relative: &Path, work: &mut DemandWork) -> Result<(NamespacePath, bool), DemandError> {
            self.names_measured(relative.components().map(|component| match component {
                std::path::Component::Normal(name) => NativeChangedName::Os(name),
                _ => NativeChangedName::Invalid,
            }), work)
        }

        #[cfg(windows)]
        fn name_wide_measured(&self, relative: &[u16], work: &mut DemandWork) -> Result<(NamespacePath, bool), DemandError> {
            self.names_measured(relative.split(|unit| *unit == u16::from(b'\\') || *unit == u16::from(b'/'))
                .filter(|name| !name.is_empty()).map(NativeChangedName::Wide), work)
        }
    }

    impl NativeChanges {
        fn append_path_change(
            reported: &mut Vec<SourceChange>, name: NamespacePath, exact: bool,
            altered: bool, metadata: Result<HostStat, DemandError>, work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            let node = match metadata {
                Ok(metadata) => {
                    work.charge(WorkCounters { bytes_hashed: identity_hash_bytes(&metadata), ..WorkCounters::default() })?;
                    Some(file_identity(&metadata))
                }
                Err(DemandError::Io(error)) if matches!(error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => None,
                Err(error) => return Err(error),
            };
            let altered = altered && exact && (node.is_some() || name.is_root());
            if altered {
                reported.push(SourceChange::Entry(name));
            } else if name.is_root() {
                let bytes = name.retained_bytes();
                drop(name);
                work.release(bytes)?;
                reported.push(SourceChange::Everything);
            } else {
                reported.push(SourceChange::Name(name));
            }
            if let Some(node) = node { reported.push(SourceChange::Node(node)); }
            Ok(())
        }
    }

    impl crate::source_watch::HostChangeSink for NativeChanges {
        fn host_changed(
            &self, changes: &[crate::source_watch::HostChange<'_>], work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            use crate::source_watch::HostChange;
            work.check()?;
            // No consumer remains when the actual source has been dropped.
            let Some(inner) = self.inner.upgrade() else { return Ok(()); };
            let capacity = changes.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
            let vector_bytes = (capacity as u64).checked_mul(std::mem::size_of::<SourceChange>() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            let prior_peak = work.work.peak_allocation_bytes;
            work.allocate(vector_bytes)?;
            let mut reported = Vec::new();
            if reported.try_reserve_exact(capacity).is_err() {
                work.release(vector_bytes)?;
                work.work.peak_allocation_bytes = prior_peak;
                return Err(DemandError::WorkerUnavailable);
            }
            for change in changes {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                match change {
                    HostChange::Everything => reported.push(SourceChange::Everything),
                    #[cfg(any(target_os = "macos", windows))]
                    HostChange::Unconfirmed => reported.push(SourceChange::Unconfirmed),
                    #[cfg(windows)]
                    HostChange::File(index) => {
                        work.charge(WorkCounters {
                            bytes_hashed: b"acyclic-fs-native-source-file-v1\0".len() as u64 + 12,
                            ..WorkCounters::default()
                        })?;
                        reported.push(SourceChange::Node(windows_file_identity(inner.root.identity().device, *index)));
                    }
                    HostChange::Rebound(relative) | HostChange::Altered(relative) => {
                        let (name, exact) = inner.name_measured(relative, work)?;
                        let metadata = inner.root.stat_measured(relative, work);
                        Self::append_path_change(&mut reported, name, exact,
                            matches!(change, HostChange::Altered(_)), metadata, work)?;
                    }
                    #[cfg(windows)]
                    HostChange::ReboundWide { path, .. } | HostChange::AlteredWide(path) => {
                        let (name, exact) = inner.name_wide_measured(path, work)?;
                        let metadata = inner.root.stat_wide_measured(path, work);
                        Self::append_path_change(&mut reported, name, exact,
                            matches!(change, HostChange::AlteredWide(_)), metadata, work)?;
                    }
                }
            }
            if !reported.is_empty() {
                let remaining = work.remaining()?;
                let cancellation = work.cancellation().clone();
                let answer = self.sink.source_changed(&reported, remaining, &cancellation);
                match answer {
                    Ok(receipt) => work.merge(receipt.work)?,
                    Err(failure) => {
                        work.merge(*failure.work)?;
                        return Err(failure.error);
                    }
                }
            }
            let names = reported.iter().try_fold(0_u64, |sum, change| {
                let bytes = match change {
                    SourceChange::Name(path) | SourceChange::Entry(path) => path.retained_bytes(),
                    _ => 0,
                };
                sum.checked_add(bytes).ok_or(crate::WorkError::Overflow)
            })?;
            drop(reported);
            work.release(vector_bytes.checked_add(names).ok_or(crate::WorkError::Overflow)?)?;
            Ok(())
        }
    }

    #[cfg(unix)]
    fn os_string_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
        use std::os::unix::ffi::OsStrExt as _;
        value.as_bytes().to_vec()
    }


    const fn version_hash_bytes() -> u64 {
        b"acyclic-fs-native-source-version-v1\0".len() as u64 + 8
            + if cfg!(unix) { 48 } else { 32 }
    }

    fn identity_hash_bytes(metadata: &HostStat) -> u64 {
        #[cfg(unix)]
        let facts = 16 + if metadata.created().ok()
            .and_then(|born| born.into_std().duration_since(std::time::UNIX_EPOCH).ok()).is_some() { 16 } else { 0 };
        #[cfg(windows)]
        let facts = { let _ = metadata; 12 };
        b"acyclic-fs-native-source-file-v1\0".len() as u64 + facts
    }

    fn version_measured(metadata: &HostStat, work: &mut DemandWork) -> Result<SourceVersion, DemandError> {
        work.charge(WorkCounters { bytes_hashed: version_hash_bytes(), ..WorkCounters::default() })?;
        Ok(version(metadata))
    }

    fn version(metadata: &HostStat) -> SourceVersion {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-version-v1\0");
        hash.update(&metadata.len().to_le_bytes());
        #[cfg(unix)]
        {
            hash.update(&metadata.dev().to_le_bytes());
            hash.update(&metadata.ino().to_le_bytes());
            hash.update(&metadata.mtime().to_le_bytes());
            hash.update(&metadata.mtime_nsec().to_le_bytes());
            hash.update(&metadata.ctime().to_le_bytes());
            hash.update(&metadata.ctime_nsec().to_le_bytes());
        }
        #[cfg(windows)]
        {
            hash.update(&metadata.file_attributes().to_le_bytes());
            hash.update(&metadata.last_write_time().to_le_bytes());
            hash.update(&metadata.creation_time().to_le_bytes());
            hash.update(&metadata.volume_serial_number().to_le_bytes());
            hash.update(&metadata.file_index().to_le_bytes());
        }
        SourceVersion(*hash.finalize().as_bytes())
    }

    /// A file's identity: its device and inode, and its birth time, which
    /// tells a new file apart from a removed one whose inode the host reused.
    /// Every name of one file shares it. A file system that records no birth
    /// time (a few do not) cannot tell reuse apart by identity alone.
    #[cfg(unix)]
    fn file_identity(metadata: &HostStat) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-file-v1\0");
        hash.update(&metadata.dev().to_le_bytes());
        hash.update(&metadata.ino().to_le_bytes());
        if let Some(born) = metadata
            .created()
            .ok()
            .and_then(|born| born.into_std().duration_since(std::time::UNIX_EPOCH).ok())
        {
            hash.update(&born.as_nanos().to_le_bytes());
        }
        *hash.finalize().as_bytes()
    }

    #[cfg(windows)]
    fn file_identity(metadata: &HostStat) -> [u8; 32] {
        windows_file_identity(
            u64::from(metadata.volume_serial_number()),
            metadata.file_index(),
        )
    }

    /// The identity of the file with host index `index` on the volume whose
    /// serial number is `volume`, as a lookup of it reports it.
    #[cfg(windows)]
    fn windows_file_identity(volume: u64, index: u64) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-file-v1\0");
        hash.update(&u32::try_from(volume).unwrap_or_default().to_le_bytes());
        hash.update(&index.to_le_bytes());
        *hash.finalize().as_bytes()
    }

    fn link_count(metadata: &HostStat) -> Option<u64> {
        #[cfg(unix)]
        {
            Some(metadata.nlink())
        }
        // Every Windows stat reads the file record, which counts every
        // name, so a listed node and a lookup report the same count.
        #[cfg(not(unix))]
        {
            metadata.number_of_links().map(u64::from)
        }
    }

    #[allow(
        clippy::useless_conversion,
        reason = "dev_t and major/minor result widths differ across Unix targets"
    )]
    fn device_identity(metadata: &HostStat, kind: SourceNodeKind) -> Option<(u32, u32)> {
        #[cfg(unix)]
        {
            if !matches!(
                kind,
                SourceNodeKind::CharacterDevice | SourceNodeKind::BlockDevice
            ) {
                return None;
            }
            let raw: libc::dev_t = metadata.rdev().try_into().ok()?;
            Some((
                libc::major(raw).try_into().ok()?,
                libc::minor(raw).try_into().ok()?,
            ))
        }
        #[cfg(not(unix))]
        {
            let _ = (metadata, kind);
            None
        }
    }

    fn source_metadata(metadata: &HostStat) -> SourceMetadata {
        let mut result = SourceMetadata {
            created_ns: metadata.created().ok().and_then(system_time_nanos),
            modified_ns: metadata.modified().ok().and_then(system_time_nanos),
            accessed_ns: metadata.accessed().ok().and_then(system_time_nanos),
            ..SourceMetadata::default()
        };
        #[cfg(unix)]
        {
            result.posix_mode = Some(metadata.mode());
            result.posix_uid = Some(metadata.uid());
            result.posix_gid = Some(metadata.gid());
            result.changed_ns = Some(
                i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec()),
            );
        }
        #[cfg(windows)]
        {
            result.windows_attributes = Some(metadata.file_attributes());
        }
        result
    }

    fn system_time_nanos(time: cap_std::time::SystemTime) -> Option<i128> {
        match time.into_std().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => Some(
                i128::from(duration.as_secs()) * 1_000_000_000
                    + i128::from(duration.subsec_nanos()),
            ),
            Err(error) => Some(
                -(i128::from(error.duration().as_secs()) * 1_000_000_000
                    + i128::from(error.duration().subsec_nanos())),
            ),
        }
    }
}

#[cfg(all(test, feature = "native-watch", not(target_arch = "wasm32")))]
mod tests {
    use super::native::NativeDemandSource;
    use super::*;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use std::error::Error;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingObserver {
        directories: Mutex<Vec<NamespacePath>>,
    }

    impl DemandDirectoryObserver for RecordingObserver {
        fn observe_directory(
            &self, directory: &NamespacePath, budget: WorkBudget, cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            let mut work = DemandWork::new(budget, cancellation);
            let result = (|| {
                work.check()?;
                let mut directories = self.directories.lock().map_err(|_| DemandError::SourceUnavailable)?;
                let old = directories.capacity() * std::mem::size_of::<NamespacePath>();
                work.adopt(old as u64)?;
                if directories.len() == directories.capacity() {
                    let capacity = directories.capacity().saturating_mul(2).max(1);
                    work.allocate((capacity * std::mem::size_of::<NamespacePath>()) as u64)?;
                    let mut grown = Vec::with_capacity(capacity);
                    grown.append(&mut directories);
                    *directories = grown;
                    work.release(old as u64)?;
                }
                directories.push(clone_namespace(directory, &mut work)?);
                Ok(())
            })();
            work.finish(result)
        }
    }

    fn path(value: &str) -> Result<NamespacePath, Box<dyn Error>> {
        let portable = crate::path::PortablePath::parse(value, VolumeLimits::default())?;
        Ok(NamespacePath::from_portable(
            &portable,
            VolumeLimits::default(),
        )?)
    }

    /// Every source change delivered, in order.
    #[derive(Default)]
    struct RecordedChanges(Mutex<Vec<SourceChange>>);

    impl SourceChangeSink for RecordedChanges {
        fn source_changed(&self, changes: &[SourceChange]) {
            if let Ok(mut recorded) = self.0.lock() {
                recorded.extend_from_slice(changes);
            }
        }
    }

    impl RecordedChanges {
        fn take(&self) -> Result<Vec<SourceChange>, Box<dyn Error>> {
            Ok(std::mem::take(
                &mut *self.0.lock().map_err(|_| "recording poisoned")?,
            ))
        }

        /// Everything reported until `reported` holds of it, or ten seconds
        /// pass. A fence proves delivery on Linux and Windows; on macOS,
        /// where it reports everything instead, the reports themselves are
        /// waited for.
        fn until(
            &self,
            watch: &dyn SourceWatch,
            reported: impl Fn(&[SourceChange]) -> bool,
        ) -> Result<Vec<SourceChange>, Box<dyn Error>> {
            watch.fence()?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut changes = self.take()?;
            while !reported(&changes) && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(10));
                changes.extend(self.take()?);
            }
            Ok(changes)
        }
    }

    /// A native source reports each change by the name it touched and by
    /// the node that name still binds, which reaches the node's other names.
    #[tokio::test(flavor = "multi_thread")]
    async fn native_source_reports_the_names_and_nodes_it_changed() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("d"))?;
        std::fs::write(root.path().join("f"), b"one")?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let recorded = Arc::new(RecordedChanges::default());
        let watch = source
            .watch(recorded.clone())?
            .ok_or("a local root is watched")?;
        let cancellation = CancellationToken::new();
        let node = source.lookup(source.reference(), &path("/f")?, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .ok_or("f is present")?;
        // Reading beneath a directory watches it where the host needs that.
        assert!(
            source
                .lookup(source.reference(), &path("/d/x")?, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .is_none()
        );

        std::fs::write(root.path().join("f"), b"changed")?;
        std::fs::write(root.path().join("d").join("x"), b"x")?;
        let (f, x) = (path("/f")?, path("/d/x")?);
        let changes = recorded.until(watch.as_ref(), |changes| {
            changes.contains(&SourceChange::Node(node.file_identity))
                && changes.contains(&SourceChange::Name(x.clone()))
                && (changes.contains(&SourceChange::Entry(f.clone()))
                    || changes.contains(&SourceChange::Name(f.clone())))
        })?;
        // `FSEvents` may coalesce the file's recent creation into its write,
        // which reports the name rebound rather than its entry altered.
        assert!(
            changes.contains(&SourceChange::Entry(path("/f")?))
                || cfg!(target_os = "macos") && changes.contains(&SourceChange::Name(path("/f")?)),
            "{changes:?}"
        );
        assert!(
            changes.contains(&SourceChange::Node(node.file_identity)),
            "{changes:?}"
        );
        assert!(
            changes.contains(&SourceChange::Name(path("/d/x")?)),
            "{changes:?}"
        );
        // Nothing was lost; a macOS fence reports everything by design.
        #[cfg(not(target_os = "macos"))]
        assert!(!changes.contains(&SourceChange::Everything));
        assert!(watch.is_exact());
        Ok(())
    }

    /// Changes beneath an excluded subtree are invisible, as its entries are.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_filtered_source_reports_nothing_beneath_its_exclusions() -> Result<(), Box<dyn Error>>
    {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join(".git"))?;
        let source = FilteredDemandSource::new(
            NativeDemandSource::open(
                root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
            vec![path("/.git")?],
        );
        let recorded = Arc::new(RecordedChanges::default());
        let watch = source
            .watch(recorded.clone())?
            .ok_or("a local root is watched")?;
        std::fs::write(root.path().join(".git").join("index"), b"x")?;
        std::fs::write(root.path().join("visible"), b"x")?;
        let visible = path("/visible")?;
        let changes = recorded.until(watch.as_ref(), |changes| {
            changes.contains(&SourceChange::Name(visible.clone()))
        })?;
        let excluded = path("/.git")?;
        assert!(
            changes.contains(&SourceChange::Name(path("/visible")?)),
            "{changes:?}"
        );
        assert!(
            !changes.iter().any(|change| matches!(
                change,
                SourceChange::Name(name) | SourceChange::Entry(name) if name.is_within(&excluded)
            )),
            "{changes:?}"
        );
        Ok(())
    }

    /// A file created where a removed one was, on the inode the host
    /// reused for it, is a different file: its birth time tells them apart.
    /// Both names of one file share its identity.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_reused_inode_is_a_new_identity_and_hard_links_share_one()
    -> Result<(), Box<dyn Error>> {
        use std::os::unix::fs::MetadataExt as _;

        let root = tempfile::tempdir()?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Posix,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let identity = |name: &'static str| {
            let source = &source;
            let cancellation = &cancellation;
            async move {
                Ok::<_, Box<dyn Error>>(
                    source.lookup(reference, &path(name)?, WorkBudget::UNBOUNDED, cancellation)
                        .await?
                        .value
                        .ok_or("file was absent")?
                        .file_identity,
                )
            }
        };
        std::fs::write(root.path().join("first"), b"first")?;
        std::fs::hard_link(root.path().join("first"), root.path().join("alias"))?;
        let first = identity("/first").await?;
        assert_eq!(identity("/alias").await?, first);

        let inode = std::fs::metadata(root.path().join("first"))?.ino();
        let born = std::fs::metadata(root.path().join("first"))?.created().ok();
        std::fs::remove_file(root.path().join("first"))?;
        std::fs::remove_file(root.path().join("alias"))?;
        // Hosts reuse a freed inode soon, but not always at once.
        for attempt in 0..64 {
            let name = format!("later-{attempt}");
            std::fs::write(root.path().join(&name), b"later")?;
            let metadata = std::fs::metadata(root.path().join(&name))?;
            if metadata.ino() == inode && born.is_some() && metadata.created().ok() != born {
                let later = source.lookup(reference, &path(&format!("/{name}"))?, WorkBudget::UNBOUNDED, &cancellation)
                    .await?
                    .value
                    .ok_or("file was absent")?
                    .file_identity;
                assert_ne!(later, first, "the reused inode names a new file");
                break;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_source_reports_symbolic_link_target_bytes() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::os::unix::fs::symlink("target", root.path().join("link"))?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Posix,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let node = source.lookup(reference, &path("/link")?, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .ok_or("symbolic link was absent")?;

        assert_eq!(node.kind, SourceNodeKind::SymbolicLink);
        assert_eq!(node.logical_bytes, Some(6));
        assert_eq!(
            source
                .read_link(reference, &path("/link")?, node.version, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .as_ref(),
            b"target"
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_source_observes_only_the_demanded_directory_before_reading()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("nested"))?;
        std::fs::write(root.path().join("nested/file"), b"content")?;
        let observer = Arc::new(RecordingObserver::default());
        let source = NativeDemandSource::open_with_observer(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            observer.clone(),
        )
        .await?;
        let reference = source.reference();
        source.lookup(reference, &path("/nested/file")?, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?;
        assert_eq!(
            observer
                .directories
                .lock()
                .map_err(|_| "observer state poisoned")?
                .as_slice(),
            &[path("/nested")?]
        );
        Ok(())
    }

    #[tokio::test]
    async fn native_provider_reads_do_not_visit_unrelated_content() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("forbidden"))?;
        std::fs::write(
            root.path().join("forbidden").join("unrelated"),
            vec![7; 8 * 1024 * 1024],
        )?;
        let mut body = vec![b'0'; 1024 * 1024];
        body[3..7].copy_from_slice(b"3456");
        std::fs::write(root.path().join("seen"), body)?;

        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let provider = &source;
        let reference = provider.reference();
        let cancellation = CancellationToken::new();
        let node = provider.lookup(reference, &path("/seen")?, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(node.work.source_path_components, 1);
        assert_eq!(node.work.source_entries_visited, 0);
        assert_eq!(node.work.source_bytes_read, 0);
        let version = node.value.ok_or("seen path was absent")?.version;

        let bytes = provider.read_range(reference, &path("/seen")?, version, 3, 4, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(bytes.value.as_ref(), b"3456");
        assert_eq!(bytes.work.source_bytes_read, 4);
        assert_eq!(bytes.work.source_entries_visited, 0);

        let mut first = provider.list_page(reference, &path("/")?, None, 1, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(first.value.entries.len(), 1);
        assert_eq!(first.work.source_entries_visited, 1);
        first.value.publication.accept(WorkBudget::UNBOUNDED, &cancellation).await?;
        let continuation = first.value.next.ok_or("second root entry missing")?;
        let mut second = provider.list_page(reference, &path("/")?, Some(continuation), 1, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(second.value.entries.len(), 1);
        assert_eq!(second.work.source_entries_visited, 1);
        second.value.publication.accept(WorkBudget::UNBOUNDED, &cancellation).await?;
        Ok(())
    }

    #[tokio::test]
    async fn metadata_version_fence_admits_its_actual_comparison() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("file"), b"unread body")?;
        let source = NativeDemandSource::open(
            root.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?;
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let requested = path("/file")?;
        let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
        if let serde_json::Value::Object(fields) = &mut allowances {
            for allowance in fields.values_mut() { *allowance = serde_json::json!(1_000_000_u64); }
        }
        let budget: WorkBudget = serde_json::from_value(allowances)?;
        // Warm the same real cancellation registry before measuring the
        // stable native lookup prefix used by the default metadata method.
        source.lookup(reference, &requested, budget, &cancellation).await?;
        let lookup = source.lookup(reference, &requested, budget, &cancellation).await?;
        let expected = lookup.value.ok_or("file absent")?.version;
        let mut exact = budget;
        exact.items_examined = lookup.work.items_examined;
        let failure = source.read_metadata(reference, &requested, Some(expected), exact, &cancellation)
            .await.err().ok_or("metadata fence exceeded its comparison allowance")?;
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(failure.work.backend_read_operations, lookup.work.backend_read_operations);
        assert_eq!(failure.work.items_examined, lookup.work.items_examined);
        assert_eq!(failure.work.source_entries_visited, 0);
        assert_eq!(failure.work.source_bytes_read, 0);
        failure.work.verify(exact)?;
        exact.items_examined += 1;
        let metadata = source.read_metadata(reference, &requested, Some(expected), exact, &cancellation).await?;
        assert_eq!(metadata.value, lookup.value);
        assert_eq!(metadata.work.items_examined, exact.items_examined);
        assert_eq!(metadata.work.source_entries_visited, 0);
        assert_eq!(metadata.work.source_bytes_read, 0);
        metadata.work.verify(exact)?;
        Ok(())
    }

    #[tokio::test]
    async fn listed_nodes_are_exactly_what_lookups_report() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("directory"))?;
        std::fs::write(root.path().join("file"), b"body")?;
        std::fs::hard_link(root.path().join("file"), root.path().join("alias"))?;
        // A link is listed as itself, never as its target.
        #[cfg(unix)]
        std::os::unix::fs::symlink("directory", root.path().join("link"))?;
        // Growing a file through one of its links leaves the other link's
        // directory record describing the old file on NTFS.
        {
            use std::io::Write as _;
            std::fs::OpenOptions::new()
                .append(true)
                .open(root.path().join("file"))?
                .write_all(b" grown through one link")?;
        }
        // A writer still open has not updated its name's directory record.
        let mut open = std::fs::File::create(root.path().join("open"))?;
        {
            use std::io::Write as _;
            open.write_all(b"written, never closed")?;
            open.flush()?;
        }
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let provider = &source;
        let reference = provider.reference();
        let cancellation = CancellationToken::new();
        let mut page = provider.list_page(reference, &path("/")?, None, 16, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(page.value.entries.len(), if cfg!(unix) { 5 } else { 4 });
        page.value.publication.accept(WorkBudget::UNBOUNDED, &cancellation).await?;
        let mut listed = Vec::new();
        for entry in &page.value.entries {
            let name = entry
                .name
                .unicode_text()
                .ok_or("listed name is not Unicode")?
                .to_string();
            let looked_up = provider.lookup(reference, &path(&format!("/{name}"))?, WorkBudget::UNBOUNDED, &cancellation)
                .await?;
            assert_eq!(looked_up.value, Some(entry.node), "{name}");
            listed.push((name, entry.node));
        }
        // Every listed version opens: a listing never pins a version that a
        // handle on the same file disagrees with.
        for (name, node) in listed {
            if node.kind != SourceNodeKind::RegularFile {
                continue;
            }
            let file = provider.open_file(reference, &path(&format!("/{name}"))?, node.version, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| format!("{name}: {:?}", failure.error))?
                .value;
            let length = node.logical_bytes.ok_or("regular file has no length")?;
            let read = file.read_range(0, length, WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|failure| format!("{name}: {:?}", failure.error))?;
            assert_eq!(read.value.len() as u64, length, "{name}");
        }
        drop(open);
        Ok(())
    }

    #[test]
    fn external_cancellation_registry_stays_owned_across_healthy_waits() -> Result<(), Box<dyn Error>> {
        let caller = CancellationToken::new();
        let lifetime = CancellationToken::new();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &caller);
        let first = meter.prepare_cancelled(&lifetime)?;
        let first_allocations = meter.work.allocation_operations;
        meter.finish_cancelled(first)?;
        let backing = meter.live_allocation_bytes();
        assert!(backing > 0);
        let second = meter.prepare_cancelled(&lifetime)?;
        assert_eq!(meter.work.allocation_operations - first_allocations, 1);
        meter.finish_cancelled(second)?;
        assert_eq!(meter.live_allocation_bytes(), backing);
        lifetime.cancel();
        assert_eq!(meter.live_allocation_bytes(), 0);
        meter.adopt(7)?;
        assert_eq!(meter.live_allocation_bytes(), 7);
        Ok(())
    }

    #[test]
    fn rebinding_cancellation_preserves_original_registry_receipt() -> Result<(), Box<dyn Error>> {
        let original = CancellationToken::new();
        let child = CancellationToken::new();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &original);
        let prepared = meter.prepare_cancelled(&original)?;
        meter.rebind_cancellation(&child);
        let (work, ledger, roots) = meter.into_current();
        let mut meter = DemandWork::from_current(WorkBudget::UNBOUNDED, &child, work, ledger, roots);
        original.cancel();
        meter.finish_cancelled(prepared)?;
        assert_eq!(meter.live_allocation_bytes(), 0);
        meter.adopt(9)?;
        assert_eq!(meter.live_allocation_bytes(), 9);
        Ok(())
    }

    #[test]
    fn completed_owned_input_is_recorded_after_cancellation() -> Result<(), Box<dyn Error>> {
        let completed = vec![0_u8; 9];
        let owned_bytes = completed.capacity() as u64;
        let original = CancellationToken::new();
        let budget = WorkBudget {
            peak_allocation_bytes: owned_bytes, ..WorkBudget::default()
        };
        let mut meter = DemandWork::new(budget, &original);
        original.cancel();
        assert!(matches!(meter.adopt(owned_bytes), Err(DemandError::Cancelled)));
        assert_eq!(meter.work, WorkCounters::UNCHARGED);
        meter.record_adoption(owned_bytes)?;
        assert_eq!(meter.live_allocation_bytes(), owned_bytes);
        assert_eq!(meter.work.peak_allocation_bytes, owned_bytes);
        assert_eq!(meter.work.allocation_operations, 0);
        meter.work.verify(budget)?;
        meter.release(owned_bytes)?;
        drop(completed);
        assert_eq!(meter.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn observer_custody_tracks_actual_cow_data_across_original_restore() -> Result<(), Box<dyn Error>> {
        use crate::kernel::{CheckoutDependencies, Dependency, DependencyRegion, DependencyState, NameEncoding, ProofDataCustody};
        let directory = crate::FileId::from_bytes([74; 16]);
        let mut padded = Vec::with_capacity(64);
        padded.extend_from_slice(b"old");
        let mut proof = CheckoutDependencies::new([Dependency {
            region: DependencyRegion::DirectoryName {
                directory_id: directory,
                name: LogicalName::new(NameEncoding::Utf8, padded, 64)?,
            },
            expected: DependencyState::Absent,
        }], [], 8)?;
        let borrowed_original = proof.clone();
        let original = CancellationToken::new();
        let control_bytes = std::mem::size_of::<ProofDataCustody>() as u64;
        let budget = WorkBudget {
            peak_allocation_bytes: control_bytes + 6,
            ..WorkBudget::UNBOUNDED
        };
        let mut meter = DemandWork::new(budget, &original);
        meter.allocate(control_bytes)?;
        let custody = Arc::new(ProofDataCustody::default());
        meter.retain_observer_custody(custody.clone())?;
        let extension = proof.prepare_observations(vec![Dependency {
            region: DependencyRegion::DirectoryName {
                directory_id: directory,
                name: LogicalName::new(NameEncoding::Utf8, b"new".to_vec(), 64)?,
            },
            expected: DependencyState::Absent,
        }], 8)?;
        let child_budget = meter.remaining()?;
        let mut child_work = WorkCounters::UNCHARGED;
        let mut child_live = 0_u64;
        proof.commit_with_data_custody::<crate::WorkError>(
            extension, Some(&custody), |bytes, operations, copied, examined| {
                let next_live = child_live.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                child_work.charge(&WorkCounters {
                    allocation_operations: operations, bytes_copied: copied,
                    items_examined: examined, peak_allocation_bytes: next_live,
                    ..WorkCounters::default()
                }, &child_budget)?;
                child_live = next_live;
                Ok(())
            },
        )?;
        assert_eq!(custody.retained_bytes(), 6, "old excess capacity was borrowed; its actual COW copy has length three");
        meter.merge(child_work)?;
        assert_eq!(meter.work.peak_allocation_bytes, control_bytes + 6, "backend DATA must not enter both the baseline and its receipt peak");
        assert_eq!(meter.live_allocation_bytes(), control_bytes + 6);
        assert_eq!(meter.work.bytes_copied, 3);
        let (work, allocations, roots) = meter.into_current();
        let mut meter = DemandWork::from_current(budget, &original, work, allocations, roots);
        drop(borrowed_original);
        assert_eq!(custody.retained_bytes(), 6, "death of borrowed prior data cannot release this observer's copies");
        original.cancel();
        proof.clear();
        assert_eq!(custody.retained_bytes(), 0);
        assert_eq!(meter.live_allocation_bytes(), control_bytes);
        meter.record(WorkCounters::UNCHARGED)?;
        assert_eq!(meter.allocations.live_bytes(), control_bytes);
        let failure = meter.finish::<()>(Err(DemandError::Cancelled)).expect_err("original cancellation remains authoritative");
        assert_eq!(failure.work.peak_allocation_bytes, control_bytes + 6);
        failure.work.verify(budget)?;
        Ok(())
    }

    #[test]
    fn observer_custody_denies_actual_cow_before_name_copy() -> Result<(), Box<dyn Error>> {
        use crate::kernel::{CheckoutDependencies, Dependency, DependencyRegion, DependencyState, NameEncoding, ProofDataCustody};
        let directory = crate::FileId::from_bytes([75; 16]);
        let mut proof = CheckoutDependencies::new([Dependency {
            region: DependencyRegion::DirectoryName {
                directory_id: directory,
                name: LogicalName::new(NameEncoding::Utf8, b"old".to_vec(), 64)?,
            },
            expected: DependencyState::Absent,
        }], [], 8)?;
        let original_proof = proof.clone();
        let original = CancellationToken::new();
        let control_bytes = std::mem::size_of::<ProofDataCustody>() as u64;
        let budget = WorkBudget { peak_allocation_bytes: control_bytes, ..WorkBudget::UNBOUNDED };
        let mut meter = DemandWork::new(budget, &original);
        meter.allocate(control_bytes)?;
        let custody = Arc::new(ProofDataCustody::default());
        meter.retain_observer_custody(custody.clone())?;
        let before = proof.logical_data();
        let extension = proof.prepare_observations(vec![Dependency {
            region: DependencyRegion::FileRecord(crate::FileId::from_bytes([76; 16])),
            expected: DependencyState::Absent,
        }], 8)?;
        let child_budget = meter.remaining()?;
        let mut child_work = WorkCounters::UNCHARGED;
        let denied = proof.commit_with_data_custody::<crate::WorkError>(
            extension, Some(&custody), |bytes, operations, copied, examined| {
                child_work.charge(&WorkCounters {
                    allocation_operations: operations, bytes_copied: copied,
                    items_examined: examined, peak_allocation_bytes: bytes,
                    ..WorkCounters::default()
                }, &child_budget)
            },
        );
        assert!(denied.is_err());
        assert_eq!(proof.logical_data(), before);
        assert_eq!(proof, original_proof);
        assert_eq!(custody.retained_bytes(), 0);
        assert_eq!(child_work.bytes_copied, 0);
        assert_eq!(child_work.allocation_operations, 0);
        meter.merge(child_work)?;
        assert_eq!(meter.live_allocation_bytes(), control_bytes);
        meter.work.verify(budget)?;
        Ok(())
    }

    #[test]
    fn native_queue_take_preserves_the_entire_original_work() -> Result<(), Box<dyn Error>> {
        let original = CancellationToken::new();
        let parent = CancellationToken::new();
        let mut carrier = DemandWork::new(WorkBudget::UNBOUNDED, &original);
        carrier.parent_cancellation = Some(parent.clone());
        let stop_bytes = std::mem::size_of::<std::sync::atomic::AtomicBool>() as u64;
        carrier.allocate(stop_bytes)?;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        carrier.stop = Some(stop.clone());
        let prepared = carrier.prepare_cancelled(&original)?;
        let expected = carrier.work;
        let mut taken = carrier.take_for_native_queue();
        assert_eq!(carrier.work, WorkCounters::UNCHARGED);
        assert_eq!(carrier.budget, WorkCounters::UNCHARGED);
        assert_eq!(carrier.live_allocation_bytes(), 0);
        assert!(carrier.cancellation.same_state(&original));
        assert_eq!(taken.work, expected);
        assert_eq!(taken.budget, WorkBudget::UNBOUNDED);
        stop.store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(taken.check(), Err(DemandError::Cancelled)));
        stop.store(false, std::sync::atomic::Ordering::Release);
        parent.cancel();
        assert!(matches!(taken.check(), Err(DemandError::Cancelled)));
        original.cancel();
        taken.finish_cancelled(prepared)?;
        assert_eq!(taken.live_allocation_bytes(), stop_bytes);
        carrier = taken;
        assert!(carrier.cancellation.same_state(&original));
        assert!(matches!(carrier.check(), Err(DemandError::Cancelled)));
        assert_eq!(carrier.live_allocation_bytes(), stop_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn native_requests_bound_queue_allocations_and_cancel_before_io() -> Result<(), Box<dyn Error>>
    {
        let root = tempfile::tempdir()?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let permits = source.occupy_all_requests().await?;
        let cancellation = CancellationToken::new();
        let root_path = path("/")?;
        let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
        if let serde_json::Value::Object(fields) = &mut allowances {
            for allowance in fields.values_mut() { *allowance = serde_json::json!(1_000_000_u64); }
        }
        let budget: WorkBudget = serde_json::from_value(allowances)?;
        let mut no_allocations = budget;
        no_allocations.allocation_operations = 0;
        let failure = source.lookup(source.reference(), &root_path, no_allocations, &cancellation)
            .await.err().ok_or("queue admitted a waiter without its allocation allowance")?;
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(*failure.work, WorkCounters::default());
        let query = source.lookup(source.reference(), &root_path, budget, &cancellation);
        tokio::pin!(query);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut query)
                .await
                .is_err()
        );
        cancellation.cancel();
        let Err(failure) =
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut query).await?
        else {
            return Err("cancelled request entered the worker".into());
        };
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.source_entries_visited, 0);
        assert_eq!(failure.work.source_bytes_read, 0);
        assert!(failure.work.allocation_operations >= 2);
        assert!(failure.work.peak_allocation_bytes > 0);
        assert!(failure.work.items_examined > 0);
        failure.work.verify(budget)?;
        drop(permits);
        Ok(())
    }

    /// Opens `/file` of a fresh source rooted at `<temporary>/source`.
    async fn held_file(
        contents: &[u8],
    ) -> Result<(tempfile::TempDir, NativeDemandSource, Box<dyn DemandFile>), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("source");
        std::fs::create_dir(&root)?;
        std::fs::write(root.join("file"), contents)?;
        let source =
            NativeDemandSource::open(&root, FilesystemProfile::Portable, VolumeLimits::default())
                .await?;
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let node = source.lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .ok_or("file absent")?;
        let file = source.open_file(reference, &path("/file")?, node.version, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        Ok((temporary, source, file))
    }

    /// A held file gives way to a save over it: replacing it by rename, as
    /// editors and `git` save with `MoveFileEx`, succeeds while it is held,
    /// and the held file then reads as changed. (Windows refuses such a
    /// rename while any handle to the file is open, however shared.)
    #[cfg(windows)]
    #[tokio::test]
    #[allow(
        unsafe_code,
        reason = "renames over the held file with MoveFileExW, as editors save"
    )]
    async fn a_held_file_gives_way_to_a_save_over_it() -> Result<(), Box<dyn Error>> {
        use std::os::windows::ffi::OsStrExt as _;
        use windows::Win32::Storage::FileSystem::{MOVEFILE_REPLACE_EXISTING, MoveFileExW};
        use windows::core::PCWSTR;

        let (temporary, _source, file) = held_file(b"before").await?;
        let root = temporary.path().join("source");
        std::fs::write(root.join("saved"), b"after!")?;
        let wide = |path: &std::path::Path| {
            path.as_os_str()
                .encode_wide()
                .chain([0])
                .collect::<Vec<u16>>()
        };
        let (saved, replaced) = (wide(&root.join("saved")), wide(&root.join("file")));
        // SAFETY: both names are live, NUL-terminated wide strings.
        unsafe {
            MoveFileExW(
                PCWSTR(saved.as_ptr()),
                PCWSTR(replaced.as_ptr()),
                MOVEFILE_REPLACE_EXISTING,
            )
        }?;
        assert_eq!(std::fs::read(root.join("file"))?, b"after!");
        assert!(matches!(
            read_failure(file.as_ref()),
            Some(DemandError::StaleVersion)
        ));
        Ok(())
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn a_benign_oplock_break_preserves_the_held_read() -> Result<(), Box<dyn Error>> {
        let (temporary, _source, file) = held_file(b"before").await?;
        let path = temporary.path().join("source/file");
        // A second opener with write access breaks a handle oplock even if
        // it never changes the file. The held read must reopen at the same
        // version instead of reporting a change that did not occur.
        drop(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?,
        );
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let reads = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        file.read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())
                            .is_ok_and(|read| read.value == Bytes::from_static(b"before"))
                    })
                })
                .collect::<Vec<_>>();
            for read in reads {
                assert!(read.join().unwrap_or(false));
            }
        });
        assert_eq!(
            file.read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?.value,
            Bytes::from_static(b"before")
        );
        assert_eq!(
            file.read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?.value,
            Bytes::from_static(b"before")
        );
        Ok(())
    }

    fn read_failure(file: &dyn DemandFile) -> Option<DemandError> {
        file.read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .err()
            .map(|failure| failure.error)
    }

    /// A held file under a root that is not local is read on a thread of
    /// its own, bounded by a deadline, with the same result and the same
    /// proof that the bytes are still the opened version.
    #[tokio::test]
    async fn held_files_under_remote_roots_read_off_the_callback_thread()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"remote")?;
        let source = NativeDemandSource::open(
            temporary.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?
        .remote();
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let node = source.lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .ok_or("file absent")?;
        let file = source.open_file(reference, &path("/file")?, node.version, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let read = file.read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
        assert_eq!(read.value.as_ref(), b"remote");
        std::fs::write(temporary.path().join("file"), b"edited again")?;
        assert!(matches!(
            read_failure(file.as_ref()),
            Some(DemandError::StaleVersion)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn remote_deadline_quiesces_receipts_and_does_not_cancel_reopened_source() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"remote")?;
        let source = NativeDemandSource::open(
            temporary.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?.remote();
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            backend_read_operations: 1024, backend_write_operations: 1024,
            bytes_hashed: 1024 * 1024, bytes_copied: 1024 * 1024, bytes_encoded: 1024 * 1024,
            source_bytes_read: 1024 * 1024, source_path_components: 128,
            source_entries_visited: 1024, output_bytes: 1024 * 1024,
            items_examined: 4096, items_returned: 1024, allocation_operations: 4096,
            peak_allocation_bytes: 1024 * 1024, ..WorkCounters::default()
        };
        let requested = path("/file")?;
        let node = source.lookup(reference, &requested, budget, &cancellation).await?.value.ok_or("file absent")?;
        match source.read_with_deadline(reference, &requested, node.version, budget, &cancellation, std::time::Duration::ZERO) {
            Ok(receipt) => {
                assert_eq!(receipt.value.as_ref(), b"remote");
                assert_eq!(receipt.work.source_bytes_read, 6);
                receipt.work.verify(budget)?;
            }
            Err(failure) => {
                assert!(matches!(failure.error, DemandError::SourceUnavailable));
                assert!(failure.work.source_bytes_read == 0 || failure.work.source_bytes_read == 6);
                assert!(failure.work.backend_read_operations > 0);
                assert!(failure.work.allocation_operations > 0);
                failure.work.verify(budget)?;
            }
        }
        assert!(!cancellation.is_cancelled());
        let reopened = NativeDemandSource::open_with_reference(
            temporary.path(), FilesystemProfile::Portable, VolumeLimits::default(), reference,
        ).await?;
        assert_eq!(source.root_identity(), reopened.root_identity());
        let read = reopened.read_range(reference, &requested, node.version, 0, 6, budget, &cancellation).await?;
        assert_eq!(read.value.as_ref(), b"remote");
        read.work.verify(budget)?;
        cancellation.cancel();
        let failure = reopened.read_range(reference, &requested, node.version, 0, 6, budget, &cancellation)
            .await.err().ok_or("cancelled reopened source read succeeded")?;
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::default());
        Ok(())
    }

    #[tokio::test]
    async fn held_file_reads_detect_in_place_modification() -> Result<(), Box<dyn Error>> {
        use std::io::Write as _;
        let (temporary, _source, file) = held_file(b"before").await?;
        let read = file.read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
        assert_eq!(read.value.as_ref(), b"before");
        assert_eq!(read.work.source_bytes_read, 6);
        std::fs::OpenOptions::new()
            .append(true)
            .open(temporary.path().join("source/file"))?
            .write_all(b" and after")?;
        assert!(matches!(
            read_failure(file.as_ref()),
            Some(DemandError::StaleVersion)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn held_file_reads_detect_path_replacement() -> Result<(), Box<dyn Error>> {
        let (temporary, _source, file) = held_file(b"before").await?;
        let root = temporary.path().join("source");
        std::fs::rename(root.join("file"), root.join("moved"))?;
        std::fs::write(root.join("file"), b"before")?;
        assert!(matches!(
            read_failure(file.as_ref()),
            Some(DemandError::StaleVersion)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn held_file_reads_detect_invalidation() -> Result<(), Box<dyn Error>> {
        let (_temporary, source, file) = held_file(b"before").await?;
        source.invalidate();
        assert!(matches!(
            read_failure(file.as_ref()),
            Some(DemandError::StaleSource)
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn held_file_reads_detect_root_replacement() -> Result<(), Box<dyn Error>> {
        let (temporary, _source, file) = held_file(b"before").await?;
        let root = temporary.path().join("source");
        std::fs::rename(&root, temporary.path().join("moved"))?;
        std::fs::create_dir(&root)?;
        std::fs::write(root.join("file"), b"before")?;
        assert!(matches!(
            read_failure(file.as_ref()),
            Some(DemandError::SourceUnavailable)
        ));
        Ok(())
    }

    /// A root that can be traversed but not opened for reading still serves
    /// reads, so no read reopens it. (Privileged users bypass the mode.)
    #[cfg(unix)]
    #[tokio::test]
    async fn reads_never_reopen_the_root() -> Result<(), Box<dyn Error>> {
        use std::os::unix::fs::PermissionsExt as _;
        let (temporary, source, file) = held_file(b"before").await?;
        let root = temporary.path().join("source");
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let version = source.lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .ok_or("file absent")?
            .version;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o100))?;
        let held = file.read_range(0, 6, WorkBudget::UNBOUNDED, &cancellation);
        let by_path = source.read_range(reference, &path("/file")?, version, 0, 6, WorkBudget::UNBOUNDED, &cancellation)
            .await;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))?;
        assert_eq!(held?.value.as_ref(), b"before");
        assert_eq!(by_path?.value.as_ref(), b"before");
        Ok(())
    }

    #[tokio::test]
    async fn dropping_request_cancels_admitted_worker() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(async move {
            source
                .wait_for_cancelled_worker(started_tx, stopped_tx)
                .await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), started_rx).await??;
        request.abort();
        let _ = request.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), stopped_rx).await??;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_directory_page_retries_without_skipping_entries()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        for name in ["a", "b", "c", "d", "e", "f"] {
            std::fs::write(root.path().join(name), b"x")?;
        }
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let directory = path("/")?;
        let live = CancellationToken::new();
        let mut first = source.list_page(reference, &directory, None, 1, WorkBudget::UNBOUNDED, &live)
            .await?;
        first.value.publication.accept(WorkBudget::UNBOUNDED, &live).await?;
        let cursor = first.value.next.ok_or("expected continuation")?;
        let interrupted = CancellationToken::new();
        source.cancel_during_page(reference, &directory, cursor.clone(), &interrupted)?;
        let mut found = first.value.entries.iter().map(|entry| entry.name.clone()).collect::<Vec<_>>();
        let mut next = Some(cursor);
        while let Some(cursor) = next {
            let mut page = source.list_page(reference, &directory, Some(cursor), 2, WorkBudget::UNBOUNDED, &live)
                .await?;
            page.value.publication.accept(WorkBudget::UNBOUNDED, &live).await?;
            found.extend(page.value.entries.iter().map(|entry| entry.name.clone()));
            next = page.value.next;
        }
        let mut names: Vec<_> = found
            .into_iter()
            .map(|name| name.as_bytes().to_vec())
            .collect();
        names.sort();
        assert_eq!(names.len(), 6);
        assert_eq!(names.first().map(Vec::as_slice), Some(&b"a"[..]));
        assert_eq!(names.last().map(Vec::as_slice), Some(&b"f"[..]));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_use_reports_busy_without_losing_cursor() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        for name in ["a", "b", "c"] {
            std::fs::write(root.path().join(name), b"x")?;
        }
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let directory = path("/")?;
        let cancellation = CancellationToken::new();
        let mut first = source.list_page(reference, &directory, None, 1, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        first.value.publication.accept(WorkBudget::UNBOUNDED, &cancellation).await?;
        let cursor = first.value.next.ok_or("expected continuation")?;
        let mut lease_work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let lease = source.cursor_lease(
            reference,
            &directory,
            first.value.version,
            Some(cursor.clone()),
            1,
            &mut lease_work,
        )?;
        assert!(matches!(
            source.cursor_lease(
                reference,
                &directory,
                first.value.version,
                Some(cursor.clone()),
                1,
                &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation)
            ),
            Err(DemandError::CursorBusy)
        ));
        assert!(matches!(
            source.cursor_lease(
                reference,
                &directory,
                first.value.version,
                None,
                1,
                &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation)
            ),
            Err(DemandError::CursorBusy)
        ));
        drop(lease);
        let mut second = source.list_page(reference, &directory, Some(cursor), 1, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(second.value.entries.len(), 1);
        second.value.publication.accept(WorkBudget::UNBOUNDED, &cancellation).await?;
        Ok(())
    }

    #[tokio::test]
    async fn version_and_cursor_evidence_fail_closed() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("directory"))?;
        std::fs::write(root.path().join("file"), b"old")?;
        let source = NativeDemandSource::open(
            root.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?;
        let reference = source.reference();
        let cancellation = CancellationToken::new();
        let node = source.lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .ok_or("file absent")?;
        std::fs::write(root.path().join("file"), b"newer contents")?;
        let Err(stale) = source.read_range(reference, &path("/file")?, node.version, 0, 3, WorkBudget::UNBOUNDED, &cancellation)
            .await
        else {
            return Err("stale source content was accepted".into());
        };
        assert!(matches!(stale.error, DemandError::StaleVersion));
        assert_eq!(stale.work.source_path_components, 1);
        assert_eq!(stale.work.source_bytes_read, 0);
        assert!(matches!(
            source
                .read_metadata(reference, &path("/file")?, Some(node.version), WorkBudget::UNBOUNDED, &cancellation)
                .await,
            Err(failure) if matches!(failure.error, DemandError::StaleVersion)
        ));

        let mut first = source.list_page(reference, &path("/")?, None, 1, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        first.value.publication.accept(WorkBudget::UNBOUNDED, &cancellation).await?;
        let cursor = first.value.next.ok_or("second root entry missing")?;
        assert!(matches!(
            source
                .list_page(
                    reference,
                    &path("/directory")?,
                    Some(cursor),
                    1,
                    WorkBudget::UNBOUNDED,
                    &cancellation
                )
                .await,
            Err(failure) if matches!(failure.error, DemandError::StaleCursor)
        ));
        source.invalidate();
        assert!(matches!(
            source
                .lookup(reference, &path("/file")?, WorkBudget::UNBOUNDED, &cancellation)
                .await,
            Err(failure) if matches!(failure.error, DemandError::StaleSource)
        ));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn native_lookup_preserves_non_utf8_posix_names() -> Result<(), Box<dyn Error>> {
        use crate::kernel::{LogicalName, NameEncoding};
        use std::os::unix::ffi::OsStringExt;

        let raw = vec![b'n', 0xff];
        let name = LogicalName::new(
            NameEncoding::PosixBytes,
            raw.clone(),
            VolumeLimits::default().maximum_component_bytes,
        )?;
        assert_lossless_name(
            FilesystemProfile::Posix,
            std::ffi::OsString::from_vec(raw),
            name,
        )
        .await
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn native_lookup_preserves_utf16_windows_names() -> Result<(), Box<dyn Error>> {
        use crate::kernel::{LogicalName, NameEncoding};

        let raw = "snowman-☃"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let name = LogicalName::new(
            NameEncoding::WindowsUtf16Le,
            raw,
            VolumeLimits::default().maximum_component_bytes,
        )?;
        assert_lossless_name(
            FilesystemProfile::Windows,
            std::ffi::OsString::from("snowman-☃"),
            name,
        )
        .await
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn native_lookup_accepts_windows_names_longer_than_127_code_units()
    -> Result<(), Box<dyn Error>> {
        use crate::kernel::{LogicalName, NameEncoding};

        let host_name = "x".repeat(200);
        let limits = crate::model::VolumeConfig::native(crate::model::Lifecycle::Durable).limits;
        let raw = host_name
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let name = LogicalName::new(
            NameEncoding::WindowsUtf16Le,
            raw,
            limits.maximum_component_bytes,
        )?;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join(&host_name), b"data")?;
        let source =
            NativeDemandSource::open(root.path(), FilesystemProfile::Windows, limits).await?;
        let exact = NamespacePath::new(vec![name], limits)?;
        assert!(
            source
                .lookup(source.reference(), &exact, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await?
                .value
                .is_some()
        );
        Ok(())
    }

    #[cfg(any(target_os = "linux", windows))]
    async fn assert_lossless_name(
        profile: FilesystemProfile,
        host_name: std::ffi::OsString,
        name: crate::kernel::LogicalName,
    ) -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join(host_name), b"data")?;
        let source =
            NativeDemandSource::open(root.path(), profile, VolumeLimits::default()).await?;
        let exact = NamespacePath::new(vec![name], VolumeLimits::default())?;
        let node = source.lookup(source.reference(), &exact, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value
            .ok_or("lossless file absent")?;
        let answer = source.read_range(source.reference(), &exact, node.version, 0, 4, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?;
        assert_eq!(answer.value.as_ref(), b"data");
        Ok(())
    }
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
mod native_io_meter {
    use crate::WorkCounters;
/// One invocation ledger shared by the existing native I/O completion machinery.
/// Read semantics belong to `T`; ordinary demand work accepts actual EOF prefixes.
pub(crate) struct NativeDemandReadMeter<T = crate::demand::DemandWork>(pub(crate) std::sync::Mutex<T>);

/// Supplies the same owned or borrowed invocation work and final read semantics.
pub(crate) trait NativeDemandReadWork {
    fn as_work(&mut self) -> &mut crate::demand::DemandWork;
    fn finish_read(&mut self, _requested: usize, _completed: usize) -> Result<(), crate::DemandError> {
        self.as_work().check()
    }
}

impl NativeDemandReadWork for crate::demand::DemandWork {
    fn as_work(&mut self) -> &mut crate::demand::DemandWork { self }
}

impl NativeDemandReadWork for &mut crate::demand::DemandWork {
    fn as_work(&mut self) -> &mut crate::demand::DemandWork { self }
}

impl<T> NativeDemandReadMeter<T> {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, T> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<T: NativeDemandReadWork> NativeDemandReadMeter<T> {
    fn admit(&self, delta: WorkCounters) -> std::io::Result<()> {
        let mut held = self.lock();
        let work = held.as_work();
        work.check().map_err(native_demand_read_error)?;
        work.work.admit(&delta, &work.budget)
            .map_err(|error| native_demand_read_error(error.into()))
    }

    fn record(&self, delta: WorkCounters) -> std::io::Result<()> {
        self.lock().as_work().record(delta).map_err(native_demand_read_error)
    }

    fn charge(&self, delta: WorkCounters) -> std::io::Result<()> {
        self.lock().as_work().charge(delta).map_err(native_demand_read_error)
    }
}

pub(crate) fn native_demand_read_error(error: crate::DemandError) -> std::io::Error {
    if matches!(error, crate::DemandError::Cancelled) {
        std::io::Error::new(std::io::ErrorKind::Interrupted, error)
    } else {
        std::io::Error::other(error)
    }
}

impl<T: NativeDemandReadWork + Send> acyclic_native_runtime::NativeReadMeter for NativeDemandReadMeter<T> {
    fn check(&self) -> std::io::Result<()> {
        self.lock().as_work().check().map_err(native_demand_read_error)
    }

    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED })
    }

    fn finish_read(&self, requested: usize, completed: usize) -> std::io::Result<()> {
        self.lock().finish_read(requested, completed).map_err(native_demand_read_error)
    }

    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.lock().as_work().allocate(bytes).map_err(native_demand_read_error)
    }

    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.lock().as_work().adopt(bytes).map_err(native_demand_read_error)
    }

    fn release(&self, bytes: u64) {
        self.lock().as_work().release(bytes).expect("native allocation ownership balances");
    }

    fn admit_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.admit(WorkCounters { items_examined: items, bytes_copied, ..WorkCounters::UNCHARGED })
    }

    fn record_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.record(WorkCounters { items_examined: items, bytes_copied, ..WorkCounters::UNCHARGED })
    }

    fn examine(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.charge(WorkCounters { items_examined: items, bytes_copied, ..WorkCounters::UNCHARGED })
    }

    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.admit(WorkCounters { source_bytes_read: maximum as u64, ..WorkCounters::UNCHARGED })
    }

    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        self.admit_bytes(maximum)?;
        self.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
    }

    fn admit_operations(&self, additional: u64) -> std::io::Result<()> {
        self.admit(WorkCounters { backend_read_operations: additional, ..WorkCounters::UNCHARGED })
    }

    fn record_operations(&self, completed: u64) -> std::io::Result<()> {
        self.record(WorkCounters { backend_read_operations: completed, ..WorkCounters::UNCHARGED })
    }

    fn record_read(&self, bytes: usize) -> std::io::Result<()> {
        self.record(WorkCounters { source_bytes_read: bytes as u64, ..WorkCounters::UNCHARGED })
    }

    fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
        self.admit(WorkCounters { output_bytes: maximum as u64, ..WorkCounters::UNCHARGED })?;
        self.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })
    }

    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        self.record(WorkCounters { output_bytes: bytes as u64, ..WorkCounters::UNCHARGED })
    }

    fn admit_write_operations(&self, additional: u64) -> std::io::Result<()> {
        self.admit(WorkCounters { backend_write_operations: additional, ..WorkCounters::UNCHARGED })
    }

    fn record_write_operations(&self, completed: u64) -> std::io::Result<()> {
        self.record(WorkCounters { backend_write_operations: completed, ..WorkCounters::UNCHARGED })
    }

    fn admit_durability(&self) -> std::io::Result<()> {
        self.charge(WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED })
    }

    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        crate::native_host::NativeHostWork::reserve_cleanup_write(self.lock().as_work())
            .map_err(native_demand_read_error)
    }

    fn cancel_cleanup_write(&self, reservation: u64) {
        crate::native_host::NativeHostWork::cancel_cleanup_write(self.lock().as_work(), reservation);
    }

    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
        crate::native_host::NativeHostWork::attempt_cleanup_write(
            self.lock().as_work(), reservation, || (),
        ).1.map_err(native_demand_read_error)
    }
}

pub(crate) fn native_demand_error(error: std::io::Error) -> crate::DemandError {
    if error.get_ref().is_some_and(|error| error.is::<crate::DemandError>()) {
        *error.into_inner().expect("meter error contains its cause")
            .downcast::<crate::DemandError>().expect("meter cause retains its type")
    } else {
        crate::DemandError::Io(error)
    }
}
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) use native_io_meter::{
    NativeDemandReadMeter, NativeDemandReadWork, native_demand_error, native_demand_read_error,
};
