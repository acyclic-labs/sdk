//! Bounded, versioned access to unresolved filesystem sources.
//!
//! A source reference identifies a provider capability. It never contains a
//! host path or provider credential. Resolving one request does not publish a
//! workspace generation or make the result part of recorded history.

use crate::cancellation::CancellationToken;
use crate::kernel::{NamespacePath, NamespacePathError};
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
#[cfg(any(feature = "native-watch", target_arch = "wasm32"))]
use crate::WorkError;
use serde::{Deserialize, Serialize};
use thiserror::Error;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

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
    name: crate::kernel::LogicalName,
    /// The entry's node, exactly as a lookup of its path would report it.
    pub node: SourceNode,
    ownership: SourcePayloadOwnership,
}

impl SourceDirectoryEntry {
    /// Constructs an entry without taking any native allocation ownership.
    #[must_use]
    pub fn new(name: crate::kernel::LogicalName, node: SourceNode) -> Self {
        Self { name, node, ownership: SourcePayloadOwnership::default() }
    }

    /// Borrows the component while retaining its actual native payload owner.
    #[must_use]
    pub fn name(&self) -> &crate::kernel::LogicalName { &self.name }
}

/// Cursor bound to one directory and one source epoch.
pub struct SourceCursor {
    source: SourceReference,
    directory: NamespacePath,
    directory_version: SourceVersion,
    token: u64,
    #[cfg(not(target_arch = "wasm32"))]
    native_ownership: NativeCursorOwnership,
}

/// One physically owned native cursor payload. The containing object places
/// this control after its data, so the original weak admission retires only
/// after the actual buffers drop. Borrowed input has no new ownership claim.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct NativeCursorOwnership {
    custody: Option<NativeMountAllocationCustody>,
    borrowed: bool,
    allocation: Option<crate::NativeAllocation>,
    raw_payload: Option<SourcePayloadOwnership>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCursorOwnership {
    pub(crate) fn mark_input_borrowed(&mut self) {
        if self.custody.is_none() && self.allocation.is_none() && self.raw_payload.is_none() { self.borrowed = true; }
    }

    pub(crate) fn unclaimed_retained_bytes(&self, bytes: u64) -> u64 {
        if self.borrowed || self.custody.is_some() || self.allocation.is_some() || self.raw_payload.is_some() { 0 } else { bytes }
    }

    /// The exact moved buffers must already be admitted on this original work.
    pub(crate) fn attach_admitted(
        &mut self, bytes: u64, work: &mut DemandWork,
    ) -> Result<u64, DemandError> {
        let bytes = self.unclaimed_retained_bytes(bytes);
        if bytes == 0 { return Ok(0); }
        self.custody = Some(crate::demand::NativeMountAllocationCustody::from_admitted(bytes, work)?);
        Ok(bytes)
    }
}

impl Clone for SourceCursor {
    fn clone(&self) -> Self {
        Self {
            source: self.source, directory: self.directory.clone(),
            directory_version: self.directory_version, token: self.token,
            #[cfg(not(target_arch = "wasm32"))]
            native_ownership: NativeCursorOwnership::default(),
        }
    }
}

impl std::fmt::Debug for SourceCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SourceCursor").field("source", &self.source)
            .field("directory", &self.directory).field("directory_version", &self.directory_version)
            .field("token", &self.token).finish()
    }
}

impl PartialEq for SourceCursor {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source && self.directory == other.directory
            && self.directory_version == other.directory_version && self.token == other.token
    }
}

impl Eq for SourceCursor {}

impl SourceCursor {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn mark_input_borrowed(&mut self) {
        self.native_ownership.mark_input_borrowed();
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn unclaimed_retained_bytes(&self) -> u64 {
        self.native_ownership.unclaimed_retained_bytes(self.retained_bytes())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn attach_native_custody(
        &mut self, work: &mut DemandWork,
    ) -> Result<u64, DemandError> {
        self.native_ownership.attach_admitted(self.directory.retained_bytes(), work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn attach_native_allocation(
        &mut self, allocation: &mut crate::NativeAllocation,
    ) -> Result<(), crate::NativeTreeMaterializationError> {
        if self.native_ownership.custody.is_some() || self.native_ownership.allocation.is_some()
            || self.native_ownership.raw_payload.is_some() || self.native_ownership.borrowed {
            return Err(crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow));
        }
        let owned = allocation.split_off(self.retained_bytes())?;
        self.native_ownership.allocation = Some(owned);
        Ok(())
    }

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
        use crate::kernel::NamespacePathStep;
        self.encode_cursor_into_with_steps(encoder, cancellation, &mut |step| {
            match step {
                NamespacePathStep::Admit(delta) => work.admit(delta, &budget)?,
                NamespacePathStep::Charge(delta) => work.charge(delta, &budget)?,
                NamespacePathStep::Performed(delta) => {
                    work.try_add_assign(delta)?;
                    work.verify_ref(&budget)?;
                }
                NamespacePathStep::Allocation(_) => return Err(DemandError::InvalidRequest),
            }
            cancellation.check().map_err(|_| DemandError::Cancelled)
        })
    }

    pub(crate) fn encode_cursor_into_with_accounting(
        &self,
        encoder: &mut crate::kernel::Encoder,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), DemandError> {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        accounting.check().map_err(Self::cursor_accounting_error)?;
        self.encode_cursor_into_with_steps(encoder, cancellation, &mut |step| {
            use crate::kernel::NamespacePathStep;
            match step {
                NamespacePathStep::Admit(delta) => accounting.admit(*delta),
                NamespacePathStep::Charge(delta) => accounting.charge(*delta),
                NamespacePathStep::Performed(delta) => accounting.record_completed(*delta),
                NamespacePathStep::Allocation(_) => return Err(DemandError::InvalidRequest),
            }.map_err(Self::cursor_accounting_error)
        })
    }

    fn encode_cursor_into_with_steps(
        &self,
        encoder: &mut crate::kernel::Encoder,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(crate::kernel::NamespacePathStep<'_>) -> Result<(), DemandError>,
    ) -> Result<(), DemandError> {
        use crate::kernel::NamespacePathStep;
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        if encoder.remaining_capacity() < self.cursor_encoded_len()? {
            return Err(DemandError::InvalidRequest);
        }
        let identity = WorkCounters { bytes_encoded: 16, bytes_copied: 16, items_examined: 1,
            ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Admit(&identity))?;
        encoder.fixed(&self.source.identity);
        steps(NamespacePathStep::Performed(&identity))?;
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        let scalar = WorkCounters { bytes_encoded: 8, items_examined: 1,
            ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Admit(&scalar))?;
        encoder.u64(self.source.epoch);
        steps(NamespacePathStep::Performed(&scalar))?;
        self.directory.encode_cursor_into_with_steps(encoder, cancellation, steps)?;
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        let version = WorkCounters { bytes_encoded: 32, bytes_copied: 32, items_examined: 1,
            ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Admit(&version))?;
        encoder.fixed(&self.directory_version.0);
        steps(NamespacePathStep::Performed(&version))?;
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        steps(NamespacePathStep::Admit(&scalar))?;
        encoder.u64(self.token);
        steps(NamespacePathStep::Performed(&scalar))?;
        cancellation.check().map_err(|_| DemandError::Cancelled)
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
        let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
            ledger, work, budget, cancellation,
        );
        Self::decode_cursor_with_accounting(
            decoder, expected_source, expected_directory, profile, limits, &mut accounting, cancellation,
        ).map(|(cursor, _allocation)| cursor)
    }

    pub(crate) fn decode_cursor_with_accounting(
        decoder: &mut crate::kernel::Decoder<'_>,
        expected_source: SourceReference,
        expected_directory: &NamespacePath,
        profile: crate::FilesystemProfile,
        limits: crate::VolumeLimits,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), DemandError> {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        let mut owned = SourceCursorAccounting::new(accounting)?;
        let result = Self::decode_cursor_with_steps(
            decoder, expected_source, expected_directory, profile, limits, cancellation,
            &mut |step| owned.observe(step),
        );
        owned.finish(result)
    }

    fn decode_cursor_with_steps(
        decoder: &mut crate::kernel::Decoder<'_>,
        expected_source: SourceReference,
        expected_directory: &NamespacePath,
        profile: crate::FilesystemProfile,
        limits: crate::VolumeLimits,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(crate::kernel::NamespacePathStep<'_>) -> Result<(), DemandError>,
    ) -> Result<Self, DemandError> {
        use crate::kernel::NamespacePathStep;
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        let identity_bytes = decoder.take_exact(16).map_err(|_| DemandError::InvalidRequest)?;
        let identity_work = WorkCounters { bytes_copied: 16, items_examined: 1,
            ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Admit(&identity_work))?;
        let identity = <[u8; 16]>::try_from(identity_bytes).map_err(|_| DemandError::InvalidRequest)?;
        steps(NamespacePathStep::Performed(&identity_work))?;
        let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Charge(&item))?;
        let epoch = decoder.u64().map_err(|_| DemandError::InvalidRequest)?;
        let source = SourceReference { identity, epoch };
        if source != expected_source {
            return Err(DemandError::StaleCursor);
        }
        let directory = NamespacePath::decode_cursor_with_steps(
            decoder, profile, limits, cancellation, steps,
        )?;
        steps(NamespacePathStep::Charge(&item))?;
        if directory.depth() != expected_directory.depth() {
            return Err(DemandError::StaleCursor);
        }
        for (actual, expected) in directory.components().iter().zip(expected_directory.components()) {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            steps(NamespacePathStep::Charge(&item))?;
            if actual.encoding() != expected.encoding() || actual.as_bytes().len() != expected.as_bytes().len() {
                return Err(DemandError::StaleCursor);
            }
            for (actual, expected) in actual.as_bytes().iter().zip(expected.as_bytes()) {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                steps(NamespacePathStep::Charge(&item))?;
                if actual != expected {
                    return Err(DemandError::StaleCursor);
                }
            }
        }
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        let version_bytes = decoder.take_exact(32).map_err(|_| DemandError::InvalidRequest)?;
        let version_work = WorkCounters { bytes_copied: 32, items_examined: 1,
            ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Admit(&version_work))?;
        let directory_version = SourceVersion(<[u8; 32]>::try_from(version_bytes)
            .map_err(|_| DemandError::InvalidRequest)?);
        steps(NamespacePathStep::Performed(&version_work))?;
        steps(NamespacePathStep::Charge(&item))?;
        let token = decoder.u64().map_err(|_| DemandError::InvalidRequest)?;
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        steps(NamespacePathStep::Performed(&WorkCounters::UNCHARGED))?;
        Ok(Self {
            source, directory, directory_version, token,
            #[cfg(not(target_arch = "wasm32"))]
            native_ownership: NativeCursorOwnership::default(),
        })
    }

    fn cursor_path_error(error: NamespacePathError) -> DemandError {
        match error {
            NamespacePathError::Work(error) => DemandError::Work(error),
            NamespacePathError::Cancelled => DemandError::Cancelled,
            _ => DemandError::InvalidRequest,
        }
    }

    fn cursor_accounting_error(error: crate::FsError) -> DemandError {
        match error {
            crate::FsError::Work(error) => DemandError::Work(error),
            crate::FsError::Cancelled(_) => DemandError::Cancelled,
            crate::FsError::EncodedAllocationFailed(error) => DemandError::Allocation(error),
            #[cfg(not(target_arch = "wasm32"))]
            crate::FsError::NativeKernel(error) => match error {
                crate::NativeTreeMaterializationError::Work(error) => DemandError::Work(error),
                crate::NativeTreeMaterializationError::Cancelled => DemandError::Cancelled,
                crate::NativeTreeMaterializationError::OriginalWorkPoisoned => DemandError::OriginalWorkPoisoned,
                crate::NativeTreeMaterializationError::AllocationFailed(error) => DemandError::Allocation(error),
                _ => DemandError::InvalidRequest,
            },
            _ => DemandError::InvalidRequest,
        }
    }
}

/// Exact claims of this codec only, including an unfinished physical buffer.
struct SourceCursorAccounting<'a, 'b> {
    accounting: &'a mut crate::kernel::allocation::KernelAccounting<'b>,
    allocation: crate::kernel::allocation::KernelOwnedAllocation,
    owned_bytes: u64,
}

impl<'a, 'b> SourceCursorAccounting<'a, 'b> {
    fn new(accounting: &'a mut crate::kernel::allocation::KernelAccounting<'b>) -> Result<Self, DemandError> {
        let allocation = accounting.claim_owned(0, 0).map_err(SourceCursor::cursor_accounting_error)?;
        Ok(Self { accounting, allocation, owned_bytes: 0 })
    }

    fn observe(&mut self, step: crate::kernel::NamespacePathStep<'_>) -> Result<(), DemandError> {
        use crate::kernel::{NamespacePathStep, ReadAllocationStep};
        let result = match step {
            NamespacePathStep::Admit(delta) => self.accounting.admit(*delta),
            NamespacePathStep::Charge(delta) => self.accounting.charge(*delta),
            NamespacePathStep::Performed(delta) => self.accounting.record_completed(*delta),
            NamespacePathStep::Allocation(step) => match step {
                ReadAllocationStep::Allocate { bytes, operations } => {
                    let next = self.owned_bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                    let mut incoming = self.accounting.claim_owned(bytes, operations)
                        .map_err(SourceCursor::cursor_accounting_error)?;
                    if let Err(error) = self.allocation.absorb(&mut incoming) {
                        self.accounting.release_owned(&mut incoming, bytes)
                            .map_err(SourceCursor::cursor_accounting_error)?;
                        return Err(SourceCursor::cursor_accounting_error(error));
                    }
                    self.owned_bytes = next;
                    Ok(())
                }
                ReadAllocationStep::Capacity { admitted_bytes, actual_bytes } => {
                    self.owned_bytes = self.owned_bytes.checked_sub(admitted_bytes)
                        .and_then(|bytes| bytes.checked_add(actual_bytes)).ok_or(crate::WorkError::Overflow)?;
                    self.accounting.reconcile_owned(&mut self.allocation, admitted_bytes, actual_bytes)
                }
                ReadAllocationStep::ReserveFailed(error) => return Err(DemandError::Allocation(error)),
                ReadAllocationStep::BeforeCopy(bytes) => self.accounting.admit(
                    WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED },
                ),
                ReadAllocationStep::Copied(bytes) => self.accounting.record_completed(
                    WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED },
                ),
                ReadAllocationStep::BeforeHash(bytes) => self.accounting.check().and_then(|()| self.accounting.admit(
                    WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED },
                )),
                ReadAllocationStep::Hashed(bytes) => self.accounting.record_completed(
                    WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED },
                ).and_then(|()| self.accounting.check()),
                ReadAllocationStep::BeforeItem => self.accounting.admit(
                    WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
                ),
                ReadAllocationStep::Item => self.accounting.record_completed(
                    WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
                ),
            },
        };
        result.map_err(SourceCursor::cursor_accounting_error)
    }

    fn finish<T>(mut self, result: Result<T, DemandError>)
    -> Result<(T, crate::kernel::allocation::KernelOwnedAllocation), DemandError> {
        match result {
            Ok(value) => Ok((value, self.allocation)),
            Err(error) => {
                // Partial data has already dropped before its exact claim retires.
                self.accounting.release_owned(&mut self.allocation, self.owned_bytes)
                    .map_err(SourceCursor::cursor_accounting_error)?;
                Err(error)
            }
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
            #[cfg(not(target_arch = "wasm32"))]
            native_ownership: NativeCursorOwnership::default(),
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

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_codec_preserves_wire_and_transfers_exact_directory_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
        let cancellation = CancellationToken::new();
        for token in [0, u64::MAX] {
            let source = cursor(token)?;
            let expected = encoded(&source)?;
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let mut input_guard = original.claim_allocation(source.retained_bytes(), 0)?;
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let capacity = DOMAIN.len() + 2 + source.cursor_encoded_len()?;
            let mut encoded_guard = accounting.claim_owned(capacity as u64, 1)?;
            let header = WorkCounters { bytes_encoded: (DOMAIN.len() + 2) as u64,
                bytes_copied: (DOMAIN.len() + 2) as u64, ..WorkCounters::UNCHARGED };
            accounting.admit(header)?;
            let mut encoder = Encoder::with_exact_capacity(DOMAIN, 1, capacity)?;
            accounting.reconcile_owned(&mut encoded_guard, capacity as u64, encoder.capacity() as u64)?;
            accounting.record_completed(header)?;
            let before = original.receipt();
            source.encode_cursor_into_with_accounting(&mut encoder, &mut accounting, &cancellation)?;
            assert_eq!(original.receipt().bytes_encoded - before.bytes_encoded, source.cursor_encoded_len()? as u64);
            assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
            let bytes = encoder.finish();
            assert_eq!(bytes, expected);
            let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
            let before = original.receipt();
            let (mut decoded, decoded_guard) = SourceCursor::decode_cursor_with_accounting(
                &mut decoder, source.source, &source.directory, crate::FilesystemProfile::Windows,
                crate::VolumeLimits::default(), &mut accounting, &cancellation,
            )?;
            decoder.finish()?;
            assert_eq!(decoded, source);
            assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 50);
            assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 2);
            let (KernelOwnedAllocation::NativeOriginal(mut decoded_guard),
                KernelOwnedAllocation::NativeOriginal(encoded_guard)) = (decoded_guard, encoded_guard)
            else { return Err("source codec lost original custody".into()); };
            assert_eq!(decoded_guard.bytes(), decoded.retained_bytes());
            assert_eq!(encoded_guard.bytes(), bytes.capacity() as u64);
            assert!(decoded_guard.shares_ledger(&encoded_guard));
            decoded.attach_native_allocation(&mut decoded_guard)?;
            assert_eq!(decoded_guard.bytes(), 0);
            assert_eq!(original.live_allocation_bytes(),
                baseline + input_guard.bytes() + encoded_guard.bytes() + decoded.retained_bytes());
            drop(decoded);
            drop(decoded_guard);
            assert_eq!(original.live_allocation_bytes(), baseline + input_guard.bytes() + encoded_guard.bytes());
            drop(bytes);
            drop(encoded_guard);
            drop(source);
            input_guard.release(input_guard.bytes())?;
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_decode_denials_retire_only_this_codec_actual_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::allocation::KernelAccounting;
        let cancellation = CancellationToken::new();
        let source = cursor(u64::MAX)?;
        let bytes = encoded(&source)?;
        let controller = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?.receipt();
        for (copied_limit, copied, allocations) in [(0, 0, 0), (17, 16, 1), (18, 18, 2)] {
            let original = crate::NativeMaterializationWork::new(WorkBudget {
                bytes_copied: controller.bytes_copied + copied_limit, ..WorkBudget::UNBOUNDED
            }, &cancellation).map_err(|(error, _)| error)?;
            let caller_guard = original.claim_allocation(source.retained_bytes() + bytes.capacity() as u64, 0)?;
            let baseline = original.live_allocation_bytes();
            let before = original.receipt();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
            let error = SourceCursor::decode_cursor_with_accounting(
                &mut decoder, source.source, &source.directory, crate::FilesystemProfile::Windows,
                crate::VolumeLimits::default(), &mut accounting, &cancellation,
            ).err().ok_or("finite original copy budget unexpectedly completed")?;
            assert!(matches!(error, DemandError::Work(_)));
            assert_eq!(original.receipt().bytes_copied - before.bytes_copied, copied);
            assert_eq!(original.receipt().allocation_operations - before.allocation_operations, allocations);
            assert_eq!(original.live_allocation_bytes(), baseline);
            assert_eq!(caller_guard.bytes(), source.retained_bytes() + bytes.capacity() as u64);
        }
        for allowance in [0, 1] {
            let original = crate::NativeMaterializationWork::new(WorkBudget {
                allocation_operations: controller.allocation_operations + allowance, ..WorkBudget::UNBOUNDED
            }, &cancellation).map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let before = original.receipt();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
            let error = SourceCursor::decode_cursor_with_accounting(
                &mut decoder, source.source, &source.directory, crate::FilesystemProfile::Windows,
                crate::VolumeLimits::default(), &mut accounting, &cancellation,
            ).err().ok_or("finite original allocation budget unexpectedly completed")?;
            assert!(matches!(error, DemandError::Work(_)));
            assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 16);
            assert_eq!(original.receipt().allocation_operations - before.allocation_operations, allowance);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_encoder_finite_bound_preserves_only_completed_output()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::allocation::KernelAccounting;
        let cancellation = CancellationToken::new();
        let source = cursor(u64::MAX)?;
        let controller = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?.receipt();
        let original = crate::NativeMaterializationWork::new(WorkBudget {
            bytes_encoded: controller.bytes_encoded + (DOMAIN.len() + 2) as u64 + 16,
            ..WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let capacity = DOMAIN.len() + 2 + source.cursor_encoded_len()?;
        let mut guard = accounting.claim_owned(capacity as u64, 1)?;
        let header = WorkCounters { bytes_encoded: (DOMAIN.len() + 2) as u64,
            bytes_copied: (DOMAIN.len() + 2) as u64, ..WorkCounters::UNCHARGED };
        accounting.admit(header)?;
        let mut encoder = Encoder::with_exact_capacity(DOMAIN, 1, capacity)?;
        let retained = encoder.capacity() as u64;
        accounting.reconcile_owned(&mut guard, capacity as u64, retained)?;
        accounting.record_completed(header)?;
        let before = original.receipt();
        let error = source.encode_cursor_into_with_accounting(&mut encoder, &mut accounting, &cancellation)
            .err().ok_or("finite original encoder unexpectedly completed")?;
        assert!(matches!(error, DemandError::Work(_)));
        assert_eq!(original.receipt().bytes_encoded - before.bytes_encoded, 16);
        assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 16);
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        let bytes = encoder.finish();
        assert_eq!(&bytes[DOMAIN.len() + 2..], &source.source.identity);
        drop(bytes);
        accounting.release_owned(&mut guard, retained)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_codec_rejects_foreign_malformed_and_unfunded_output()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::allocation::KernelAccounting;
        let cancellation = CancellationToken::new();
        let source = cursor(0)?;
        let bytes = encoded(&source)?;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        for (foreign, truncated) in [(true, false), (false, true)] {
            let input = if truncated { &bytes[..bytes.len() - 9] } else { &bytes[..] };
            let mut decoder = Decoder::new(input, DOMAIN, 1, u64::MAX)?;
            let expected_source = if foreign {
                SourceReference { epoch: source.source.epoch + 1, ..source.source }
            } else { source.source };
            let before = original.receipt();
            let error = SourceCursor::decode_cursor_with_accounting(
                &mut decoder, expected_source, &source.directory, crate::FilesystemProfile::Windows,
                crate::VolumeLimits::default(), &mut accounting, &cancellation,
            ).err().ok_or("invalid original cursor unexpectedly decoded")?;
            assert!(matches!((&error, foreign),
                (DemandError::StaleCursor, true) | (DemandError::InvalidRequest, false)));
            assert_eq!(original.receipt().bytes_copied - before.bytes_copied, if foreign { 16 } else { 18 });
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        let foreign_directory = NamespacePath::new(Vec::new(), crate::VolumeLimits::default())?;
        let before = original.receipt();
        let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
        let error = SourceCursor::decode_cursor_with_accounting(
            &mut decoder, source.source, &foreign_directory, crate::FilesystemProfile::Windows,
            crate::VolumeLimits::default(), &mut accounting, &cancellation,
        ).err().ok_or("foreign original directory binding unexpectedly decoded")?;
        assert!(matches!(error, DemandError::StaleCursor));
        assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 18);
        assert_eq!(original.live_allocation_bytes(), baseline);
        let mut encoder = Encoder::with_exact_capacity(DOMAIN, 1, DOMAIN.len() + 2)?;
        let before = original.receipt();
        let error = source.encode_cursor_into_with_accounting(&mut encoder, &mut accounting, &cancellation)
            .err().ok_or("unfunded encoder unexpectedly grew")?;
        assert!(matches!(error, DemandError::InvalidRequest));
        assert_eq!(original.receipt(), before);
        assert_eq!(encoder.finish().len(), DOMAIN.len() + 2);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_codec_cancellation_and_typed_reserve_failure_preserve_real_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{NamespacePathStep, ReadAllocationStep};
        use crate::kernel::allocation::KernelAccounting;
        let source = cursor(0)?;
        let bytes = encoded(&source)?;
        for cancel_original in [true, false] {
            let root_cancellation = CancellationToken::new();
            let cancellation = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &root_cancellation)
                .map_err(|(error, _)| error)?;
            if cancel_original { root_cancellation.cancel(); } else { cancellation.cancel(); }
            let before = original.receipt();
            let baseline = original.live_allocation_bytes();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
            let error = SourceCursor::decode_cursor_with_accounting(
                &mut decoder, source.source, &source.directory, crate::FilesystemProfile::Windows,
                crate::VolumeLimits::default(), &mut accounting, &cancellation,
            ).err().ok_or("cancelled original codec unexpectedly decoded")?;
            assert!(matches!(error, DemandError::Cancelled));
            assert_eq!(original.receipt(), before);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let caller_bytes = vec![0_u8; 7];
        let caller_guard = original.claim_allocation(caller_bytes.capacity() as u64, 0)?;
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut owned = SourceCursorAccounting::new(&mut accounting)?;
        let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX)?;
        let result = SourceCursor::decode_cursor_with_steps(
            &mut decoder, source.source, &source.directory, crate::FilesystemProfile::Windows,
            crate::VolumeLimits::default(), &cancellation, &mut |step| {
                let completed_path_copy = matches!(&step,
                    NamespacePathStep::Performed(delta) if delta.bytes_copied == 2);
                owned.observe(step)?;
                if completed_path_copy { cancellation.cancel(); }
                Ok(())
            },
        );
        let error = owned.finish(result).err().ok_or("late cancellation unexpectedly decoded")?;
        assert!(matches!(error, DemandError::Cancelled));
        assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 18);
        assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 2);
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(caller_guard.bytes(), 7);
        drop(caller_bytes);
        drop(caller_guard);

        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut owned = SourceCursorAccounting::new(&mut accounting)?;
        owned.observe(NamespacePathStep::Allocation(ReadAllocationStep::Allocate { bytes: 4, operations: 1 }))?;
        let reserve_error = Vec::<u8>::new().try_reserve_exact(usize::MAX)
            .err().ok_or("impossible physical reserve succeeded")?;
        let result = owned.observe(NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(reserve_error)));
        let error = owned.finish(result).err().ok_or("reserve failure was erased")?;
        assert!(matches!(error, DemandError::Allocation(_)));
        assert_eq!(original.live_allocation_bytes(), baseline);
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
    selector_ownership: SourcePayloadOwnership,
}

#[derive(Debug)]
enum SourceEntryStorage {
    Owned(Vec<SourceDirectoryEntry>),
    Shared(std::sync::Arc<SourceDirectoryBacking>),
}

/// The page's physical owners drop after the entries and their name buffers.
#[derive(Debug)]
struct SourceDirectoryBacking {
    entries: Vec<SourceDirectoryEntry>,
    ownership: SourcePayloadOwnership,
}

impl std::ops::Deref for SourceDirectoryBacking {
    type Target = Vec<SourceDirectoryEntry>;
    fn deref(&self) -> &Self::Target { &self.entries }
}

#[derive(Default)]
pub(crate) struct SourcePayloadOwnership {
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<NativeMountAllocationCustody>,
    #[cfg(not(target_arch = "wasm32"))]
    owned_bytes: std::sync::atomic::AtomicU64,
    // Last: physical SDK data and its custody retire before the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    original: Option<OriginalDemandWork>,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::NativeAllocation>,
}

impl Clone for SourcePayloadOwnership {
    fn clone(&self) -> Self { Self::default() }
}

impl PartialEq for SourcePayloadOwnership {
    fn eq(&self, _other: &Self) -> bool { true }
}

impl Eq for SourcePayloadOwnership {}

impl std::fmt::Debug for SourcePayloadOwnership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SourcePayloadOwnership")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SourcePayloadOwnership {
    /// Moves the already-paid packet guard without another SDK control or claim.
    pub(crate) fn from_native_allocation(allocation: crate::NativeAllocation) -> Self {
        Self { custody: None, owned_bytes: Default::default(), original: None, allocation: Some(allocation) }
    }

    pub(crate) fn into_native_allocation(mut self) -> Option<crate::NativeAllocation> {
        self.allocation.take()
    }

    pub(crate) fn new<W: SourceDemandReadWork>(work: &mut W) -> Result<Self, DemandError> {
        work.check()?;
        if work.is_native_original() {
            let source = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?;
            let original = source.original_demand_work().allocation_work();
            let custody = NativeMountAllocationCustody::new_on_original(&original)?;
            return Ok(Self { custody: Some(custody), owned_bytes: Default::default(), original: Some(original), allocation: None });
        }
        Ok(Self::default())
    }

    pub(crate) fn retain<W: SourceDemandReadWork>(&self, bytes: u64, work: &mut W) -> Result<(), DemandError> {
        if self.allocation.is_some() { return Err(DemandError::InvalidRequest); }
        if let Some(custody) = &self.custody {
            self.owned_bytes.fetch_update(
                std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                |retained| retained.checked_add(bytes),
            ).map_err(|_| crate::WorkError::Overflow)?;
            if let Err(error) = work.with_original_leaf(|work| custody.retain_completed_admitted(bytes, work)) {
                self.owned_bytes.fetch_sub(bytes, std::sync::atomic::Ordering::AcqRel);
                return Err(error.into());
            }
            return Ok(());
        }
        Ok(())
    }

    pub(crate) fn retire(&self, bytes: u64) -> Result<(), DemandError> {
        if self.allocation.is_some() { return Err(DemandError::InvalidRequest); }
        if let Some(custody) = &self.custody {
            self.owned_bytes.fetch_update(
                std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                |retained| retained.checked_sub(bytes),
            ).map_err(|_| crate::WorkError::Overflow)?;
            custody.retire(bytes)?;
        }
        Ok(())
    }

    pub(crate) fn has_native_custody(&self) -> bool {
        self.custody.is_some() || self.allocation.is_some()
    }

    /// Partitions already-owned physical backing, without allocating or charging
    /// a new claim. Each returned guard follows its data's physical drop.
    /// An explicit ordinary/unowned packet remains unowned; this does not
    /// manufacture a native claim or authorize a native provider fallback.
    pub(crate) fn split_owned_payload(
        &mut self, bytes: u64,
    ) -> Result<Self, crate::NativeTreeMaterializationError> {
        if let Some(allocation) = &mut self.allocation {
            return allocation.split_off(bytes).map(Self::from_native_allocation);
        }
        if let Some(custody) = &self.custody {
            self.owned_bytes.fetch_update(
                std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                |retained| retained.checked_sub(bytes),
            ).map_err(|_| crate::WorkError::Overflow)?;
            return Ok(Self {
                custody: Some(custody.clone()),
                owned_bytes: std::sync::atomic::AtomicU64::new(bytes),
                original: self.original.clone(), allocation: None,
            });
        }
        if self.original.is_some() || self.owned_bytes.load(std::sync::atomic::Ordering::Acquire) != 0 {
            return Err(crate::WorkError::Overflow.into());
        }
        Ok(Self::default())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SourcePayloadOwnership {
    fn drop(&mut self) {
        let bytes = *self.owned_bytes.get_mut();
        if bytes != 0 && let Some(custody) = &self.custody
            && let Err(error) = custody.retire(bytes)
            && let Some(original) = &self.original {
            let _recorded = original.with_original_leaf(|work|
                work.record_native_io_failure(DemandError::Work(error)));
        }
    }
}

impl SourceDirectoryEntries {
    fn buffer_bytes(entries: &Vec<SourceDirectoryEntry>) -> u64 {
        (entries.capacity() * std::mem::size_of::<SourceDirectoryEntry>()) as u64
            + entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>()
    }

    fn owned(entries: Vec<SourceDirectoryEntry>) -> Self {
        let end = entries.len();
        let backing_bytes = Self::buffer_bytes(&entries);
        Self { storage: SourceEntryStorage::Owned(entries), range: 0..end, indices: None, backing_bytes, selector_ownership: SourcePayloadOwnership::default() }
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    fn shared(
        entries: std::sync::Arc<SourceDirectoryBacking>, range: std::ops::Range<usize>, backing_bytes: u64,
    ) -> Self {
        Self { storage: SourceEntryStorage::Shared(entries), range, indices: None, backing_bytes, selector_ownership: SourcePayloadOwnership::default() }
    }

    fn set_selection(&mut self, indices: Vec<usize>, ownership: SourcePayloadOwnership) {
        // Retire the actual previous vector before its corresponding token.
        self.indices = Some(indices);
        self.selector_ownership = ownership;
    }

    fn backing(&self) -> &[SourceDirectoryEntry] {
        match &self.storage {
            SourceEntryStorage::Owned(entries) => entries,
            SourceEntryStorage::Shared(entries) => &entries.entries,
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

    /// Uses the original native publication reservation, without a child ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn accept_with_native_work(&mut self, _work: NativeSourceWork<'_>) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }

    /// Restores native progress against the original reserved cleanup credit.
    #[cfg(not(target_arch = "wasm32"))]
    fn abort_with_native_work(&mut self, _work: NativeSourceWork<'_>) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }
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

    /// Publishes using cleanup already reserved by the original native producer.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn accept_with_native_work(&mut self, work: NativeSourceWork<'_>) -> Result<(), DemandError> {
        let answer = if let Some(guard) = self.guard.as_mut() {
            guard.accept_with_native_work(work)
        } else {
            crate::native_host::NativeHostWork::check(&work)
        };
        if answer.is_ok() { self.guard.take(); }
        answer
    }

    /// Aborts using the producer's original native cleanup reservation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn abort_with_native_work(&mut self, work: NativeSourceWork<'_>) -> Result<(), DemandError> {
        let answer = self.guard.as_mut().map_or(Ok(()), |guard| guard.abort_with_native_work(work));
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
    /// This provider cannot operate against the original native ledger.
    #[error("source does not support original native work custody")]
    NativeWorkUnavailable,
    /// An original leaf panicked; no new work may use its poisoned state.
    #[error("original native work state is poisoned")]
    OriginalWorkPoisoned,
    /// Explicit native preparation allowance was exhausted.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// A real owned-buffer reservation failed without discarding its cause.
    #[error("source buffer allocation failed: {0}")]
    Allocation(#[source] std::collections::TryReserveError),
    /// A started native watcher remains owned until measured cleanup succeeds.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[error("{0}")]
    SourceWatchStartup(#[source] std::sync::Arc<crate::source_watch::SourceWatchStartupFailure>),
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
    #[cfg(all(target_os = "linux", any(feature = "native-mount", test)))]
    #[error(transparent)]
    LinuxMetadata(#[from] crate::native_host::LinuxMetadataError),
    /// A held macOS metadata mutation failed without losing its typed cause.
    #[cfg(all(target_os = "macos", any(feature = "native-mount", test)))]
    #[error(transparent)]
    MacMetadata(#[from] crate::native_host::MacMetadataError),
    /// A held Windows metadata mutation failed without losing its typed cause.
    #[cfg(all(windows, any(feature = "native-mount", test)))]
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

    /// Projects this batch against its original native grant and cancellation.
    /// Unknown sinks must refuse before executing an unbounded callback.
    #[cfg(not(target_arch = "wasm32"))]
    fn source_changed_with_native_work(
        &self, _changes: &[SourceChange], work: NativeSourceWork<'_>,
    ) -> Result<(), DemandError> {
        crate::native_host::NativeHostWork::check(&work)?;
        Err(DemandError::NativeWorkUnavailable)
    }
}

/// Read-only physical custody of one newly allocated native watch-registration
/// backing buffer. It is accounting evidence, never source authority.
pub struct SourceWatchAllocationCustody {
    retained: std::sync::atomic::AtomicU64,
    #[cfg(not(target_arch = "wasm32"))]
    ownership: SourcePayloadOwnership,
}

impl SourceWatchAllocationCustody {
    /// Actual logical buffer bytes still retained by this original backing.
    #[must_use]
    pub fn retained_bytes(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    fn new(ownership: SourcePayloadOwnership) -> Self {
        Self { retained: std::sync::atomic::AtomicU64::new(0), ownership }
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    fn publish(&self, bytes: u64) {
        self.retained.store(bytes, std::sync::atomic::Ordering::Release);
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    fn retired(&self) {
        self.retained.store(0, std::sync::atomic::Ordering::Release);
    }
}

/// Live observation of every change made to one source outside every view
/// of it. Dropping it requests original birth-paid notification retirement;
/// unresolved native cleanup is not an attestation of physical quiescence.
pub trait SourceWatch: Send + Sync {
    /// Returns once every change that completed before the call has reached
    /// the sink.
    ///
    /// Ordinary convenience delegates to the same measured provider engine.
    fn fence(&self) -> Result<(), DemandError> {
        self.fence_measured(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Delivers the original watch's preceding changes within this allowance,
    /// retaining actual work on budget, cancellation and host failures.
    /// Native completion waits assume eventual OS quiescence; this is not a
    /// hard wall-clock guarantee for an unresponsive filesystem.
    fn fence_measured(
        &self, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> DemandResult<()>;

    /// Fences this same subscription against the caller's original native
    /// invocation. No numeric child allowance may replace retained custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn fence_with_native_work(&self, work: NativeSourceWork<'_>) -> Result<(), DemandError> {
        crate::native_host::NativeHostWork::check(&work)?;
        Err(DemandError::NativeWorkUnavailable)
    }

    /// Quiesces this original subscription using the same native engine.
    /// On failure the caller must retain the owner and retry cleanup rather
    /// than dropping it as though an unmeasured destructor were free.
    fn stop_measured(
        &self, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> DemandResult<()>;

    /// Quiesces this subscription against the original native invocation.
    /// Providers without this capability fail before native cleanup begins.
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_native_work(&self, _work: NativeSourceWork<'_>) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }

    /// Retires this owned subscription against its original birth-paid cleanup.
    /// The independent token controls waiting only; no new allowance or source
    /// authority is created. Unknown providers reject before native effects.
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        Err(DemandError::NativeWorkUnavailable)
    }

    /// Completes only the already-accepted native physical retirement plan,
    /// with no observer wait token, new allowance or namespace permission.
    /// Unknown providers reject before cleanup begins.
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }

    /// True only after this provider has observed physical watch quiescence.
    /// Unknown implementations cannot attest that failed cleanup completed.
    fn is_stopped(&self) -> bool { false }

    /// SDK logical payload retained by this returned owner. Existing borrowed
    /// provider authority and allocator-private metadata are not newly owned.
    fn logical_owned_bytes(&self) -> u64;

    /// Only this call's newly created registration backing, not a preexisting
    /// provider buffer. Import it once into the original operation's custody;
    /// its control and data are separate from [`Self::logical_owned_bytes`].
    fn allocation_custody(&self) -> Option<std::sync::Arc<SourceWatchAllocationCustody>>;

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
        crate::cancellation::CancellationWaitError::Native(error) => crate::native_name::original_name_error(error),
    }
}


#[cfg(not(target_arch = "wasm32"))]
struct DemandCancellationRoot {
    token: CancellationToken,
    admission: crate::cancellation::CancellationWaitAdmission,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub(crate) struct DemandObserverCustody {
    token: std::sync::Arc<crate::kernel::ProofDataCustody>,
    adopted_bytes: u64,
    native: Option<NativeControlCursor>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, Default, Eq, PartialEq)]
struct NativeControlCursor {
    born: u128,
    retired: u128,
    root_metadata_bytes: u64,
}

/// The original observer overflow vector, with independent synchronization
/// only so its paid backing can grow outside the invocation mutex.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct NativeObserverOverflow {
    entries: std::sync::Mutex<Vec<DemandObserverCustody>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeObserverOverflow {
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<DemandObserverCustody>> {
        // Poison is checked by every original admission. Completed accounting
        // and physical retirement still inspect this exact original backing.
        self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandObserverCustody {
    fn imported_payload_bytes(&self) -> u64 {
        if self.native.is_some() { self.token.non_native_retained_payload_bytes() }
        else { self.token.retained_payload_bytes() }
    }

    fn pending_native_retired(&self) -> u64 {
        self.native.as_ref().map_or(0, |cursor| {
            let (_, retired) = self.token.native_control_events();
            u64::try_from(retired.checked_sub(cursor.retired)
                .expect("native control retirement is monotonic"))
                .expect("unreconciled original retirements cannot exceed its live ledger")
        })
    }

    fn reconcile_native(&mut self, allocations: &mut crate::kernel::allocation::AllocationLedger) -> Result<(), crate::WorkError> {
        let Some(cursor) = &mut self.native else { return Ok(()); };
        let (born, retired) = self.token.native_control_events();
        if born < cursor.born || retired < cursor.retired || retired > born {
            return Err(crate::WorkError::Overflow);
        }
        let released = u64::try_from(retired - cursor.retired).map_err(|_| crate::WorkError::Overflow)?;
        allocations.release(released).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        cursor.born = born;
        cursor.retired = retired;
        Ok(())
    }
}

/// Immutable membership in one already registered original checkout producer.
/// It is custody identity, not namespace, resource or mutation permission.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCheckoutControlWitness {
    original: std::sync::Arc<NativeDemandReadMeter>,
    producer: std::sync::Arc<crate::kernel::ProofDataCustody>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCheckoutControlWitness {
    pub(crate) fn owns(
        &self, original: &std::sync::Arc<NativeDemandReadMeter>,
        producer: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> bool {
        std::sync::Arc::ptr_eq(&self.original, original)
            && std::sync::Arc::ptr_eq(&self.producer, producer)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeDemandReadMeter {
    pub(crate) fn checkout_control_witness(
        self: &std::sync::Arc<Self>, producer: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> Option<NativeCheckoutControlWitness> {
        if !self.lock().owns_native_checkout_token(producer) { return None; }
        Some(NativeCheckoutControlWitness {
            original: std::sync::Arc::clone(self), producer: std::sync::Arc::clone(producer),
        })
    }

    pub(crate) fn register_native_checkout_control(
        self: &std::sync::Arc<Self>,
        native: &crate::materializer::NativeMaterializationWork,
        producer: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        header_guard: &mut crate::materializer::NativeAllocation,
    ) -> Result<NativeCheckoutControlWitness, crate::NativeTreeMaterializationError> {
        use crate::NativeTreeMaterializationError as Error;
        if !header_guard.is_checkout_header_for(self)
            || producer.retained_payload_bytes() != 0 || producer.native_control_events() != (0, 0) {
            return Err(Error::InvalidLayout);
        }
        loop {
            let overflow = {
                let mut held = self.lock();
                let work = held.as_work();
                work.with_resource_scope(None, |work| work.with_native_capture_scope(None, |work| {
                    work.with_invocation_scope(native.invocation_view_state().map(std::sync::Arc::downgrade), |work| work.check())
                })).map_err(crate::materializer::native_materialization_work_error)?;
                if work.cancellation_wait.owns_native_control(producer) {
                    return Err(Error::InvalidLayout);
                }
                if let Some(index) = work.cancellation_wait.observers.iter().position(Option::is_none) {
                    if !producer.bind_native_original(self) { return Err(Error::InvalidLayout); }
                    let bytes = header_guard.transfer_checkout_metadata(self, std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64)?;
                    work.cancellation_wait.observers[index] = Some(DemandObserverCustody {
                        token: std::sync::Arc::clone(producer), adopted_bytes: 0,
                        native: Some(NativeControlCursor { root_metadata_bytes: bytes, ..NativeControlCursor::default() }),
                    });
                    return Ok(NativeCheckoutControlWitness { original: std::sync::Arc::clone(self), producer: std::sync::Arc::clone(producer) });
                }
                work.cancellation_wait.observer_overflow.as_ref().map(std::sync::Arc::clone)
            };
            let Some(overflow) = overflow else {
                let mut metadata = native.claim_checkout_registry_header()?;
                let overflow = std::sync::Arc::new(NativeObserverOverflow::default());
                let mut held = self.lock();
                let work = held.as_work();
                work.with_resource_scope(None, |work| work.with_native_capture_scope(None, |work| {
                    work.with_invocation_scope(native.invocation_view_state().map(std::sync::Arc::downgrade), |work| work.check())
                })).map_err(crate::materializer::native_materialization_work_error)?;
                if work.cancellation_wait.observer_overflow.is_none() {
                    let bytes = metadata.transfer_checkout_metadata(self, std::mem::size_of::<NativeObserverOverflow>() as u64)?;
                    work.cancellation_wait.observer_metadata_bytes = bytes;
                    work.cancellation_wait.observer_overflow = Some(overflow);
                } else {
                    drop(held);
                    drop(overflow);
                }
                continue;
            };
            {
                let mut held = self.lock();
                let work = held.as_work();
                work.with_resource_scope(None, |work| work.with_native_capture_scope(None, |work| {
                    work.with_invocation_scope(native.invocation_view_state().map(std::sync::Arc::downgrade), |work| work.check())
                })).map_err(crate::materializer::native_materialization_work_error)?;
                if work.cancellation_wait.owns_native_control(producer) { return Err(Error::InvalidLayout); }
                let mut entries = overflow.lock();
                if entries.len() < entries.capacity() {
                    if !producer.bind_native_original(self) { return Err(Error::InvalidLayout); }
                    let bytes = header_guard.transfer_checkout_metadata(self, std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64)?;
                    entries.push(DemandObserverCustody {
                        token: std::sync::Arc::clone(producer), adopted_bytes: 0,
                        native: Some(NativeControlCursor { root_metadata_bytes: bytes, ..NativeControlCursor::default() }),
                    });
                    return Ok(NativeCheckoutControlWitness { original: std::sync::Arc::clone(self), producer: std::sync::Arc::clone(producer) });
                }
            }
            let (length, capacity) = {
                let entries = overflow.lock();
                (entries.len(), entries.capacity())
            };
            let target = capacity.max(1).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
            let mut backing = native.claim_checkout_registry_backing(target)?;
            let mut replacement = Vec::new();
            replacement.try_reserve_exact(target).map_err(Error::AllocationFailed)?;
            let actual = replacement.capacity().checked_mul(std::mem::size_of::<DemandObserverCustody>())
                .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
            backing.reconcile_capacity(actual)?;
            let copied = length.checked_mul(std::mem::size_of::<DemandObserverCustody>())
                .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
            let copy = native.reserve_checkout_registry_copy(length)?;
            {
                let entries = overflow.lock();
                if entries.len() != length || entries.capacity() != capacity { continue; }
                replacement.extend(entries.iter().cloned());
            }
            copy.record(copied)?;
            let old_bytes = {
                let mut held = self.lock();
                let work = held.as_work();
                work.with_resource_scope(None, |work| work.with_native_capture_scope(None, |work| {
                    work.with_invocation_scope(native.invocation_view_state().map(std::sync::Arc::downgrade), |work| work.check())
                })).map_err(crate::materializer::native_materialization_work_error)?;
                let mut entries = overflow.lock();
                if entries.len() != replacement.len() || entries.capacity() != capacity
                    || !entries.iter().zip(&replacement).all(|(left, right)| {
                        std::sync::Arc::ptr_eq(&left.token, &right.token)
                            && left.adopted_bytes == right.adopted_bytes && left.native == right.native
                    }) {
                    continue;
                }
                let old_bytes = work.cancellation_wait.observer_capacity.logical_bytes();
                let mut capacity = work.cancellation_wait.observer_capacity;
                capacity.record_owned_capacity(&replacement)?;
                backing.transfer_checkout_metadata(self, actual)?;
                std::mem::swap(&mut *entries, &mut replacement);
                work.cancellation_wait.observer_capacity = capacity;
                old_bytes
            };
            // Actual old backing and Arc aliases go first; only its original
            // scalar is retired afterwards. Neither allocator nor copy runs
            // under the invocation mutex.
            drop(replacement);
            let mut held = self.lock();
            held.as_work().release_native_owned(old_bytes, &None)
                .map_err(crate::materializer::native_materialization_work_error)?;
        }
    }
}

/// Registers only directly original-ledger constructor primitives, before
/// their actual allocations. This borrows the existing observer-root registry.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCheckoutControlSink<'a> {
    roots: &'a mut DemandCancellationRoots,
    original: &'a std::sync::Arc<NativeDemandReadMeter>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCheckoutControlSink<'_> {
    /// Pure ledger membership, not resource or mutation authority.
    pub(crate) fn owns(
        &self, original: &std::sync::Arc<NativeDemandReadMeter>,
        token: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> bool {
        std::sync::Arc::ptr_eq(self.original, original)
            && self.roots.owns_native_control(token)
    }

    /// For a fresh primitive token, every error precedes registry insertion.
    /// Its caller may therefore drop that actual Arc before refunding the paid
    /// token bytes. A token already retained by this root remains retained.
    pub(crate) fn register(
        &mut self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<(), DemandError> {
        let registered_native = self.roots.owns_native_control(token);
        if !registered_native
            && (token.retained_payload_bytes() != 0 || token.native_control_events() != (0, 0)) {
            return Err(DemandError::InvalidRequest);
        }
        if !token.bind_native_original(self.original) { return Err(DemandError::InvalidRequest); }
        self.roots.retain_observer(std::sync::Arc::clone(token), allocations, work, budget)?;
        let mut overflow = self.roots.observer_overflow.as_ref().map(|overflow| overflow.lock());
        let custody = self.roots.observers.iter_mut().flatten()
            .chain(overflow.iter_mut().flat_map(|overflow| overflow.iter_mut()))
            .find(|custody| std::sync::Arc::ptr_eq(&custody.token, token))
            .expect("registered original constructor token");
        custody.native.get_or_insert_with(NativeControlCursor::default);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct DemandWatchCustody {
    token: std::sync::Weak<SourceWatchAllocationCustody>,
    adopted_bytes: u64,
    pending: bool,
}

/// A pre-admitted slot in the original work's existing watch proof storage.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct PreparedWatchCustody {
    slot: WatchCustodySlot,
}

#[cfg(not(target_arch = "wasm32"))]
enum WatchCustodySlot {
    Inline(usize),
    Overflow(usize),
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandWatchCustody {
    fn current_bytes(&self) -> u64 {
        self.token.upgrade().map_or(0, |token| {
            std::mem::size_of::<SourceWatchAllocationCustody>() as u64 + token.retained_bytes()
        })
    }
}

/// Registry ownership travels with the one operation ledger, including
/// original cancellation roots and measured mount-view gate backing.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct DemandCancellationRoots {
    inline: [Option<DemandCancellationRoot>; 3],
    overflow: Vec<DemandCancellationRoot>,
    observers: [Option<DemandObserverCustody>; 3],
    observer_overflow: Option<std::sync::Arc<NativeObserverOverflow>>,
    observer_capacity: crate::kernel::allocation::LogicalVecCapacity,
    observer_metadata_bytes: u64,
    // Credits for actual future notification visits on this original
    // publication. They travel with the complete invocation, not a callback.
    pending_items: u64,
    pending_outputs: u64,
    // Additive completion credits on this same original budget. Allocation
    // operations and live/peak bytes remain actual allocation boundaries.
    pending_work: WorkCounters,
    // First typed runtime admission/completion failure, paid inline with this
    // original root. Native io::Error values are allocation-free relay markers.
    native_io_failure: Option<DemandError>,
    // Poisoned native state remains available only for completed accounting,
    // snapshots and physical retirement, never another native admission.
    native_poisoned: bool,
    // Only original durability tokens may consume or restore these credits.
    pending_native_durability: u64,
    watches: [Option<DemandWatchCustody>; 3],
    watch_overflow: Vec<DemandWatchCustody>,
    watch_capacity: crate::kernel::allocation::LogicalVecCapacity,
    #[cfg(feature = "native-mount")]
    view_gates: crate::native_mount::ViewGateReadAdmissions,
    #[cfg(feature = "native-mount")]
    view_observers: crate::native_mount::ViewObserverAdmissions,
    #[cfg(feature = "native-mount")]
    view_ledgers: crate::native_mount::ViewLedgerAdmissions,
    native_mount_admissions: NativeMountAllocationAdmissions,
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandCancellationRoots {
    fn owns_native_control(&self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>) -> bool {
        let overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock());
        self.observers.iter().flatten()
            .chain(overflow.iter().flat_map(|overflow| overflow.iter()))
            .any(|custody| custody.native.is_some() && std::sync::Arc::ptr_eq(&custody.token, token))
    }

    fn reconcile(&mut self, allocations: &mut crate::kernel::allocation::AllocationLedger) -> Result<(), crate::WorkError> {
        for root in self.inline.iter_mut().flatten().chain(self.overflow.iter_mut()) {
            root.admission.reconcile_registry(&root.token, allocations)?;
        }
        let mut overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock());
        for custody in self.observers.iter_mut().flatten().chain(overflow.iter_mut().flat_map(|overflow| overflow.iter_mut())) {
            custody.reconcile_native(allocations)?;
            let current = custody.imported_payload_bytes();
            if current < custody.adopted_bytes {
                allocations.release(custody.adopted_bytes - current)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                custody.adopted_bytes = current;
            }
        }
        drop(overflow);
        for custody in self.watches.iter_mut().flatten().chain(self.watch_overflow.iter_mut()) {
            let current = custody.current_bytes();
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
        self.view_ledgers.reconcile(allocations)?;
        self.native_mount_admissions.reconcile(allocations)?;
        Ok(())
    }

    fn freed_bytes(&self) -> u64 {
        let freed = self.inline.iter().flatten().chain(self.overflow.iter())
            .map(|root| root.admission.freed_registry_bytes(&root.token))
            .sum::<u64>();
        let freed = self.watches.iter().flatten().chain(self.watch_overflow.iter())
            .try_fold(freed, |sum, custody| sum.checked_add(
                custody.adopted_bytes.saturating_sub(custody.current_bytes()),
            )).expect("original watch backing custody belongs to this ledger");
        #[cfg(feature = "native-mount")]
        let freed = freed.checked_add(self.view_gates.freed_bytes())
            .expect("retained registry proofs cannot exceed the ledger's owned byte range");
        #[cfg(feature = "native-mount")]
        let freed = freed.checked_add(self.view_observers.freed_bytes())
            .expect("retained observer registry proofs belong to the original ledger");
        #[cfg(feature = "native-mount")]
        let freed = freed.checked_add(self.view_ledgers.freed_bytes())
            .expect("known native change-map admissions belong to the original ledger");
        let freed = freed.checked_add(self.native_mount_admissions.freed_bytes())
            .expect("native lifecycle admissions belong to the original ledger");
        let overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock());
        self.observers.iter().flatten().chain(overflow.iter().flat_map(|overflow| overflow.iter()))
            .try_fold(freed, |sum, custody| sum.checked_add(
                custody.adopted_bytes.saturating_sub(custody.imported_payload_bytes()),
            ).and_then(|sum| sum.checked_add(custody.pending_native_retired())))
            .expect("owned observer and registry data cannot exceed original ledger custody")
    }

    fn unrecorded_observer_bytes(&self) -> u64 {
        let overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock());
        self.observers.iter().flatten().chain(overflow.iter().flat_map(|overflow| overflow.iter()))
            .try_fold(0_u64, |sum, custody| sum.checked_add(
                custody.imported_payload_bytes().saturating_sub(custody.adopted_bytes),
            ))
            .expect("completed observer data remains representable in its original ledger")
    }

    fn retain_observer(
        &mut self, token: std::sync::Arc<crate::kernel::ProofDataCustody>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<bool, DemandError> {
        let overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock());
        for custody in self.observers.iter().flatten().chain(overflow.iter().flat_map(|overflow| overflow.iter())) {
            work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
            if std::sync::Arc::ptr_eq(&custody.token, &token) { return Ok(false); }
        }
        drop(overflow);
        let custody = DemandObserverCustody { token, adopted_bytes: 0, native: None };
        if let Some(index) = self.observers.iter().position(Option::is_none) {
            self.observers[index] = Some(custody);
            return Ok(true);
        }
        if self.observer_overflow.is_none() {
            let bytes = std::mem::size_of::<NativeObserverOverflow>() as u64;
            allocations.claim_bytes(bytes, 1, work, budget)
                .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
            self.observer_overflow = Some(std::sync::Arc::new(NativeObserverOverflow::default()));
            self.observer_metadata_bytes = bytes;
        }
        let mut overflow = self.observer_overflow.as_ref().expect("original overflow owner").lock();
        self.observer_capacity.ensure_for_push(&mut overflow, usize::MAX, allocations, work, budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        overflow.push(custody);
        Ok(true)
    }

    fn prepare_watch(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<PreparedWatchCustody, DemandError> {
        for (index, custody) in self.watches.iter_mut().enumerate() {
            work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, &budget)?;
            if custody.as_ref().is_none_or(|custody| !custody.pending && custody.adopted_bytes == 0 && custody.token.strong_count() == 0) {
                *custody = Some(DemandWatchCustody { token: std::sync::Weak::new(), adopted_bytes: 0, pending: true });
                return Ok(PreparedWatchCustody { slot: WatchCustodySlot::Inline(index) });
            }
        }
        for (index, custody) in self.watch_overflow.iter_mut().enumerate() {
            work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, &budget)?;
            if !custody.pending && custody.adopted_bytes == 0 && custody.token.strong_count() == 0 {
                *custody = DemandWatchCustody { token: std::sync::Weak::new(), adopted_bytes: 0, pending: true };
                return Ok(PreparedWatchCustody { slot: WatchCustodySlot::Overflow(index) });
            }
        }
        if let Err(error) = self.watch_capacity.ensure_for_push::<DemandWatchCustody>(
            &mut self.watch_overflow, usize::MAX, allocations, work, budget,
        ) {
            return Err(error.into_work_or(DemandError::WorkerUnavailable));
        }
        let index = self.watch_overflow.len();
        self.watch_overflow.push(DemandWatchCustody { token: std::sync::Weak::new(), adopted_bytes: 0, pending: true });
        Ok(PreparedWatchCustody { slot: WatchCustodySlot::Overflow(index) })
    }

    fn retain_prepared_watch(
        &mut self, prepared: PreparedWatchCustody, token: &std::sync::Arc<SourceWatchAllocationCustody>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<(), DemandError> {
        let custody = match prepared.slot {
            WatchCustodySlot::Inline(index) => self.watches[index].as_mut().expect("prepared original watch slot"),
            WatchCustodySlot::Overflow(index) => &mut self.watch_overflow[index],
        };
        custody.token = std::sync::Arc::downgrade(token);
        let bytes = custody.current_bytes();
        let recorded = record_completed_retention(allocations, work, budget, bytes);
        custody.adopted_bytes = bytes;
        custody.pending = false;
        recorded.map_err(DemandError::from)
    }

    fn abandon_prepared_watch(&mut self, prepared: PreparedWatchCustody) {
        let custody = match prepared.slot {
            WatchCustodySlot::Inline(index) => self.watches[index].as_mut().expect("prepared original watch slot"),
            WatchCustodySlot::Overflow(index) => &mut self.watch_overflow[index],
        };
        custody.pending = false;
    }

    fn retain_watch(
        &mut self, token: &std::sync::Arc<SourceWatchAllocationCustody>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<(), DemandError> {
        let weak = std::sync::Arc::downgrade(token);
        for custody in self.watches.iter().flatten().chain(self.watch_overflow.iter()) {
            work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, &budget)?;
            if custody.token.ptr_eq(&weak) { return Ok(()); }
        }
        let inline = self.watches.iter().position(Option::is_none);
        if inline.is_none() {
            if let Err(error) = self.watch_capacity.ensure_for_push::<DemandWatchCustody>(
                &mut self.watch_overflow, usize::MAX, allocations, work, budget,
            ) {
                return Err(error.into_work_or(DemandError::WorkerUnavailable));
            }
        }
        let custody = DemandWatchCustody { token: weak, adopted_bytes: 0, pending: false };
        let custody = if let Some(index) = inline {
            self.watches[index] = Some(custody);
            self.watches[index].as_mut().expect("inserted original backing proof")
        } else {
            self.watch_overflow.push(custody);
            self.watch_overflow.last_mut().expect("inserted original backing proof")
        };
        let bytes = custody.current_bytes();
        let recorded = record_completed_retention(allocations, work, budget, bytes);
        custody.adopted_bytes = bytes;
        recorded.map_err(DemandError::from)
    }

    fn record_observer_growth(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<(), crate::WorkError> {
        let mut failure = None;
        let mut overflow = self.observer_overflow.as_ref().map(|overflow| overflow.lock());
        for custody in self.observers.iter_mut().flatten().chain(overflow.iter_mut().flat_map(|overflow| overflow.iter_mut())) {
            custody.reconcile_native(allocations)?;
            let current = custody.imported_payload_bytes();
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
#[cfg(not(target_arch = "wasm32"))]
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

/// Exclusive synchronous native-capture scratch ownership, not an allocation
/// ledger or a proof-tree/global live-byte estimate.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct CaptureTransientCustody {
    retained: std::sync::atomic::AtomicU64,
    native_header: Option<crate::NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
impl CaptureTransientCustody {
    pub(crate) fn new_native(
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, crate::NativeTreeMaterializationError> {
        let header = work.claim_capture_custody_metadata()?;
        Ok(Arc::new(Self {
            retained: std::sync::atomic::AtomicU64::new(0),
            native_header: Some(header),
        }))
    }

    pub(crate) fn owns_native_work(&self, work: &crate::NativeMaterializationWork) -> bool {
        self.native_header.as_ref().is_some_and(|header|
            header.work().shares_ledger(work) && header.work().shares_resource_allowance(work))
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The owner calls this only after its actual scratch containers drop.
    pub(crate) fn take_retained_bytes(&self) -> u64 {
        self.retained.swap(0, std::sync::atomic::Ordering::AcqRel)
    }

    /// Moves only this capture's already-owned claim into its actual SDK
    /// payload guard. It neither claims quota nor changes the original AL.
    pub(crate) fn transfer_retained_bytes(&self, bytes: u64) -> Result<(), crate::WorkError> {
        self.retained.fetch_update(
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
            |retained| retained.checked_sub(bytes),
        ).map(|_| ()).map_err(|_| crate::WorkError::Overflow)
    }

    fn claim(&self, bytes: u64) {
        self.retained.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
    }

    fn release(&self, bytes: u64) {
        let _ = self.retained.fetch_update(
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
            |retained| Some(retained.saturating_sub(bytes)),
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
enum CaptureCustodyBinding {
    Ordinary(Arc<CaptureTransientCustody>),
    Native(std::sync::Weak<CaptureTransientCustody>),
}

#[cfg(not(target_arch = "wasm32"))]
impl CaptureCustodyBinding {
    fn from_owner(owner: Arc<CaptureTransientCustody>) -> Self {
        if owner.native_header.is_some() { Self::Native(Arc::downgrade(&owner)) }
        else { Self::Ordinary(owner) }
    }

    fn owner(&self) -> Option<Arc<CaptureTransientCustody>> {
        match self {
            Self::Ordinary(owner) => Some(Arc::clone(owner)),
            Self::Native(owner) => owner.upgrade(),
        }
    }
}

/// One native attempt's admission and allocation state. Helpers charge at the
/// actual boundary, before starting work; denied work never enters the receipt.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct DemandWork {
    pub(crate) work: WorkCounters,
    pub(crate) budget: WorkBudget,
    native_full_budget: WorkBudget,
    pub(crate) original_work: Option<WorkCounters>,
    allocations: crate::kernel::allocation::AllocationLedger,
    cancellation: CancellationToken,
    parent_cancellation: Option<CancellationToken>,
    stop: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    cancellation_wait: DemandCancellationRoots,
    capture_custody: Option<CaptureCustodyBinding>,
    native_resource: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
    native_invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
}

/// The complete original invocation shared by native admission callbacks.
/// Its mutex is a synchronous leaf boundary, never an SDK/OS/observer loan.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeDemandReadMeter<T = DemandWork>(pub(crate) std::sync::Mutex<T>);

/// The native caller keeps this exact original state after a queue/SDK loan.
/// Intrinsic payloads may retain the same state through their physical Drop;
/// caller restoration never requires those legitimate aliases to disappear.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub(crate) struct OriginalDemandWork {
    state: std::sync::Arc<NativeDemandReadMeter>,
    resource: Option<std::sync::Arc<crate::materializer::NativeResourceAllowance>>,
    capture_origin: Option<std::sync::Weak<CaptureTransientCustody>>,
    invocation: Option<Arc<crate::materializer::NativeInvocationView>>,
    #[cfg(feature = "native-mount")]
    execution: Option<Arc<crate::materializer::NativeSourceExecutionView>>,
}

#[cfg(not(target_arch = "wasm32"))]
fn same_resource_view(
    left: Option<&Arc<crate::materializer::NativeResourceAllowance>>,
    right: Option<&Arc<crate::materializer::NativeResourceAllowance>>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl OriginalDemandWork {
    pub(crate) fn from_meter(state: std::sync::Arc<NativeDemandReadMeter>) -> Self {
        Self {
            state, resource: None, capture_origin: None, invocation: None,
            #[cfg(feature = "native-mount")]
            execution: None,
        }
    }

    pub(crate) fn from_native_materialization_work(work: &crate::materializer::NativeMaterializationWork) -> Self {
        Self {
            state: Arc::clone(work.native_read_meter()),
            resource: work.resource_allowance().cloned(),
            capture_origin: work.capture_origin().cloned(),
            invocation: work.invocation_view_state().cloned(),
            #[cfg(feature = "native-mount")]
            execution: work.source_execution_view().cloned(),
        }
    }

    pub(crate) fn resource_allowance(&self) -> Option<&Arc<crate::materializer::NativeResourceAllowance>> {
        self.resource.as_ref()
    }

    pub(crate) fn capture_origin(&self) -> Option<&std::sync::Weak<CaptureTransientCustody>> {
        self.capture_origin.as_ref()
    }

    pub(crate) fn invocation_view_state(&self) -> Option<&Arc<crate::materializer::NativeInvocationView>> {
        self.invocation.as_ref()
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn source_execution_view(&self) -> Option<&Arc<crate::materializer::NativeSourceExecutionView>> {
        self.execution.as_ref()
    }

    /// Data custody preserves the original accounting family without retaining
    /// an accepted physical job's source-reader lease after that job returns.
    pub(crate) fn allocation_work(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            resource: self.resource.clone(),
            capture_origin: self.capture_origin.clone(),
            invocation: self.invocation.clone(),
            #[cfg(feature = "native-mount")]
            execution: None,
        }
    }

    fn from_source_work(work: NativeSourceWork<'_>) -> Self {
        Self {
            state: Arc::clone(work.original), resource: work.resource.cloned(),
            capture_origin: work.capture_origin.cloned(),
            invocation: work.invocation.cloned(),
            #[cfg(feature = "native-mount")]
            execution: work.execution.cloned(),
        }
    }

    pub(crate) fn meter(&self) -> &std::sync::Arc<NativeDemandReadMeter> { &self.state }

    /// Only allocator-free accounting/cancellation/invariant leaves belong
    /// here. Allocation, OS calls, SDK/user callbacks and awaits run outside.
    pub(crate) fn with_original_leaf<T>(
        &self, operation: impl FnOnce(&mut DemandWork) -> T,
    ) -> Result<T, DemandError> {
        let mut state = self.state.0.lock().map_err(|_| DemandError::OriginalWorkPoisoned)?;
        Ok(state.with_resource_scope(self.resource.as_ref().map(Arc::downgrade), |state| {
            state.with_native_capture_scope(self.capture_origin.clone(), |state| {
                state.with_invocation_scope(self.invocation.as_ref().map(Arc::downgrade), operation)
            })
        }))
    }

    pub(crate) fn counters(&self) -> Result<WorkCounters, DemandError> {
        self.with_original_leaf(|state| state.work)
    }

    pub(crate) fn full_budget(&self) -> Result<WorkBudget, DemandError> {
        self.with_original_leaf(|state| state.original_budget())
    }

    pub(crate) fn remaining(&self) -> Result<WorkBudget, DemandError> {
        self.with_original_leaf(DemandWork::remaining)?
    }

    pub(crate) fn check(&self) -> Result<(), DemandError> {
        self.with_original_leaf(|state| state.check())?
    }

    /// Snapshot only the receipt, never the ledger, roots or original grant.
    /// The failed-lock path retains its real observed prefix while refusing
    /// to recover the poisoned state for new allocation or I/O.
    pub(crate) fn finish_operation<T>(&self, result: Result<T, DemandError>) -> DemandResult<T> {
        let (checked, counters) = match self.state.0.lock() {
            Ok(mut state) => {
                let checked = state.reconcile_cancellation_registry()
                    .map_err(DemandError::from)
                    .and_then(|()| state.work.verify(state.budget).map_err(DemandError::from));
                (checked, state.work)
            }
            Err(poisoned) => (Err(DemandError::OriginalWorkPoisoned), poisoned.get_ref().work),
        };
        checked.and(result).map(|value| OperationReceipt { value, work: counters })
            .map_err(|error| OperationFailure::new(error, counters))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: NativeDemandReadWork> NativeDemandReadMeter<T> {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, T> {
        match self.0.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                // Keep the original ledger for physical retirement, but make
                // every subsequent admission reject its actual poison cause.
                let mut state = poisoned.into_inner();
                let work = state.as_work();
                work.cancellation_wait.native_poisoned = true;
                let _ = work.record_native_io_failure(DemandError::OriginalWorkPoisoned);
                state
            }
        }
    }

    /// Runs only synchronous admission/accounting against the original state.
    /// Callers must invoke SDK/OS/observer work after this closure returns.
    pub(crate) fn with_original_leaf<R>(&self, operation: impl FnOnce(&mut T) -> R) -> R {
        operation(&mut *self.lock())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeDemandReadMeter {
    /// Borrows the registered original only for numeric metadata and completed
    /// custody transfers. Producers admit physical work before invoking it.
    pub(crate) fn with_native_checkout_controls<T>(
        self: &std::sync::Arc<Self>,
        operation: impl FnOnce(
            &mut NativeCheckoutControlSink<'_>,
            &mut crate::kernel::allocation::AllocationLedger,
            &mut WorkCounters, WorkBudget, &CancellationToken,
        ) -> T,
    ) -> Result<T, DemandError> {
        let mut original = self.lock();
        let (mut sink, allocations, work, budget, cancellation) = original.native_checkout_control_parts(self);
        Ok(operation(&mut sink, allocations, work, budget, cancellation))
    }
}

/// Borrow of the original native invocation. Construction is restricted to the
/// native directory driver; this is accounting custody, not source authority.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
pub struct NativeSourceWork<'a> {
    original: &'a std::sync::Arc<NativeDemandReadMeter>,
    resource: Option<&'a Arc<crate::materializer::NativeResourceAllowance>>,
    capture_origin: Option<&'a std::sync::Weak<CaptureTransientCustody>>,
    invocation: Option<&'a Arc<crate::materializer::NativeInvocationView>>,
    #[cfg(feature = "native-mount")]
    execution: Option<&'a Arc<crate::materializer::NativeSourceExecutionView>>,
    native_original: bool,
    operation_stop: Option<&'a std::sync::atomic::AtomicBool>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> NativeSourceWork<'a> {
    pub(crate) fn new(original: &'a std::sync::Arc<NativeDemandReadMeter>) -> Self {
        Self {
            original, resource: None, capture_origin: None, invocation: None,
            #[cfg(feature = "native-mount")]
            execution: None,
            native_original: true, operation_stop: None,
        }
    }

    pub(crate) fn ordinary(original: &'a std::sync::Arc<NativeDemandReadMeter>) -> Self {
        Self {
            original, resource: None, capture_origin: None, invocation: None,
            #[cfg(feature = "native-mount")]
            execution: None,
            native_original: false, operation_stop: None,
        }
    }

    pub(crate) fn from_native_materialization_work(work: &'a crate::materializer::NativeMaterializationWork) -> Self {
        Self {
            original: work.native_read_meter(), resource: work.resource_allowance(),
            capture_origin: work.capture_origin(),
            invocation: work.invocation_view_state(),
            #[cfg(feature = "native-mount")]
            execution: work.source_execution_view(),
            native_original: true, operation_stop: None,
        }
    }

    pub(crate) fn from_original_work(work: &'a OriginalDemandWork) -> Self {
        Self {
            original: work.meter(), resource: work.resource_allowance(),
            capture_origin: work.capture_origin(),
            invocation: work.invocation_view_state(),
            #[cfg(feature = "native-mount")]
            execution: work.source_execution_view(),
            native_original: true, operation_stop: None,
        }
    }

    pub(crate) fn original_arc(&self) -> &'a std::sync::Arc<NativeDemandReadMeter> {
        self.original
    }

    /// Borrows the full original accounting context, including its resource
    /// view. The request facade still owns its private stop check.
    pub(crate) fn original_demand_work(&self) -> OriginalDemandWork {
        OriginalDemandWork::from_source_work(*self)
    }

    pub(crate) fn materialization_work(&self) -> Result<crate::materializer::NativeMaterializationWork, DemandError> {
        crate::native_host::NativeHostWork::check(self)?;
        crate::materializer::NativeMaterializationWork::from_original_demand_work(&self.original_demand_work())
    }
}

/// Native source helpers borrow the same ledger only for synchronous leaves.
/// Host operations and callbacks run after the leaf lock has been released.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait SourceDemandReadWork:
    crate::native_host::NativeHostWork<Error = DemandError, Allocation = u64, CleanupWrite = u64>
{
    fn with_native_read_meter<T>(
        &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T;
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T;
    fn native_source_work(&self) -> Option<NativeSourceWork<'_>> { None }
    fn with_ordinary_work<T>(&mut self, _operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> { None }
    fn original_meter(&self) -> Option<&std::sync::Arc<NativeDemandReadMeter>> { None }
    fn reserve_source_copy(
        &mut self, bytes: u64,
    ) -> Result<Option<crate::materializer::NativeCopyReservation>, DemandError> {
        self.check()?;
        if let Some(source) = self.native_source_work() {
            return source.materialization_work()?.reserve_copy(bytes)
                .map(Some).map_err(crate::native_name::original_name_error);
        }
        self.admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED })?;
        Ok(None)
    }
    fn charge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.charge(delta))
    }
    fn claim_allocation(&mut self, bytes: u64, operations: u64) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.claim_allocation(bytes, operations))
    }
    fn allocate(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.allocate(bytes))
    }
    fn allocate_for(&mut self, bytes: u64, delta: WorkCounters) -> Result<(), DemandError> {
        self.check()?;
        self.with_original_leaf(|work| work.allocate_for(bytes, delta))
    }
    fn remaining(&mut self) -> Result<WorkBudget, DemandError> {
        self.with_original_leaf(|work| work.remaining())
    }
    fn adopt(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.adopt(bytes))
    }
    fn merge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.merge(delta))
    }
    fn reserve_output(&mut self) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.reserve_pending_outputs(1))
    }
    fn commit_output(&mut self) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.record_pending_outputs(1))
    }
    fn cancel_output(&mut self) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.refund_pending_outputs(1))
    }
    fn preflight(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.admit(delta)
    }
    fn is_native_original(&self) -> bool { false }
}

#[cfg(not(target_arch = "wasm32"))]
impl SourceDemandReadWork for DemandWork {
    fn with_ordinary_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        Some(operation(self))
    }
    fn with_native_read_meter<T>(
        &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        let meter = NativeDemandReadMeter(std::sync::Mutex::new(self));
        operation(&meter)
    }
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        operation(self)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::native_host::NativeHostWork for NativeSourceWork<'_> {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        <DemandWork as crate::native_host::NativeHostWork>::io_error_kind(error)
    }
    fn invalid_request() -> Self::Error { DemandError::InvalidRequest }
    fn check(&self) -> Result<(), Self::Error> {
        self.original.with_original_leaf(|work| work.check())?;
        if let Some(invocation) = self.invocation { invocation.check()?; }
        if self.capture_origin.is_some_and(|origin| origin.strong_count() == 0) {
            return Err(DemandError::NativeWorkUnavailable);
        }
        if let Some(resource) = self.resource { resource.admit(WorkCounters::UNCHARGED, 0)?; }
        if self.operation_stop.is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
            return Err(DemandError::Cancelled);
        }
        Ok(())
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.check()?;
        self.with_original_leaf(|work| work.admit(delta))
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.with_original_leaf(|work| work.record(delta))
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.check()?;
        self.with_original_leaf(|work| work.charge(delta))
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, Self::Error> {
        self.check()?;
        self.with_original_leaf(|work| { work.allocate(bytes)?; Ok(bytes) })
    }
    fn reconcile_allocation(&mut self, allocation: &mut u64, actual_bytes: u64) -> Result<(), Self::Error> {
        self.with_original_leaf(|work| work.reconcile_native_allocation(allocation, actual_bytes))
    }
    fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, Self::Error> {
        self.check()?;
        self.with_original_leaf(|work| crate::native_host::NativeHostWork::reserve_opaque_decode(work, maximum))
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        DemandError::Allocation(error)
    }
    fn release(&mut self, bytes: u64) -> Result<(), Self::Error> {
        self.with_original_leaf(|work| work.release(bytes))
    }
    fn reserve_cleanup_write(&mut self) -> Result<u64, Self::Error> {
        self.with_original_leaf(|work| crate::native_host::NativeHostWork::reserve_cleanup_write(work))
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        self.with_original_leaf(|work| crate::native_host::NativeHostWork::cancel_cleanup_write(work, reservation));
    }
    fn attempt_cleanup_write<T>(&mut self, reservation: u64, operation: impl FnOnce() -> T)
        -> (T, Result<(), Self::Error>) {
        let recorded = self.with_original_leaf(|work| {
            let (_, recorded) = crate::native_host::NativeHostWork::attempt_cleanup_write(work, reservation, || ());
            recorded
        });
        (operation(), recorded)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SourceDemandReadWork for NativeSourceWork<'_> {
    fn is_native_original(&self) -> bool { self.native_original }
    fn native_source_work(&self) -> Option<NativeSourceWork<'_>> {
        self.native_original.then_some(*self)
    }
    fn original_meter(&self) -> Option<&std::sync::Arc<NativeDemandReadMeter>> {
        self.native_original.then_some(self.original)
    }
    fn with_native_read_meter<T>(
        &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        let original: &dyn acyclic_native_runtime::NativeReadMeter = {
            #[cfg(feature = "native-mount")]
            if let Some(execution) = self.execution {
                if let Some(stop) = self.operation_stop {
                    return operation(&ScopedSourceReadMeter {
                        original: execution.as_ref(), failure_root: self.original.as_ref(), stop,
                    });
                }
                return operation(execution.as_ref());
            }
            match (self.invocation, self.resource) {
                (Some(invocation), _) if invocation.owns_resource(self.resource) => invocation.as_ref(),
                (_, Some(resource)) => resource.as_ref(),
                (_, None) => self.original.as_ref(),
            }
        };
        if let Some(stop) = self.operation_stop {
            operation(&ScopedSourceReadMeter { original, failure_root: self.original.as_ref(), stop })
        } else { operation(original) }
    }
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        let resource = self.resource.map(Arc::downgrade);
        self.original.with_original_leaf(|work| work.with_resource_scope(resource, |work| {
            work.with_native_capture_scope(self.capture_origin.cloned(), |work| {
                work.with_invocation_scope(self.invocation.map(Arc::downgrade), operation)
            })
        }))
    }
}

/// A stack-only cancellation view; every admission and completion still uses
/// the original native-runtime meter and its sole allocation ledger.
#[cfg(not(target_arch = "wasm32"))]
struct ScopedSourceReadMeter<'a> {
    original: &'a dyn acyclic_native_runtime::NativeReadMeter,
    failure_root: &'a NativeDemandReadMeter,
    stop: &'a std::sync::atomic::AtomicBool,
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_native_runtime::NativeReadMeter for ScopedSourceReadMeter<'_> {
    fn check(&self) -> std::io::Result<()> {
        if self.stop.load(std::sync::atomic::Ordering::Acquire) {
            return Err(self.failure_root.with_original_leaf(|work| work.record_native_io_failure(DemandError::Cancelled)));
        }
        self.original.check()
    }
    fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.reserve_operations(kind, count)
    }
    fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
        self.original.cancel_reserved_operations(kind, count);
    }
    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        self.original.record_reserved_operation(kind)
    }
    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_write_bytes(maximum)
    }
    fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.reserve_bytes(kind, maximum)
    }
    fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
        self.original.cancel_reserved_bytes(kind, maximum);
    }
    fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
        self.original.record_reserved_bytes(kind, maximum, actual)
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.allocate(bytes)
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        self.original.allocation_failed(error)
    }
    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.reserve_allocation(bytes)
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        self.original.cancel_reserved_allocation(bytes);
    }
    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        // The action was admitted before the transport began. A local stop
        // cannot revoke the Ready control needed to drain that accepted action.
        self.original.record_reserved_allocation(bytes)
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.check()?;
        self.original.allocate_refcount_header()
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.adopt(bytes)
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        self.original.record_completed_adoption(bytes)
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_codec(encoded, copied, hashed)
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.original.record_codec(encoded, copied, hashed)
    }
    fn examine(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.examine(items, bytes_copied)
    }
    fn admit_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_examination(items, bytes_copied)
    }
    fn record_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.original.record_examination(items, bytes_copied)
    }
    fn release(&self, bytes: u64) { self.original.release(bytes); }
    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_bytes(maximum)
    }
    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_read(maximum)
    }
    fn admit_operations(&self, additional: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_operations(additional)
    }
    fn record_operations(&self, completed: u64) -> std::io::Result<()> {
        self.original.record_operations(completed)
    }
    fn record_read(&self, bytes: usize) -> std::io::Result<()> { self.original.record_read(bytes) }
    fn finish_read(&self, requested: usize, completed: usize) -> std::io::Result<()> {
        let completed = self.original.finish_read(requested, completed);
        completed.and(self.check())
    }
    fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_write(maximum)
    }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> { self.original.record_write(bytes) }
    fn admit_write_operations(&self, additional: u64) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_write_operations(additional)
    }
    fn record_write_operations(&self, completed: u64) -> std::io::Result<()> {
        self.original.record_write_operations(completed)
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        self.check()?;
        self.original.admit_durability()
    }
    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        self.check()?;
        self.original.reserve_cleanup_write()
    }
    fn cancel_cleanup_write(&self, reservation: u64) { self.original.cancel_cleanup_write(reservation); }
    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
        self.original.record_cleanup_write(reservation)
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct DemandResourceScope<'a> {
    work: &'a mut DemandWork,
    previous: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for DemandResourceScope<'_> {
    fn drop(&mut self) { self.work.native_resource = self.previous.take(); }
}

#[cfg(not(target_arch = "wasm32"))]
struct DemandNativeCaptureScope<'a> {
    work: &'a mut DemandWork,
    previous: Option<CaptureCustodyBinding>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for DemandNativeCaptureScope<'_> {
    fn drop(&mut self) { self.work.capture_custody = self.previous.take(); }
}

#[cfg(not(target_arch = "wasm32"))]
struct DemandInvocationScope<'a> {
    work: &'a mut DemandWork,
    previous: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for DemandInvocationScope<'_> {
    fn drop(&mut self) { self.work.native_invocation = self.previous.take(); }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeMaterializationLeafContext {
    resource: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
    invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
    capture: Option<CaptureCustodyBinding>,
}

#[cfg(not(target_arch = "wasm32"))]
impl DemandWork {
    pub(crate) fn enter_materialization_leaf(
        &mut self, invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
    ) -> NativeMaterializationLeafContext {
        NativeMaterializationLeafContext {
            resource: self.native_resource.take(),
            invocation: std::mem::replace(&mut self.native_invocation, invocation),
            capture: self.capture_custody.take(),
        }
    }

    pub(crate) fn restore_materialization_leaf(&mut self, context: NativeMaterializationLeafContext) {
        self.native_resource = context.resource;
        self.native_invocation = context.invocation;
        self.capture_custody = context.capture;
    }

    /// Applies only numeric accounting leaves to the caller's exact resource.
    /// Physical allocation, host IO and callbacks stay outside this scope.
    pub(crate) fn with_resource_scope<T>(
        &mut self,
        resource: Option<std::sync::Weak<crate::materializer::NativeResourceAllowance>>,
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let previous = std::mem::replace(&mut self.native_resource, resource);
        let scope = DemandResourceScope { work: self, previous };
        operation(&mut *scope.work)
    }

    /// Binds the already-validated claim-time capture view to fixed leaves.
    /// None deliberately excludes an unrelated ambient capture observer.
    pub(crate) fn with_native_capture_scope<T>(
        &mut self, capture: Option<std::sync::Weak<CaptureTransientCustody>>,
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let previous = std::mem::replace(&mut self.capture_custody, capture.map(CaptureCustodyBinding::Native));
        let scope = DemandNativeCaptureScope { work: self, previous };
        operation(&mut *scope.work)
    }

    pub(crate) fn with_invocation_scope<T>(
        &mut self, invocation: Option<std::sync::Weak<crate::materializer::NativeInvocationView>>,
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let previous = std::mem::replace(&mut self.native_invocation, invocation);
        let scope = DemandInvocationScope { work: self, previous };
        operation(&mut *scope.work)
    }

    fn resource_scope(&self) -> Option<Arc<crate::materializer::NativeResourceAllowance>> {
        self.native_resource.as_ref().and_then(std::sync::Weak::upgrade)
    }
    pub(crate) const fn original_budget(&self) -> WorkBudget { self.native_full_budget }


    pub(crate) fn new(budget: WorkBudget, cancellation: &CancellationToken) -> Self {
        Self {
            work: WorkCounters::default(),
            budget,
            native_full_budget: budget,
            original_work: None,
            allocations: crate::kernel::allocation::AllocationLedger::default(),
            cancellation: cancellation.clone(),
            parent_cancellation: None,
            stop: None,
            cancellation_wait: DemandCancellationRoots::default(),
            capture_custody: None,
            native_resource: None,
            native_invocation: None,
        }
    }

    /// Constructs only the inert placement shell, before a queue envelope is
    /// allocated. It must never execute operation work or own original proofs.
    pub(crate) fn native_queue_shell(&self) -> Self {
        Self::new(WorkBudget::default(), &self.cancellation)
    }

    /// Moves the complete original invocation into the existing native queue.
    /// The exclusive caller must restore it on every terminal path before
    /// using this inert shell; no operation may execute against the shell.
    pub(crate) fn take_for_native_queue(&mut self) -> Self {
        let shell = self.native_queue_shell();
        std::mem::replace(self, shell)
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
        let mut original = Self {
            work, budget, allocations, cancellation: cancellation.clone(),
            native_full_budget: budget,
            parent_cancellation: None, stop: None, cancellation_wait,
            capture_custody: None,
            native_resource: None,
            native_invocation: None,
            original_work: None,
        };
        original.restore_budget(budget);
        original
    }

    /// Claims through the original ledger for one concrete owner's captured
    /// origin. The caller performs its resource gate while holding this leaf.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn claim_native_owned(
        &mut self, bytes: u64, operations: u64,
        capture: &Option<std::sync::Arc<CaptureTransientCustody>>,
    ) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let delta = WorkCounters { allocation_operations: operations, ..WorkCounters::UNCHARGED };
            return resource.admission(delta, bytes, || self.with_resource_scope(None, |work| work.claim_native_owned(bytes, operations, capture)));
        }
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.allocations.claim_bytes(bytes, operations, &mut self.work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        if let Some(capture) = capture { capture.claim(bytes); }
        Ok(())
    }

    /// Adjusts an existing owner's logical quota, not a new allocator attempt.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn retain_native_owned(
        &mut self, bytes: u64,
        capture: &Option<std::sync::Arc<CaptureTransientCustody>>,
    ) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.admission(WorkCounters::UNCHARGED, bytes, || self.with_resource_scope(None, |work| work.retain_native_owned(bytes, capture)));
        }
        if self.cancellation_wait.native_poisoned { return Err(DemandError::OriginalWorkPoisoned); }
        self.allocations.claim_bytes(bytes, 0, &mut self.work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        if let Some(capture) = capture { capture.claim(bytes); }
        Ok(())
    }

    /// Records real completed backing in its claim-time origin before a late
    /// refusal. This never reconstructs or moves the original allocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn record_native_owned_completed(
        &mut self, bytes: u64,
        capture: &Option<std::sync::Arc<CaptureTransientCustody>>,
    ) -> (u64, Result<(), DemandError>) {
        if let Some(resource) = self.resource_scope() {
            let (claimed, recorded) = self.with_resource_scope(None, |work| work.record_native_owned_completed(bytes, capture));
            let scoped = resource.completed(WorkCounters::UNCHARGED, claimed);
            return (claimed, recorded.and(scoped));
        }
        if let Err(error) = self.allocations.record_owned_bytes(bytes, &mut self.work) {
            return (0, Err(error.into_work_or(DemandError::InvalidRequest)));
        }
        if let Some(capture) = capture { capture.claim(bytes); }
        let result = self.check().and_then(|()| self.work.verify(self.budget).map_err(DemandError::from));
        (bytes, result)
    }

    /// Retires only the concrete physical owner's original capture claim.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn release_native_owned(
        &mut self, bytes: u64,
        capture: &Option<std::sync::Arc<CaptureTransientCustody>>,
    ) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let released = self.with_resource_scope(None, |work| work.release_native_owned(bytes, capture));
            let scoped = resource.retire(bytes);
            if released.is_err() || scoped.is_err() { self.poison_native_original_work(); }
            return released.and(scoped);
        }
        let released = self.allocations.release(bytes)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest));
        let capture_released = capture.as_ref().map_or(Ok(()), |capture| {
            capture.transfer_retained_bytes(bytes).map_err(DemandError::from)
        });
        if let Err(error) = released.and(capture_released) {
            self.record_native_io_failure(error);
            self.poison_native_original_work();
            return Err(DemandError::OriginalWorkPoisoned);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn poison_native_original_work(&mut self) {
        self.cancellation_wait.native_poisoned = true;
        self.record_native_io_failure(DemandError::OriginalWorkPoisoned);
    }

    /// Reserves the original live quota and one allocator attempt without
    /// reporting either as physical work before the accepted Ready boundary.
    pub(crate) fn reserve_native_allocation(&mut self, bytes: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.reserve_allocation(bytes, || {
                self.with_resource_scope(None, |work| work.reserve_native_allocation(bytes))
            });
        }
        self.reconcile_cancellation_registry()?;
        self.check()?;
        let pending = self.cancellation_wait.pending_work.allocation_operations
            .checked_add(1).ok_or(crate::WorkError::Overflow)?;
        self.allocations.reserve_allocation(bytes, &self.work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        // Successful admission proved that this effective budget contains the
        // withheld operation; no callback can change it inside this leaf.
        self.budget.allocation_operations -= 1;
        self.cancellation_wait.pending_work.allocation_operations = pending;
        Ok(())
    }

    pub(crate) fn cancel_reserved_native_allocation(&mut self, bytes: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.cancel_reserved_allocation(bytes, || {
                self.with_resource_scope(None, |work| work.cancel_reserved_native_allocation(bytes))
            });
        }
        let pending = self.cancellation_wait.pending_work.allocation_operations
            .checked_sub(1).ok_or(crate::WorkError::Overflow)?;
        let budget = self.budget.allocation_operations.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        self.allocations.cancel_reserved_allocation(bytes)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        self.cancellation_wait.pending_work.allocation_operations = pending;
        self.budget.allocation_operations = budget;
        Ok(())
    }

    pub(crate) fn record_reserved_native_allocation(&mut self, bytes: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.record_reserved_allocation(bytes, || {
                self.with_resource_scope(None, |work| work.record_reserved_native_allocation(bytes))
            });
        }
        if self.cancellation_wait.native_poisoned { return Err(DemandError::OriginalWorkPoisoned); }
        let pending = self.cancellation_wait.pending_work.allocation_operations
            .checked_sub(1).ok_or(crate::WorkError::Overflow)?;
        let budget = self.budget.allocation_operations.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        self.allocations.record_reserved_allocation(bytes, &mut self.work)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        self.cancellation_wait.pending_work.allocation_operations = pending;
        self.budget.allocation_operations = budget;
        // Only the explicit current capture loan owns this newly begun control.
        // Plain/root meters have no capture binding; reservations never tag data.
        if let Some(custody) = self.capture_custody() { custody.claim(bytes); }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn reserve_native_cleanup_write(&mut self) -> Result<u64, DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.reserve_credit(crate::materializer::NativeResourceCredit::WriteOperations, 1, || {
                self.with_resource_scope(None, Self::reserve_native_cleanup_write)
            });
        }
        self.check()?;
        self.work.admit(&WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }, &self.budget)?;
        self.budget.backend_write_operations = self.budget.backend_write_operations
            .checked_sub(1).ok_or(crate::WorkError::Overflow)?;
        Ok(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn cancel_native_cleanup_write(&mut self, reservation: u64) {
        if let Some(resource) = self.resource_scope() {
            self.with_resource_scope(None, |work| work.cancel_native_cleanup_write(reservation));
            if let Err(error) = resource.refund_credit(crate::materializer::NativeResourceCredit::WriteOperations, reservation) {
                self.record_native_io_failure(error);
                self.poison_native_original_work();
            }
            return;
        }
        match self.budget.backend_write_operations.checked_add(reservation) {
            Some(restored) => self.budget.backend_write_operations = restored,
            None => self.poison_native_original_work(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn record_native_cleanup_write(&mut self, reservation: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let original = self.with_resource_scope(None, |work| work.record_native_cleanup_write(reservation));
            let scoped = resource.complete_credit(crate::materializer::NativeResourceCredit::WriteOperations, reservation, 1);
            return original.and(scoped);
        }
        self.cancel_native_cleanup_write(reservation);
        let delta = WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED };
        match self.work.charge(&delta, &self.budget) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.work.try_add_assign(&delta)?;
                Err(error.into())
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn reserve_native_work(&mut self, maximum: WorkCounters) -> Result<(), DemandError> {
        if maximum.peak_allocation_bytes != 0 || maximum.allocation_operations != 0 {
            return Err(DemandError::InvalidRequest);
        }
        if let Some(resource) = self.resource_scope() {
            return resource.reserve_work(maximum, || self.with_resource_scope(None, |work| work.reserve_native_work(maximum)));
        }
        self.admit(maximum)?;
        let budget = maximum.remaining(self.budget)?;
        let pending = self.cancellation_wait.pending_work.checked_add(maximum)?;
        self.budget = budget;
        self.cancellation_wait.pending_work = pending;
        Ok(())
    }
    /// Transfers already-withheld work into the existing item escrow. This is
    /// accounting custody only: no new allowance, work, or cancellation grant.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn partition_native_work_items(&mut self, items: u64) -> Result<(), DemandError> {
        let pending_work = self.cancellation_wait.pending_work.items_examined
            .checked_sub(items).ok_or(DemandError::InvalidRequest)?;
        let pending_items = self.cancellation_wait.pending_items.checked_add(items)
            .ok_or(crate::WorkError::Overflow)?;
        self.cancellation_wait.pending_work.items_examined = pending_work;
        self.cancellation_wait.pending_items = pending_items;
        Ok(())
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn refund_native_work(&mut self, unused: WorkCounters) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let original = self.with_resource_scope(None, |work| work.refund_native_work(unused));
            let scoped = resource.refund_work(unused);
            return original.and(scoped);
        }
        let pending = unused.remaining(self.cancellation_wait.pending_work)?;
        let budget = self.budget.checked_add(unused)?;
        self.cancellation_wait.pending_work = pending;
        self.budget = budget;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn record_native_work(&mut self, actual: WorkCounters) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let original = self.with_resource_scope(None, |work| work.record_native_work(actual));
            let scoped = resource.complete_work(actual);
            return original.and(scoped);
        }
        let restored = self.refund_native_work(actual);
        let recorded = self.record(actual);
        // This only records work against credit accepted before the action.
        // Cancellation must not prevent its truthful terminal bookkeeping;
        // permission for the physical/append action is verified separately.
        let invariant = if self.cancellation_wait.native_poisoned
            || self.cancellation_wait.observer_overflow.as_ref()
                .is_some_and(|overflow| overflow.entries.is_poisoned()) {
            Err(DemandError::OriginalWorkPoisoned)
        } else { Ok(()) };
        restored.and(recorded).and(invariant)
    }

    /// Spends accepted credit at an actual start boundary, never after a new
    /// root admission. Unlike completed-prefix recording, rejection here must
    /// leave all credit and counters untouched because no operation has begun.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn start_native_reserved_work(&mut self, actual: WorkCounters) -> Result<(), DemandError> {
        if self.cancellation_wait.native_poisoned
            || self.cancellation_wait.observer_overflow.as_ref()
                .is_some_and(|overflow| overflow.entries.is_poisoned()) {
            return Err(DemandError::OriginalWorkPoisoned);
        }
        let pending = actual.remaining(self.cancellation_wait.pending_work)?;
        let budget = self.budget.checked_add(actual)?;
        let work = self.work.checked_add(actual)?;
        work.verify(budget)?;
        self.cancellation_wait.pending_work = pending;
        self.budget = budget;
        self.work = work;
        Ok(())
    }

    /// Restores an original full budget without losing retained quota proofs.
    pub(crate) fn restore_budget(&mut self, budget: WorkBudget) {
        self.budget = budget;
        self.budget.items_examined = self.budget.items_examined
            .checked_sub(self.cancellation_wait.pending_items)
            .expect("original publication credits remain within its full grant");
        self.budget.items_returned = self.budget.items_returned
            .checked_sub(self.cancellation_wait.pending_outputs)
            .expect("original output credits remain within its full grant");
        match self.cancellation_wait.pending_work.remaining(self.budget) {
            Ok(effective) => self.budget = effective,
            Err(error) => {
                self.record_native_io_failure(error.into());
                self.poison_native_original_work();
            }
        }
        #[cfg(feature = "native-mount")]
        self.cancellation_wait.view_gates.budget_reconstructed();
    }

    pub(crate) fn into_current(mut self) -> (
        WorkCounters, crate::kernel::allocation::AllocationLedger,
        DemandCancellationRoots,
    ) {
        self.reconcile_cancellation_registry()
            .expect("original reserved registry cleanup remains accountable across ownership transfer");
        (self.work, std::mem::take(&mut self.allocations), std::mem::take(&mut self.cancellation_wait))
    }

    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub(crate) fn capture_custody(&self) -> Option<std::sync::Arc<CaptureTransientCustody>> {
        self.capture_custody.as_ref().and_then(CaptureCustodyBinding::owner)
    }

    /// Only direct ownership events in this exclusive synchronous leaf are
    /// captured. SDK receipt merges and original root reconciliation bypass
    /// the hook. The closure must not start an operative asynchronous loan.
    pub(crate) fn with_capture_custody<T>(
        &mut self, custody: &std::sync::Arc<CaptureTransientCustody>,
        run: impl FnOnce(&mut Self) -> T,
    ) -> T {
        self.capture_scope(Some(std::sync::Arc::clone(custody)), run)
    }

    /// Excludes borrowed authority and SDK-owned transfers from scratch data.
    pub(crate) fn without_capture_custody<T>(&mut self, run: impl FnOnce(&mut Self) -> T) -> T {
        self.capture_scope(None, run)
    }

    /// A synchronous leaf-lock guard restores the returned hook before
    /// unlocking or unwinding. Never leave a hook installed across SDK await.
    pub(crate) fn replace_capture_custody(
        &mut self, next: Option<std::sync::Arc<CaptureTransientCustody>>,
    ) -> Option<std::sync::Arc<CaptureTransientCustody>> {
        let previous = self.capture_custody();
        self.capture_custody = next.map(CaptureCustodyBinding::from_owner);
        previous
    }

    fn capture_scope<T>(
        &mut self, custody: Option<std::sync::Arc<CaptureTransientCustody>>,
        run: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let previous = self.replace_capture_custody(custody);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(self)));
        let _ = self.replace_capture_custody(previous);
        match result {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    pub(crate) fn check(&self) -> Result<(), DemandError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.cancellation_wait.native_poisoned || self.cancellation_wait.observer_overflow.as_ref()
            .is_some_and(|overflow| overflow.entries.is_poisoned()) {
            return Err(DemandError::OriginalWorkPoisoned);
        }
        self.cancellation.check().map_err(|_| DemandError::Cancelled)?;
        if self.parent_cancellation.as_ref().is_some_and(CancellationToken::is_cancelled) {
            return Err(DemandError::Cancelled);
        }
        if self.stop.as_ref().is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire)) {
            return Err(DemandError::Cancelled);
        }
        if let Some(invocation) = self.native_invocation.as_ref().and_then(std::sync::Weak::upgrade) {
            invocation.check()?;
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

    /// Borrows original controlled native change-map allocation proofs.
    #[cfg(feature = "native-mount")]
    pub(crate) fn view_ledger_admission_parts(&mut self) -> Result<(
        &mut crate::native_mount::ViewLedgerAdmissions,
        &mut crate::kernel::allocation::AllocationLedger,
        &mut WorkCounters,
        WorkBudget,
    ), DemandError> {
        self.reconcile_cancellation_registry()?;
        Ok((&mut self.cancellation_wait.view_ledgers, &mut self.allocations, &mut self.work, self.budget))
    }

    /// Restores an already admitted loan, including after cancellation/panic.
    #[cfg(feature = "native-mount")]
    pub(crate) fn restore_view_ledger_admission(
        &mut self, slot: usize, admission: crate::native_mount::ViewLedgerAdmission,
    ) {
        self.cancellation_wait.view_ledgers.restore(slot, admission);
    }

    /// Borrows only this invocation's physical native-lifecycle admissions.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_mount_admission_parts(&mut self) -> Result<(
        &mut NativeMountAllocationAdmissions,
        &mut crate::kernel::allocation::AllocationLedger,
        &mut WorkCounters,
        WorkBudget,
    ), DemandError> {
        self.reconcile_cancellation_registry()?;
        Ok((&mut self.cancellation_wait.native_mount_admissions, &mut self.allocations, &mut self.work, self.budget))
    }

    /// Completed physical ownership only: never permission for new work.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_mount_completed_admission_parts(&mut self) -> (
        &mut NativeMountAllocationAdmissions,
        &mut crate::kernel::allocation::AllocationLedger,
        &mut WorkCounters,
        WorkBudget,
    ) {
        (&mut self.cancellation_wait.native_mount_admissions, &mut self.allocations, &mut self.work, self.budget)
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
        let reconciled = self.reconcile_cancellation_registry().map_err(crate::cancellation::CancellationWaitError::Work);
        let finished = if let Some(admission) = self.cancellation_wait.for_prepared(&prepared) {
            prepared.finish(&mut self.allocations, &mut self.work, self.budget, admission)
        } else {
            assert_eq!(prepared.retained_waiter_bytes(), 0, "prepared cancellation retains its original root proof");
            prepared.finish(&mut self.allocations, &mut self.work, self.budget, &mut Default::default())
        };
        reconciled.and(finished)
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
        ).map(|_| ())
    }

    /// Imports actual completed private checkout controls through the same
    /// canonical observer-custody root, never a by-value checkout size.
    pub(crate) fn retain_checkout_control(
        &mut self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        let inserted = self.cancellation_wait.retain_observer(
            std::sync::Arc::clone(token), &mut self.allocations, &mut self.work, self.budget,
        )?;
        let metadata = if inserted {
            record_completed_retention(
                &mut self.allocations, &mut self.work, self.budget, token.logical_owned_bytes(),
            ).map_err(DemandError::from)
        } else { Ok(()) };
        let payload = self.cancellation_wait.record_observer_growth(
            &mut self.allocations, &mut self.work, self.budget,
        ).map_err(DemandError::from);
        metadata.and(payload)
    }

    pub(crate) fn prepare_watch_custody(&mut self) -> Result<PreparedWatchCustody, DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.cancellation_wait.prepare_watch(&mut self.allocations, &mut self.work, self.budget)
    }

    /// Imports completed backing without late registration allocation, scan,
    /// cancellation, or permission to create new payload.
    pub(crate) fn retain_prepared_watch_custody(
        &mut self, prepared: PreparedWatchCustody, token: &std::sync::Arc<SourceWatchAllocationCustody>,
    ) -> Result<(), DemandError> {
        self.cancellation_wait.retain_prepared_watch(
            prepared, token, &mut self.allocations, &mut self.work, self.budget,
        )
    }

    pub(crate) fn abandon_prepared_watch_custody(&mut self, prepared: PreparedWatchCustody) {
        self.cancellation_wait.abandon_prepared_watch(prepared);
    }

    /// Imports only this SDK call's actual new watch-registration backing.
    /// Already completed ownership is recorded even after cancellation.
    pub(crate) fn retain_watch_custody(
        &mut self, token: &std::sync::Arc<SourceWatchAllocationCustody>,
    ) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.cancellation_wait.retain_watch(
            token, &mut self.allocations, &mut self.work, self.budget,
        )
    }

    /// Borrows only the original accounting fields for synchronous algorithms.
    /// The returned references must not outlive their enclosing leaf lock.
    pub(crate) fn allocation_parts(&mut self) -> (
        &mut crate::kernel::allocation::AllocationLedger, &mut WorkCounters,
        WorkBudget, &CancellationToken,
    ) {
        (&mut self.allocations, &mut self.work, self.budget, &self.cancellation)
    }

    fn native_checkout_control_parts<'a>(
        &'a mut self, original: &'a std::sync::Arc<NativeDemandReadMeter>,
    ) -> (
        NativeCheckoutControlSink<'a>, &'a mut crate::kernel::allocation::AllocationLedger,
        &'a mut WorkCounters, WorkBudget, &'a CancellationToken,
    ) {
        let sink = NativeCheckoutControlSink { roots: &mut self.cancellation_wait, original };
        (sink, &mut self.allocations, &mut self.work, self.budget, &self.cancellation)
    }

    pub(crate) fn owns_native_checkout_token(
        &self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> bool {
        self.cancellation_wait.owns_native_control(token)
    }

    pub(crate) fn reconcile_native_controls(&mut self) -> Result<(), crate::WorkError> {
        let mut overflow = self.cancellation_wait.observer_overflow.as_ref().map(|overflow| overflow.lock());
        for custody in self.cancellation_wait.observers.iter_mut().flatten()
            .chain(overflow.iter_mut().flat_map(|overflow| overflow.iter_mut())) {
            custody.reconcile_native(&mut self.allocations)?;
        }
        Ok(())
    }

    pub(crate) fn receipt(&mut self) -> WorkCounters {
        self.reconcile_native_controls().expect("original native constructor claims remain balanced");
        self.work
    }

    pub(crate) fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            resource.admit(delta, 0)?;
            return self.with_resource_scope(None, |work| work.admit(delta));
        }
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.work.admit(&delta, &self.budget)?;
        Ok(())
    }

    pub(crate) fn admit_retained(&mut self, bytes: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            resource.admit(WorkCounters::UNCHARGED, bytes)?;
            return self.with_resource_scope(None, |work| work.admit_retained(bytes));
        }
        self.reconcile_cancellation_registry()?;
        self.check()?;
        let peak = self.allocations.live_bytes().checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.work.admit(&WorkCounters { peak_allocation_bytes: peak, ..WorkCounters::UNCHARGED }, &self.budget)?;
        Ok(())
    }

    /// Retains completed work alongside the invocation's still-live payloads.
    /// Cancellation cannot erase work that has already physically completed.
    pub(crate) fn spent(&mut self, mut delta: WorkCounters) -> Result<(), DemandError> {
        let resource = self.resource_scope().map_or(Ok(()), |resource| resource.completed(delta, 0));
        let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
        delta.peak_allocation_bytes = delta.peak_allocation_bytes
            .checked_add(self.allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
        self.work.try_add_assign(&delta)?;
        reconciled.and(resource)
    }

    pub(crate) fn charge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.admission(delta, 0, || self.with_resource_scope(None, |work| work.charge(delta)));
        }
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.work.charge(&delta, &self.budget)?;
        Ok(())
    }

    /// Records admitted work that already completed, even if cancellation
    /// arrived during its native call. Actual work remains in the receipt when
    /// reconciliation or final budget verification returns an error.
    pub(crate) fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        let resource = self.resource_scope().map_or(Ok(()), |resource| resource.completed(delta, 0));
        let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
        let recorded = self.work.try_add_assign(&delta)
            .and_then(|()| self.work.verify(self.budget)).map_err(DemandError::from);
        reconciled.and(recorded).and(resource)
    }

    /// The full current item grant, before withholding original delivery
    /// credits. Budget restoration must snapshot this instead of the shadow.
    pub(crate) fn budget_before_pending_items(&self) -> WorkBudget {
        let mut budget = self.budget;
        budget.items_examined = budget.items_examined
            .checked_add(self.cancellation_wait.pending_items)
            .expect("withheld item credits came from this original grant");
        budget.items_returned = budget.items_returned
            .checked_add(self.cancellation_wait.pending_outputs)
            .expect("withheld output credits came from this original grant");
        budget.checked_add(self.cancellation_wait.pending_work)
            .expect("original additive completion credits came from this grant")
    }

    pub(crate) fn reserve_pending_outputs(&mut self, items: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.reserve_credit(crate::materializer::NativeResourceCredit::ReturnedItems, items,
                || self.with_resource_scope(None, |work| work.reserve_pending_outputs(items)));
        }
        self.admit(WorkCounters { items_returned: items, ..WorkCounters::UNCHARGED })?;
        let reserved = self.cancellation_wait.pending_outputs.checked_add(items).ok_or(crate::WorkError::Overflow)?;
        let budget = self.budget.items_returned.checked_sub(items).ok_or(crate::WorkError::Overflow)?;
        self.cancellation_wait.pending_outputs = reserved;
        self.budget.items_returned = budget;
        Ok(())
    }

    pub(crate) fn record_pending_outputs(&mut self, items: u64) -> Result<(), DemandError> {
        if items > self.cancellation_wait.pending_outputs { return Err(DemandError::InvalidRequest); }
        let resource = self.resource_scope();
        self.with_resource_scope(None, |work| {
            work.work.try_add_assign(&WorkCounters { items_returned: items, ..WorkCounters::UNCHARGED })?;
            work.refund_pending_outputs(items)
        })?;
        resource.map_or(Ok(()), |resource| resource.complete_credit(
            crate::materializer::NativeResourceCredit::ReturnedItems, items, items))
    }

    pub(crate) fn refund_pending_outputs(&mut self, items: u64) -> Result<(), DemandError> {
        let reserved = self.cancellation_wait.pending_outputs.checked_sub(items).ok_or(DemandError::InvalidRequest)?;
        let budget = self.budget.items_returned.checked_add(items).ok_or(crate::WorkError::Overflow)?;
        self.cancellation_wait.pending_outputs = reserved;
        self.budget.items_returned = budget;
        self.resource_scope().map_or(Ok(()), |resource| resource.refund_credit(
            crate::materializer::NativeResourceCredit::ReturnedItems, items))
    }

    pub(crate) fn reserve_pending_items(&mut self, items: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.reserve_credit(crate::materializer::NativeResourceCredit::Items, items,
                || self.with_resource_scope(None, |work| work.reserve_pending_items(items)));
        }
        self.admit(WorkCounters { items_examined: items, ..WorkCounters::UNCHARGED })?;
        let reserved = self.cancellation_wait.pending_items.checked_add(items)
            .ok_or(crate::WorkError::Overflow)?;
        let budget = self.budget.items_examined.checked_sub(items)
            .ok_or(crate::WorkError::Overflow)?;
        self.cancellation_wait.pending_items = reserved;
        self.budget.items_examined = budget;
        Ok(())
    }

    /// Completes a real visit against its pre-admitted original credit.
    /// Cancellation cannot revoke mandatory delivery after an accepted seal.
    pub(crate) fn record_pending_items(&mut self, items: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let recorded = self.with_resource_scope(None, |work| work.record_pending_items(items));
            return recorded.and(resource.complete_credit(crate::materializer::NativeResourceCredit::Items, items, items));
        }
        if items > self.cancellation_wait.pending_items {
            return Err(DemandError::InvalidRequest);
        }
        self.work.try_add_assign(&WorkCounters {
            items_examined: items, ..WorkCounters::UNCHARGED
        })?;
        self.refund_pending_items(items);
        Ok(())
    }

    pub(crate) fn refund_pending_items(&mut self, items: u64) {
        if let Some(resource) = self.resource_scope() {
            self.with_resource_scope(None, |work| work.refund_pending_items(items));
            if let Err(error) = resource.refund_credit(crate::materializer::NativeResourceCredit::Items, items) {
                self.record_native_io_failure(error);
            }
            return;
        }
        self.cancellation_wait.pending_items = self.cancellation_wait.pending_items
            .checked_sub(items).expect("only this original publication owns its credits");
        self.budget.items_examined = self.budget.items_examined.checked_add(items)
            .expect("returning withheld credits restores the original grant");
    }

    pub(crate) fn reserve_copy(&mut self, maximum: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            return resource.reserve_credit(crate::materializer::NativeResourceCredit::Copy, maximum,
                || self.with_resource_scope(None, |work| work.reserve_copy(maximum)));
        }
        self.admit(WorkCounters { bytes_copied: maximum, ..WorkCounters::UNCHARGED })?;
        self.budget.bytes_copied = self.budget.bytes_copied.checked_sub(maximum)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(())
    }

    pub(crate) fn cancel_reserved_copy(&mut self, maximum: u64) {
        if let Some(resource) = self.resource_scope() {
            self.with_resource_scope(None, |work| work.cancel_reserved_copy(maximum));
            if let Err(error) = resource.refund_credit(crate::materializer::NativeResourceCredit::Copy, maximum) {
                self.record_native_io_failure(error);
            }
            return;
        }
        self.budget.bytes_copied = self.budget.bytes_copied.checked_add(maximum)
            .expect("copy reservation restores only its own withheld allowance");
    }

    pub(crate) fn record_reserved_copy(&mut self, maximum: u64, actual: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let recorded = self.with_resource_scope(None, |work| work.record_reserved_copy(maximum, actual));
            return recorded.and(resource.complete_credit(crate::materializer::NativeResourceCredit::Copy, maximum, actual));
        }
        self.cancel_reserved_copy(maximum);
        let recorded = self.record(WorkCounters { bytes_copied: actual, ..WorkCounters::UNCHARGED });
        if actual > maximum { return Err(crate::WorkError::Overflow.into()); }
        recorded
    }

    pub(crate) fn record_native_io_failure(&mut self, error: DemandError) -> std::io::Error {
        if self.cancellation_wait.native_io_failure.is_none() {
            self.cancellation_wait.native_io_failure = Some(error);
        }
        // This is a terminal protocol marker, not an interrupted OS syscall.
        // Retrying Interrupted must not spin a cancelled native SDK reader.
        if matches!(self.cancellation_wait.native_io_failure, Some(DemandError::Allocation(_))) {
            std::io::ErrorKind::OutOfMemory.into()
        } else {
            std::io::ErrorKind::Other.into()
        }
    }

    pub(crate) fn take_native_io_failure(&mut self) -> Option<DemandError> {
        self.cancellation_wait.native_io_failure.take()
    }

    fn native_operation_credit(kind: acyclic_native_runtime::NativeOperationKind) -> crate::materializer::NativeResourceCredit {
        match kind {
            acyclic_native_runtime::NativeOperationKind::Read => crate::materializer::NativeResourceCredit::ReadOperations,
            acyclic_native_runtime::NativeOperationKind::Write => crate::materializer::NativeResourceCredit::WriteOperations,
            acyclic_native_runtime::NativeOperationKind::Durability => crate::materializer::NativeResourceCredit::DurabilityOperations,
        }
    }

    fn native_byte_credit(kind: acyclic_native_runtime::NativeOperationKind) -> Option<crate::materializer::NativeResourceCredit> {
        match kind {
            acyclic_native_runtime::NativeOperationKind::Read => Some(crate::materializer::NativeResourceCredit::ReadBytes),
            acyclic_native_runtime::NativeOperationKind::Write => Some(crate::materializer::NativeResourceCredit::WriteBytes),
            acyclic_native_runtime::NativeOperationKind::Durability => None,
        }
    }

    pub(crate) fn reserve_native_operations(
        &mut self, kind: acyclic_native_runtime::NativeOperationKind, count: u64,
    ) -> std::io::Result<()> {
        if let Some(resource) = self.resource_scope() {
            let credit = Self::native_operation_credit(kind);
            let result = resource.reserve_credit(credit, count, || self.with_resource_scope(None,
                |work| work.reserve_native_operations(kind, count).map_err(DemandError::Io)));
            return result.map_err(|error| self.record_native_io_failure(error));
        }
        self.check().map_err(|error| self.record_native_io_failure(error))?;
        let delta = match kind {
            acyclic_native_runtime::NativeOperationKind::Read => WorkCounters {
                backend_read_operations: count, ..WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Write => WorkCounters {
                backend_write_operations: count, ..WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Durability => WorkCounters {
                durability_operations: count, ..WorkCounters::UNCHARGED
            },
        };
        self.work.admit(&delta, &self.budget)
            .map_err(|error| self.record_native_io_failure(error.into()))?;
        if kind == acyclic_native_runtime::NativeOperationKind::Durability {
            self.cancellation_wait.pending_native_durability += count;
        }
        let allowance = match kind {
            acyclic_native_runtime::NativeOperationKind::Read => &mut self.budget.backend_read_operations,
            acyclic_native_runtime::NativeOperationKind::Write => &mut self.budget.backend_write_operations,
            acyclic_native_runtime::NativeOperationKind::Durability => &mut self.budget.durability_operations,
        };
        *allowance -= count;
        Ok(())
    }

    pub(crate) fn cancel_reserved_native_operations(
        &mut self, kind: acyclic_native_runtime::NativeOperationKind, count: u64,
    ) {
        if let Some(resource) = self.resource_scope() {
            self.with_resource_scope(None, |work| work.cancel_reserved_native_operations(kind, count));
            if let Err(error) = resource.refund_credit(Self::native_operation_credit(kind), count) {
                self.record_native_io_failure(error);
            }
            return;
        }
        if kind == acyclic_native_runtime::NativeOperationKind::Durability {
            self.cancellation_wait.pending_native_durability = self.cancellation_wait.pending_native_durability
                .checked_sub(count).expect("durability completion owns its original reservation");
        }
        let allowance = match kind {
            acyclic_native_runtime::NativeOperationKind::Read => &mut self.budget.backend_read_operations,
            acyclic_native_runtime::NativeOperationKind::Write => &mut self.budget.backend_write_operations,
            acyclic_native_runtime::NativeOperationKind::Durability => &mut self.budget.durability_operations,
        };
        *allowance = allowance.checked_add(count)
            .expect("unused native reservation restores only its own withheld allowance");
    }

    pub(crate) fn record_reserved_native_operation(
        &mut self, kind: acyclic_native_runtime::NativeOperationKind,
    ) -> std::io::Result<()> {
        if let Some(resource) = self.resource_scope() {
            let recorded = self.with_resource_scope(None, |work| work.record_reserved_native_operation(kind));
            let resource = resource.complete_credit(Self::native_operation_credit(kind), 1, 1)
                .map_err(|error| self.record_native_io_failure(error));
            return recorded.and(resource);
        }
        self.cancel_reserved_native_operations(kind, 1);
        let delta = match kind {
            acyclic_native_runtime::NativeOperationKind::Read => WorkCounters {
                backend_read_operations: 1, ..WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Write => WorkCounters {
                backend_write_operations: 1, ..WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Durability => WorkCounters {
                durability_operations: 1, ..WorkCounters::UNCHARGED
            },
        };
        self.work.try_add_assign(&delta)
            .and_then(|()| self.work.verify(self.budget))
            .map_err(|error| self.record_native_io_failure(error.into()))
    }

    pub(crate) fn admit_native_write_bytes(&mut self, maximum: usize) -> std::io::Result<()> {
        if let Some(resource) = self.resource_scope() {
            resource.admit(WorkCounters { output_bytes: maximum as u64, ..WorkCounters::UNCHARGED }, 0)
                .map_err(|error| self.record_native_io_failure(error))?;
            return self.with_resource_scope(None, |work| work.admit_native_write_bytes(maximum));
        }
        self.check().map_err(|error| self.record_native_io_failure(error))?;
        self.work.admit(&WorkCounters { output_bytes: maximum as u64, ..WorkCounters::UNCHARGED }, &self.budget)
            .map_err(|error| self.record_native_io_failure(error.into()))
    }

    pub(crate) fn reserve_native_bytes(
        &mut self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64,
    ) -> std::io::Result<()> {
        if let Some(resource) = self.resource_scope() {
            let credit = Self::native_byte_credit(kind).ok_or(std::io::ErrorKind::Unsupported)?;
            let result = resource.reserve_credit(credit, maximum, || self.with_resource_scope(None,
                |work| work.reserve_native_bytes(kind, maximum).map_err(DemandError::Io)));
            return result.map_err(|error| self.record_native_io_failure(error));
        }
        self.check().map_err(|error| self.record_native_io_failure(error))?;
        let delta = match kind {
            acyclic_native_runtime::NativeOperationKind::Read => WorkCounters {
                source_bytes_read: maximum, ..WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Write => WorkCounters {
                output_bytes: maximum, ..WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Durability => return Err(std::io::ErrorKind::Unsupported.into()),
        };
        self.work.admit(&delta, &self.budget)
            .map_err(|error| self.record_native_io_failure(error.into()))?;
        let allowance = match kind {
            acyclic_native_runtime::NativeOperationKind::Read => &mut self.budget.source_bytes_read,
            acyclic_native_runtime::NativeOperationKind::Write => &mut self.budget.output_bytes,
            acyclic_native_runtime::NativeOperationKind::Durability => unreachable!("durability does not reserve bytes"),
        };
        *allowance -= maximum;
        Ok(())
    }

    pub(crate) fn cancel_reserved_native_bytes(
        &mut self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64,
    ) {
        if let Some(resource) = self.resource_scope() {
            self.with_resource_scope(None, |work| work.cancel_reserved_native_bytes(kind, maximum));
            if let Some(credit) = Self::native_byte_credit(kind) {
                if let Err(error) = resource.refund_credit(credit, maximum) { self.record_native_io_failure(error); }
            }
            return;
        }
        let allowance = match kind {
            acyclic_native_runtime::NativeOperationKind::Read => &mut self.budget.source_bytes_read,
            acyclic_native_runtime::NativeOperationKind::Write => &mut self.budget.output_bytes,
            acyclic_native_runtime::NativeOperationKind::Durability => unreachable!("durability does not reserve bytes"),
        };
        *allowance = allowance.checked_add(maximum)
            .expect("unused native byte reservation restores only its withheld allowance");
    }

    pub(crate) fn record_reserved_native_bytes(
        &mut self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64,
    ) -> std::io::Result<()> {
        if let Some(resource) = self.resource_scope() {
            let credit = Self::native_byte_credit(kind).ok_or(std::io::ErrorKind::Unsupported)?;
            let recorded = self.with_resource_scope(None, |work| work.record_reserved_native_bytes(kind, maximum, actual));
            let resource = resource.complete_credit(credit, maximum, actual)
                .map_err(|error| self.record_native_io_failure(error));
            return recorded.and(resource);
        }
        if kind == acyclic_native_runtime::NativeOperationKind::Durability {
            return Err(std::io::ErrorKind::Unsupported.into());
        }
        self.cancel_reserved_native_bytes(kind, maximum);
        let delta = match kind {
            acyclic_native_runtime::NativeOperationKind::Read => WorkCounters {
                source_bytes_read: actual, ..WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Write => WorkCounters {
                output_bytes: actual, ..WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Durability => unreachable!("durability does not reserve bytes"),
        };
        self.work.try_add_assign(&delta)
            .map_err(|error| self.record_native_io_failure(error.into()))?;
        if actual > maximum {
            return Err(self.record_native_io_failure(DemandError::InvalidRequest));
        }
        self.work.verify(self.budget).map_err(|error| self.record_native_io_failure(error.into()))
    }

    pub(crate) fn allocate(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.claim_allocation(bytes, u64::from(bytes != 0))
    }

    /// Claims the actual producer's allocation attempts before its allocator.
    pub(crate) fn claim_allocation(&mut self, bytes: u64, operations: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let delta = WorkCounters { allocation_operations: operations, ..WorkCounters::UNCHARGED };
            return resource.admission(delta, bytes, || self.with_resource_scope(None, |work| work.claim_allocation(bytes, operations)));
        }
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.allocations
            .claim_bytes(bytes, operations, &mut self.work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        if let Some(custody) = self.capture_custody() { custody.claim(bytes); }
        Ok(())
    }

    /// One numeric permit for the actual allocation and its immediately
    /// following infallible copy/item work. No allocator or callback runs here.
    pub(crate) fn claim_native_owned_with_work(
        &mut self, bytes: u64, operations: u64, delta: WorkCounters,
        capture: &Option<std::sync::Arc<CaptureTransientCustody>>,
    ) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        let mut allocations = self.allocations;
        let mut work = self.work;
        work.charge(&delta, &self.budget)?;
        allocations.claim_bytes(bytes, operations, &mut work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        self.allocations = allocations;
        self.work = work;
        if let Some(capture) = capture { capture.claim(bytes); }
        Ok(())
    }


    fn allocate_for(&mut self, bytes: u64, delta: WorkCounters) -> Result<(), DemandError> {
        self.work.admit(&delta, &self.budget)?;
        self.allocate(bytes)?;
        self.record(delta)
    }

    pub(crate) fn adopt(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.adopt_retained(bytes)
    }

    /// Adjusts an existing buffer guard's retained quota without cancellation.
    /// It still admits exact live bytes under the original finite allowance;
    /// this is not permission for an allocator or a new native operation.
    pub(crate) fn adopt_retained(&mut self, bytes: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let delta = WorkCounters::UNCHARGED;
            return resource.admission(delta, bytes, || self.with_resource_scope(None, |work| work.adopt_retained(bytes)));
        }
        self.allocations.claim_bytes(bytes, 0, &mut self.work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        if let Some(custody) = self.capture_custody() { custody.claim(bytes); }
        Ok(())
    }
    /// Reconciles a concrete owner's completed backing before any late refusal.
    /// On failure, `allocation` still contains all physically retained bytes.
    pub(crate) fn reconcile_native_allocation(
        &mut self, allocation: &mut u64, actual: u64,
    ) -> Result<(), DemandError> {
        if actual < *allocation {
            self.release(*allocation - actual)?;
            *allocation = actual;
        }
        let (claimed, result) = self.record_native_completed_adoption(actual - *allocation);
        *allocation += claimed;
        result
    }

    /// Records actual completed backing before any late budget/cancellation
    /// refusal. The returned byte count is already owned even on failure and
    /// must remain in the concrete buffer's guard until its physical drop.
    pub(crate) fn record_native_completed_adoption(
        &mut self, bytes: u64,
    ) -> (u64, Result<(), DemandError>) {
        if let Err(error) = self.allocations.record_owned_bytes(bytes, &mut self.work) {
            return (0, Err(error.into_work_or(DemandError::InvalidRequest)));
        }
        if let Some(custody) = self.capture_custody() { custody.claim(bytes); }
        let resource = self.resource_scope().map_or(Ok(()), |resource| resource.completed(
            WorkCounters::UNCHARGED, bytes,
        ));
        let result = self.check().and_then(|()| self.work.verify(self.budget).map_err(DemandError::from)).and(resource);
        (bytes, result)
    }


    /// Records known, already-completed ownership even after cancellation.
    /// This is not admission for creating or allocating new data.
    pub(crate) fn record_adoption(&mut self, bytes: u64) -> Result<(), DemandError> {
        let resource = self.resource_scope().map_or(Ok(()), |resource| resource.completed(
            WorkCounters::UNCHARGED, bytes,
        ));
        let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
        let recorded = record_completed_retention(&mut self.allocations, &mut self.work, self.budget, bytes)
            .map_err(DemandError::from);
        reconciled.and(recorded).and(resource)
    }

    /// Records only an explicitly known completed capture scratch buffer.
    /// SDK proof/root custody continues to use `record_adoption` without tags.
    pub(crate) fn record_capture_adoption(&mut self, bytes: u64) -> Result<(), DemandError> {
        let resource = self.resource_scope().map_or(Ok(()), |resource| resource.completed(
            WorkCounters::UNCHARGED, bytes,
        ));
        let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
        let prior = self.allocations.live_bytes();
        let recorded = record_completed_retention(&mut self.allocations, &mut self.work, self.budget, bytes)
            .map_err(DemandError::from);
        if prior.checked_add(bytes) == Some(self.allocations.live_bytes()) {
            if let Some(custody) = self.capture_custody() { custody.claim(bytes); }
        }
        reconciled.and(recorded).and(resource)
    }

    pub(crate) fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource_scope() {
            let released = self.with_resource_scope(None, |work| work.release(bytes));
            return released.and(resource.retire(bytes));
        }
        self.allocations
            .release(bytes)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        if let Some(custody) = self.capture_custody() { custody.release(bytes); }
        Ok(())
    }

    pub(crate) fn remaining(&mut self) -> Result<WorkBudget, DemandError> {
        self.reconcile_native_controls()?;
        let mut budget = self.work.remaining(self.budget)?;
        budget.peak_allocation_bytes = budget.peak_allocation_bytes
            .checked_sub(self.live_allocation_bytes())
            .and_then(|bytes| bytes.checked_sub(self.allocations.reserved_bytes()))
            .ok_or(crate::WorkError::Overflow)?;
        if let Some(resource) = self.resource_scope() { budget = budget.intersect_budget(resource.remaining()?); }
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

    /// The caller captures its original live bytes before invoking the same
    /// SDK watch. Compose the real old/new overlap before retiring dead roots.
    pub(crate) fn merge_with_prior_live(
        &mut self, spent: WorkCounters, prior_live: u64,
    ) -> Result<(), DemandError> {
        self.work = self.work.with_backend(spent, prior_live)?;
        self.reconcile_cancellation_registry()?;
        self.work.verify(self.budget)?;
        Ok(())
    }

    /// Completed native callbacks have already quiesced. Fold their actual
    /// prefix without cancellation, allocations or unrelated root effects.
    pub(crate) fn merge_completed_native_read(&mut self, spent: WorkCounters) -> Result<(), DemandError> {
        self.work = self.work.with_backend(spent, self.allocations.live_bytes())?;
        self.work.verify(self.budget)?;
        Ok(())
    }

    pub(crate) fn finish<T>(mut self, result: Result<T, DemandError>) -> DemandResult<T> {
        let reconciled = self.reconcile_cancellation_registry().map_err(DemandError::from);
        reconciled.and(result).map(|value| OperationReceipt { value, work: self.work })
            .map_err(|error| OperationFailure::new(error, self.work))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for DemandWork {
    fn drop(&mut self) {
        self.reconcile_native_controls().expect("final original native retirement remains balanced");
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn lowercase_unicode_with_work(text: &str, work: &mut DemandWork) -> Result<Vec<u8>, DemandError> {
    work.reconcile_cancellation_registry()?;
    work.check()?;
    let bytes = crate::kernel::types::lowercase_unicode_with_ledger(
        text, &mut work.allocations, &mut work.work, work.budget, &work.cancellation,
    )?;
    if let Err(error) = work.check() {
        let retained = bytes.capacity() as u64;
        drop(bytes);
        work.release(retained)?;
        return Err(error);
    }
    Ok(bytes)
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
fn namespace_owned_bytes(path: &NamespacePath) -> Result<u64, DemandError> {
    Ok(path.retained_bytes())
}

#[cfg(not(target_arch = "wasm32"))]
fn clone_namespace<W: SourceDemandReadWork>(path: &NamespacePath, work: &mut W) -> Result<NamespacePath, DemandError> {
    use crate::kernel::{NamespacePathStep, ReadAllocationStep};
    work.check()?;
    let cancellation = work.with_original_leaf(|work| work.cancellation().clone());
    let mut owned_bytes = 0_u64;
    let mut copy = None;
    let mut item = None;
    let mut hash = None;
    let mut event = None;
    let cloned = path.clone_with_steps(&cancellation, &mut |step| {
        match step {
            NamespacePathStep::Allocation(ReadAllocationStep::Allocate { bytes, operations }) => {
                work.claim_allocation(bytes, operations)?;
                owned_bytes = owned_bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                Ok(())
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                let mut allocation = admitted_bytes;
                let completed = work.reconcile_allocation(&mut allocation, actual_bytes);
                owned_bytes = owned_bytes.checked_sub(admitted_bytes)
                    .and_then(|owned| owned.checked_add(allocation)).ok_or(crate::WorkError::Overflow)?;
                completed
            }
            NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)) => Err(DemandError::Allocation(error)),
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeCopy(bytes)) => {
                if copy.is_some() { return Err(DemandError::InvalidRequest); }
                copy = work.reserve_source_copy(bytes)?;
                Ok(())
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Copied(bytes)) => {
                if let Some(reservation) = copy.take() {
                    reservation.record(bytes).map_err(crate::native_name::original_name_error)
                } else {
                    work.record(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED })
                }
            }
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeHash(bytes)) => {
                if hash.is_some() { return Err(DemandError::InvalidRequest); }
                work.check()?;
                let delta = WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED };
                if let Some(source) = work.native_source_work() {
                    hash = Some(source.materialization_work()?.reserve_work(delta)
                        .map_err(crate::native_name::original_name_error)?);
                    Ok(())
                } else {
                    work.admit(delta)
                }
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Hashed(bytes)) => {
                let delta = WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED };
                if let Some(mut reservation) = hash.take() {
                    reservation.record_completed(delta).map_err(crate::native_name::original_name_error)?;
                } else {
                    work.record(delta)?;
                }
                work.check()
            }
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeItem) => {
                if item.is_some() { return Err(DemandError::InvalidRequest); }
                let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                if let Some(source) = work.native_source_work() {
                    item = Some(source.materialization_work()?.reserve_work(delta)
                        .map_err(crate::native_name::original_name_error)?);
                    Ok(())
                } else {
                    work.admit(delta)
                }
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Item) => {
                let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                if let Some(mut reservation) = item.take() {
                    reservation.record_completed(delta).map_err(crate::native_name::original_name_error)
                } else {
                    work.record(delta)
                }
            }
            NamespacePathStep::Admit(delta) => {
                if event.is_some() { return Err(DemandError::InvalidRequest); }
                if let Some(source) = work.native_source_work() {
                    event = Some(source.materialization_work()?.reserve_work(*delta)
                        .map_err(crate::native_name::original_name_error)?);
                    Ok(())
                } else {
                    work.admit(*delta)
                }
            }
            NamespacePathStep::Charge(delta) => work.charge(*delta),
            NamespacePathStep::Performed(delta) => {
                if let Some(mut reservation) = event.take() {
                    reservation.record_completed(*delta).map_err(crate::native_name::original_name_error)
                } else {
                    work.record(*delta)
                }
            }
        }
    });
    let cloned = match cloned {
        Ok(cloned) => cloned,
        Err(error) => {
            // Cleanup is attempted; the first producer failure remains primary.
            let _cleanup = work.release(owned_bytes);
            return Err(error);
        }
    };
    if let Err(error) = work.check() {
        drop(cloned);
        let _cleanup = work.release(owned_bytes);
        return Err(error);
    }
    Ok(cloned)
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
/// Providers are trusted cooperative Rust implementations: this contract is
/// not a sandbox for arbitrary providers that disregard admission. Native and
/// filtered producers enforce logical owned payload bounds, not process RSS or
/// opaque allocator metadata. Owned chunk capacity is witnessed by the producer.
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

    /// Native directory projection against its original invocation ledger.
    /// Unsupported providers fail before IO rather than running a child meter
    /// over buffers whose custody belongs to the original invocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn list_page_with_native_work(
        &self, source: SourceReference, directory: &NamespacePath,
        cursor: Option<SourceCursor>, maximum_entries: u32, work: NativeSourceWork<'_>,
    ) -> impl Future<Output = Result<SourceDirectoryPage, DemandError>> + Send {
        async move {
            crate::native_host::NativeHostWork::check(&work)?;
            let _ = (source, directory, cursor, maximum_entries);
            Err(DemandError::NativeWorkUnavailable)
        }
    }

    /// Native lookup using the original operation's ledger and cancellation.
    #[cfg(not(target_arch = "wasm32"))]
    fn lookup_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, work: NativeSourceWork<'_>,
    ) -> impl Future<Output = Result<Option<SourceNode>, DemandError>> + Send {
        async move {
            crate::native_host::NativeHostWork::check(&work)?;
            let _ = (source, path);
            Err(DemandError::NativeWorkUnavailable)
        }
    }

    /// Reads a native range whose buffer is admitted on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_range_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
        offset: u64, length: u64, work: NativeSourceWork<'_>,
    ) -> impl Future<Output = Result<crate::kernel::OwnedSourceChunk, DemandError>> + Send {
        async move {
            crate::native_host::NativeHostWork::check(&work)?;
            let _ = (source, path, expected, offset, length);
            Err(DemandError::NativeWorkUnavailable)
        }
    }

    /// Reads a native link without substituting a child allocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_link_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
        maximum_bytes: u64, work: NativeSourceWork<'_>,
    ) -> impl Future<Output = Result<crate::kernel::OwnedSourceChunk, DemandError>> + Send {
        async move {
            crate::native_host::NativeHostWork::check(&work)?;
            let _ = (source, path, expected, maximum_bytes);
            Err(DemandError::NativeWorkUnavailable)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open_file_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
        work: NativeSourceWork<'_>,
    ) -> impl Future<Output = Result<Box<dyn DemandFile>, DemandError>> + Send {
        async move {
            crate::native_host::NativeHostWork::check(&work)?;
            let _ = (source, path, expected);
            Err(DemandError::NativeWorkUnavailable)
        }
    }

    /// Reads exact native metadata using the same original lookup engine.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_metadata_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, expected: Option<SourceVersion>,
        work: NativeSourceWork<'_>,
    ) -> impl Future<Output = Result<Option<SourceNode>, DemandError>> + Send {
        async move {
            let mut node = self.lookup_with_native_work(source, path, work).await?;
            crate::native_host::NativeHostWork::check(&work)?;
            if let Some(expected) = expected
                && node.as_ref().map(|node| node.version) != Some(expected)
            {
                return Err(DemandError::StaleVersion);
            }
            if let Some(node) = &mut node
                && node.kind == SourceNodeKind::RegularFile
            {
                node.link_count = None;
            }
            Ok(node)
        }
    }

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

    /// Starts this source's real subscription on the original native ledger.
    /// No provider is silently called through an unbounded ordinary default.
    #[cfg(not(target_arch = "wasm32"))]
    fn watch_with_native_work(
        &self, _sink: std::sync::Arc<dyn SourceChangeSink>, work: NativeSourceWork<'_>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        crate::native_host::NativeHostWork::check(&work)?;
        Err(DemandError::NativeWorkUnavailable)
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

    #[cfg(not(target_arch = "wasm32"))]
    fn read_range_with_native_work(
        &self, _offset: u64, _length: u64,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        crate::native_host::NativeHostWork::check(&NativeSourceWork::from_native_materialization_work(work))?;
        Err(DemandError::NativeWorkUnavailable)
    }
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

    /// Observes one directory using the caller's exact original invocation.
    ///
    /// # Errors
    /// Unknown observers fail before I/O rather than create a child ledger.
    fn observe_directory_with_native_work(
        &self, _directory: &NamespacePath, work: NativeSourceWork<'_>,
    ) -> Result<(), DemandError> {
        crate::native_host::NativeHostWork::check(&work)?;
        Err(DemandError::NativeWorkUnavailable)
    }
}

/// A composable source view that hides exact subtrees before they enter lazy
/// observation state. This is the SDK boundary used for host-private entries
/// such as a physical checkout's `.git` directory.
#[derive(Clone)]
pub struct FilteredDemandSource<D> {
    inner: D,
    excluded: std::sync::Arc<FilteredExcludedPaths>,
}

struct FilteredExcludedPaths {
    roots: Vec<NamespacePath>,
    // Field order is intentional: physical path/array data drops before its
    // original claim. Clones share this payload, never duplicate its custody.
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<crate::materializer::NativeAllocation>,
}

impl std::ops::Deref for FilteredExcludedPaths {
    type Target = Vec<NamespacePath>;

    fn deref(&self) -> &Self::Target { &self.roots }
}

impl<D> FilteredDemandSource<D> {
    /// Wraps canonical exclusions during explicitly unbounded startup.
    /// Finite native operations use `new_with_native_work` instead.
    #[must_use]
    pub fn new(inner: D, excluded: Vec<NamespacePath>) -> Self {
        Self::from_excluded(inner, FilteredExcludedPaths {
            roots: excluded,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        })
    }

    fn from_excluded(inner: D, excluded: FilteredExcludedPaths) -> Self {
        Self { inner, excluded: std::sync::Arc::new(excluded) }
    }

    /// Transfers already admitted exclusion data into an original native view.
    ///
    /// # Errors
    /// Rejects foreign or insufficient payload custody before allocating the
    /// shared owner, and preserves the original typed allocation/CPU denial.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        inner: D, excluded: Vec<NamespacePath>,
        excluded_custody: crate::materializer::NativeAllocation,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Self, crate::materializer::NativeTreeMaterializationError> {
        use crate::materializer::NativeTreeMaterializationError;
        let mut excluded = FilteredExcludedPaths {
            roots: excluded, custody: Some(excluded_custody),
        };
        let custody = excluded.custody.as_mut().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        if !custody.work().shares_ledger(work) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut bytes = (excluded.roots.capacity() as u64)
            .checked_mul(std::mem::size_of::<NamespacePath>() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        for path in &excluded.roots {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            for _ in path.components() {
                work.charge(WorkCounters {
                    items_examined: 1, source_path_components: 1, ..WorkCounters::UNCHARGED
                })?;
            }
            bytes = bytes.checked_add(path.retained_bytes()).ok_or(crate::WorkError::Overflow)?;
        }
        if custody.bytes() < bytes {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut shared_owner = work.claim(std::mem::size_of::<FilteredExcludedPaths>() as u64)?;
        custody.absorb(&mut shared_owner)?;
        Ok(Self::from_excluded(inner, excluded))
    }

    /// Retains an enclosing SDK owner allocation on this same shared payload.
    ///
    /// # Errors
    /// Rejects foreign custody, an ordinary startup view, or an already shared
    /// payload without changing either the retained or incoming claim.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn retain_owner_allocation(
        &mut self, owner: &mut crate::materializer::NativeAllocation,
    ) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
        use crate::materializer::NativeTreeMaterializationError;
        let excluded = std::sync::Arc::get_mut(&mut self.excluded)
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let custody = excluded.custody.as_mut().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        custody.absorb(owner)
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

    #[cfg(not(target_arch = "wasm32"))]
    fn excludes_original(
        &self, path: &NamespacePath, work: &mut NativeSourceWork<'_>,
    ) -> Result<bool, DemandError> {
        for root in self.excluded.iter() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if path.components().len() < root.components().len() { continue; }
            if Self::matches_original_components(
                path.components().iter().take(root.components().len()),
                root.components().iter(), work,
            )? { return Ok(true); }
        }
        work.with_original_leaf(|work| work.check())?;
        Ok(false)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn excludes_child_original(
        &self, directory: &NamespacePath, name: &crate::kernel::LogicalName,
        work: &mut NativeSourceWork<'_>,
    ) -> Result<bool, DemandError> {
        for root in self.excluded.iter() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if root.components().len() != directory.components().len().saturating_add(1) {
                continue;
            }
            if Self::matches_original_components(
                directory.components().iter().chain(std::iter::once(name)),
                root.components().iter(), work,
            )? { return Ok(true); }
        }
        work.with_original_leaf(|work| work.check())?;
        Ok(false)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn matches_original_components<'a>(
        actual: impl Iterator<Item = &'a crate::kernel::LogicalName>,
        expected: impl Iterator<Item = &'a crate::kernel::LogicalName>,
        work: &mut NativeSourceWork<'_>,
    ) -> Result<bool, DemandError> {
        for (actual, expected) in actual.zip(expected) {
            work.charge(WorkCounters {
                items_examined: 1, source_path_components: 1, ..WorkCounters::UNCHARGED
            })?;
            if actual != expected { return Ok(false); }
        }
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn project_original_page(
        &self, directory: &NamespacePath, mut page: SourceDirectoryPage,
        work: &mut NativeSourceWork<'_>,
    ) -> Result<SourceDirectoryPage, DemandError> {
        if self.excluded.is_empty() || page.entries.is_empty() { return Ok(page); }
        // Keep the producer's publication and original backing held through
        // projection. Any failure drops that same lease for cursor replay.
        let ownership = SourcePayloadOwnership::new(work)?;
        let count = page.entries.len();
        let bytes = (count as u64).checked_mul(std::mem::size_of::<usize>() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        if count != 0 { work.allocate(bytes)?; }
        let mut selected = Vec::new();
        if selected.try_reserve_exact(count).is_err() {
            work.with_original_leaf(|work| work.release(bytes))?;
            return Err(DemandError::Io(std::io::Error::from(std::io::ErrorKind::OutOfMemory)));
        }
        ownership.retain((selected.capacity() * std::mem::size_of::<usize>()) as u64, work)?;
        let projection = (|| {
            for index in 0..count {
                let entry = &page.entries[index];
                if !self.excludes_child_original(directory, entry.name(), work)? {
                    selected.push(page.entries.backing_index(index).ok_or(DemandError::InvalidRequest)?);
                }
            }
            Ok::<_, DemandError>(())
        })();
        // Install custody even on failure so physical selectors drop before
        // their original allocation control, then the raw page lease replays.
        page.entries.set_selection(selected, ownership);
        projection?;
        Ok(page)
    }
}

/// Forwards changes outside a filtered source's excluded subtrees, which no
/// view of it can hold facts about.
struct FilteredChangeSink {
    excluded: std::sync::Arc<FilteredExcludedPaths>,
    sink: std::sync::Arc<dyn SourceChangeSink>,
    ownership: SourcePayloadOwnership,
}

struct FilteredWatch {
    inner: Box<dyn SourceWatch>,
    // Keeps the actual adapter and exclusion custody alive through physical
    // teardown, including sources whose returned watch does not retain sinks.
    projection: std::sync::Arc<FilteredChangeSink>,
    ownership: SourcePayloadOwnership,
}

impl SourceWatch for FilteredWatch {
    fn fence_measured(&self, budget: WorkBudget, cancellation: &CancellationToken) -> DemandResult<()> {
        self.inner.fence_measured(budget, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn fence_with_native_work(&self, work: NativeSourceWork<'_>) -> Result<(), DemandError> {
        self.inner.fence_with_native_work(work)
    }

    fn stop_measured(&self, budget: WorkBudget, cancellation: &CancellationToken) -> DemandResult<()> {
        self.inner.stop_measured(budget, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_native_work(&self, work: NativeSourceWork<'_>) -> Result<(), DemandError> {
        self.inner.stop_with_native_work(work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
        self.inner.stop_with_cleanup_wait(wait)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.inner.stop_accepted_cleanup()
    }

    fn is_stopped(&self) -> bool { self.inner.is_stopped() }

    fn logical_owned_bytes(&self) -> u64 {
        self.inner.logical_owned_bytes()
            + std::mem::size_of::<Self>() as u64
            + std::mem::size_of::<FilteredChangeSink>() as u64
    }

    fn allocation_custody(&self) -> Option<std::sync::Arc<SourceWatchAllocationCustody>> {
        self.inner.allocation_custody()
    }

    fn is_exact(&self) -> bool { self.inner.is_exact() }
}

#[cfg(not(target_arch = "wasm32"))]
impl FilteredChangeSink {
    fn visible<W: SourceDemandReadWork>(&self, change: &SourceChange, work: &mut W) -> Result<bool, DemandError> {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if let SourceChange::Name(path) | SourceChange::Entry(path) = change {
            for root in self.excluded.iter() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                if path.depth() < root.depth() { continue; }
                let mut matches = true;
                for (actual, expected) in path.components().iter().zip(root.components()) {
                    work.charge(WorkCounters { items_examined: 1, source_path_components: 1, ..WorkCounters::UNCHARGED })?;
                    if actual != expected { matches = false; break; }
                }
                if matches { return Ok(false); }
            }
        }
        Ok(true)
    }

    fn source_changed_with_work<W: SourceDemandReadWork>(
        &self, changes: &[SourceChange], work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        let mut count = 0_usize;
        for change in changes { count += usize::from(self.visible(change, work)?); }
        if count == 0 { return Ok(()); }
        let ownership = if count != changes.len() { SourcePayloadOwnership::new(work)? } else { SourcePayloadOwnership::default() };
        let mut visible = Vec::new();
        let mut retained = 0_u64;
        let result = (|| {
            if count != changes.len() {
                let requested = (count as u64).checked_mul(std::mem::size_of::<SourceChange>() as u64)
                    .ok_or(crate::WorkError::Overflow)?;
                work.allocate(requested)?;
                retained = requested;
                if let Err(error) = visible.try_reserve_exact(count) {
                    drop(std::mem::take(&mut visible));
                    work.release(retained)?;
                    retained = 0;
                    return Err(DemandError::Allocation(error));
                }
                let completed = work.reconcile_allocation(
                    &mut retained, (visible.capacity() * std::mem::size_of::<SourceChange>()) as u64,
                );
                ownership.retain(retained, work)?;
                completed?;
                for change in changes {
                    if !self.visible(change, work)? { continue; }
                    let projected = match change {
                        SourceChange::Name(path) | SourceChange::Entry(path) => {
                            let path = clone_namespace(path, work)?;
                            let bytes = path.retained_bytes();
                            if let Err(error) = ownership.retain(bytes, work) {
                                drop(path);
                                work.release(bytes)?;
                                return Err(error);
                            }
                            retained = retained.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                            if matches!(change, SourceChange::Name(_)) { SourceChange::Name(path) } else { SourceChange::Entry(path) }
                        }
                        SourceChange::Node(identity) => {
                            let copy = WorkCounters { bytes_copied: identity.len() as u64, ..WorkCounters::UNCHARGED };
                            work.admit(copy)?;
                            let projected = SourceChange::Node(*identity);
                            work.record(copy)?;
                            projected
                        }
                        SourceChange::Everything => SourceChange::Everything,
                        SourceChange::Unconfirmed => SourceChange::Unconfirmed,
                    };
                    visible.push(projected);
                }
            }
            let batch = if count == changes.len() { changes } else { visible.as_slice() };
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Some(source) = work.native_source_work() {
                    self.sink.source_changed_with_native_work(batch, source)
                } else {
                    work.with_ordinary_work(|work| {
                        let budget = work.remaining()?;
                        let cancellation = work.cancellation().clone();
                        let live = work.allocations.live_bytes();
                        match self.sink.source_changed(batch, budget, &cancellation) {
                            Ok(receipt) => work.merge_with_prior_live(receipt.work, live),
                            Err(failure) => {
                                work.merge_with_prior_live(*failure.work, live)?;
                                Err(failure.error)
                            }
                        }
                    }).ok_or(DemandError::NativeWorkUnavailable)?
                }
            })).map_err(|_| DemandError::WorkerUnavailable)?
        })();
        drop(visible);
        let cleanup = if ownership.has_native_custody() { ownership.retire(retained) } else { work.release(retained) };
        result.and(cleanup)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<D: DemandSource> FilteredDemandSource<D> {
    #[allow(unsafe_code, reason = "initializes the preadmitted filtered result after successful startup")]
    fn watch_with_work<W: SourceDemandReadWork>(
        &self, sink: std::sync::Arc<dyn SourceChangeSink>, work: &mut W,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        work.check()?;
        let sink_ownership = SourcePayloadOwnership::new(work)?;
        let sink_bytes = std::mem::size_of::<FilteredChangeSink>() as u64;
        work.allocate(sink_bytes)?;
        let projection = std::sync::Arc::new(FilteredChangeSink {
            excluded: std::sync::Arc::clone(&self.excluded), sink, ownership: sink_ownership,
        });
        projection.ownership.retain(sink_bytes, work)?;
        let ownership = SourcePayloadOwnership::new(work)?;
        let wrapper_bytes = std::mem::size_of::<FilteredWatch>() as u64;
        work.allocate(wrapper_bytes)?;
        let mut wrapper = Box::<FilteredWatch>::new_uninit();
        ownership.retain(wrapper_bytes, work)?;
        let sink = std::sync::Arc::clone(&projection) as std::sync::Arc<dyn SourceChangeSink>;
        let inner = if let Some(source) = work.native_source_work() {
            self.inner.watch_with_native_work(sink, source)?
        } else {
            work.with_ordinary_work(|work| {
                let remaining = work.remaining()?;
                let cancellation = work.cancellation().clone();
                let live = work.allocations.live_bytes();
                match self.inner.watch(sink, remaining, &cancellation) {
                    Ok(receipt) => {
                        work.merge_with_prior_live(receipt.work, live)?;
                        Ok(receipt.value)
                    }
                    Err(failure) => {
                        work.merge_with_prior_live(*failure.work, live)?;
                        Err(failure.error)
                    }
                }
            }).ok_or(DemandError::NativeWorkUnavailable)??
        };
        let Some(inner) = inner else { return Ok(None); };
        wrapper.write(FilteredWatch { inner, projection, ownership });
        // SAFETY: startup succeeded, the actual box is initialized, and no
        // fallible check/allocation follows this physical owner transfer.
        Ok(Some(unsafe { wrapper.assume_init() } as Box<dyn SourceWatch>))
    }
}

#[cfg(target_arch = "wasm32")]
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
    #[cfg(not(target_arch = "wasm32"))]
    fn source_changed(
        &self, changes: &[SourceChange], budget: WorkBudget, cancellation: &CancellationToken,
    ) -> DemandResult<()> {
        let mut work = DemandWork::new(budget, cancellation);
        let result = self.source_changed_with_work(changes, &mut work);
        work.finish(result)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn source_changed_with_native_work(
        &self, changes: &[SourceChange], mut work: NativeSourceWork<'_>,
    ) -> Result<(), DemandError> {
        self.source_changed_with_work(changes, &mut work)
    }

    #[cfg(target_arch = "wasm32")]
    fn source_changed(
        &self, changes: &[SourceChange], budget: WorkBudget, cancellation: &CancellationToken,
    ) -> DemandResult<()> {
        let mut work = WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = (|| {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            if changes.is_empty() { return Ok(()); }
            let is_visible = |change: &SourceChange, work: &mut WorkCounters| -> Result<bool, DemandError> {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
                if let SourceChange::Name(path) | SourceChange::Entry(path) = change {
                    for root in self.excluded.iter() {
                        cancellation.check().map_err(|_| DemandError::Cancelled)?;
                        work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
                        if path.is_within(root) { return Ok(false); }
                    }
                }
                Ok(true)
            };
            let mut visible_count = 0_usize;
            for change in changes {
                if is_visible(change, &mut work)? { visible_count += 1; }
            }
            if visible_count == 0 { return Ok(()); }
            // The unchanged batch is already live in the caller. Only a
            // genuinely filtered batch needs an owned contiguous projection.
            let mut visible = Vec::new();
            if visible_count != changes.len() {
                let bytes = (visible_count as u64).checked_mul(std::mem::size_of::<SourceChange>() as u64)
                    .ok_or(crate::WorkError::Overflow)?;
                let prior_peak = work.peak_allocation_bytes;
                allocations.claim_bytes(bytes, 1, &mut work, budget)
                    .map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
                if visible.try_reserve_exact(visible_count).is_err() {
                    allocations.release(bytes).map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
                    work.peak_allocation_bytes = prior_peak;
                    return Err(DemandError::WorkerUnavailable);
                }
                for change in changes {
                    if is_visible(change, &mut work)? {
                        visible.push(clone_source_change_with_ledger(change, &mut allocations, &mut work, budget, cancellation)?);
                    }
                }
            }
            let batch = if visible_count == changes.len() { changes } else { visible.as_slice() };
            let mut remaining = work.remaining(budget)?;
            remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                .checked_sub(allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
            let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || self.sink.source_changed(batch, remaining, cancellation),
            )).map_err(|_| DemandError::WorkerUnavailable)?;
            match answer {
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

    #[cfg(not(target_arch = "wasm32"))]
    async fn lookup_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, mut work: NativeSourceWork<'_>,
    ) -> Result<Option<SourceNode>, DemandError> {
        if self.excludes_original(path, &mut work)? { return Ok(None); }
        self.inner.lookup_with_native_work(source, path, work).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_metadata_with_native_work(
        &self, source: SourceReference, path: &NamespacePath,
        expected: Option<SourceVersion>, mut work: NativeSourceWork<'_>,
    ) -> Result<Option<SourceNode>, DemandError> {
        if self.excludes_original(path, &mut work)? { return Ok(None); }
        self.inner.read_metadata_with_native_work(source, path, expected, work).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_range_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
        offset: u64, length: u64, mut work: NativeSourceWork<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
        if self.excludes_original(path, &mut work)? { return Err(DemandError::Absent); }
        self.inner.read_range_with_native_work(source, path, expected, offset, length, work).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_link_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
        maximum_bytes: u64, mut work: NativeSourceWork<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
        if self.excludes_original(path, &mut work)? { return Err(DemandError::Absent); }
        self.inner.read_link_with_native_work(source, path, expected, maximum_bytes, work).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn open_file_with_native_work(
        &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
        mut work: NativeSourceWork<'_>,
    ) -> Result<Box<dyn DemandFile>, DemandError> {
        if self.excludes_original(path, &mut work)? { return Err(DemandError::Absent); }
        self.inner.open_file_with_native_work(source, path, expected, work).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn list_page_with_native_work(
        &self, source: SourceReference, directory: &NamespacePath,
        cursor: Option<SourceCursor>, maximum_entries: u32, mut work: NativeSourceWork<'_>,
    ) -> Result<SourceDirectoryPage, DemandError> {
        if self.excludes_original(directory, &mut work)? { return Err(DemandError::Absent); }
        let page = self.inner.list_page_with_native_work(
            source, directory, cursor, maximum_entries, work,
        ).await?;
        self.project_original_page(directory, page, &mut work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn watch(
        &self, sink: std::sync::Arc<dyn SourceChangeSink>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<Box<dyn SourceWatch>>> {
        let mut work = DemandWork::new(budget, cancellation);
        let result = self.watch_with_work(sink, &mut work);
        result.map(|value| OperationReceipt { value, work: work.work })
            .map_err(|error| OperationFailure::new(error, work.work))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn watch_with_native_work(
        &self, sink: std::sync::Arc<dyn SourceChangeSink>, mut work: NativeSourceWork<'_>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        self.watch_with_work(sink, &mut work)
    }

    #[cfg(target_arch = "wasm32")]
    #[allow(unsafe_code, reason = "initializes the preadmitted filtered watch owner after native startup")]
    fn watch(
        &self,
        sink: std::sync::Arc<dyn SourceChangeSink>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<Box<dyn SourceWatch>>> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        let mut work = WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let sink_bytes = std::mem::size_of::<FilteredChangeSink>() as u64;
        allocations.claim_bytes(sink_bytes, 1, &mut work, budget)
            .map_err(|error| OperationFailure::new(error.into_work_or(DemandError::WorkerUnavailable), work))?;
        let sink = std::sync::Arc::new(FilteredChangeSink {
            excluded: std::sync::Arc::clone(&self.excluded), sink,
            ownership: SourcePayloadOwnership::default(),
        });
        let projection = std::sync::Arc::clone(&sink);
        cancellation.check().map_err(|_| OperationFailure::new(DemandError::Cancelled, work))?;
        let wrapper_bytes = std::mem::size_of::<FilteredWatch>() as u64;
        allocations.claim_bytes(wrapper_bytes, 1, &mut work, budget)
            .map_err(|error| OperationFailure::new(error.into_work_or(DemandError::WorkerUnavailable), work))?;
        let mut wrapper = Box::<FilteredWatch>::new_uninit();
        let bytes = allocations.live_bytes();
        let mut remaining = work.remaining(budget).map_err(|error| OperationFailure::new(error.into(), work))?;
        remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(bytes)
            .ok_or_else(|| OperationFailure::new(DemandError::Work(WorkError::Overflow), work))?;
        match self.inner.watch(sink, remaining, cancellation) {
            Ok(receipt) => {
                let total = work.with_backend(receipt.work, bytes)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                let value = match receipt.value {
                    Some(inner) => {
                        wrapper.write(FilteredWatch { inner, projection, ownership: SourcePayloadOwnership::default() });
                        // SAFETY: initialized once after successful startup;
                        // no fallible allocation can now invoke native Drop.
                        Some(unsafe { wrapper.assume_init() } as Box<dyn SourceWatch>)
                    }
                    None => None,
                };
                Ok(OperationReceipt { value, work: total })
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
    use crate::native_host::NativeHostWork as _;
    #[cfg(unix)]
    use crate::native_host::HostListedEntry;
    #[cfg(windows)]
    use crate::native_host::HostListedWideEntry as HostListedEntry;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
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
        entries: Option<DirectoryReader>,
        pending: Option<DirectoryPending>,
        replay: Option<Arc<SourceDirectoryBacking>>,
        replay_bytes: u64,
        replay_offset: usize,
        eof: bool,
        in_use: bool,
        unpublished_start: bool,
        ownership: SourcePayloadOwnership,
    }

    impl DirectoryState {
        fn private_bytes(&self) -> u64 {
            std::mem::size_of::<Mutex<Self>>() as u64
                + self.directory.retained_bytes()
                + self.entries.as_ref().map_or(0, |reader| reader.reader.logical_owned_bytes())
                + self.pending.as_ref().map_or(0, |pending| pending.entry.logical_owned_bytes())
        }
    }

    struct CursorState {
        directory: Arc<Mutex<DirectoryState>>,
    }

    #[derive(Default)]
    struct CursorSlots {
        slots: Vec<Option<(u64, CursorState)>>,
        ownership: SourcePayloadOwnership,
    }

    impl std::ops::Deref for CursorSlots {
        type Target = Vec<Option<(u64, CursorState)>>;
        fn deref(&self) -> &Self::Target { &self.slots }
    }

    impl std::ops::DerefMut for CursorSlots {
        fn deref_mut(&mut self) -> &mut Self::Target { &mut self.slots }
    }

    struct DirectoryReader {
        reader: HostStatReader,
        ownership: SourcePayloadOwnership,
    }

    struct DirectoryPending {
        entry: HostListedEntry,
        ownership: SourcePayloadOwnership,
    }

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
        original_cleanup: Option<OriginalDemandWork>,
        ownership: SourcePayloadOwnership,
    }

    impl NativePagePublication {
        fn accept_progress(
            &mut self, check: impl Fn() -> Result<(), DemandError>,
            cleanup: impl FnOnce() -> Result<(), DemandError>,
        ) -> Result<(), DemandError> {
            check()?;
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
            check()?;
            cleanup()?;
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
            drop(active);
            self.lease.take();
            Ok(())
        }
    }

    impl Drop for NativePagePublication {
        fn drop(&mut self) {
            if let Some(original) = self.original_cleanup.take() {
                if let Some(mut lease) = self.lease.take() {
                    lease.restore();
                    let _cleanup = original.with_original_leaf(|work| work.record_pending_items(1))
                        .and_then(std::convert::identity);
                } else {
                    let _cleanup = original.with_original_leaf(|work| work.refund_pending_items(1));
                }
            }
        }
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
            if self.original_cleanup.is_some() {
                return std::future::ready(Err(OperationFailure::before_work(DemandError::NativeWorkUnavailable)));
            }
            let answer = self.accept_progress(
                || cancellation.check().map_err(|_| DemandError::Cancelled),
                || work.charge(&publication_cleanup(), &budget).map_err(DemandError::from),
            );
            std::future::ready(answer.map(|()| OperationReceipt { value: (), work })
                .map_err(|error| OperationFailure::new(error, work)))
        }

        fn abort(&mut self, budget: WorkBudget) -> DemandResult<()> {
            let mut work = WorkCounters::UNCHARGED;
            if self.original_cleanup.is_some() {
                return Err(OperationFailure::before_work(DemandError::NativeWorkUnavailable));
            }
            if self.lease.is_some() {
                work.charge(&publication_cleanup(), &budget)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                if let Some(mut lease) = self.lease.take() { lease.restore(); }
            }
            Ok(OperationReceipt { value: (), work })
        }

        #[cfg(not(target_arch = "wasm32"))]
        fn accept_with_native_work(&mut self, work: NativeSourceWork<'_>) -> Result<(), DemandError> {
            let original = self.original_cleanup.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
            if !Arc::ptr_eq(original.meter(), work.original)
                || !same_resource_view(original.resource_allowance(), work.resource) {
                return Err(DemandError::NativeWorkUnavailable);
            }
            let original = original.clone();
            self.accept_progress(
                || crate::native_host::NativeHostWork::check(&work),
                || original.with_original_leaf(|work| work.record_pending_items(1)).and_then(std::convert::identity),
            )?;
            self.original_cleanup.take();
            Ok(())
        }

        #[cfg(not(target_arch = "wasm32"))]
        fn abort_with_native_work(&mut self, work: NativeSourceWork<'_>) -> Result<(), DemandError> {
            let original = self.original_cleanup.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
            if !Arc::ptr_eq(original.meter(), work.original)
                || !same_resource_view(original.resource_allowance(), work.resource) {
                return Err(DemandError::NativeWorkUnavailable);
            }
            if let Some(mut lease) = self.lease.take() {
                lease.restore();
                original.with_original_leaf(|work| work.record_pending_items(1)).and_then(std::convert::identity)?;
            } else {
                original.with_original_leaf(|work| work.refund_pending_items(1))?;
            }
            self.original_cleanup.take();
            Ok(())
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

    struct SourceRootInput {
        path: DemandPath,
        absolute: bool,
        custody: SourceRootPathCustody,
    }

    fn source_vec<T, W: SourceDemandReadWork>(
        capacity: usize, work: &mut W,
    ) -> Result<(Vec<T>, u64), DemandError> {
        let width = std::mem::size_of::<T>();
        let mut bytes = capacity.checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        work.allocate(bytes)?;
        let mut values = Vec::new();
        if let Err(error) = values.try_reserve_exact(capacity) {
            drop(values);
            let _cleanup = work.release(bytes);
            return Err(DemandError::Allocation(error));
        }
        let actual = values.capacity().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        if let Err(error) = work.reconcile_allocation(&mut bytes, actual) {
            drop(values);
            let _cleanup = work.release(bytes);
            return Err(error);
        }
        Ok((values, bytes))
    }

    struct SourceRootPathCustody {
        ownership: SourcePayloadOwnership,
        original: std::sync::Weak<NativeDemandReadMeter>,
        native_original: bool,
        bytes: u64,
    }

    impl SourceRootPathCustody {
        fn new(work: &mut NativeSourceWork<'_>) -> Result<Self, DemandError> {
            Ok(Self {
                ownership: SourcePayloadOwnership::new(work)?,
                original: Arc::downgrade(work.original),
                native_original: work.is_native_original(),
                bytes: 0,
            })
        }

        fn retain<W: SourceDemandReadWork>(&mut self, bytes: u64, work: &mut W) -> Result<(), DemandError> {
            self.ownership.retain(bytes, work)?;
            self.bytes = bytes;
            Ok(())
        }
    }

    impl Drop for SourceRootPathCustody {
        fn drop(&mut self) {
            if self.ownership.has_native_custody() {
                self.ownership.retire(self.bytes).expect("native root retires only its physical path");
                return;
            }
            if let Some(original) = self.original.upgrade() {
                original.with_original_leaf(|work| work.release(self.bytes))
                    .expect("ordinary root path retires after its physical buffer");
            }
        }
    }

    impl SourceRootInput {
        fn copy(path: &Path, work: &mut NativeSourceWork<'_>) -> Result<Self, DemandError> {
            let mut custody = SourceRootPathCustody::new(work)?;
            #[cfg(unix)]
            let (owned, bytes) = crate::native_host::clone_path_with_work(path, work)?;
            #[cfg(windows)]
            let (owned, bytes) = crate::native_host::encode_root_path_name_with_work(path, work)?;
            if let Err(error) = custody.retain(bytes, work) {
                drop(owned);
                work.release(bytes)?;
                return Err(error);
            }
            Ok(Self { path: owned, absolute: path.is_absolute(), custody })
        }

        fn from_admitted(path: PathBuf, work: &mut NativeSourceWork<'_>) -> Result<Self, DemandError> {
            #[cfg(unix)]
            {
                let mut custody = SourceRootPathCustody::new(work)?;
                let bytes = path.capacity() as u64;
                if let Err(error) = custody.retain(bytes, work) {
                    drop(path);
                    work.release(bytes)?;
                    return Err(error);
                }
                Ok(Self { absolute: path.is_absolute(), path, custody })
            }
            #[cfg(windows)]
            {
                let bytes = path.capacity() as u64;
                let result = Self::copy(&path, work);
                drop(path);
                work.release(bytes)?;
                result
            }
        }

        fn normalize<W: SourceDemandReadWork>(self, work: &mut W) -> Result<Self, DemandError> {
            #[cfg(unix)]
            if self.absolute { return Ok(self); }
            #[cfg(windows)]
            if self.absolute && self.path.len() < 248
                || self.path.starts_with(&[92, 92, 63, 92])
                || self.path.starts_with(&[92, 63, 63, 92]) {
                return Ok(self);
            }
            let mut custody = SourceRootPathCustody {
                ownership: SourcePayloadOwnership::new(work)?,
                original: self.custody.original.clone(),
                native_original: self.custody.native_original,
                bytes: 0,
            };
            #[cfg(unix)]
            let path = crate::native_host::normalize_root_path_measured(&self.path, work)?;
            #[cfg(windows)]
            let path = crate::native_host::normalize_root_wide_name_with_work(&self.path, self.absolute, work)?;
            #[cfg(unix)]
            let bytes = path.capacity() as u64;
            #[cfg(windows)]
            let bytes = (path.capacity() * std::mem::size_of::<u16>()) as u64;
            if let Err(error) = custody.retain(bytes, work) {
                drop(path);
                work.release(bytes)?;
                return Err(error);
            }
            drop(self);
            Ok(Self { path, absolute: true, custody })
        }
    }

    fn retain_root_control<T: std::any::Any + Send + Sync, W: SourceDemandReadWork>(
        owner: &Arc<T>, ownership: &SourcePayloadOwnership, work: &mut W,
    ) -> Result<(), DemandError> {
        if let Some(custody) = &ownership.custody {
            let owner: Arc<dyn std::any::Any + Send + Sync> = owner.clone();
            let weak = Arc::downgrade(&owner);
            drop(owner);
            return work.with_original_leaf(|work| custody.attach_known_control(
                crate::demand::NativeMountControlAllocation::SharedCheckout(weak), work,
            ));
        }
        Ok(())
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
        watches: std::sync::RwLock<NativeSourceWatches>,
        // The native path is physically dropped before its custody retires.
        root_custody: SourceRootPathCustody,
        // Intrinsic constructor ownership, never a cached per-request alias.
        // Last: the original ledger outlives every physical provider field.
        _original: Option<OriginalDemandWork>,
    }

    #[cfg(any(target_os = "linux", windows))]
    #[derive(Clone)]
    struct NativeSourceWatchSlot {
        watch: std::sync::Weak<NativeSourceWatchBacking>,
        pending: bool,
    }

    #[cfg(any(target_os = "linux", windows))]
    impl NativeSourceWatchSlot {
        fn upgrade(&self) -> Option<Arc<NativeSourceWatchBacking>> { self.watch.upgrade() }
        fn strong_count(&self) -> usize { self.watch.strong_count() }
    }

    #[cfg(any(target_os = "linux", windows))]
    #[derive(Default)]
    struct NativeSourceWatches {
        entries: Vec<NativeSourceWatchSlot>,
        backing: Option<Arc<SourceWatchAllocationCustody>>,
        ownership: SourcePayloadOwnership,
    }

    #[cfg(any(target_os = "linux", windows))]
    impl std::ops::Deref for NativeSourceWatches {
        type Target = Vec<NativeSourceWatchSlot>;
        fn deref(&self) -> &Self::Target { &self.entries }
    }

    #[cfg(any(target_os = "linux", windows))]
    impl Drop for NativeSourceWatches {
        fn drop(&mut self) {
            drop(std::mem::take(&mut self.entries));
            if let Some(backing) = &self.backing { backing.retired(); }
        }
    }

    /// A prepaid slot in the existing registry, before any OS watch starts.
    #[cfg(any(target_os = "linux", windows))]
    struct NativeSourceRegistration {
        inner: Arc<NativeDemandInner>,
        index: usize,
        pending: bool,
        backing: Option<Arc<SourceWatchAllocationCustody>>,
    }

    #[cfg(any(target_os = "linux", windows))]
    impl NativeSourceRegistration {
        fn prepare<W: SourceDemandReadWork>(
            inner: &Arc<NativeDemandInner>, work: &mut W,
        ) -> Result<Self, DemandError> {
            loop {
                let (length, capacity) = {
                    let watches = inner.watches.read().unwrap_or_else(std::sync::PoisonError::into_inner);
                    (watches.len(), watches.entries.capacity())
                };
                work.admit(WorkCounters { items_examined: length as u64, ..WorkCounters::UNCHARGED })?;
                let mut visited = 0_u64;
                let (stable, reserved) = {
                    let mut watches = inner.watches.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if watches.len() != length || watches.entries.capacity() != capacity {
                        (false, None)
                    } else {
                        let mut available = None;
                        for (index, slot) in watches.entries.iter().enumerate() {
                            visited += 1;
                            if !slot.pending && slot.strong_count() == 0 { available = Some(index); break; }
                        }
                        if let Some(index) = available {
                            watches.entries[index].pending = true;
                            (true, Some(Self { inner: Arc::clone(inner), index, pending: true, backing: watches.backing.as_ref().map(Arc::clone) }))
                        } else if length < capacity {
                            watches.entries.push(NativeSourceWatchSlot { watch: std::sync::Weak::new(), pending: true });
                            (true, Some(Self { inner: Arc::clone(inner), index: length, pending: true, backing: watches.backing.as_ref().map(Arc::clone) }))
                        } else {
                            (true, None)
                        }
                    }
                };
                if visited != 0 { work.record(WorkCounters { items_examined: visited, ..WorkCounters::UNCHARGED })?; }
                if let Some(registration) = reserved { return Ok(registration); }
                if !stable { continue; }
                let count = capacity.checked_add(1).ok_or(WorkError::Overflow)?;
                let width = std::mem::size_of::<NativeSourceWatchSlot>() as u64;
                let ownership = SourcePayloadOwnership::new(work)?;
                let control_ownership = SourcePayloadOwnership::new(work)?;
                let control = std::mem::size_of::<SourceWatchAllocationCustody>() as u64;
                work.allocate(control)?;
                let backing = Arc::new(SourceWatchAllocationCustody::new(control_ownership));
                backing.ownership.retain(control, work)?;
                let mut bytes = (count as u64).checked_mul(width).ok_or(WorkError::Overflow)?;
                work.allocate(bytes)?;
                let mut entries = Vec::new();
                if let Err(error) = entries.try_reserve_exact(count) {
                    drop(entries);
                    work.release(bytes)?;
                    return Err(DemandError::Allocation(error));
                }
                let reconciled = work.reconcile_allocation(&mut bytes, (entries.capacity() as u64) * width);
                ownership.retain(bytes, work)?;
                reconciled?;
                backing.publish(bytes);
                let mut replacement = Self::replacement(entries, backing, ownership);
                let copied = WorkCounters {
                    bytes_copied: (length as u64).checked_mul(width).ok_or(WorkError::Overflow)?,
                    ..WorkCounters::UNCHARGED
                };
                work.admit(copied)?;
                let published = {
                    let mut watches = inner.watches.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if watches.len() != length || watches.entries.capacity() != capacity {
                        None
                    } else {
                        replacement.entries.extend(watches.entries.iter().cloned());
                        replacement.entries.push(NativeSourceWatchSlot { watch: std::sync::Weak::new(), pending: true });
                        let backing = replacement.backing.as_ref().map(Arc::clone);
                        std::mem::swap(&mut *watches, &mut replacement);
                        Some(Self { inner: Arc::clone(inner), index: length, pending: true, backing })
                    }
                };
                drop(replacement);
                if let Some(registration) = published {
                    work.record(copied)?;
                    return Ok(registration);
                }
                if !work.is_native_original() { work.release(bytes + control)?; }
            }
        }

        fn replacement(
            entries: Vec<NativeSourceWatchSlot>, backing: Arc<SourceWatchAllocationCustody>,
            ownership: SourcePayloadOwnership,
        ) -> NativeSourceWatches {
            NativeSourceWatches { entries, backing: Some(backing), ownership }
        }

        fn publish(mut self, watch: &Arc<NativeSourceWatchBacking>) -> Option<Arc<SourceWatchAllocationCustody>> {
            let retired = {
                let mut watches = self.inner.watches.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                let slot = &mut watches.entries[self.index];
                slot.pending = false;
                std::mem::replace(&mut slot.watch, Arc::downgrade(watch))
            };
            drop(retired);
            self.pending = false;
            self.backing.take()
        }
    }

    #[cfg(any(target_os = "linux", windows))]
    impl Drop for NativeSourceRegistration {
        fn drop(&mut self) {
            if self.pending {
                self.inner.watches.write().unwrap_or_else(std::sync::PoisonError::into_inner)
                    .entries[self.index].pending = false;
            }
        }
    }

    struct CancelWorkerOnDrop(CancellationToken);

    impl Drop for CancelWorkerOnDrop {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }

    struct SourceRequestPath {
        path: NamespacePath,
        ownership: SourcePayloadOwnership,
    }

    struct SourceRequestStop {
        stopped: AtomicBool,
        ownership: SourcePayloadOwnership,
    }

    struct StopOriginalRequestOnDrop(Arc<SourceRequestStop>);

    impl Drop for StopOriginalRequestOnDrop {
        fn drop(&mut self) { self.0.stopped.store(true, Ordering::Release); }
    }

    /// Owns only this waiter's reserved cleanup credit. Retirement runs after
    /// the physical waiter is quiescent, outside the original meter lock.
    struct OriginalSourceCancelled {
        prepared: Option<crate::cancellation::PreparedCancelled>,
    }

    impl OriginalSourceCancelled {
        fn prepare(work: NativeSourceWork<'_>) -> Result<Self, DemandError> {
            let original = work.materialization_work()?;
            let prepared = original.cancellation().prepare_cancelled_with_native_work(&original)
                .map_err(cancellation_error)?;
            Ok(Self { prepared: Some(prepared) })
        }

        fn finish(&mut self) -> Result<(), DemandError> {
            if let Some(mut prepared) = self.prepared.take() {
                prepared.retire_waker();
                // The canonical native waiter owns its actual cleanup escrow;
                // its physical retirement never runs under the Root mutex.
                drop(prepared);
            }
            Ok(())
        }
    }

    impl std::future::Future for OriginalSourceCancelled {
        type Output = ();
        fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
            match self.prepared.as_mut() {
                Some(prepared) => std::pin::Pin::new(prepared).poll(cx),
                None => std::task::Poll::Ready(()),
            }
        }
    }

    impl Drop for OriginalSourceCancelled {
        fn drop(&mut self) {
            // Future abandonment still retires the actual waiter and charges
            // the original invocation; no detached zero-work receipt exists.
            let _ = self.finish();
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
                        return OriginalDemandWork::from_meter(shared)
                            .finish_operation(Err(cancellation_error(error)));
                    }
                }
            };
            let worker = shared.clone();
            let task_meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = shared.clone();
            let task = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(crate::obs::caller_context(), move || {
                    let mut work = NativeSourceWork::ordinary(&worker);
                    let input = SourceRootInput::from_admitted(path, &mut work)?;
                    Self::open_blocking(input, profile, limits, reference, observer, &mut work)
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
            let original = OriginalDemandWork::from_meter(shared);
            let closed = original.with_original_leaf(|work| {
                work.restore_budget(budget);
                let cleanup = work.finish_cancelled(prepared).map_err(cancellation_error);
                (cleanup, work.take_native_io_failure(), work.check())
            });
            let result = match closed {
                Ok((cleanup, failure, checked)) =>
                    cleanup.and(failure.map_or(result, Err)).and_then(|value| checked.map(|()| value)),
                Err(error) => Err(error),
            };
            original.finish_operation(result)
        }

        /// Opens the real root using the caller's original native invocation.
        /// The borrowed spelling is copied into SDK-owned native storage before
        /// queueing; its physical custody remains with the returned provider.
        /// Native I/O services must already exist; this never creates a meter.
        pub async fn open_with_reference_and_native_work(
            path: &Path,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Option<Arc<dyn DemandDirectoryObserver>>,
            work: &crate::materializer::NativeMaterializationWork,
        ) -> Result<Self, DemandError> {
            let mut work = NativeSourceWork::from_native_materialization_work(work);
            let original = work.original_demand_work();
            work.check()?;
            work.preflight(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
            let input = SourceRootInput::copy(path, &mut work)?;
            let mut cancelled = OriginalSourceCancelled::prepare(work)?;
            let worker = original.clone();
            let task_meter = work.materialization_work()?.runtime_meter_owner();
            let spawned = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(crate::obs::caller_context(), move || {
                    let mut work = NativeSourceWork::from_original_work(&worker);
                    Self::open_blocking(input, profile, limits, reference, observer, &mut work)
                }), task_meter,
            );
            let mut selected_cancellation = false;
            let result = match spawned {
                Ok(mut queued) => tokio::select! {
                    result = &mut queued => result.map_err(native_demand_error).and_then(std::convert::identity),
                    () = &mut cancelled => {
                        selected_cancellation = true;
                        queued.await.map_err(native_demand_error).and_then(std::convert::identity)
                    },
                },
                Err(error) => Err(native_demand_error(error)),
            };
            let cleanup = cancelled.finish();
            let native_failure = work.with_original_leaf(DemandWork::take_native_io_failure);
            match result {
                Err(error) => Err(native_failure.unwrap_or(error)),
                Ok(value) => {
                    if let Some(error) = native_failure { return Err(error); }
                    cleanup?;
                    if selected_cancellation { return Err(DemandError::Cancelled); }
                    work.check()?;
                    Ok(value)
                }
            }
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

        fn open_blocking<W: SourceDemandReadWork>(
            input: SourceRootInput,
            profile: FilesystemProfile,
            limits: VolumeLimits,
            reference: SourceReference,
            observer: Option<Arc<dyn DemandDirectoryObserver>>,
            work: &mut W,
        ) -> Result<Self, DemandError> {
            work.check()?;
            let input = input.normalize(work)?;
            #[cfg(unix)]
            let root = HostRoot::open_with_work(&input.path, work)?;
            #[cfg(windows)]
            let root = HostRoot::open_root_wide_measured(&input.path, work)?;
            let inline = root.is_local_with_work(work)?;
            let requests_ownership = SourcePayloadOwnership::new(work)?;
            let requests_bytes = (std::mem::size_of::<Semaphore>() + 2 * std::mem::size_of::<usize>()) as u64;
            work.allocate(requests_bytes)?;
            let requests = Arc::new(Semaphore::new(MAXIMUM_NATIVE_REQUESTS));
            requests_ownership.retain(requests_bytes, work)?;
            retain_root_control(&requests, &requests_ownership, work)?;
            let inner_ownership = SourcePayloadOwnership::new(work)?;
            let inner_bytes = (std::mem::size_of::<NativeDemandInner>() + 2 * std::mem::size_of::<usize>()) as u64;
            work.allocate(inner_bytes)?;
            let inner = Arc::new(NativeDemandInner {
                path: input.path,
                inline,
                root,
                identity: reference.identity,
                epoch: AtomicU64::new(reference.epoch),
                profile,
                limits,
                cursors: Mutex::new(CursorSlots::default()),
                next_cursor: AtomicU64::new(0),
                requests,
                observer,
                #[cfg(any(target_os = "linux", windows))]
                watches: std::sync::RwLock::new(NativeSourceWatches::default()),
                root_custody: input.custody,
                _original: work.native_source_work().map(|work| work.original_demand_work().allocation_work()),
            });
            inner_ownership.retain(inner_bytes, work)?;
            retain_root_control(&inner, &inner_ownership, work)?;
            Ok(Self {
                inner,
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

        fn observe_directory<W: SourceDemandReadWork>(&self, directory: &NamespacePath, work: &mut W) -> Result<(), DemandError> {
            self.admit_to_watches(directory, work)?;
            if let Some(observer) = &self.inner.observer {
                if let Some(original) = work.native_source_work() {
                    return observer.observe_directory_with_native_work(directory, original);
                }
                let cancellation = work.with_original_leaf(|work| work.cancellation.clone());
                let answer = observer.observe_directory(directory, work.remaining()?, &cancellation);
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
        fn admit_to_watches<W: SourceDemandReadWork>(&self, directory: &NamespacePath, work: &mut W) -> Result<bool, DemandError> {
            let watches = self.inner.watches.read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut count = 0usize;
            for watch in watches.iter() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                count += usize::from(watch.upgrade().is_some_and(|watch| !watch.is_stopped()));
            }
            if count == 0 { return Ok(false); }
            let bytes = count * std::mem::size_of::<Arc<NativeSourceWatchBacking>>();
            work.allocate(bytes as u64)?;
            let mut live = Vec::with_capacity(count);
            for watch in watches.iter() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if let Some(watch) = watch.upgrade() {
                    if watch.is_stopped() { continue; }
                    work.charge(WorkCounters {
                        bytes_copied: std::mem::size_of::<Arc<NativeSourceWatchBacking>>() as u64,
                        ..WorkCounters::default()
                    })?;
                    live.push(watch);
                }
            }
            // A notification may reenter the source; never hold the registry
            // lock while admitting or abandoning a native watch.
            drop(watches);
            let relative = self.relative(directory, work)?;
            let answer = (|| {
                let mut added = false;
                for watch in &live {
                    if !watch.is_stopped() { added |= watch.admit_with_work(&relative, work)?; }
                }
                Ok(added)
            })();
            let path_bytes = path_owned_bytes(&relative);
            drop(relative);
            work.release(path_bytes)?;
            drop(live);
            work.release(bytes as u64)?;
            answer
        }

        /// One watch covers the whole root on every other host.
        #[cfg(not(target_os = "linux"))]
        #[allow(
            clippy::unused_self,
            clippy::unnecessary_wraps,
            reason = "the Linux variant admits directories to inotify watches and can fail; this one keeps its signature"
        )]
        fn admit_to_watches<W: SourceDemandReadWork>(&self, _directory: &NamespacePath, work: &mut W) -> Result<bool, DemandError> {
            work.check()?;
            Ok(false)
        }

        fn observe_parent<W: SourceDemandReadWork>(&self, path: &NamespacePath, work: &mut W) -> Result<(), DemandError> {
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
            let answer = self.observe_directory(&parent, work);
            let bytes = namespace_owned_bytes(&parent)?;
            drop(parent);
            work.release(bytes)?;
            answer
        }

        async fn run_original_request<T: Send + 'static>(
            &self,
            path: &NamespacePath,
            mut work: NativeSourceWork<'_>,
            job: impl FnOnce(Self, &NamespacePath, &mut NativeSourceWork<'_>) -> Result<T, DemandError> + Send + 'static,
        ) -> Result<T, DemandError> {
            work.check()?;
            let mut path_ownership = SourcePayloadOwnership::new(&mut work)?;
            work.charge(WorkCounters {
                source_path_components: path.depth() as u64, ..WorkCounters::UNCHARGED
            })?;
            let path = clone_namespace(path, &mut work)?;
            path_ownership.retain(namespace_owned_bytes(&path)?, &mut work)?;
            let path = SourceRequestPath { path, ownership: path_ownership };
            let mut stop_ownership = SourcePayloadOwnership::new(&mut work)?;
            let stop_bytes = (std::mem::size_of::<SourceRequestStop>() + 2 * std::mem::size_of::<usize>()) as u64;
            work.allocate(stop_bytes)?;
            let stop = Arc::new(SourceRequestStop { stopped: AtomicBool::new(false), ownership: stop_ownership });
            stop.ownership.retain(stop_bytes, &mut work)?;
            let stop_on_drop = StopOriginalRequestOnDrop(Arc::clone(&stop));
            let original = work.original_demand_work();
            let mut cancelled = OriginalSourceCancelled::prepare(work)?;
            let permit = tokio::select! {
                acquired = self.inner.requests.clone().acquire_owned() =>
                    acquired.map_err(|_| DemandError::WorkerUnavailable),
                () = &mut cancelled => Err(DemandError::Cancelled),
            };
            let permit = match permit.and_then(|permit| work.check().map(|()| permit)) {
                Ok(permit) => permit,
                Err(error) => { cancelled.finish()?; return Err(error); }
            };
            let worker_work = original.clone();
            let task_meter = work.materialization_work()?.runtime_meter_owner();
            let source = self.clone();
            let context = crate::obs::caller_context();
            let spawned = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(context, move || {
                    let _permit = permit;
                    let mut worker = NativeSourceWork::from_original_work(&worker_work);
                    worker.operation_stop = Some(&stop.stopped);
                    // Host operations and SDK callbacks run without holding
                    // the original ledger mutex. Only their meter leaves lock.
                    job(source, &path.path, &mut worker)
                }), task_meter,
            );
            let mut selected_cancellation = false;
            let result = match spawned {
                Ok(mut queued) => tokio::select! {
                    result = &mut queued => result.map_err(native_demand_error).and_then(std::convert::identity),
                    () = &mut cancelled => {
                        selected_cancellation = true;
                        stop_on_drop.0.stopped.store(true, Ordering::Release);
                        // Cancellation is cooperative, not an OS deadline:
                        // retain the actual physical prefix until quiescence.
                        queued.await.map_err(native_demand_error).and_then(std::convert::identity)
                    },
                },
                Err(error) => Err(native_demand_error(error)),
            };
            cancelled.finish()?;
            let native_failure = work.with_original_leaf(DemandWork::take_native_io_failure);
            drop(stop_on_drop);
            if let Some(error) = native_failure { return Err(error); }
            if selected_cancellation { return Err(DemandError::Cancelled); }
            // Awaiting the actual queue fence establishes quiescence. Returned
            // Bytes/page guards may legitimately keep the original meter alive.
            result.and_then(|value| work.check().map(|()| value))
        }

        async fn run_request<T: Send + 'static>(
            &self,
            path: &NamespacePath,
            budget: WorkBudget,
            cancellation: &CancellationToken,
            job: impl FnOnce(Self, NamespacePath, &mut NativeSourceWork<'_>) -> Result<T, DemandError> + Send + 'static,
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
            job: impl FnOnce(Self, &mut NativeSourceWork<'_>) -> Result<T, DemandError> + Send + 'static,
        ) -> DemandResult<T> {
            if let Err(error) = work.check() { return work.finish(Err(error)); }
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
            let spawned = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(context, move || {
                    let _permit = permit;
                    let mut held = NativeSourceWork::ordinary(&worker_work);
                    job(source, &mut held)
                }), task_meter,
            );
            let result = match spawned {
                Ok(mut worker) => tokio::select! {
                    result = &mut worker => result.map_err(native_demand_error).and_then(std::convert::identity),
                    () = &mut prepared => {
                        cancel_on_drop.0.cancel();
                        worker.await.map_err(native_demand_error).and_then(std::convert::identity)
                    },
                },
                Err(error) => Err(native_demand_error(error)),
            };
            prepared.retire_waker();
            // Completion quiesces native I/O, not legitimate intrinsic data
            // owners. Keep the same state; never wait for their physical Drop.
            let original = OriginalDemandWork::from_meter(shared);
            let closed = original.with_original_leaf(|work| {
                work.restore_budget(budget);
                (work.finish_cancelled(prepared).map_err(cancellation_error),
                    work.take_native_io_failure())
            });
            let result = match closed {
                Ok((cleanup, failure)) => cleanup.and(failure.map_or(result, Err)),
                Err(error) => Err(error),
            };
            original.finish_operation(result)
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

        /// Invalidates this native-created provider on the original invocation.
        /// Cleanup visits are reserved before changing its epoch or any slot.
        pub fn invalidate_with_native_work(
            &self, work: &crate::materializer::NativeMaterializationWork,
        ) -> Result<SourceReference, DemandError> {
            let mut work = NativeSourceWork::from_native_materialization_work(work);
            work.check()?;
            if !self.inner.root_custody.native_original { return Err(DemandError::NativeWorkUnavailable); }
            let mut cursors = self.inner.cursors.lock().map_err(|_| DemandError::StaleCursor)?;
            let visits = cursors.len() as u64;
            work.with_original_leaf(|actual| actual.reserve_pending_items(visits))?;
            let previous = match self.inner.epoch.fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| epoch.checked_add(1)) {
                Ok(previous) => previous,
                Err(_) => {
                    work.with_original_leaf(|actual| actual.refund_pending_items(visits));
                    return Err(crate::WorkError::Overflow.into());
                }
            };
            for slot in cursors.iter_mut() {
                work.with_original_leaf(|actual| actual.record_pending_items(1))?;
                // Actual source data drops outside the original ledger lock.
                drop(slot.take());
            }
            cursors.clear();
            Ok(SourceReference { identity: self.inner.identity, epoch: previous + 1 })
        }

        /// Proves that an existing caller reference still names the held root.
        /// This cannot promote an ordinary constructor into native custody.
        pub fn validate_current_with_native_work(
            &self, source: SourceReference, work: &crate::materializer::NativeMaterializationWork,
        ) -> Result<(), DemandError> {
            let mut work = NativeSourceWork::from_native_materialization_work(work);
            work.check()?;
            if !self.inner.root_custody.native_original { return Err(DemandError::NativeWorkUnavailable); }
            self.validate_current_with_work(source, &mut work)
        }

        /// Binds the caller's canonical root spelling and expected real identity
        /// to this provider, then checks both its held handle and current name.
        pub fn validate_root_with_native_work(
            &self, path: &Path, expected: crate::NativeRootIdentity,
            work: &crate::materializer::NativeMaterializationWork,
        ) -> Result<SourceReference, DemandError> {
            let mut work = NativeSourceWork::from_native_materialization_work(work);
            work.check()?;
            if !self.inner.root_custody.native_original { return Err(DemandError::NativeWorkUnavailable); }
            if !path.is_absolute() { return Err(DemandError::InvalidRequest); }
            if self.inner.root.identity() != expected { return Err(DemandError::SourceUnavailable); }
            #[cfg(unix)]
            let input = path.as_os_str().as_encoded_bytes();
            #[cfg(unix)]
            let held = self.inner.path.as_os_str().as_encoded_bytes();
            #[cfg(windows)]
            let owned = SourceRootInput::copy(path, &mut work)?.normalize(&mut work)?;
            #[cfg(windows)]
            let input = owned.path.as_slice();
            #[cfg(windows)]
            let held = self.inner.path.as_slice();
            if input.len() != held.len() { return Err(DemandError::SourceUnavailable); }
            for (input, held) in input.iter().zip(held) {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                if input != held { return Err(DemandError::SourceUnavailable); }
            }
            let source = self.reference();
            self.validate_current_with_work(source, &mut work)?;
            Ok(source)
        }

        fn validate_current_with_work<W: SourceDemandReadWork>(
            &self, source: SourceReference, work: &mut W,
        ) -> Result<(), DemandError> {
            if source != self.reference() { return Err(DemandError::StaleSource); }
            work.check()?;
            let held = self.inner.root.held_identity_with_work(work)?;
            #[cfg(unix)]
            let named = crate::native_host::root_path_identity_with_work(&self.inner.path, work)?;
            #[cfg(windows)]
            let named = crate::native_host::root_path_identity_wide_with_work(&self.inner.path, work)?;
            if held != self.inner.root.identity() || named != held { return Err(DemandError::SourceUnavailable); }
            if source != self.reference() { return Err(DemandError::StaleSource); }
            work.check()
        }

        fn check<W: SourceDemandReadWork>(&self, source: SourceReference, work: &mut W) -> Result<(), DemandError> {
            if source != self.reference() {
                return Err(DemandError::StaleSource);
            }
            work.check()?;
            if self.root_named_while_watched(work)? {
                return Ok(());
            }
            #[cfg(unix)]
            let identity = crate::native_host::root_path_identity_with_work(&self.inner.path, work);
            #[cfg(windows)]
            let identity = crate::native_host::root_path_identity_wide_with_work(&self.inner.path, work);
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
        fn root_named_while_watched<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<bool, DemandError> {
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
        fn root_named_while_watched<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<bool, DemandError> {
            work.check()?;
            Ok(false)
        }

        #[cfg(unix)]
        fn relative<W: SourceDemandReadWork>(&self, path: &NamespacePath, work: &mut W) -> Result<DemandPath, DemandError> {
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
                let name = work.with_original_leaf(|work| host_component(component, work))?;
                work.charge(WorkCounters { bytes_copied: name.as_encoded_bytes().len() as u64, ..WorkCounters::default() })?;
                result.push(&name);
                let bytes = name.as_encoded_bytes().len() as u64;
                drop(name);
                work.release(bytes)?;
            }
            Ok(result)
        }

        #[cfg(windows)]
        fn relative<W: SourceDemandReadWork>(&self, path: &NamespacePath, work: &mut W) -> Result<DemandPath, DemandError> {
            work.with_original_leaf(|work| native_wide_path(path, self.inner.limits, work))
        }

        fn stat_relative<W: SourceDemandReadWork>(&self, relative: &DemandPath, work: &mut W) -> Result<HostStat, DemandError> {
            #[cfg(unix)]
            { self.inner.root.stat_with_work(relative, work) }
            #[cfg(windows)]
            { self.inner.root.stat_wide_with_work(relative, work) }
        }

        fn metadata<W: SourceDemandReadWork>(&self, path: &NamespacePath, work: &mut W) -> Result<Option<HostStat>, DemandError> {
            let relative = self.relative(path, work)?;
            let result = self.stat_relative(&relative, work);
            let bytes = path_owned_bytes(&relative);
            drop(relative);
            work.release(bytes)?;
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

        fn node_measured<W: SourceDemandReadWork>(metadata: &HostStat, work: &mut W) -> Result<SourceNode, DemandError> {
            work.charge(WorkCounters {
                bytes_hashed: version_hash_bytes() + identity_hash_bytes(metadata),
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            Ok(Self::node(metadata))
        }

        pub(super) fn cursor_lease<W: SourceDemandReadWork>(
            &self,
            source: SourceReference,
            directory: &NamespacePath,
            observed: SourceVersion,
            cursor: Option<SourceCursor>,
            maximum_entries: u32,
            work: &mut W,
        ) -> Result<CursorLease, DemandError> {
            let mut active = self.inner.cursors.lock().map_err(|_| DemandError::StaleCursor)?;
            let slot_bytes = std::mem::size_of::<Option<(u64, CursorState)>>();
            if !work.is_native_original() { work.adopt((active.capacity() * slot_bytes) as u64)?; }
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
                if !work.is_native_original() {
                    work.adopt(directory_state.private_bytes().checked_add(directory_state.replay_bytes)
                        .ok_or(crate::WorkError::Overflow)?)?;
                }
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
                let copy = work.reserve_source_copy(copied as u64)?;
                let ownership = SourcePayloadOwnership::new(work)?;
                let (mut grown, bytes) = source_vec(capacity, work)?;
                if let Err(error) = ownership.retain(bytes, work) {
                    drop(grown);
                    let _cleanup = work.release(bytes);
                    return Err(error);
                }
                grown.append(&mut active.slots);
                let retired = std::mem::replace(&mut *active, CursorSlots { slots: grown, ownership });
                let completed = if let Some(copy) = copy {
                    copy.record(copied as u64).map_err(crate::native_name::original_name_error)
                } else {
                    work.record(WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED })
                };
                drop(retired);
                if !work.is_native_original() { work.release(old as u64)?; }
                completed?;
            }
            if index >= active.len() {
                work.charge(WorkCounters { items_examined: (index + 1 - active.len()) as u64, ..WorkCounters::UNCHARGED })?;
                active.resize_with(index + 1, || None);
            }
            let ownership = SourcePayloadOwnership::new(work)?;
            let directory = clone_namespace(directory, work)?;
            ownership.retain(directory.retained_bytes(), work)?;
            let (replay, replay_bytes) = Self::page_buffer(maximum_entries, work)?;
            work.allocate(std::mem::size_of::<Mutex<DirectoryState>>() as u64)?;
            ownership.retain(std::mem::size_of::<Mutex<DirectoryState>>() as u64, work)?;
            let state = Arc::new(Mutex::new(DirectoryState {
                source, directory, version: observed, entries: None, pending: None,
                replay: Some(replay), replay_bytes, replay_offset: 0,
                eof: false, in_use: true, unpublished_start: true, ownership,
            }));
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            active[index] = Some((token, CursorState { directory: Arc::clone(&state) }));
            Ok(CursorLease {
                source: self.clone(), reference: source, version: observed, token, state,
                active: true,
            })
        }

        fn page_buffer<W: SourceDemandReadWork>(maximum_entries: u32, work: &mut W) -> Result<(Arc<SourceDirectoryBacking>, u64), DemandError> {
            let capacity = maximum_entries as usize;
            let arc_bytes = std::mem::size_of::<SourceDirectoryBacking>() as u64;
            let ownership = SourcePayloadOwnership::new(work)?;
            let (entries, buffer_bytes) = source_vec(capacity, work)?;
            if let Err(error) = ownership.retain(buffer_bytes, work) {
                drop(entries);
                let _cleanup = work.release(buffer_bytes);
                return Err(error);
            }
            work.allocate(arc_bytes)?;
            ownership.retain(arc_bytes, work)?;
            Ok((Arc::new(SourceDirectoryBacking { entries, ownership }), buffer_bytes + arc_bytes))
        }

        fn open_directory_state<W: SourceDemandReadWork>(&self, state: &mut DirectoryState, work: &mut W) -> Result<(), DemandError> {
            if state.entries.is_some() { return Ok(()); }
            let ownership = SourcePayloadOwnership::new(work)?;
            let relative = self.relative(&state.directory, work)?;
            #[cfg(unix)]
            let result = self.inner.root.read_dir_stats_with_work(&relative, work);
            #[cfg(windows)]
            let result = self.inner.root.read_dir_stats_wide_with_work(&relative, work);
            match result {
                Ok((reader, allocation)) => {
                    ownership.retain(allocation, work)?;
                    state.entries = Some(DirectoryReader { reader, ownership });
                }
                Err(error) => {
                    let bytes = path_owned_bytes(&relative);
                    drop(relative);
                    work.release(bytes)?;
                    return Err(error);
                }
            }
            let bytes = path_owned_bytes(&relative);
            drop(relative);
            work.release(bytes)?;
            Ok(())
        }

        /// Converts one enumerated entry into the node a lookup reports,
        /// stat'ing the name only where enumeration could not report it.
        /// `None` means the entry vanished after enumeration.
        #[cfg(unix)]
        fn next_entry<W: SourceDemandReadWork>(
            &self,
            directory: &DemandPath,
            entry: &HostListedEntry,
            work: &mut W,
        ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
            let file_name = &entry.name;
            let ((encoding, bytes), allocation) = crate::native_name::host_name_bytes_with_source_work(
                file_name, self.inner.profile, self.inner.limits.maximum_component_bytes, work,
            )?;
            let ownership = if let Some(allocation) = allocation {
                SourcePayloadOwnership::from_native_allocation(allocation)
            } else {
                let ownership = SourcePayloadOwnership::new(work)?;
                ownership.retain(bytes.capacity() as u64, work)?;
                ownership
            };
            let name = crate::kernel::LogicalName::new(
                encoding,
                bytes,
                self.inner.limits.maximum_component_bytes,
            )
            .map_err(|_| DemandError::InvalidRequest)?;
            let node = match &entry.stat {
                Some(stat) => Self::node_measured(stat, work)?,
                None => {
                    let result = self.inner.root.open_dir_held_with_work(directory, work)
                        .and_then(|parent| crate::native_host::stat_leaf_with_work(&parent, file_name, work));
                    match result {
                        Ok(stat) => Self::node_measured(&stat, work)?,
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                            let bytes = name.as_bytes().len() as u64;
                            drop(name);
                            drop(ownership);
                            if !work.is_native_original() { work.release(bytes)?; }
                            return Ok(None);
                        }
                        Err(error) => return Err(error),
                    }
                }
            };
            Ok(Some(SourceDirectoryEntry { name, node, ownership }))
        }

        #[cfg(windows)]
        fn next_entry<W: SourceDemandReadWork>(
            &self, directory: &DemandPath, entry: &HostListedEntry, work: &mut W,
        ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
            let ((encoding, bytes), allocation) = crate::native_name::host_wide_name_bytes_with_source_work(
                &entry.name, self.inner.profile, self.inner.limits.maximum_component_bytes, work,
            )?;
            let ownership = if let Some(allocation) = allocation {
                SourcePayloadOwnership::from_native_allocation(allocation)
            } else {
                let ownership = SourcePayloadOwnership::new(work)?;
                ownership.retain(bytes.capacity() as u64, work)?;
                ownership
            };
            let name = crate::kernel::LogicalName::new(encoding, bytes, self.inner.limits.maximum_component_bytes)
                .map_err(|_| DemandError::InvalidRequest)?;
            let node = match &entry.stat {
                Some(stat) => Self::node_measured(stat, work)?,
                None => {
                    let result = self.inner.root.open_dir_held_wide_with_work(directory, work)
                        .and_then(|parent| crate::native_host::stat_windows_leaf_with_work(&parent, &entry.name, work));
                    match result {
                        Ok(stat) => Self::node_measured(&stat, work)?,
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                            let bytes = name.as_bytes().len() as u64;
                            drop(name);
                            drop(ownership);
                            if !work.is_native_original() { work.release(bytes)?; }
                            return Ok(None);
                        }
                        Err(error) => return Err(error),
                    }
                }
            };
            Ok(Some(SourceDirectoryEntry { name, node, ownership }))
        }

        fn read_entry<W: SourceDemandReadWork>(reader: &mut DirectoryReader, work: &mut W) -> Result<Option<DirectoryPending>, DemandError> {
            let ownership = SourcePayloadOwnership::new(work)?;
            #[cfg(unix)]
            let result = reader.reader.next_with_work(work)?;
            #[cfg(windows)]
            let result = reader.reader.next_wide_with_work(work)?;
            let Some((entry, allocation)) = result else { return Ok(None) };
            ownership.retain(allocation, work)?;
            Ok(Some(DirectoryPending { entry, ownership }))
        }

        fn page_entries<W: SourceDemandReadWork>(
            &self,
            state: &mut DirectoryState,
            maximum_entries: u32,
            work: &mut W,
            mut on_entry: impl FnMut(),
        ) -> Result<std::ops::Range<usize>, DemandError> {
            if let Some(entries) = &state.replay
                && state.replay_offset < entries.len()
            {
                let end = state.replay_offset.saturating_add(maximum_entries as usize).min(entries.len());
                for index in state.replay_offset..end {
                    work.preflight(WorkCounters { items_returned: (index - state.replay_offset + 1) as u64, ..WorkCounters::UNCHARGED })?;
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
                    work.preflight(WorkCounters {
                        source_entries_visited: 1, items_returned: count as u64 + 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    work.charge(WorkCounters { source_entries_visited: 1, items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    let converted = self.next_entry(&directory, &entry.entry, work)?;
                    let raw_bytes = entry.entry.logical_owned_bytes();
                    if let Some(converted) = converted {
                        let name_bytes = converted.name.retained_bytes() as u64;
                        let bytes = state.replay_bytes.checked_add(name_bytes).ok_or(crate::WorkError::Overflow)?;
                        let entries = Arc::get_mut(state.replay.as_mut().ok_or(DemandError::StaleCursor)?)
                            .ok_or(DemandError::CursorBusy)?;
                        entries.entries.push(converted);
                        state.replay_bytes = bytes;
                    }
                    drop(state.pending.take());
                    if !work.is_native_original() { work.release(raw_bytes)?; }
                    on_entry();
                }
                if !state.eof && state.pending.is_none() {
                    state.pending = Self::read_entry(state.entries.as_mut().ok_or(DemandError::StaleCursor)?, work)?;
                    if state.pending.is_none() { state.eof = true; }
                }
                work.check()?;
                Ok(0..state.replay.as_ref().map_or(0, |entries| entries.len()))
            })();
            let bytes = path_owned_bytes(&directory);
            drop(directory);
            work.release(bytes)?;
            answer
        }

        fn lookup_with_work<W: SourceDemandReadWork>(
            &self, source: SourceReference, path: &NamespacePath, work: &mut W,
        ) -> Result<Option<SourceNode>, DemandError> {
            self.check(source, work)?;
            self.observe_parent(path, work)?;
            let mut node = self.metadata(path, work)?.as_ref()
                .map(|metadata| Self::node_measured(metadata, work)).transpose()?;
            if node.is_some_and(|node| node.kind == SourceNodeKind::Directory)
                && self.admit_to_watches(path, work)?
            {
                node = self.metadata(path, work)?.as_ref()
                    .map(|metadata| Self::node_measured(metadata, work)).transpose()?;
            }
            self.check(source, work)?;
            Ok(node)
        }

        fn list_page_with_work<W: SourceDemandReadWork>(
            &self, source: SourceReference, directory: &NamespacePath,
            cursor: Option<SourceCursor>, maximum_entries: u32, work: &mut W,
        ) -> Result<SourceDirectoryPage, DemandError> {
            if maximum_entries == 0 || maximum_entries > 4096 { return Err(DemandError::InvalidRequest); }
            let ownership = SourcePayloadOwnership::new(work)?;
            let guard_bytes = std::mem::size_of::<NativePagePublication>() as u64;
            work.allocate(guard_bytes)?;
            let mut publication = Box::new(NativePagePublication {
                lease: None, end: 0, private_bytes: 0,
                original_cleanup: None,
                ownership,
            });
            publication.ownership.retain(guard_bytes, work)?;
            let original_budget = work.with_original_leaf(|actual| actual.budget_before_pending_items());
            if let Some(original) = work.native_source_work().map(|work| work.original_demand_work().allocation_work()) {
                work.with_original_leaf(|actual| actual.reserve_pending_items(1))?;
                publication.original_cleanup = Some(original);
            } else {
                work.with_original_leaf(|actual| {
                    actual.budget = publication_cleanup().remaining(actual.budget)?;
                    Ok::<_, DemandError>(())
                })?;
            }
            let answer = (|| {
                self.check(source, work)?;
                self.observe_directory(directory, work)?;
                let metadata = self.metadata(directory, work)?.ok_or(DemandError::Absent)?;
                if !metadata.is_dir() { return Err(DemandError::NotDirectory); }
                let observed = work.with_original_leaf(|actual| version_measured(&metadata, actual))?;
                publication.lease = Some(self.cursor_lease(source, directory, observed, cursor, maximum_entries, work)?);
                let lease = publication.lease.as_ref().ok_or(DemandError::StaleCursor)?;
                let mut state = lease.state()?;
                let range = self.page_entries(&mut state, maximum_entries, work, || {})?;
                let backing = state.replay.as_ref().ok_or(DemandError::StaleCursor)?;
                let has_next = !state.eof || state.pending.is_some() || range.end < backing.len();
                let entries = SourceDirectoryEntries::shared(Arc::clone(backing), range.clone(), state.replay_bytes);
                let private_bytes = state.private_bytes().checked_add(guard_bytes).ok_or(crate::WorkError::Overflow)?;
                drop(state);
                self.check(source, work)?;
                let current = self.metadata(directory, work)?;
                if current.as_ref().map(|metadata| work.with_original_leaf(|actual| version_measured(metadata, actual)))
                    .transpose()? != Some(observed) {
                    return Err(DemandError::StaleVersion);
                }
                let next = if has_next {
                    let mut name_ownership = SourcePayloadOwnership::new(work)?;
                    let directory = clone_namespace(directory, work)?;
                    name_ownership.retain(namespace_owned_bytes(&directory)?, work)?;
                    Some(SourceCursor {
                        source, directory, directory_version: observed, token: lease.token,
                        #[cfg(not(target_arch = "wasm32"))]
                        native_ownership: NativeCursorOwnership {
                            custody: None, borrowed: false, allocation: None,
                            raw_payload: Some(name_ownership),
                        },
                    })
                } else { None };
                work.check()?;
                work.record(WorkCounters { items_returned: range.len() as u64, ..WorkCounters::UNCHARGED })?;
                publication.end = range.end;
                publication.private_bytes = private_bytes;
                Ok((entries, next, observed))
            })();
            if !work.is_native_original() {
                work.with_original_leaf(|actual| actual.restore_budget(original_budget));
            }
            match answer {
                Ok((entries, next, version)) => Ok(SourceDirectoryPage {
                    entries, next, version, publication: SourcePagePublication::guarded(publication),
                }),
                Err(error) => {
                    if !work.is_native_original() && publication.lease.is_some() {
                        work.record(publication_cleanup())?;
                        publication.lease.take();
                    }
                    // Native Drop consumes the original reserved cleanup and
                    // restores the SAME lease/raw page even after cancellation.
                    drop(publication);
                    Err(error)
                }
            }
        }

        fn read_link_with_work<W: SourceDemandReadWork>(
            &self, source: SourceReference, path: &NamespacePath,
            expected: SourceVersion, maximum_bytes: u64, work: &mut W,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            self.check(source, work)?;
            self.observe_parent(path, work)?;
            let before = self.metadata(path, work)?.ok_or(DemandError::Absent)?;
            if !before.file_type().is_symlink() { return Err(DemandError::InvalidRequest); }
            #[cfg(unix)]
            if before.len() > maximum_bytes { return Err(DemandError::InvalidRequest); }
            if work.with_original_leaf(|actual| version_measured(&before, actual))? != expected {
                return Err(DemandError::StaleVersion);
            }
            #[cfg(unix)]
            work.preflight(WorkCounters { output_bytes: before.len(), ..WorkCounters::UNCHARGED })?;
            #[cfg(unix)]
            if work.is_native_original() {
                work.preflight(WorkCounters { source_bytes_read: before.len(), ..WorkCounters::UNCHARGED })?;
            }
            let relative = self.relative(path, work)?;
            let result = (|| {
                let remaining = work.remaining()?;
                let maximum = remaining.output_bytes.min(remaining.source_bytes_read).min(maximum_bytes);
                let bytes = if work.is_native_original() {
                    self.inner.root.read_source_link_with_work(&relative, maximum, work)?
                } else {
                let ownership = SourcePayloadOwnership::new(work)?;
                #[cfg(windows)]
                let target_ownership = SourcePayloadOwnership::new(work)?;
                #[cfg(unix)]
                let (target, target_bytes) = self.inner.root.read_link_with_work(&relative, maximum, work)?;
                #[cfg(windows)]
                let (target, target_bytes) = self.inner.root.read_link_wide_with_work(&relative, maximum, work)?;
                #[cfg(unix)]
                ownership.retain(target_bytes, work)?;
                #[cfg(windows)]
                target_ownership.retain(target_bytes, work)?;
                #[cfg(unix)]
                let bytes = target;
                #[cfg(windows)]
                let bytes = {
                    let length = target.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
                    work.preflight(WorkCounters { output_bytes: length as u64, ..WorkCounters::UNCHARGED })?;
                    work.allocate(length as u64)?;
                    let mut bytes = Vec::new();
                    if let Err(error) = bytes.try_reserve_exact(length) {
                        drop(bytes);
                        let _released = work.release(length as u64);
                        return Err(DemandError::Allocation(error));
                    }
                    let mut owned = length as u64;
                    if let Err(error) = work.with_original_leaf(|work|
                        work.reconcile_native_allocation(&mut owned, bytes.capacity() as u64)) {
                        drop(bytes);
                        let _released = work.release(owned);
                        return Err(error);
                    }
                    if let Err(error) = ownership.retain(owned, work) {
                        drop(bytes);
                        let _released = work.release(owned);
                        return Err(error);
                    }
                    for unit in &target {
                        if let Err(error) = work.charge(WorkCounters {
                            bytes_encoded: 2, bytes_copied: 2, items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        }) {
                            drop(bytes);
                            if work.is_native_original() { ownership.retire(owned)?; }
                            else { work.release(owned)?; }
                            return Err(error);
                        }
                        bytes.extend_from_slice(&unit.to_le_bytes());
                    }
                    drop(target);
                    drop(target_ownership);
                    if !work.is_native_original() { work.release(target_bytes)?; }
                    bytes
                };
                bytes.into()
                };
                let after = self.metadata(path, work)?.ok_or(DemandError::Absent)?;
                if work.with_original_leaf(|actual| version_measured(&after, actual))? != expected {
                    return Err(DemandError::StaleVersion);
                }
                self.check(source, work)?;
                work.charge(WorkCounters { output_bytes: bytes.len() as u64, ..WorkCounters::UNCHARGED })?;
                let logical_owned_bytes = bytes.retained_capacity().unwrap_or(0) as u64;
                if work.is_native_original() {
                    Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, logical_owned_bytes))
                } else {
                    Ok(crate::kernel::OwnedSourceChunk::claimed(bytes, logical_owned_bytes))
                }
            })();
            let bytes = path_owned_bytes(&relative);
            drop(relative);
            work.release(bytes)?;
            result
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
        file: Arc<NativeSourceFileSlot>,
        #[cfg(not(windows))]
        file: Arc<NativeSourceFile>,
        ownership: SourcePayloadOwnership,
    }

    /// A source file held for reading. On Windows it gives way to whoever
    /// renames or deletes it, as an editor saving over it does, instead of
    /// making that fail while it is read.
    #[cfg(windows)]
    type SourceFile = crate::native_host::YieldingFile;
    #[cfg(not(windows))]
    type SourceFile = std::fs::File;

    struct NativeSourceFile {
        file: SourceFile,
        ownership: SourcePayloadOwnership,
    }

    impl std::ops::Deref for NativeSourceFile {
        type Target = SourceFile;
        fn deref(&self) -> &Self::Target { &self.file }
    }

    #[cfg(windows)]
    struct NativeSourceFileSlot {
        file: std::sync::RwLock<Arc<NativeSourceFile>>,
        ownership: SourcePayloadOwnership,
    }


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
        #[allow(unsafe_code, reason = "initializes the preadmitted held-file box exactly once")]
        fn open_boxed<W: SourceDemandReadWork>(
            provider: NativeDemandSource, source: SourceReference,
            path: &NamespacePath, expected: SourceVersion, work: &mut W,
        ) -> Result<Box<dyn DemandFile>, DemandError> {
            work.reserve_output()?;
            let bytes = std::mem::size_of::<Self>() as u64;
            if let Err(error) = work.allocate(bytes) {
                let _cleanup = work.cancel_output();
                return Err(error);
            }
            let mut slot = Box::<Self>::new_uninit();
            let file = match Self::open(provider, source, path, expected, work) {
                Ok(file) => file,
                Err(error) => {
                    drop(slot);
                    let _released = work.release(bytes);
                    let _cancelled = work.cancel_output();
                    return Err(error);
                }
            };
            if let Err(error) = file.ownership.retain(bytes, work) {
                drop(file);
                drop(slot);
                let _released = work.release(bytes);
                let _cancelled = work.cancel_output();
                return Err(error);
            }
            slot.write(file);
            let file = unsafe { slot.assume_init() };
            work.commit_output()?;
            Ok(file as Box<dyn DemandFile>)
        }

        fn open<W: SourceDemandReadWork>(
            provider: NativeDemandSource,
            source: SourceReference,
            path: &NamespacePath,
            expected: SourceVersion,
            work: &mut W,
        ) -> Result<Self, DemandError> {
            provider.check(source, work)?;
            provider.observe_parent(path, work)?;
            let ownership = SourcePayloadOwnership::new(work)?;
            let relative = provider.relative(path, work)?;
            ownership.retain(path_owned_bytes(&relative), work)?;
            let file_ownership = SourcePayloadOwnership::new(work)?;
            #[cfg(windows)]
            let file = provider.inner.root.open_file_yielding_wide_with_work(&relative, work);
            #[cfg(not(windows))]
            let file = provider
                .inner
                .root
                .open_file_with_work(&relative, work)
                .map(cap_std::fs::File::into_std);
            let file = match file {
                Ok(file) => file,
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(DemandError::Absent);
                }
                Err(error) => return Err(error),
            };
            #[cfg(windows)]
            file_ownership.retain(file.logical_owned_bytes(), work)?;
            let opened = stat_held(&provider, &file, work)?;
            if !opened.file_type().is_file() {
                return Err(DemandError::NotRegularFile);
            }
            if work.with_original_leaf(|work| version_measured(&opened, work))? != expected {
                return Err(DemandError::StaleVersion);
            }
            let file_bytes = std::mem::size_of::<NativeSourceFile>() as u64;
            work.allocate(file_bytes)?;
            file_ownership.retain(file_bytes, work)?;
            let file = Arc::new(NativeSourceFile { file, ownership: file_ownership });
            #[cfg(windows)]
            let file = {
                let slot_ownership = SourcePayloadOwnership::new(work)?;
                let bytes = std::mem::size_of::<NativeSourceFileSlot>() as u64;
                work.allocate(bytes)?;
                slot_ownership.retain(bytes, work)?;
                Arc::new(NativeSourceFileSlot {
                    file: std::sync::RwLock::new(file), ownership: slot_ownership,
                })
            };
            Ok(Self {
                provider,
                source,
                relative,
                expected,
                logical_bytes: opened.len(),
                file,
                ownership,
            })
        }

        fn read<W: SourceDemandReadWork>(
            &self,
            range: ReadRange,
            work: &mut W,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            #[cfg(windows)]
            let file = self
                .file
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
                let reopened = Arc::new(self.reopen(work)?);
                let current = {
                    let mut held = self
                        .file
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
        fn reopen<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<NativeSourceFile, DemandError> {
            self.provider.check(self.source, work)?;
            let ownership = SourcePayloadOwnership::new(work)?;
            let file = match self.provider.inner.root.open_file_yielding_wide_with_work(&self.relative, work) {
                Ok(file) => file,
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(DemandError::StaleVersion);
                }
                Err(error) => return Err(error),
            };
            ownership.retain(file.logical_owned_bytes(), work)?;
            let opened = stat_held(&self.provider, &file, work)?;
            if work.with_original_leaf(|work| version_measured(&opened, work))? != self.expected {
                return Err(DemandError::StaleVersion);
            }
            let bytes = std::mem::size_of::<NativeSourceFile>() as u64;
            work.allocate(bytes)?;
            ownership.retain(bytes, work)?;
            Ok(NativeSourceFile { file, ownership })
        }

        fn read_once<W: SourceDemandReadWork>(
            &self,
            file: &SourceFile,
            ReadRange { offset, length }: ReadRange,
            work: &mut W,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            work.check()?;
            let length = length.min(self.logical_bytes.saturating_sub(offset));
            let size = usize::try_from(length).map_err(|_| DemandError::InvalidRequest)?;
            work.preflight(WorkCounters { output_bytes: length, ..WorkCounters::UNCHARGED })?;
            if work.is_native_original() {
                work.preflight(WorkCounters { source_bytes_read: length, ..WorkCounters::UNCHARGED })?;
            }
            let mut ownership = Some(SourcePayloadOwnership::new(work)?);
            let mut capacity = length;
            work.allocate(capacity)?;
            let mut bytes = Vec::new();
            if let Err(error) = bytes.try_reserve_exact(size) {
                drop(bytes);
                let _cleanup = work.release(capacity);
                return Err(DemandError::Allocation(error));
            }
            if let Err(error) = work.reconcile_allocation(&mut capacity, bytes.capacity() as u64) {
                drop(bytes);
                let _cleanup = work.release(capacity);
                return Err(error);
            }
            if let Err(error) = ownership.as_ref().expect("untransferred source custody").retain(capacity, work) {
                drop(bytes);
                let _cleanup = work.release(capacity);
                return Err(error);
            }
            let mut prepared = if work.is_native_original() {
                let original = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?
                    .materialization_work()?;
                let copied = original.reserve_copy(capacity).map_err(crate::native_name::original_name_error)?;
                bytes.resize(bytes.capacity(), 0);
                copied.record(capacity).map_err(crate::native_name::original_name_error)?;
                Some(acyclic_stream::PreparedOwnedBytes::new(
                    std::mem::take(&mut bytes), ownership.take().expect("original source custody"),
                    original.runtime_meter_owner(),
                ).map_err(|failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                        crate::native_name::original_name_error(original.runtime_error(error))
                    }
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                    | acyclic_stream::PreparedOwnedBytesError::NotUnique => DemandError::InvalidRequest,
                })?)
            } else {
                bytes.resize(size, 0);
                None
            };
            let result = (|| {
                let mut filled = 0;
                let destination = match prepared.as_mut() {
                    Some(prepared) => &mut prepared.writable()[..size],
                    None => bytes.as_mut_slice(),
                };
                while let Some(unfilled) = destination.get_mut(filled..).filter(|rest| !rest.is_empty()) {
                    work.check()?;
                    #[cfg(unix)]
                    let read = work.with_native_read_meter(|meter| {
                        acyclic_native_runtime::read_declared_source_bytes(
                            file, offset + filled as u64, unfilled, meter,
                        )
                    }).map_err(native_demand_error);
                    #[cfg(windows)]
                    let read = file.read_at_with_work(offset + filled as u64, unfilled, work);
                    match read {
                        Ok(0) => break,
                        Ok(count) => filled += count,
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error),
                    }
                }
                self.prove_current(file, work)?;
                work.charge(WorkCounters { output_bytes: filled as u64, ..WorkCounters::UNCHARGED })?;
                if !work.is_native_original() && filled != bytes.capacity() {
                    work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED })?;
                }
                Ok(filled)
            })();
            match result {
                Ok(filled) => {
                    if let Some(prepared) = prepared {
                        let bytes = prepared.freeze(filled).map_err(|failed| {
                            drop(failed);
                            DemandError::InvalidRequest
                        })?;
                        Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, capacity))
                    } else {
                        bytes.truncate(filled);
                        Ok(crate::kernel::OwnedSourceChunk::claimed(bytes.into(), capacity))
                    }
                }
                Err(error) => {
                    drop(prepared);
                    drop(bytes);
                    drop(ownership);
                    if !work.is_native_original() { work.release(capacity)?; }
                    Err(error)
                }
            }
        }

        /// What the file's source name holds now, through its held root.
        fn named_stat<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<HostStat, DemandError> {
            self.provider.stat_relative(&self.relative, work)
        }

        /// Proves the bytes just read belong to the opened version: the held
        /// file is unmodified (in-place writes), the source path still names
        /// it (replacement by rename), and the root and reference are current.
        fn prove_current<W: SourceDemandReadWork>(
            &self,
            file: &SourceFile,
            work: &mut W,
        ) -> Result<(), DemandError> {
            let held = stat_held(&self.provider, file, work)?;
            let named = self.named_stat(work);
            if matches!(named, Err(DemandError::Work(_) | DemandError::Cancelled)) {
                return named.map(|_| ());
            }
            let held_version = work.with_original_leaf(|work| version_measured(&held, work))?;
            let named_version = if held_version == self.expected {
                named.as_ref().ok().map(|metadata| work.with_original_leaf(|work| version_measured(metadata, work))).transpose()?
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

    #[cfg(all(test, unix))]
    mod original_file_tests {
        use super::*;

        async fn opened(
            cancellation: &CancellationToken,
        ) -> Result<(tempfile::TempDir, NativeDemandFile, Arc<NativeDemandReadMeter>), Box<dyn std::error::Error>> {
            let root = tempfile::tempdir()?;
            std::fs::write(root.path().join("file"), b"native")?;
            let provider = NativeDemandSource::open(
                root.path(), FilesystemProfile::Portable, VolumeLimits::default(),
            ).await?;
            let limits = VolumeLimits::default();
            let path = NamespacePath::from_portable(
                &crate::path::PortablePath::parse("/file", limits)?, limits,
            )?;
            let expected = provider.lookup(provider.reference(), &path, WorkBudget::UNBOUNDED, cancellation)
                .await?.value.ok_or("missing source file")?.version;
            let original = Arc::new(NativeDemandReadMeter(Mutex::new(
                DemandWork::new(WorkBudget::UNBOUNDED, cancellation),
            )));
            let file = NativeDemandFile::open(
                provider.clone(), provider.reference(), &path, expected, &mut NativeSourceWork::new(&original),
            )?;
            Ok((root, file, original))
        }

        #[tokio::test]
        async fn original_bytes_retire_only_after_last_physical_owner_and_survive_origin_drop()
        -> Result<(), Box<dyn std::error::Error>> {
            let cancellation = CancellationToken::new();
            let (_root, file, original) = opened(&cancellation).await?;
            let chunk = file.read(ReadRange::new(0, 6)?, &mut NativeSourceWork::new(&original))?;
            assert_eq!(chunk.bytes.as_ref(), b"native");
            assert_eq!(chunk.logical_owned_bytes, 6);
            let before = original.with_original_leaf(DemandWork::receipt);
            let live = original.with_original_leaf(|work| work.live_allocation_bytes());
            let alias = chunk.bytes.clone();
            let empty = chunk.bytes.slice(..0);
            drop(chunk);
            original.with_original_leaf(DemandWork::reconcile_native_controls)?;
            assert_eq!(original.with_original_leaf(|work| work.live_allocation_bytes()), live);
            assert_eq!(original.with_original_leaf(DemandWork::receipt), before);
            drop(alias);
            original.with_original_leaf(DemandWork::reconcile_native_controls)?;
            assert_eq!(original.with_original_leaf(|work| work.live_allocation_bytes()), live);
            drop(empty);
            original.with_original_leaf(DemandWork::reconcile_native_controls)?;
            assert!(original.with_original_leaf(|work| work.live_allocation_bytes()) <= live - 6);
            let chunk = file.read(ReadRange::new(0, 6)?, &mut NativeSourceWork::new(&original))?;
            let empty = chunk.bytes.slice(..0);
            let origin = Arc::downgrade(&original);
            drop(file);
            drop(original);
            assert_eq!(chunk.bytes.as_ref(), b"native");
            assert!(origin.upgrade().ok_or("byte owner retired its original early")?
                .with_original_leaf(|work| work.live_allocation_bytes())
                >= chunk.logical_owned_bytes);
            drop(chunk);
            assert!(origin.upgrade().is_some(), "the empty alias retains the original context");
            drop(empty);
            assert!(origin.upgrade().is_none());
            Ok(())
        }

        #[tokio::test]
        async fn guarded_source_bytes_transfer_scratch_without_extending_capture_lifetime()
        -> Result<(), Box<dyn std::error::Error>> {
            let cancellation = CancellationToken::new();
            let (_root, file, meter) = opened(&cancellation).await?;
            let original = OriginalDemandWork::from_meter(Arc::clone(&meter));
            let native = crate::NativeMaterializationWork::from_original_demand_work(&original)?;
            let capture = CaptureTransientCustody::new_native(&native)?;
            let scoped = native.with_capture_custody(&capture)?;
            let chunk = file.read(ReadRange::new(0, 6)?, &mut NativeSourceWork::from_native_materialization_work(&scoped))?;
            assert_eq!(chunk.bytes.as_ref(), b"native");
            assert_eq!(capture.retained_bytes(), 0, "the SDK payload is intrinsic, not capture scratch");
            let capture_origin = Arc::downgrade(&capture);
            drop(capture);
            assert!(capture_origin.upgrade().is_none(), "the SDK payload retains only the original ledger and resource");
            assert_eq!(chunk.bytes.as_ref(), b"native");
            drop(chunk);
            original.with_original_leaf(DemandWork::reconcile_native_controls)??;
            Ok(())
        }

        #[tokio::test]
        async fn original_partial_stale_read_preserves_source_prefix_and_denials_start_no_body()
        -> Result<(), Box<dyn std::error::Error>> {
            let cancellation = CancellationToken::new();
            let (root, file, original) = opened(&cancellation).await?;
            original.with_original_leaf(|work| work.budget.source_bytes_read = 0);
            assert!(matches!(file.read(ReadRange::new(0, 6)?, &mut NativeSourceWork::new(&original)),
                Err(DemandError::Work(_))));
            assert_eq!(original.with_original_leaf(|work| work.work.source_bytes_read), 0);
            original.with_original_leaf(|work| work.budget.source_bytes_read = u64::MAX);
            std::fs::OpenOptions::new().write(true).open(root.path().join("file"))?.set_len(2)?;
            assert!(matches!(file.read(ReadRange::new(0, 6)?, &mut NativeSourceWork::new(&original)),
                Err(DemandError::StaleVersion)));
            assert_eq!(original.with_original_leaf(|work| work.work.source_bytes_read), 2);
            assert_eq!(original.with_original_leaf(|work| work.work.output_bytes), 0);
            original.with_original_leaf(DemandWork::reconcile_native_controls)?;
            cancellation.cancel();
            let before = original.with_original_leaf(DemandWork::receipt);
            assert!(matches!(file.read(ReadRange::new(0, 6)?, &mut NativeSourceWork::new(&original)),
                Err(DemandError::Cancelled)));
            assert_eq!(original.with_original_leaf(DemandWork::receipt), before);
            Ok(())
        }
    }

    impl DemandFile for NativeDemandFile {
        fn logical_owned_bytes(&self) -> u64 {
            let bytes = (std::mem::size_of::<Self>() + std::mem::size_of::<NativeSourceFile>()) as u64
                + path_owned_bytes(&self.relative);
            #[cfg(windows)]
            let bytes = bytes
                + std::mem::size_of::<NativeSourceFileSlot>() as u64
                + self.file.file.read().unwrap_or_else(std::sync::PoisonError::into_inner).logical_owned_bytes();
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
                let result = self.read(range, &mut work);
                let result = work.take_native_io_failure().map_or(result, Err);
                return work.finish(result);
            }
            self.read_remote(range, work, REMOTE_READ_DEADLINE)
        }

        fn read_range_with_native_work(
            &self, offset: u64, length: u64,
            original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            let mut work = NativeSourceWork::from_native_materialization_work(original);
            work.check()?;
            let range = ReadRange::new(offset, length)?;
            #[cfg(windows)]
            let file = self.file.file.read().map_err(|_| DemandError::OriginalWorkPoisoned)?.clone();
            #[cfg(not(windows))]
            let file = Arc::clone(&self.file);
            // A yielded Windows handle is stale here: a native held-file read
            // must not silently switch to a replacement reached by its name.
            let result = self.read_once(&file, range, &mut work);
            let result = work.with_original_leaf(DemandWork::take_native_io_failure).map_or(result, Err);
            let checked = cancellation.check().map_err(|_| DemandError::Cancelled);
            result.and_then(|chunk| checked.map(|()| chunk))
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
                ownership: SourcePayloadOwnership::default(),
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
                        return OriginalDemandWork::from_meter(shared)
                            .finish_operation(Err(cancellation_error(error)));
                    }
                }
            };
            let worker_work = Arc::clone(&shared);
            let task_meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = shared.clone();
            let task = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(crate::obs::caller_context(), move || {
                    let mut work = NativeSourceWork::ordinary(&worker_work);
                    let path_bytes = path_owned_bytes(&file.relative);
                    let result = file.read(range, &mut work);
                    drop(file);
                    let released = work.release(path_bytes);
                    released.and(result)
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
            // Intrinsic file/data owners may keep the same original state.
            // The terminal queue proves quiescence without unique-Arc restore.
            let original = OriginalDemandWork::from_meter(shared);
            let closed = original.with_original_leaf(|work| {
                work.restore_budget(budget);
                let cleanup = work.finish_cancelled(prepared).map_err(cancellation_error);
                (cleanup, work.take_native_io_failure(), work.check(), work.stop.take())
            });
            let result = match closed {
                Ok((cleanup, failure, checked, owned_stop)) => {
                    drop(owned_stop);
                    drop(stop);
                    let released = original.with_original_leaf(|work| work.release(
                        std::mem::size_of::<std::sync::atomic::AtomicBool>() as u64,
                    )).and_then(std::convert::identity);
                    let result = failure.map_or(result, Err);
                    let result = if expired && !caller_cancellation.is_cancelled()
                        && matches!(&result, Ok(_) | Err(DemandError::Cancelled)) {
                        Err(DemandError::SourceUnavailable)
                    } else { result.and_then(|value| checked.map(|()| value)) };
                    released.and(cleanup).and(result)
                }
                Err(error) => Err(error),
            };
            original.finish_operation(result)
        }
    }



    /// Stats the held `file` as its source root stats a name.
    #[cfg(windows)]
    fn stat_held<W: SourceDemandReadWork>(
        provider: &NativeDemandSource,
        file: &SourceFile,
        work: &mut W,
    ) -> Result<HostStat, DemandError> {
        file.with_measured(|file| provider.inner.root.stat_file_with_work(file, work))
    }

    /// Stats the held `file` as its source root stats a name.
    #[cfg(not(windows))]
    fn stat_held<W: SourceDemandReadWork>(
        provider: &NativeDemandSource,
        file: &SourceFile,
        work: &mut W,
    ) -> Result<HostStat, DemandError> {
        provider.inner.root.stat_file_with_work(file, work)
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
        fn watch(
            &self, sink: Arc<dyn SourceChangeSink>, budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Option<Box<dyn SourceWatch>>> {
            let mut work = DemandWork::new(budget, cancellation);
            let result = self.watch_with_work(sink, &mut work);
            result.map(|value| OperationReceipt { value, work: work.work })
                .map_err(|error| OperationFailure::new(error, work.work))
        }

        fn watch_with_native_work(
            &self, sink: Arc<dyn SourceChangeSink>, mut work: NativeSourceWork<'_>,
        ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
            self.watch_with_work(sink, &mut work)
        }

        async fn lookup(
            &self,
            source: SourceReference,
            path: &NamespacePath,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Option<SourceNode>> {
            self.run_request(path, budget, cancellation, move |provider, path, work| {
                provider.lookup_with_work(source, &path, work)
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
                provider.list_page_with_work(source, &directory, cursor, maximum_entries, work)
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
                NativeDemandFile::open_boxed(provider, source, &path, expected, work)
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
                provider.read_link_with_work(source, &path, expected, u64::MAX, work)
            }).await
        }

        async fn lookup_with_native_work(
            &self, source: SourceReference, path: &NamespacePath, work: NativeSourceWork<'_>,
        ) -> Result<Option<SourceNode>, DemandError> {
            self.run_original_request(path, work, move |provider, path, work| {
                provider.lookup_with_work(source, path, work)
            }).await
        }

        async fn open_file_with_native_work(
            &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
            work: NativeSourceWork<'_>,
        ) -> Result<Box<dyn DemandFile>, DemandError> {
            self.run_original_request(path, work, move |provider, path, work| {
                NativeDemandFile::open_boxed(provider, source, path, expected, work)
            }).await
        }

        async fn list_page_with_native_work(
            &self, source: SourceReference, directory: &NamespacePath,
            cursor: Option<SourceCursor>, maximum_entries: u32, work: NativeSourceWork<'_>,
        ) -> Result<SourceDirectoryPage, DemandError> {
            if maximum_entries == 0 || maximum_entries > 4096 { return Err(DemandError::InvalidRequest); }
            self.run_original_request(directory, work, move |provider, directory, work| {
                provider.list_page_with_work(source, directory, cursor, maximum_entries, work)
            }).await
        }

        async fn read_range_with_native_work(
            &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
            offset: u64, length: u64, work: NativeSourceWork<'_>,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            let range = ReadRange::new(offset, length)?;
            self.run_original_request(path, work, move |provider, path, work| {
                NativeDemandFile::open(provider, source, path, expected, work)?.read(range, work)
            }).await
        }

        async fn read_link_with_native_work(
            &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
            maximum_bytes: u64, work: NativeSourceWork<'_>,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            self.run_original_request(path, work, move |provider, path, work| {
                provider.read_link_with_work(source, path, expected, maximum_bytes, work)
            }).await
        }
    }

    impl NativeDemandSource {
        #[allow(unsafe_code, reason = "initializes preadmitted watch owners before infallible registration")]
        fn watch_with_work<W: SourceDemandReadWork>(
            &self, sink: Arc<dyn SourceChangeSink>, work: &mut W,
        ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
            work.check()?;
            if !self.inner.inline { return Ok(None); }
            #[cfg(test)]
            if !self.watchable { return Ok(None); }
            #[cfg(any(target_os = "linux", windows))]
            let registration = NativeSourceRegistration::prepare(&self.inner, work)?;
            let changes_ownership = SourcePayloadOwnership::new(work)?;
            let bytes = std::mem::size_of::<NativeChanges>() as u64;
            work.allocate(bytes)?;
            let changes = Arc::new(NativeChanges {
                inner: Arc::downgrade(&self.inner), sink, ownership: changes_ownership,
            });
            changes.ownership.retain(bytes, work)?;
            let watch_ownership = SourcePayloadOwnership::new(work)?;
            let watch_bytes = std::mem::size_of::<NativeSourceWatchBacking>() as u64;
            work.allocate(watch_bytes)?;
            let mut watch = Arc::<NativeSourceWatchBacking>::new_uninit();
            watch_ownership.retain(watch_bytes, work)?;
            let ownership = SourcePayloadOwnership::new(work)?;
            let lease_bytes = std::mem::size_of::<NativeWatch>() as u64;
            work.allocate(lease_bytes)?;
            let mut lease = Box::<NativeWatch>::new_uninit();
            ownership.retain(lease_bytes, work)?;
            // Withhold real output credit before startup. The shared SDK
            // engine commits it while retaining its preallocated failure owner.
            work.reserve_output()?;
            let producer = match crate::source_watch::NativeSourceWatch::start_with_output_with_work(&self.inner.root, changes, work) {
                Ok(producer) => producer,
                Err(error) => {
                    let _cleanup = work.cancel_output();
                    return Err(error);
                }
            };
            Arc::get_mut(&mut watch).expect("private uninitialized watch owner")
                .write(NativeSourceWatchBacking { watch: producer, ownership: watch_ownership });
            // SAFETY: the unique Arc was initialized above; all further
            // publication is fixed storage with no allocator or late callback.
            let watch = unsafe { watch.assume_init() };
            #[cfg(any(target_os = "linux", windows))]
            let backing = registration.publish(&watch);
            #[cfg(not(any(target_os = "linux", windows)))]
            let backing = None;
            lease.write(NativeWatch { watch, backing, ownership });
            // SAFETY: the preallocated result owner was initialized above.
            Ok(Some(unsafe { lease.assume_init() } as Box<dyn SourceWatch>))
        }
    }

    /// The actual shared SDK body, with custody through its last physical drop.
    /// Registry upgrades borrow this same owner; no numeric ownership moves.
    struct NativeSourceWatchBacking {
        watch: crate::source_watch::NativeSourceWatch,
        ownership: SourcePayloadOwnership,
    }

    impl std::ops::Deref for NativeSourceWatchBacking {
        type Target = crate::source_watch::NativeSourceWatch;
        fn deref(&self) -> &Self::Target { &self.watch }
    }

    /// The live watch of one native source.
    struct NativeWatch {
        watch: Arc<NativeSourceWatchBacking>,
        backing: Option<Arc<SourceWatchAllocationCustody>>,
        ownership: SourcePayloadOwnership,
    }

    impl SourceWatch for NativeWatch {
        fn logical_owned_bytes(&self) -> u64 {
            std::mem::size_of::<Self>() as u64
                + std::mem::size_of::<NativeSourceWatchBacking>() as u64
                + self.watch.logical_owned_bytes()
        }

        fn allocation_custody(&self) -> Option<Arc<SourceWatchAllocationCustody>> {
            self.backing.as_ref().map(Arc::clone)
        }

        fn fence_measured(
            &self, budget: WorkBudget, cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            let mut work = DemandWork::new(budget, cancellation);
            let result = self.watch.fence_measured(Duration::from_secs(10), &mut work).map(|_| ());
            work.finish(result)
        }

        fn fence_with_native_work(&self, mut work: NativeSourceWork<'_>) -> Result<(), DemandError> {
            self.watch.fence_with_work(Duration::from_secs(10), &mut work).map(|_| ())
        }

        fn stop_measured(
            &self, budget: WorkBudget, cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            let mut work = DemandWork::new(budget, cancellation);
            let result = self.watch.stop_measured(&mut work);
            work.finish(result)
        }

        fn stop_with_native_work(&self, mut work: NativeSourceWork<'_>) -> Result<(), DemandError> {
            self.watch.stop_with_work(&mut work)
        }

        fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
            self.watch.stop_with_cleanup_wait(wait)
        }

        fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
            self.watch.stop_accepted_cleanup()
        }

        fn is_stopped(&self) -> bool { self.watch.is_stopped() }

        fn is_exact(&self) -> bool {
            self.watch.is_exact()
        }
    }

    /// Turns host reports into source changes: each path into its name in
    /// the source's namespace and, where the host still names a node there,
    /// that node. It refers to the source weakly, as the sink does to its
    /// view: nothing a host keeps after the watch stops may hold the source.
    struct NativeChanges {
        inner: std::sync::Weak<NativeDemandInner>,
        sink: Arc<dyn SourceChangeSink>,
        ownership: SourcePayloadOwnership,
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
        fn names_with_work<'a, W: SourceDemandReadWork>(
            &self, parts: impl Iterator<Item = NativeChangedName<'a>> + Clone,
            work: &mut W,
        ) -> Result<(NamespacePath, bool, Option<crate::NativeAllocation>), DemandError> {
            let mut count = 0_usize;
            for _ in parts.clone() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
            }
            let capacity = count.min(usize::from(self.limits.maximum_path_depth));
            let mut vector_bytes = (capacity as u64)
                .checked_mul(std::mem::size_of::<crate::kernel::LogicalName>() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            work.allocate(vector_bytes)?;
            let mut names = Vec::new();
            if let Err(error) = names.try_reserve_exact(capacity) {
                drop(names);
                work.release(vector_bytes)?;
                return Err(DemandError::Allocation(error));
            }
            let actual = names.capacity().checked_mul(std::mem::size_of::<crate::kernel::LogicalName>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            let completed = work.reconcile_allocation(&mut vector_bytes, actual);
            if let Err(error) = completed {
                drop(names);
                work.release(vector_bytes)?;
                return Err(error);
            }
            let mut owned_bytes = vector_bytes;
            let mut native_names: Option<crate::NativeAllocation> = None;
            let result = (|| {
                let mut exact = true;
                let mut encoded_bytes = 1_u32;
                for part in parts {
                    work.charge(WorkCounters { items_examined: 1, source_path_components: 1, ..WorkCounters::UNCHARGED })?;
                    if names.len() == capacity { exact = false; break; }
                    let encoded = match part {
                        NativeChangedName::Os(name) => crate::native_name::host_name_bytes_with_source_work(
                            name, self.profile, self.limits.maximum_component_bytes, work,
                        ),
                        #[cfg(windows)]
                        NativeChangedName::Wide(name) => crate::native_name::host_wide_name_bytes_with_source_work(
                            name, self.profile, self.limits.maximum_component_bytes, work,
                        ),
                        NativeChangedName::Invalid => { exact = false; break; }
                    };
                    let ((encoding, bytes), mut guard) = match encoded {
                        Ok(encoded) => encoded,
                        Err(DemandError::InvalidRequest) => { exact = false; break; }
                        Err(error) => return Err(error),
                    };
                    let retained = bytes.capacity() as u64;
                    let next = encoded_bytes.checked_add(u32::from(!names.is_empty()))
                        .and_then(|length| length.checked_add(u32::try_from(bytes.len()).ok()?));
                    if next.is_none_or(|length| length > self.limits.maximum_path_bytes) {
                        drop(bytes);
                        if guard.is_none() { work.release(retained)?; }
                        drop(guard);
                        exact = false;
                        break;
                    }
                    let name = match crate::kernel::LogicalName::new(
                        encoding, bytes, self.limits.maximum_component_bytes,
                    ) {
                        Ok(name) => name,
                        Err(_) => {
                            if guard.is_none() { work.release(retained)?; }
                            drop(guard);
                            exact = false;
                            break;
                        }
                    };
                    if let Some(mut incoming) = guard.take() {
                        if let Some(owned) = native_names.as_mut() {
                            if let Err(error) = owned.absorb(&mut incoming) {
                                drop(name);
                                drop(incoming);
                                return Err(crate::native_name::original_name_error(error));
                            }
                        } else { native_names = Some(incoming); }
                    } else {
                        owned_bytes = owned_bytes.checked_add(retained).ok_or(crate::WorkError::Overflow)?;
                    }
                    encoded_bytes = next.expect("checked native namespace length");
                    names.push(name);
                }
                work.charge(WorkCounters { items_examined: names.len() as u64, ..WorkCounters::UNCHARGED })?;
                let path = NamespacePath::new(std::mem::take(&mut names), self.limits)?;
                Ok((path, exact, native_names.take()))
            })();
            if result.is_err() {
                drop(names);
                drop(native_names);
                work.release(owned_bytes)?;
            }
            result
        }

        fn name_with_work<W: SourceDemandReadWork>(&self, relative: &Path, work: &mut W) -> Result<(NamespacePath, bool, Option<crate::NativeAllocation>), DemandError> {
            self.names_with_work(relative.components().map(|component| match component {
                std::path::Component::Normal(name) => NativeChangedName::Os(name),
                _ => NativeChangedName::Invalid,
            }), work)
        }

        #[cfg(windows)]
        fn name_wide_with_work<W: SourceDemandReadWork>(&self, relative: &[u16], work: &mut W) -> Result<(NamespacePath, bool, Option<crate::NativeAllocation>), DemandError> {
            self.names_with_work(relative.split(|unit| *unit == u16::from(b'\\') || *unit == u16::from(b'/'))
                .filter(|name| !name.is_empty()).map(NativeChangedName::Wide), work)
        }
    }

    enum NativeChangedItem<'a> {
        Everything,
        #[cfg(any(target_os = "macos", windows))]
        Unconfirmed,
        #[cfg(windows)]
        File(u64),
        Path { relative: &'a Path, altered: bool },
        #[cfg(windows)]
        WidePath { relative: &'a [u16], altered: bool },
    }

    // Native readers normally deliver one record at a time. Its name and
    // optional node fit in this stack-owned batch without a temporary heap.
    enum NativeChangeReport {
        Inline { entries: [SourceChange; 2], len: usize },
        Heap(Vec<SourceChange>),
    }

    impl NativeChangeReport {
        fn push(&mut self, change: SourceChange) {
            match self {
                Self::Inline { entries, len } => {
                    entries[*len] = change;
                    *len += 1;
                }
                Self::Heap(entries) => entries.push(change),
            }
        }

        fn as_slice(&self) -> &[SourceChange] {
            match self {
                Self::Inline { entries, len } => &entries[..*len],
                Self::Heap(entries) => entries,
            }
        }
    }

    impl NativeChanges {
        fn append_path_change<W: SourceDemandReadWork>(
            reported: &mut NativeChangeReport, name: NamespacePath, exact: bool,
            mut name_guard: Option<crate::NativeAllocation>,
            native_names: &mut Option<crate::NativeAllocation>,
            ownership: &SourcePayloadOwnership,
            altered: bool, metadata: Result<HostStat, DemandError>, work: &mut W,
        ) -> Result<(), DemandError> {
            let guarded = name_guard.as_ref().map_or(0, crate::NativeAllocation::bytes);
            let owned = name.retained_bytes().checked_sub(guarded).ok_or(crate::WorkError::Overflow)?;
            let observed = (|| {
                Ok(match metadata {
                    Ok(metadata) => {
                        work.charge(WorkCounters { bytes_hashed: identity_hash_bytes(&metadata), ..WorkCounters::UNCHARGED })?;
                        Some(file_identity(&metadata))
                    }
                    Err(DemandError::Io(error)) if matches!(error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => None,
                    Err(error) => return Err(error),
                })
            })();
            let node = match observed {
                Ok(node) => node,
                Err(error) => {
                    drop(name);
                    drop(name_guard);
                    let cleanup = work.release(owned);
                    return Err(error).and(cleanup);
                }
            };
            let altered = altered && exact && (node.is_some() || name.is_root());
            if !altered && name.is_root() {
                drop(name);
                drop(name_guard);
                work.release(owned)?;
                reported.push(SourceChange::Everything);
            } else {
                if let Err(error) = ownership.retain(owned, work) {
                    drop(name);
                    let cleanup = work.release(owned);
                    return Err(error).and(cleanup);
                }
                if let Some(mut incoming) = name_guard.take() {
                    if let Some(existing) = native_names.as_mut() {
                        if let Err(error) = existing.absorb(&mut incoming) {
                            drop(name);
                            drop(incoming);
                            let cleanup = if ownership.has_native_custody() {
                                ownership.retire(owned)
                            } else { work.release(owned) };
                            return Err(crate::native_name::original_name_error(error)).and(cleanup);
                        }
                    } else { *native_names = Some(incoming); }
                }
                reported.push(if altered { SourceChange::Entry(name) } else { SourceChange::Name(name) });
            }
            if let Some(node) = node { reported.push(SourceChange::Node(node)); }
            Ok(())
        }
        fn deliver_changes<'a, W: SourceDemandReadWork>(
            &self, changes: impl ExactSizeIterator<Item = NativeChangedItem<'a>>,
            work: &mut W,
        ) -> Result<(), DemandError> {
            work.check()?;
            let Some(inner) = self.inner.upgrade() else { return Ok(()); };
            let mut native_names: Option<crate::NativeAllocation> = None;
            let ownership = SourcePayloadOwnership::new(work)?;
            let capacity = changes.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
            let (mut reported, vector_bytes) = if capacity <= 2 {
                (NativeChangeReport::Inline { entries: [SourceChange::Everything, SourceChange::Everything], len: 0 }, 0)
            } else {
                let mut vector_bytes = (capacity as u64).checked_mul(std::mem::size_of::<SourceChange>() as u64)
                    .ok_or(crate::WorkError::Overflow)?;
                work.allocate(vector_bytes)?;
                let mut entries = Vec::new();
                if let Err(error) = entries.try_reserve_exact(capacity) {
                    drop(entries);
                    let cleanup = work.release(vector_bytes);
                    return Err(DemandError::Allocation(error)).and(cleanup);
                }
                let actual = entries.capacity().checked_mul(std::mem::size_of::<SourceChange>())
                    .ok_or(crate::WorkError::Overflow)? as u64;
                if let Err(error) = work.reconcile_allocation(&mut vector_bytes, actual) {
                    drop(entries);
                    let cleanup = work.release(vector_bytes);
                    return Err(error).and(cleanup);
                }
                if let Err(error) = ownership.retain(vector_bytes, work) {
                    drop(entries);
                    let cleanup = work.release(vector_bytes);
                    return Err(error).and(cleanup);
                }
                (NativeChangeReport::Heap(entries), vector_bytes)
            };
            let result = (|| {
                for change in changes {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    match change {
                        NativeChangedItem::Everything => reported.push(SourceChange::Everything),
                        #[cfg(any(target_os = "macos", windows))]
                        NativeChangedItem::Unconfirmed => reported.push(SourceChange::Unconfirmed),
                        #[cfg(windows)]
                        NativeChangedItem::File(index) => {
                            work.charge(WorkCounters {
                                bytes_hashed: b"acyclic-fs-native-source-file-v1\0".len() as u64 + 12,
                                ..WorkCounters::UNCHARGED
                            })?;
                            reported.push(SourceChange::Node(windows_file_identity(inner.root.identity().device, index)));
                        }
                        NativeChangedItem::Path { relative, altered } => {
                            let (name, exact, guard) = inner.name_with_work(relative, work)?;
                            let metadata = inner.root.stat_with_work(relative, work);
                            Self::append_path_change(&mut reported, name, exact, guard, &mut native_names, &ownership, altered, metadata, work)?;
                        }
                        #[cfg(windows)]
                        NativeChangedItem::WidePath { relative, altered } => {
                            let (name, exact, guard) = inner.name_wide_with_work(relative, work)?;
                            let metadata = inner.root.stat_wide_with_work(relative, work);
                            Self::append_path_change(&mut reported, name, exact, guard, &mut native_names, &ownership, altered, metadata, work)?;
                        }
                    }
                }
                if !reported.as_slice().is_empty() {
                    if let Some(source) = work.native_source_work() {
                        self.sink.source_changed_with_native_work(reported.as_slice(), source)?;
                    } else {
                        work.with_ordinary_work(|work| {
                            let remaining = work.remaining()?;
                            let cancellation = work.cancellation().clone();
                            let prior_live = work.allocations.live_bytes();
                            match self.sink.source_changed(reported.as_slice(), remaining, &cancellation) {
                                Ok(receipt) => work.merge_with_prior_live(receipt.work, prior_live),
                                Err(failure) => {
                                    work.merge_with_prior_live(*failure.work, prior_live)?;
                                    Err(failure.error)
                                }
                            }
                        }).ok_or(DemandError::NativeWorkUnavailable)??;
                    }
                }
                Ok(())
            })();
            let names = reported.as_slice().iter().try_fold(0_u64, |sum, change| {
                let bytes = match change {
                    SourceChange::Name(path) | SourceChange::Entry(path) => path.retained_bytes(),
                    _ => 0,
                };
                sum.checked_add(bytes).ok_or(crate::WorkError::Overflow)
            })?;
            let owned = vector_bytes.checked_add(names).and_then(|bytes|
                bytes.checked_sub(native_names.as_ref().map_or(0, crate::NativeAllocation::bytes)))
                .ok_or(crate::WorkError::Overflow)?;
            drop(reported);
            drop(native_names);
            let cleanup = if ownership.has_native_custody() {
                drop(ownership);
                Ok(())
            } else {
                drop(ownership);
                work.release(owned)
            };
            result.and(cleanup)
        }
    }

    impl crate::source_watch::HostChangeSink for NativeChanges {
        fn logical_owned_bytes(&self) -> u64 {
            // The incoming source sink and provider root already exist in
            // the caller; only this newly allocated adapter body is ours.
            std::mem::size_of::<Self>() as u64
        }

        fn host_changed(
            &self, changes: &[crate::source_watch::HostChange], work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            use crate::source_watch::HostChange;
            self.deliver_changes(changes.iter().map(|change| match change {
                HostChange::Everything => NativeChangedItem::Everything,
                #[cfg(any(target_os = "macos", windows))]
                HostChange::Unconfirmed => NativeChangedItem::Unconfirmed,
                #[cfg(windows)]
                HostChange::File(index) => NativeChangedItem::File(*index),
                HostChange::Rebound(relative) => NativeChangedItem::Path { relative, altered: false },
                HostChange::Altered(relative) => NativeChangedItem::Path { relative, altered: true },
            }), work)
        }

        #[cfg(windows)]
        fn host_changed_wide(
            &self, changes: &[crate::source_watch::HostChangeWide<'_>], work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            use crate::source_watch::HostChangeWide;
            self.deliver_changes(changes.iter().map(|change| match change {
                HostChangeWide::Everything => NativeChangedItem::Everything,
                HostChangeWide::Unconfirmed => NativeChangedItem::Unconfirmed,
                HostChangeWide::File(index) => NativeChangedItem::File(*index),
                HostChangeWide::Path { action, path } => NativeChangedItem::WidePath {
                    relative: path, altered: *action == 3,
                },
            }), work)
        }

        fn host_changed_with_native_work(
            &self, changes: &[crate::source_watch::HostChange], work: &mut NativeSourceWork<'_>,
        ) -> Result<(), DemandError> {
            use crate::source_watch::HostChange;
            self.deliver_changes(changes.iter().map(|change| match change {
                HostChange::Everything => NativeChangedItem::Everything,
                #[cfg(any(target_os = "macos", windows))]
                HostChange::Unconfirmed => NativeChangedItem::Unconfirmed,
                #[cfg(windows)]
                HostChange::File(index) => NativeChangedItem::File(*index),
                HostChange::Rebound(relative) => NativeChangedItem::Path { relative, altered: false },
                HostChange::Altered(relative) => NativeChangedItem::Path { relative, altered: true },
            }), work)
        }

        #[cfg(windows)]
        fn host_changed_wide_with_native_work(
            &self, changes: &[crate::source_watch::HostChangeWide<'_>], work: &mut NativeSourceWork<'_>,
        ) -> Result<(), DemandError> {
            use crate::source_watch::HostChangeWide;
            self.deliver_changes(changes.iter().map(|change| match change {
                HostChangeWide::Everything => NativeChangedItem::Everything,
                HostChangeWide::Unconfirmed => NativeChangedItem::Unconfirmed,
                HostChangeWide::File(index) => NativeChangedItem::File(*index),
                HostChangeWide::Path { action, path } => NativeChangedItem::WidePath {
                    relative: path, altered: *action == 3,
                },
            }), work)
        }
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
    /// A real native provider that exposes only the ordinary capability.
    struct OrdinaryNativeSource(NativeDemandSource);

    impl DemandSource for OrdinaryNativeSource {
        fn reference(&self) -> SourceReference { self.0.reference() }
        async fn lookup(&self, source: SourceReference, path: &NamespacePath, budget: WorkBudget,
            cancellation: &CancellationToken) -> DemandResult<Option<SourceNode>> {
            self.0.lookup(source, path, budget, cancellation).await
        }
        async fn list_page(&self, source: SourceReference, directory: &NamespacePath,
            cursor: Option<SourceCursor>, maximum_entries: u32, budget: WorkBudget,
            cancellation: &CancellationToken) -> DemandResult<SourceDirectoryPage> {
            self.0.list_page(source, directory, cursor, maximum_entries, budget, cancellation).await
        }
        async fn read_range(&self, source: SourceReference, path: &NamespacePath,
            expected: SourceVersion, offset: u64, length: u64, budget: WorkBudget,
            cancellation: &CancellationToken) -> DemandResult<crate::kernel::OwnedSourceChunk> {
            self.0.read_range(source, path, expected, offset, length, budget, cancellation).await
        }
        async fn read_link(&self, source: SourceReference, path: &NamespacePath,
            expected: SourceVersion, budget: WorkBudget,
            cancellation: &CancellationToken) -> DemandResult<crate::kernel::OwnedSourceChunk> {
            self.0.read_link(source, path, expected, budget, cancellation).await
        }
        async fn open_file(&self, source: SourceReference, path: &NamespacePath,
            expected: SourceVersion, budget: WorkBudget,
            cancellation: &CancellationToken) -> DemandResult<Box<dyn DemandFile>> {
            self.0.open_file(source, path, expected, budget, cancellation).await
        }
    }

    #[tokio::test]
    async fn unknown_original_source_preserves_precancellation_without_io()
    -> Result<(), Box<dyn Error>> {
        use crate::materializer::NativeMaterializationWork;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("actual"), b"actual")?;
        let source = OrdinaryNativeSource(NativeDemandSource::open(
            root.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?);
        let requested = path("/actual")?;
        let node = source.lookup(source.reference(), &requested, WorkBudget::UNBOUNDED,
            &CancellationToken::new()).await?.value.ok_or("actual file absent")?;
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        budget.source_bytes_read = 0;
        budget.bytes_copied = 0;
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let before = work.receipt();
        for cancelled in [false, true] {
            if cancelled { cancellation.cancel(); }
            let native = NativeSourceWork::from_native_materialization_work(&work);
            let errors = [
                source.lookup_with_native_work(source.reference(), &requested, native).await.err(),
                source.list_page_with_native_work(source.reference(), &path("/")?, None, 1, native).await.err(),
                source.read_range_with_native_work(source.reference(), &requested, node.version, 0, 6, native).await.err(),
                source.read_link_with_native_work(source.reference(), &requested, node.version, 6, native).await.err(),
                source.open_file_with_native_work(source.reference(), &requested, node.version, native).await.err(),
                source.read_metadata_with_native_work(source.reference(), &requested, None, native).await.err(),
            ];
            for error in errors {
                assert!(matches!((cancelled, error),
                    (false, Some(DemandError::NativeWorkUnavailable)) |
                    (true, Some(DemandError::Cancelled))));
            }
            assert_eq!(work.receipt(), before);
        }
        Ok(())
    }


    #[tokio::test]
    async fn original_root_invalidation_reserves_actual_cursor_retirement_before_epoch_change()
        -> Result<(), Box<dyn Error>>
    {
        use crate::materializer::NativeMaterializationWork;
        acyclic_native_runtime::initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("a"), b"first")?;
        std::fs::write(temporary.path().join("b"), b"second")?;
        let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
        if let serde_json::Value::Object(fields) = &mut allowances {
            for allowance in fields.values_mut() { *allowance = serde_json::json!(1_000_000_u64); }
        }
        let budget: WorkBudget = serde_json::from_value(allowances)?;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        let reference = SourceReference { identity: [94; 16], epoch: 20 };
        let source = NativeDemandSource::open_with_reference_and_native_work(
            temporary.path(), FilesystemProfile::Portable, VolumeLimits::default(), reference, None, &work,
        ).await?;
        let directory = path("/")?;
        let mut page = source.list_page_with_native_work(
            reference, &directory, None, 1, NativeSourceWork::new(work.native_read_meter()),
        ).await?;
        assert_eq!(page.entries.len(), 1);
        page.publication.accept_with_native_work(NativeSourceWork::new(work.native_read_meter()))?;
        let cursor = page.next.take().ok_or("native continuation absent")?;
        drop(page);
        let before = work.receipt();
        let retained = work.live_allocation_bytes();
        let original_budget = work.native_read_meter().with_original_leaf(|actual| {
            let original_budget = actual.budget;
            actual.budget.items_examined = actual.work.items_examined;
            original_budget
        });
        assert!(matches!(source.invalidate_with_native_work(&work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "items_examined", .. }))));
        assert_eq!(source.reference(), reference);
        assert_eq!(work.receipt(), before);
        work.native_read_meter().with_original_leaf(|actual| actual.restore_budget(original_budget));
        let advanced = source.invalidate_with_native_work(&work)?;
        assert_eq!(advanced.epoch, reference.epoch + 1);
        assert!(work.receipt().items_examined > before.items_examined);
        assert!(work.live_allocation_bytes() < retained, "actual retired cursor storage leaves its original ledger");
        assert!(matches!(source.validate_current_with_native_work(reference, &work), Err(DemandError::StaleSource)));
        drop(cursor);
        work.receipt().verify(budget)?;
        Ok(())
    }

    fn original_filtered_exclusions(
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(Vec<NamespacePath>, crate::materializer::NativeAllocation), Box<dyn Error>> {
        let mut custody = work.claim(std::mem::size_of::<NamespacePath>() as u64)?;
        let mut excluded = Vec::new();
        excluded.try_reserve_exact(1)?;
        custody.reconcile_capacity(
            (excluded.capacity() * std::mem::size_of::<NamespacePath>()) as u64,
        )?;
        let cancellation = work.cancellation();
        let mut accounting = crate::kernel::allocation::KernelAccounting::NativeOriginal {
            work, cancellation,
        };
        let (path, owned) = NamespacePath::parse_in_profile_with_accounting(
            "/private", FilesystemProfile::Portable, VolumeLimits::default(),
            &mut accounting, cancellation,
        )?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut owned) = owned else {
            drop(path);
            return Err(DemandError::InvalidRequest.into());
        };
        if let Err(error) = custody.absorb(&mut owned) {
            drop(path);
            drop(owned);
            return Err(error.into());
        }
        excluded.push(path);
        work.admit(WorkCounters::UNCHARGED)?;
        Ok((excluded, custody))
    }

    #[tokio::test]
    async fn filtered_original_constructor_denies_before_owner_allocation_and_retires_input()
    -> Result<(), Box<dyn Error>> {
        use crate::materializer::{NativeMaterializationWork, NativeTreeMaterializationError};
        let root = tempfile::tempdir()?;
        let native = NativeDemandSource::open(
            root.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = work.native_read_meter().lock().live_allocation_bytes();
        let (excluded, custody) = original_filtered_exclusions(&work)?;
        let before = work.receipt();
        work.native_read_meter().with_original_leaf(|state| {
            state.budget.peak_allocation_bytes = state.live_allocation_bytes()
                + std::mem::size_of::<FilteredExcludedPaths>() as u64 - 1;
        });
        assert!(matches!(FilteredDemandSource::new_with_native_work(
            native.clone(), excluded, custody, &work,
        ), Err(NativeTreeMaterializationError::Work(_))));
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), baseline);
        work.native_read_meter().with_original_leaf(|state| {
            state.budget.peak_allocation_bytes = u64::MAX;
        });
        let (excluded, custody) = original_filtered_exclusions(&work)?;
        let foreign = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let foreign_before = foreign.receipt();
        let before = work.receipt();
        assert!(matches!(FilteredDemandSource::new_with_native_work(
            native, excluded, custody, &foreign,
        ), Err(NativeTreeMaterializationError::InvalidLayout)));
        assert_eq!(foreign.receipt(), foreign_before);
        assert_eq!(work.receipt(), before);
        assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), baseline);
        Ok(())
    }

    #[tokio::test]
    async fn filtered_original_clone_retains_the_single_payload_until_final_sdk_drop()
    -> Result<(), Box<dyn Error>> {
        use crate::materializer::NativeMaterializationWork;
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("private"))?;
        let native = NativeDemandSource::open(
            root.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = work.native_read_meter().lock().live_allocation_bytes();
        let (excluded, custody) = original_filtered_exclusions(&work)?;
        let mut source = FilteredDemandSource::new_with_native_work(native, excluded, custody, &work)?;
        let mut owner = work.claim(std::mem::size_of_val(&source) as u64)?;
        source.retain_owner_allocation(&mut owner)?;
        assert_eq!(owner.bytes(), 0);
        let held = work.native_read_meter().lock().live_allocation_bytes();
        work.native_read_meter().with_original_leaf(|state| state.budget.peak_allocation_bytes = held);
        let before = work.receipt();
        let clone = source.clone();
        assert!(Arc::ptr_eq(&source.excluded, &clone.excluded));
        assert_eq!(work.receipt(), before);
        drop(source);
        assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), held);
        let private = path("/private")?;
        assert!(clone.lookup_with_native_work(
            clone.reference(), &private, NativeSourceWork::new(work.native_read_meter()),
        ).await?.is_none());
        assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
        assert_eq!(work.receipt().items_examined, before.items_examined + 2);
        work.native_read_meter().with_original_leaf(|state| {
            state.budget.items_examined = state.work.items_examined + 1;
        });
        assert!(matches!(clone.lookup_with_native_work(
            clone.reference(), &private, NativeSourceWork::new(work.native_read_meter()),
        ).await, Err(DemandError::Work(_))));
        assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
        drop(clone);
        assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn unknown_original_directory_observer_fails_without_ordinary_callback()
    -> Result<(), Box<dyn Error>> {
        use crate::materializer::NativeMaterializationWork;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let observer = RecordingObserver::default();
        let before = work.receipt();
        assert!(matches!(observer.observe_directory_with_native_work(
            &path("/")?, NativeSourceWork::new(work.native_read_meter()),
        ), Err(DemandError::NativeWorkUnavailable)));
        assert!(observer.directories.lock().map_err(|_| "observer lock poisoned")?.is_empty());
        assert_eq!(work.receipt(), before);
        cancellation.cancel();
        assert!(matches!(observer.observe_directory_with_native_work(
            &path("/")?, NativeSourceWork::new(work.native_read_meter()),
        ), Err(DemandError::Cancelled)));
        assert!(observer.directories.lock().map_err(|_| "observer lock poisoned")?.is_empty());
        assert_eq!(work.receipt(), before);
        Ok(())
    }

    #[tokio::test]
    async fn filtered_original_reads_charge_the_original_and_exclusions_do_no_io()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("visible"), b"actual")?;
        let source = FilteredDemandSource::new(NativeDemandSource::open(
            root.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?, vec![path("/hidden")?]);
        let cancellation = CancellationToken::new();
        let original = Arc::new(NativeDemandReadMeter(Mutex::new(
            DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )));
        original.with_original_leaf(|work| work.charge(WorkCounters {
            bytes_hashed: 7, ..WorkCounters::UNCHARGED
        }))?;
        let hidden = path("/hidden/missing")?;
        assert!(source.lookup_with_native_work(
            source.reference(), &hidden, NativeSourceWork::new(&original),
        ).await?.is_none());
        assert_eq!(original.lock().work.backend_read_operations, 0);
        assert_eq!(original.lock().work.items_examined, 2);
        let visible = path("/visible")?;
        let node = source.read_metadata_with_native_work(
            source.reference(), &visible, None, NativeSourceWork::new(&original),
        ).await?.ok_or("visible file absent")?;
        let chunk = source.read_range_with_native_work(
            source.reference(), &visible, node.version, 0, 6, NativeSourceWork::new(&original),
        ).await?;
        assert_eq!(chunk.bytes.as_ref(), b"actual");
        assert_eq!(original.lock().work.source_bytes_read, 6);
        assert!(original.lock().work.backend_read_operations > 0);
        assert!(original.lock().work.bytes_hashed >= 7);
        drop(chunk);
        original.with_original_leaf(DemandWork::reconcile_native_controls)?;
        cancellation.cancel();
        let before = original.lock().work;
        assert!(matches!(source.lookup_with_native_work(
            source.reference(), &hidden, NativeSourceWork::new(&original),
        ).await, Err(DemandError::Cancelled)));
        assert_eq!(original.lock().work, before);
        Ok(())
    }

    #[tokio::test]
    async fn filtered_original_selector_denial_replays_raw_page_and_drop_retires_selectors()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("hidden"), b"x")?;
        std::fs::write(root.path().join("visible"), b"x")?;
        let source = FilteredDemandSource::new(NativeDemandSource::open(
            root.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?, vec![path("/hidden")?]);
        let directory = path("/")?;
        let cancellation = CancellationToken::new();
        let original = Arc::new(NativeDemandReadMeter(Mutex::new(
            DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )));
        let raw = source.inner().list_page_with_native_work(
            source.reference(), &directory, None, 16, NativeSourceWork::new(&original),
        ).await?;
        assert_eq!(raw.entries.len(), 2);
        let raw_address = raw.entries.get(0).ok_or("empty raw page")? as *const SourceDirectoryEntry;
        original.with_original_leaf(|work| { work.budget.items_examined = work.work.items_examined; });
        assert!(matches!(source.project_original_page(
            &directory, raw, &mut NativeSourceWork::new(&original),
        ), Err(DemandError::Work(_))));
        original.with_original_leaf(|work| { work.budget.items_examined = u64::MAX; });
        let replay = source.inner().list_page_with_native_work(
            source.reference(), &directory, None, 16, NativeSourceWork::new(&original),
        ).await?;
        assert_eq!(replay.entries.get(0).ok_or("empty replay")? as *const SourceDirectoryEntry, raw_address);
        let page = source.project_original_page(
            &directory, replay, &mut NativeSourceWork::new(&original),
        )?;
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].name(), path("/visible")?.components().last().ok_or("name absent")?);
        let selector_bytes = (page.entries.indices.as_ref().ok_or("selector absent")?.capacity()
            * std::mem::size_of::<usize>()) as u64;
        assert_eq!(selector_bytes, (2 * std::mem::size_of::<usize>()) as u64);
        let live = original.with_original_leaf(|work| work.live_allocation_bytes());
        drop(page);
        original.with_original_leaf(DemandWork::reconcile_native_controls)?;
        assert!(original.with_original_leaf(|work| work.live_allocation_bytes()) <= live - selector_bytes);
        Ok(())
    }

    #[test]
    fn pending_original_publication_visits_are_not_early_work_or_callback_allowance()
    -> Result<(), Box<dyn Error>> {
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.items_examined = 3;
        let mut original = DemandWork::new(budget, &cancellation);
        original.reserve_pending_items(2)?;
        assert_eq!(original.work.items_examined, 0);
        assert_eq!(original.remaining()?.items_examined, 1);
        assert!(original.charge(WorkCounters {
            items_examined: 2, ..WorkCounters::UNCHARGED
        }).is_err());
        assert_eq!(original.work.items_examined, 0);
        original.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        let full = original.budget_before_pending_items();
        assert_eq!(full.items_examined, 3);
        let (counters, allocations, roots) = original.into_current();
        let mut original = DemandWork::from_current(full, &cancellation, counters, allocations, roots);
        assert_eq!(original.remaining()?.items_examined, 0);
        cancellation.cancel();
        original.record_pending_items(1)?;
        assert_eq!(original.work.items_examined, 2);
        original.refund_pending_items(1);
        assert_eq!(original.budget.items_examined, 3);
        assert_eq!(original.remaining()?.items_examined, 1);
        assert!(matches!(original.check(), Err(DemandError::Cancelled)));
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn native_original_alias_preserves_cancellation_roots_parent_stop_and_capture_custody() -> Result<(), Box<dyn Error>> {
        let parent = CancellationToken::new();
        let mut original = DemandWork::new(WorkBudget::UNBOUNDED, &parent);
        let child = original.new_cancellation()?;
        original.rebind_cancellation(&child);
        original.parent_cancellation = Some(parent.clone());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        original.stop = Some(Arc::clone(&stop));
        let capture = Arc::new(CaptureTransientCustody::default());
        original.replace_capture_custody(Some(Arc::clone(&capture)));
        let prefix = WorkCounters { bytes_hashed: 9, ..WorkCounters::UNCHARGED };
        original.original_work = Some(prefix);
        let prepared = original.prepare_cancelled(&parent)?;
        let shared = Arc::new(NativeDemandReadMeter(Mutex::new(original)));
        let sdk = crate::native_mount::NativeMountMeterWork::from_meter(Arc::clone(&shared));
        let alias = sdk.native_materialization_work();
        shared.with_original_leaf(|work| {
            assert!(work.cancellation_wait.for_prepared(&prepared).is_some());
            assert!(Arc::ptr_eq(work.stop.as_ref().expect("original stop"), &stop));
            assert!(Arc::ptr_eq(&work.capture_custody().expect("original capture custody"), &capture));
            assert_eq!(work.original_work, Some(prefix));
            assert_eq!(work.budget.backend_write_operations, WorkBudget::UNBOUNDED.backend_write_operations);
        });
        let custody = alias.claim(4)?;
        let mut payload = Vec::<u8>::new();
        payload.try_reserve_exact(4)?;
        payload.extend_from_slice(b"data");
        assert_eq!(capture.retained_bytes(), 0, "intrinsically guarded data is not capture scratch");
        stop.store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(alias.admit(WorkCounters::UNCHARGED), Err(crate::NativeTreeMaterializationError::Cancelled)));
        stop.store(false, std::sync::atomic::Ordering::Release);
        alias.charge(WorkCounters { bytes_copied: 4, ..WorkCounters::UNCHARGED })?;
        assert_eq!(shared.lock().work.bytes_copied, 4);
        parent.cancel();
        assert!(!child.is_cancelled(), "parent cancellation remains distinct from the original child");
        assert!(matches!(alias.admit(WorkCounters::UNCHARGED), Err(crate::NativeTreeMaterializationError::Cancelled)));
        shared.with_original_leaf(|work| work.finish_cancelled(prepared))?;
        drop(payload);
        drop(custody);
        assert_eq!(capture.retained_bytes(), 0);
        assert_eq!(alias.journal_work()?, prefix.checked_add(shared.lock().work)?);
        Ok(())
    }

    #[test]
    fn funded_capture_origin_retains_original_without_a_root_backedge_cycle()
    -> Result<(), Box<dyn Error>> {
        let cancellation = CancellationToken::new();
        let shared = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(
            WorkBudget::UNBOUNDED, &cancellation,
        ))));
        let original = OriginalDemandWork::from_meter(Arc::clone(&shared));
        let native = crate::NativeMaterializationWork::from_original_demand_work(&original)?;
        let capture = CaptureTransientCustody::new_native(&native)?;
        assert!(capture.owns_native_work(&native));
        assert_eq!(original.counters()?.allocation_operations, 1);
        original.with_original_leaf(|work| {
            work.replace_capture_custody(Some(Arc::clone(&capture)));
        })?;
        let root = Arc::downgrade(&shared);
        let alias = Arc::clone(&capture);
        drop(native);
        drop(original);
        drop(shared);
        drop(capture);
        assert!(root.upgrade().is_some(), "the actual header alias retains its original ledger");
        drop(alias);
        assert!(root.upgrade().is_none(), "the original stores only a weak native capture hook");
        Ok(())
    }

    #[test]
    fn intrinsic_source_control_alias_keeps_original_without_registry_cycle()
    -> Result<(), Box<dyn Error>> {
        let cancellation = CancellationToken::new();
        let native = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let resource = native.native_volume_work(WorkBudget::UNBOUNDED)?;
        let original = OriginalDemandWork::from_native_materialization_work(&resource);
        let root = Arc::downgrade(original.meter());
        let custody = NativeMountAllocationCustody::new_on_original(&original)?;
        let alias = custody.clone();
        let owner = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let owner: Arc<dyn std::any::Any + Send + Sync> = owner;
        original.with_original_leaf(|work| custody.attach_known_control(
            NativeMountControlAllocation::SharedCheckout(Arc::downgrade(&owner)), work,
        ))??;
        drop(owner);
        drop(resource);
        drop(native);
        drop(original);
        drop(custody);
        assert!(root.upgrade().is_some(), "the physical control alias keeps its exact original");
        drop(alias);
        assert!(root.upgrade().is_none(), "known control bookkeeping holds only weak body ownership");
        Ok(())
    }

    #[test]
    fn intrinsic_source_control_denies_before_arc_and_retires_original_resource()
    -> Result<(), Box<dyn Error>> {
        let cancellation = CancellationToken::new();
        let native = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let denied = native.native_volume_work(WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED })?;
        let original = OriginalDemandWork::from_native_materialization_work(&denied);
        let before = native.receipt();
        assert!(matches!(NativeMountAllocationCustody::new_on_original(&original),
            Err(DemandError::Work(_))));
        assert_eq!(native.receipt(), before);
        let accepted = native.native_volume_work(WorkBudget::UNBOUNDED)?;
        let allowance = accepted.resource_allowance().ok_or("resource allowance absent")?;
        let original = OriginalDemandWork::from_native_materialization_work(&accepted);
        let custody = NativeMountAllocationCustody::new_on_original(&original)?;
        assert!(allowance.remaining()?.peak_allocation_bytes < WorkBudget::UNBOUNDED.peak_allocation_bytes);
        let alias = custody.clone();
        drop(custody);
        assert!(allowance.remaining()?.peak_allocation_bytes < WorkBudget::UNBOUNDED.peak_allocation_bytes);
        drop(alias);
        assert_eq!(allowance.remaining()?.peak_allocation_bytes, WorkBudget::UNBOUNDED.peak_allocation_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn real_native_lookup_miss_does_not_poison_the_original_next_read()
        -> Result<(), Box<dyn Error>>
    {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("present"), b"real source")?;
        let source = NativeDemandSource::open(
            temporary.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?;
        let cancellation = CancellationToken::new();
        let budget = finite_watch_budget()?;
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let before = original.receipt();
        assert!(source.lookup_with_native_work(
            source.reference(), &path("/absent")?,
            NativeSourceWork::from_native_materialization_work(&original),
        ).await?.is_none());
        let missed = original.receipt();
        assert!(missed.backend_read_operations > before.backend_read_operations);
        assert_eq!(missed.source_entries_visited, before.source_entries_visited);
        original.native_read_meter().with_original_leaf(|work| {
            assert!(work.take_native_io_failure().is_none(),
                "normal held-path NotFound is an observed miss, not a fatal meter marker");
            work.check()
        })?;
        assert!(source.lookup_with_native_work(
            source.reference(), &path("/present")?,
            NativeSourceWork::from_native_materialization_work(&original),
        ).await?.is_some());
        assert!(original.receipt().backend_read_operations > missed.backend_read_operations);
        original.receipt().verify(budget)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn source_packet_aliases_retire_only_physically_dropped_link_buffers()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::os::unix::fs::symlink("first actual target", root.path().join("first"))?;
        std::os::unix::fs::symlink("second actual target", root.path().join("second"))?;
        let directory = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let cancellation = CancellationToken::new();
        let native = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let scope = native.native_volume_work(WorkBudget::UNBOUNDED)?;
        let allowance = scope.resource_allowance().ok_or("resource allowance absent")?;
        let mut work = NativeSourceWork::from_native_materialization_work(&scope);
        let first = crate::native_host::read_source_link_held_with_work(
            &directory, std::path::Path::new("first"), 512, &mut work,
        )?;
        let second = crate::native_host::read_source_link_held_with_work(
            &directory, std::path::Path::new("second"), 512, &mut work,
        )?;
        let first_alias = first.clone();
        let first_empty = first.slice(..0);
        let before = allowance.remaining()?.peak_allocation_bytes;
        drop(first);
        assert_eq!(allowance.remaining()?.peak_allocation_bytes, before);
        drop(first_alias);
        assert_eq!(allowance.remaining()?.peak_allocation_bytes, before,
            "the empty window keeps the same original byte custody");
        drop(first_empty);
        let after_first = allowance.remaining()?.peak_allocation_bytes;
        assert!(after_first > before);
        assert_eq!(second.as_ref(), b"second actual target",
            "the surviving packet remains physically owned");
        drop(second);
        assert!(allowance.remaining()?.peak_allocation_bytes > after_first);
        assert!(scope.receipt().backend_read_operations >= 2, "both real readlink attempts were retained");
        Ok(())
    }

    #[test]
    fn explicit_ordinary_payload_partition_never_manufactures_native_custody()
        -> Result<(), Box<dyn Error>>
    {
        let first = Vec::from(b"ordinary first");
        let second = Vec::from(b"ordinary second");
        let mut ordinary = SourcePayloadOwnership::default();
        let first_owner = ordinary.split_owned_payload(first.capacity() as u64)?;
        assert!(!first_owner.has_native_custody());
        assert!(!ordinary.has_native_custody());
        drop(first);
        drop(first_owner);
        assert_eq!(second, b"ordinary second");
        drop(second);
        drop(ordinary);
        Ok(())
    }

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

    fn finite_watch_budget() -> Result<WorkBudget, Box<dyn Error>> {
        let mut value = serde_json::to_value(WorkBudget::UNBOUNDED)?;
        if let serde_json::Value::Object(fields) = &mut value {
            for allowance in fields.values_mut() { *allowance = serde_json::json!(1_000_000_u64); }
        }
        Ok(serde_json::from_value(value)?)
    }

    /// Regression sink borrows the original work and records only fixed scalars.
    struct OriginalWatchSink {
        original: std::sync::Weak<NativeDemandReadMeter>,
        resource: std::sync::Weak<crate::materializer::NativeResourceAllowance>,
        native_calls: std::sync::atomic::AtomicU64,
        changes: std::sync::atomic::AtomicU64,
    }

    impl OriginalWatchSink {
        fn new(work: &crate::materializer::NativeMaterializationWork) -> Result<Self, Box<dyn Error>> {
            Ok(Self {
                original: Arc::downgrade(work.native_read_meter()),
                resource: Arc::downgrade(work.resource_allowance().ok_or("resource scope absent")?),
                native_calls: std::sync::atomic::AtomicU64::new(0),
                changes: std::sync::atomic::AtomicU64::new(0),
            })
        }
    }

    impl SourceChangeSink for OriginalWatchSink {
        fn source_changed(
            &self, _changes: &[SourceChange], _budget: WorkBudget, _cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            Err(OperationFailure::before_work(DemandError::NativeWorkUnavailable))
        }

        fn source_changed_with_native_work(
            &self, changes: &[SourceChange], mut work: NativeSourceWork<'_>,
        ) -> Result<(), DemandError> {
            let original = work.original_demand_work();
            assert!(self.original.ptr_eq(&Arc::downgrade(original.meter())));
            assert!(self.resource.ptr_eq(&Arc::downgrade(original.resource_allowance().expect("same original resource"))));
            work.charge(WorkCounters { items_examined: changes.len() as u64, ..WorkCounters::UNCHARGED })?;
            self.native_calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.changes.fetch_add(changes.len() as u64, std::sync::atomic::Ordering::AcqRel);
            Ok(())
        }
    }

    #[tokio::test]
    async fn stock_and_filtered_watch_factories_retain_paid_original_callback_context() -> Result<(), Box<dyn Error>> {
        use crate::materializer::NativeMaterializationWork;
        acyclic_native_runtime::initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let source = NativeDemandSource::open(
            temporary.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?;
        let source = FilteredDemandSource::new(source, vec![path("/private")?]);
        let budget = finite_watch_budget()?;
        let cancellation = CancellationToken::new();
        let root = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        let scope = root.native_volume_work(budget)?;
        let original = OriginalDemandWork::from_native_materialization_work(&scope);
        let sink = Arc::new(OriginalWatchSink::new(&scope)?);
        let before = scope.receipt();
        let watch = source.watch_with_native_work(
            sink.clone(), NativeSourceWork::from_original_work(&original),
        )?.ok_or("native subscription absent")?;
        let started = scope.receipt();
        assert!(started.allocation_operations > before.allocation_operations);
        assert!(started.backend_read_operations > before.backend_read_operations);
        assert_eq!(started.items_returned, before.items_returned + 1);
        started.verify(budget)?;
        std::fs::write(temporary.path().join("visible"), b"changed")?;
        watch.fence_with_native_work(NativeSourceWork::from_original_work(&original))?;
        let fenced = scope.receipt();
        assert!(fenced.backend_read_operations > started.backend_read_operations,
            "the actual native fence charges the same original resource, not a numeric child");
        fenced.verify(budget)?;
        drop(original);
        drop(scope);
        drop(root);
        assert!(sink.original.upgrade().is_some(), "the actual watch retains its original root");
        assert!(sink.resource.upgrade().is_some(), "the actual watch retains its birth resource");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while sink.native_calls.load(std::sync::atomic::Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(sink.native_calls.load(std::sync::atomic::Ordering::Acquire) > 0);
        let cleanup = NativeMaterializationWork::new(budget, &CancellationToken::new()).map_err(|(error, _)| error)?;
        watch.stop_with_native_work(NativeSourceWork::from_native_materialization_work(&cleanup))?;
        assert!(watch.is_stopped());
        drop(watch);
        drop(source);
        assert!(sink.original.upgrade().is_none(), "last physical owner retires the original root");
        assert!(sink.resource.upgrade().is_none(), "last physical owner retires its exact resource");
        Ok(())
    }

    #[test]
    fn filtered_native_projection_forwards_original_resource_and_preserves_denied_prefix() -> Result<(), Box<dyn Error>> {
        use crate::materializer::NativeMaterializationWork;
        let budget = finite_watch_budget()?;
        let cancellation = CancellationToken::new();
        let root = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        let scope = root.native_volume_work(budget)?;
        let original = OriginalDemandWork::from_native_materialization_work(&scope);
        let accepted = Arc::new(OriginalWatchSink::new(&scope)?);
        let sink = FilteredChangeSink {
            excluded: Arc::new(FilteredExcludedPaths { roots: vec![path("/private")?], custody: None }),
            sink: accepted.clone(), ownership: SourcePayloadOwnership::default(),
        };
        let changes = [SourceChange::Name(path("/private/index")?), SourceChange::Entry(path("/visible")?)];
        let before = scope.receipt();
        sink.source_changed_with_native_work(&changes, NativeSourceWork::from_original_work(&original))?;
        let after = scope.receipt();
        assert_eq!(accepted.native_calls.load(std::sync::atomic::Ordering::Acquire), 1);
        assert_eq!(accepted.changes.load(std::sync::atomic::Ordering::Acquire), 1);
        assert!(after.bytes_copied > before.bytes_copied);
        assert!(after.source_path_components > before.source_path_components);
        assert!(after.allocation_operations > before.allocation_operations);
        root.native_read_meter().with_original_leaf(|state| { state.budget.bytes_copied = state.work.bytes_copied; });
        assert!(matches!(sink.source_changed_with_native_work(
            &changes, NativeSourceWork::from_original_work(&original),
        ), Err(DemandError::Work(_))));
        assert_eq!(scope.receipt().bytes_copied, after.bytes_copied);
        assert_eq!(accepted.native_calls.load(std::sync::atomic::Ordering::Acquire), 1);
        cancellation.cancel();
        let denied = scope.receipt();
        assert!(matches!(sink.source_changed_with_native_work(
            &changes, NativeSourceWork::from_original_work(&original),
        ), Err(DemandError::Cancelled)));
        assert_eq!(scope.receipt(), denied);
        Ok(())
    }

    #[tokio::test]
    async fn native_watch_output_denial_precedes_physical_startup() -> Result<(), Box<dyn Error>> {
        use crate::materializer::NativeMaterializationWork;
        let temporary = tempfile::tempdir()?;
        let source = NativeDemandSource::open(
            temporary.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?;
        let mut budget = finite_watch_budget()?;
        budget.items_returned = 0;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        assert!(matches!(source.watch_with_native_work(
            Arc::new(RecordedChanges::default()), NativeSourceWork::from_native_materialization_work(&work),
        ), Err(DemandError::Work(_))));
        assert_eq!(work.receipt().items_returned, 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        work.receipt().verify(budget)?;
        Ok(())
    }

    /// Every source change delivered, in order.
    #[derive(Default)]
    struct RecordedChanges(Mutex<Vec<SourceChange>>);

    impl SourceChangeSink for RecordedChanges {
        fn source_changed(
            &self, changes: &[SourceChange], budget: WorkBudget, cancellation: &CancellationToken,
        ) -> DemandResult<()> {
            let mut work = WorkCounters::UNCHARGED;
            let mut ledger = crate::kernel::allocation::AllocationLedger::default();
            let result = (|| {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                if changes.is_empty() { return Ok(()); }
                let incoming_bytes = (changes.len() as u64)
                    .checked_mul(std::mem::size_of::<SourceChange>() as u64).ok_or(crate::WorkError::Overflow)?;
                let prior_peak = work.peak_allocation_bytes;
                ledger.claim_bytes(incoming_bytes, 1, &mut work, budget)
                    .map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
                let mut incoming = Vec::new();
                if incoming.try_reserve_exact(changes.len()).is_err() {
                    ledger.release(incoming_bytes).map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
                    work.peak_allocation_bytes = prior_peak;
                    return Err(DemandError::WorkerUnavailable);
                }
                for change in changes {
                    work.charge(&WorkCounters { items_examined: 1, ..WorkCounters::default() }, &budget)?;
                    incoming.push(clone_source_change_with_ledger(change, &mut ledger, &mut work, budget, cancellation)?);
                }
                let mut recorded = self.0.lock().map_err(|_| DemandError::WorkerUnavailable)?;
                let total = recorded.len().checked_add(incoming.len()).ok_or(crate::WorkError::Overflow)?;
                if total > recorded.capacity() {
                    let bytes = (total as u64).checked_mul(std::mem::size_of::<SourceChange>() as u64)
                        .ok_or(crate::WorkError::Overflow)?;
                    let copy = WorkCounters { bytes_copied: bytes, ..WorkCounters::default() };
                    work.admit(&copy, &budget)?;
                    let prior_peak = work.peak_allocation_bytes;
                    ledger.claim_bytes(bytes, 1, &mut work, budget)
                        .map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
                    let mut replacement = Vec::new();
                    if replacement.try_reserve_exact(total).is_err() {
                        ledger.release(bytes).map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
                        work.peak_allocation_bytes = prior_peak;
                        return Err(DemandError::WorkerUnavailable);
                    }
                    cancellation.check().map_err(|_| DemandError::Cancelled)?;
                    work.charge(&copy, &budget)?;
                    replacement.append(&mut recorded);
                    replacement.append(&mut incoming);
                    *recorded = replacement;
                } else {
                    cancellation.check().map_err(|_| DemandError::Cancelled)?;
                    work.charge(&WorkCounters { bytes_copied: incoming_bytes, ..WorkCounters::default() }, &budget)?;
                    recorded.append(&mut incoming);
                }
                drop(incoming);
                ledger.release(incoming_bytes).map_err(|error| error.into_work_or(DemandError::WorkerUnavailable))?;
                Ok(())
            })();
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error| OperationFailure::new(error, work))
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

    #[test]
    fn filtered_change_delivery_admits_actual_copies_before_recording() -> Result<(), Box<dyn Error>> {
        let cancellation = CancellationToken::new();
        let excluded = Arc::new(FilteredExcludedPaths { roots: vec![path("/.git")?], custody: None });
        let changes = [SourceChange::Name(path("/.git/index")?), SourceChange::Entry(path("/visible")?)];
        let accepted = Arc::new(RecordedChanges::default());
        let sink = FilteredChangeSink { excluded: Arc::clone(&excluded), sink: accepted.clone(), ownership: SourcePayloadOwnership::default() };
        let receipt = sink.source_changed(&changes, WorkBudget::UNBOUNDED, &cancellation)?;
        assert_eq!(accepted.take()?, vec![SourceChange::Entry(path("/visible")?)]);
        assert!(receipt.work.bytes_copied >= b"visible".len() as u64);
        assert!(receipt.work.allocation_operations > 0);
        let rejected = Arc::new(RecordedChanges::default());
        let sink = FilteredChangeSink { excluded, sink: rejected.clone(), ownership: SourcePayloadOwnership::default() };
        let budget = WorkBudget { bytes_copied: 0, ..receipt.work };
        let failure = sink.source_changed(&changes, budget, &cancellation).expect_err("copy denied before the sink accepts any change");
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(failure.work.bytes_copied, 0);
        assert!(rejected.take()?.is_empty());
        failure.work.verify(budget)?;
        cancellation.cancel();
        let failure = sink.source_changed(&changes, WorkBudget::UNBOUNDED, &cancellation).expect_err("original cancellation");
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::UNCHARGED);
        Ok(())
    }

    #[test]
    fn filtered_change_delivery_borrows_unchanged_batches_and_skips_excluded_ones() -> Result<(), Box<dyn Error>> {
        let cancellation = CancellationToken::new();
        let changes = [SourceChange::Entry(path("/visible")?)];
        let direct = RecordedChanges::default();
        let direct_work = direct.source_changed(&changes, WorkBudget::UNBOUNDED, &cancellation)?.work;
        let accepted = Arc::new(RecordedChanges::default());
        let sink = FilteredChangeSink {
            excluded: Arc::new(FilteredExcludedPaths { roots: vec![path("/.git")?], custody: None }),
            sink: accepted.clone(), ownership: SourcePayloadOwnership::default(),
        };
        let filtered_work = sink.source_changed(&changes, WorkBudget::UNBOUNDED, &cancellation)?.work;
        assert_eq!(filtered_work.bytes_copied, direct_work.bytes_copied);
        assert_eq!(filtered_work.allocation_operations, direct_work.allocation_operations);
        assert_eq!(filtered_work.peak_allocation_bytes, direct_work.peak_allocation_bytes);
        assert_eq!(accepted.take()?, changes);
        let excluded = [SourceChange::Name(path("/.git/index")?)];
        let budget = WorkBudget {
            peak_allocation_bytes: 0, allocation_operations: 0, bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        };
        let receipt = sink.source_changed(&excluded, budget, &cancellation)?;
        receipt.work.verify(budget)?;
        assert!(accepted.take()?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn native_source_watch_fence_honors_original_pre_cancellation() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let source = NativeDemandSource::open(root.path(), FilesystemProfile::Portable, VolumeLimits::default()).await?;
        let subscription_cancellation = CancellationToken::new();
        let recorded = Arc::new(RecordedChanges::default());
        let subscribed = source.watch(recorded, WorkBudget::UNBOUNDED, &subscription_cancellation)?;
        let watch = subscribed.value.ok_or("native source is watched")?;
        assert!(watch.logical_owned_bytes() > 0);
        assert!(watch.logical_owned_bytes() <= subscribed.work.peak_allocation_bytes);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let failure = watch.fence_measured(WorkBudget::default(), &cancellation)
            .expect_err("pre-cancelled fence must not start host work");
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::UNCHARGED);
        let failure = watch.stop_measured(WorkBudget::default(), &cancellation)
            .expect_err("pre-cancelled stop retains the original live subscription");
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::UNCHARGED);
        watch.fence_measured(WorkBudget::UNBOUNDED, &subscription_cancellation)?;
        let failure = watch.stop_measured(WorkBudget::default(), &subscription_cancellation)
            .expect_err("native quiescence is admitted before starting cleanup");
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(*failure.work, WorkCounters::UNCHARGED);
        watch.fence_measured(WorkBudget::UNBOUNDED, &subscription_cancellation)?;
        watch.stop_measured(WorkBudget::UNBOUNDED, &subscription_cancellation)?;
        Ok(())
    }

    #[cfg(any(target_os = "linux", windows))]
    #[tokio::test]
    async fn native_watch_backing_custody_tracks_real_replacement_failure_and_original_drop() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        let source = NativeDemandSource::open(root.path(), FilesystemProfile::Portable, VolumeLimits::default()).await?;
        let cancellation = CancellationToken::new();
        let first = source.watch(Arc::new(RecordedChanges::default()), WorkBudget::UNBOUNDED, &cancellation)?
            .value.ok_or("first native subscription")?;
        let first_backing = first.allocation_custody().ok_or("first native backing is created")?;
        let original_bytes = first_backing.retained_bytes();
        assert!(original_bytes > 0);
        let control = std::mem::size_of::<super::SourceWatchAllocationCustody>() as u64;
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        meter.retain_watch_custody(&first_backing)?;
        assert_eq!(meter.live_allocation_bytes(), control + original_bytes);
        let denied = WorkBudget { backend_read_operations: 0, ..WorkBudget::UNBOUNDED };
        let failure = match source.watch(Arc::new(RecordedChanges::default()), denied, &cancellation) {
            Err(failure) => failure,
            Ok(receipt) => {
                if let Some(watch) = receipt.value {
                    watch.stop_measured(WorkBudget::UNBOUNDED, &cancellation)?;
                }
                return Err("native startup must be denied after staging a replacement".into());
            }
        };
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(failure.work.backend_read_operations, 0);
        failure.work.verify(denied)?;
        // The prepaid pending slot already owns its replacement buffer. A
        // startup refusal abandons only the slot, never rolls back real bytes.
        assert_eq!(first_backing.retained_bytes(), 0);
        let prior_live = meter.live_allocation_bytes();
        let second = source.watch(Arc::new(RecordedChanges::default()), WorkBudget::UNBOUNDED, &cancellation)?;
        let expected_peak = prior_live.checked_add(second.work.peak_allocation_bytes).ok_or("peak overflow")?;
        let second_watch = second.value.ok_or("second native subscription")?;
        let second_backing = second_watch.allocation_custody().ok_or("larger native backing is created")?;
        assert_eq!(first_backing.retained_bytes(), 0);
        assert!(second_backing.retained_bytes() > original_bytes);
        meter.merge_with_prior_live(second.work, prior_live)?;
        assert_eq!(meter.work.peak_allocation_bytes, expected_peak);
        assert_eq!(meter.live_allocation_bytes(), control);
        meter.retain_watch_custody(&second_backing)?;
        assert_eq!(meter.live_allocation_bytes(), 2 * control + second_backing.retained_bytes());
        first.stop_measured(WorkBudget::UNBOUNDED, &cancellation)?;
        second_watch.stop_measured(WorkBudget::UNBOUNDED, &cancellation)?;
        drop(source);
        assert_eq!(second_backing.retained_bytes(), 0);
        meter.record(WorkCounters::UNCHARGED)?;
        assert_eq!(meter.live_allocation_bytes(), 2 * control);
        drop(first);
        drop(first_backing);
        meter.record(WorkCounters::UNCHARGED)?;
        assert_eq!(meter.live_allocation_bytes(), control);
        drop(second_watch);
        drop(second_backing);
        meter.record(WorkCounters::UNCHARGED)?;
        assert_eq!(meter.live_allocation_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn native_owned_file_and_ranges_report_actual_retained_payloads() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("file"), b"native")?;
        let source = NativeDemandSource::open(root.path(), FilesystemProfile::Portable, VolumeLimits::default()).await?;
        let cancellation = CancellationToken::new();
        let requested = path("/file")?;
        let node = source.lookup(source.reference(), &requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?.value.ok_or("file absent")?;
        let opened = source.open_file(source.reference(), &requested, node.version, WorkBudget::UNBOUNDED, &cancellation).await?;
        assert!(opened.value.logical_owned_bytes() > 0);
        assert!(opened.value.logical_owned_bytes() <= opened.work.peak_allocation_bytes);
        let read = opened.value.read_range(0, 6, WorkBudget::UNBOUNDED, &cancellation)?;
        assert_eq!(read.value.bytes.as_ref(), b"native");
        assert_eq!(read.value.logical_owned_bytes, 6);
        assert_eq!(read.work.source_bytes_read, 6);
        let empty = opened.value.read_range(6, 6, WorkBudget::UNBOUNDED, &cancellation)?;
        assert!(empty.value.bytes.is_empty());
        assert_eq!(empty.value.logical_owned_bytes, 0);
        let mut denied = read.work;
        denied.peak_allocation_bytes = 0;
        let failure = opened.value.read_range(0, 6, denied, &cancellation).expect_err("native output allocation denied before body IO");
        assert_eq!(failure.work.source_bytes_read, 0);
        assert_eq!(failure.work.output_bytes, 0);
        failure.work.verify(denied)?;
        Ok(())
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
        let cancellation = CancellationToken::new();
        let watch = source
            .watch(recorded.clone(), WorkBudget::UNBOUNDED, &cancellation)?
            .value
            .ok_or("a local root is watched")?;
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
        let cancellation = CancellationToken::new();
        let watch = source
            .watch(recorded.clone(), WorkBudget::UNBOUNDED, &cancellation)?
            .value
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
                .bytes
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
        assert_eq!(bytes.value.bytes.as_ref(), b"3456");
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
            assert_eq!(read.value.bytes.len() as u64, length, "{name}");
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
        drop(completed);
        meter.release(owned_bytes)?;
        assert_eq!(meter.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn capture_scratch_custody_retires_actual_native_path_payload_after_drop() -> Result<(), Box<dyn Error>> {
        let cancellation = CancellationToken::new();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let control = std::mem::size_of::<super::CaptureTransientCustody>() as u64;
        meter.allocate(control)?;
        let custody = Arc::new(super::CaptureTransientCustody::default());
        let cloned = meter.with_capture_custody(&custody, |work| {
            crate::native_host::clone_path_measured(std::path::Path::new("native-scratch"), work)
        })?;
        let owned = cloned.capacity() as u64;
        assert_eq!(custody.retained_bytes(), owned);
        assert!(meter.capture_custody().is_none());
        assert_eq!(meter.live_allocation_bytes(), control + owned);
        drop(cloned);
        assert_eq!(custody.take_retained_bytes(), owned);
        assert_eq!(custody.retained_bytes(), 0);
        meter.release(owned)?;
        assert_eq!(meter.live_allocation_bytes(), control);
        let mut denied = DemandWork::new(WorkBudget::default(), &cancellation);
        let result = denied.with_capture_custody(&custody, |work| {
            crate::native_host::clone_path_measured(std::path::Path::new("native-scratch"), work)
        });
        assert!(matches!(result, Err(DemandError::Work(_))));
        assert_eq!(custody.retained_bytes(), 0);
        assert_eq!(denied.work, WorkCounters::UNCHARGED);
        assert!(denied.capture_custody().is_none());
        drop(custody);
        meter.release(control)?;
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
                            .is_ok_and(|read| read.value.bytes.as_ref() == b"before")
                    })
                })
                .collect::<Vec<_>>();
            for read in reads {
                assert!(read.join().unwrap_or(false));
            }
        });
        assert_eq!(
            file.read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?.value.bytes.as_ref(),
            b"before"
        );
        assert_eq!(
            file.read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())?.value.bytes.as_ref(),
            b"before"
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
        assert_eq!(read.value.bytes.as_ref(), b"remote");
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
                assert_eq!(receipt.value.bytes.as_ref(), b"remote");
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
        assert_eq!(read.value.bytes.as_ref(), b"remote");
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
        assert_eq!(read.value.bytes.as_ref(), b"before");
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
        assert_eq!(held?.value.bytes.as_ref(), b"before");
        assert_eq!(by_path?.value.bytes.as_ref(), b"before");
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
        assert_eq!(answer.value.bytes.as_ref(), b"data");
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod native_io_meter {
    use crate::WorkCounters;
    use super::NativeDemandReadMeter;

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


impl<T: NativeDemandReadWork> NativeDemandReadMeter<T> {
    /// Runs one synchronous admission/completion leaf and retains its actual
    /// typed failure inline before returning a non-allocating runtime marker.
    fn with_io<R>(
        &self, operation: impl FnOnce(&mut T) -> Result<R, crate::DemandError>,
    ) -> std::io::Result<R> {
        let mut held = self.lock();
        operation(&mut held).map_err(|error| held.as_work().record_native_io_failure(error))
    }

    pub(crate) fn relay_io_failure(&self, error: crate::DemandError) -> std::io::Error {
        self.lock().as_work().record_native_io_failure(error)
    }

    pub(crate) fn take_native_io_failure(&self) -> Option<crate::DemandError> {
        self.lock().as_work().take_native_io_failure()
    }

    fn admit(&self, delta: WorkCounters) -> std::io::Result<()> {
        self.with_io(|held| held.as_work().admit(delta))
    }

    fn record(&self, delta: WorkCounters) -> std::io::Result<()> {
        self.with_io(|held| held.as_work().record(delta))
    }

    fn charge(&self, delta: WorkCounters) -> std::io::Result<()> {
        self.with_io(|held| held.as_work().charge(delta))
    }
}

#[cfg(all(feature = "native-watch", target_os = "macos"))]
pub(crate) fn native_demand_read_error(error: crate::DemandError) -> std::io::Error {
    if matches!(error, crate::DemandError::Cancelled) {
        std::io::Error::new(std::io::ErrorKind::Interrupted, error)
    } else {
        std::io::Error::other(error)
    }
}

impl<T: NativeDemandReadWork + Send> acyclic_native_runtime::NativeReadMeter for NativeDemandReadMeter<T> {
    fn check(&self) -> std::io::Result<()> {
        self.with_io(|held| held.as_work().check())
    }

    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED })
    }

    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        self.relay_io_failure(crate::DemandError::Allocation(error))
    }

    fn finish_read(&self, requested: usize, completed: usize) -> std::io::Result<()> {
        self.with_io(|held| held.finish_read(requested, completed))
    }

    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_work().claim_native_owned(bytes, u64::from(bytes != 0), &None))
    }

    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_work().reserve_native_allocation(bytes))
    }

    fn cancel_reserved_allocation(&self, bytes: u64) {
        let mut held = self.lock();
        let work = held.as_work();
        if let Err(error) = work.cancel_reserved_native_allocation(bytes) {
            work.record_native_io_failure(error);
            work.poison_native_original_work();
        }
    }

    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_work().record_reserved_native_allocation(bytes))
    }

    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|held| held.as_work().claim_native_owned(bytes, 0, &None))
    }

    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        let mut held = self.lock();
        let work = held.as_work();
        let (claimed, result) = work.record_native_owned_completed(bytes, &None);
        (claimed, result.map_err(|error| work.record_native_io_failure(error)))
    }

    fn release(&self, bytes: u64) {
        let mut held = self.lock();
        let work = held.as_work();
        if let Err(error) = work.release_native_owned(bytes, &None) {
            work.record_native_io_failure(error);
            work.poison_native_original_work();
        }
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

    fn reserve_operations(
        &self, kind: acyclic_native_runtime::NativeOperationKind, count: u64,
    ) -> std::io::Result<()> {
        self.lock().as_work().reserve_native_operations(kind, count)
    }

    fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
        self.lock().as_work().cancel_reserved_native_operations(kind, count);
    }

    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        self.lock().as_work().record_reserved_native_operation(kind)
    }

    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.lock().as_work().admit_native_write_bytes(maximum)
    }

    fn reserve_bytes(
        &self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64,
    ) -> std::io::Result<()> {
        self.lock().as_work().reserve_native_bytes(kind, maximum)
    }

    fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
        self.lock().as_work().cancel_reserved_native_bytes(kind, maximum);
    }

    fn record_reserved_bytes(
        &self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64,
    ) -> std::io::Result<()> {
        self.lock().as_work().record_reserved_native_bytes(kind, maximum, actual)
    }

    fn admit_durability(&self) -> std::io::Result<()> {
        self.charge(WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED })
    }

    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        self.with_io(|held| crate::native_host::NativeHostWork::reserve_cleanup_write(held.as_work()))
    }

    fn cancel_cleanup_write(&self, reservation: u64) {
        crate::native_host::NativeHostWork::cancel_cleanup_write(self.lock().as_work(), reservation);
    }

    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
        self.with_io(|held| crate::native_host::NativeHostWork::attempt_cleanup_write(
            held.as_work(), reservation, || (),
        ).1)
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

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native_io_meter::{
    NativeDemandReadWork, native_demand_error,
};
#[cfg(all(feature = "native-watch", target_os = "macos"))]
pub(crate) use native_io_meter::native_demand_read_error;
/// Physical ownership of known native SDK payloads admitted by one original
/// lifecycle ledger. Clones are aliases, not new allocations or authorities.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct NativeMountAllocationCustody {
    live: Arc<NativeMountAllocationCustodyData>,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeMountAllocationCustodyData {
    live: std::sync::atomic::AtomicU64,
    capture_claim_origin: Option<std::sync::Weak<CaptureTransientCustody>>,
    original: Option<OriginalDemandWork>,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::ops::Deref for NativeMountAllocationCustodyData {
    type Target = std::sync::atomic::AtomicU64;
    fn deref(&self) -> &Self::Target { &self.live }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeMountAllocationCustodyData {
    fn drop(&mut self) {
        if let Some(original) = &self.original
            && let Some(resource) = original.resource_allowance()
            && let Err(error) = resource.retire(self.live.load(std::sync::atomic::Ordering::Acquire)) {
            let _recorded = original.with_original_leaf(|work| work.record_native_io_failure(error));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeMountAllocationAdmission {
    live: std::sync::Weak<NativeMountAllocationCustodyData>,
    adopted: u64,
    known_control: Option<NativeMountKnownControl>,
}

/// Only known native-source constructor controls and the actual Darwin detach
/// meter. Real weak backing remains owned until this original ledger drops its
/// registration; strong-count zero never asserts destructor completion.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) enum NativeMountControlAllocation {
    Atomic(std::sync::Weak<std::sync::atomic::AtomicU64>),
    ViewLedger(std::sync::Weak<dyn std::any::Any + Send + Sync>),
    ViewGate(std::sync::Weak<dyn std::any::Any + Send + Sync>),
    SharedCheckout(std::sync::Weak<dyn std::any::Any + Send + Sync>),
    #[cfg(all(feature = "native-mount", target_os = "macos"))]
    DarwinDetachMeter(std::sync::Weak<dyn std::any::Any + Send + Sync>),
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMountControlAllocation {
    fn has_strong_owner(&self) -> bool {
        match self {
            Self::Atomic(owner) => owner.strong_count() != 0,
            Self::ViewLedger(owner) => owner.strong_count() != 0,
            Self::ViewGate(owner) => owner.strong_count() != 0,
            Self::SharedCheckout(owner) => owner.strong_count() != 0,
            #[cfg(all(feature = "native-mount", target_os = "macos"))]
            Self::DarwinDetachMeter(owner) => owner.strong_count() != 0,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeMountKnownControl {
    // Field order is physical: real weak allocation first, its marker second.
    _owner: NativeMountControlAllocation,
    _custody: std::sync::Weak<NativeMountAllocationCustodyData>,
}

/// Original invocation's weak retirement proofs; fresh invocations do not
/// acquire permission to release borrowed driver allocations.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct NativeMountAllocationAdmissions {
    first: Option<NativeMountAllocationAdmission>,
    more: Vec<NativeMountAllocationAdmission>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMountAllocationAdmissions {
    pub(crate) fn freed_bytes(&self) -> u64 {
        self.first.iter().chain(self.more.iter()).try_fold(0_u64, |freed, entry| {
            let actual = entry.live.upgrade().map_or(0, |live|
                live.load(std::sync::atomic::Ordering::Acquire));
            freed.checked_add(entry.adopted.checked_sub(actual)?)
        }).expect("native physical retirement cannot exceed original admitted ownership")
    }

    pub(crate) fn reconcile(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
    ) -> Result<(), crate::WorkError> {
        for entry in self.first.iter_mut().chain(self.more.iter_mut()) {
            let actual = match entry.live.upgrade() {
                Some(live) => live.load(std::sync::atomic::Ordering::Acquire),
                None => {
                    // No logical body remains. Real Arc/Weak backing is opaque
                    // bookkeeping, not another retained payload claim.
                    drop(std::mem::take(&mut entry.live));
                    0
                }
            };
            let retired = entry.adopted.checked_sub(actual).ok_or(crate::WorkError::Overflow)?;
            allocations.release(retired).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            entry.adopted = actual;
        }
        Ok(())
    }

    fn reserve_slot(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
        counters: &mut crate::WorkCounters, budget: crate::WorkBudget,
    ) -> Result<(), DemandError> {
        if self.first.is_none() || self.more.len() < self.more.capacity() { return Ok(()); }
        let width = std::mem::size_of::<NativeMountAllocationAdmission>();
        let capacity = self.more.capacity().max(1).checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?;
        let bytes = capacity.checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        let old_bytes = self.more.capacity().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        let copied = crate::WorkCounters {
            bytes_copied: self.more.len().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        counters.admit(&copied, &budget)?;
        allocations.claim_bytes(bytes, 1, counters, budget)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        let mut vector = Vec::new();
        if let Err(error) = vector.try_reserve_exact(capacity) {
            allocations.release(bytes).map_err(|failure| failure.into_work_or(crate::WorkError::Overflow))?;
            return Err(DemandError::Allocation(error));
        }
        vector.append(&mut self.more);
        self.more = vector;
        allocations.release(old_bytes).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        counters.charge(&copied, &budget)?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMountAllocationCustody {
    /// Preclaims the existing registry/control on the same original ledger,
    /// but reserves vectors and allocates Arc bodies outside its mutex.
    pub(crate) fn new_on_original(original: &OriginalDemandWork) -> Result<Self, DemandError> {
        let metadata = std::mem::size_of::<NativeMountAllocationCustodyData>() as u64;
        original.with_original_leaf(|work| work.claim_native_owned(metadata, 1, &None))??;
        let mut keeper = original.allocation_work();
        keeper.capture_origin = None;
        let live = Arc::new(NativeMountAllocationCustodyData {
            live: std::sync::atomic::AtomicU64::new(metadata),
            capture_claim_origin: original.capture_origin().cloned(),
            original: Some(keeper),
        });
        let result = (|| {
            loop {
                let plan = original.with_original_leaf(|work| {
                    work.check()?;
                    let (admissions, allocations, counters, budget) = work.native_mount_admission_parts()?;
                    if admissions.first.is_none() || admissions.more.len() < admissions.more.capacity() {
                        return Ok(None);
                    }
                    let capacity = admissions.more.capacity().max(1).checked_mul(2)
                        .ok_or(crate::WorkError::Overflow)?;
                    let bytes = capacity.checked_mul(std::mem::size_of::<NativeMountAllocationAdmission>())
                        .ok_or(crate::WorkError::Overflow)? as u64;
                    allocations.claim_bytes(bytes, 1, counters, budget)
                        .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                    Ok::<_, DemandError>(Some((capacity, bytes)))
                })??;
                let mut replacement = Vec::new();
                let mut claimed = 0;
                if let Some((capacity, bytes)) = plan {
                    claimed = bytes;
                    if let Err(error) = replacement.try_reserve_exact(capacity) {
                        drop(replacement);
                        original.with_original_leaf(|work| work.with_resource_scope(None,
                            |work| work.release_native_owned(claimed, &None)))??;
                        return Err(DemandError::Allocation(error));
                    }
                    let actual = replacement.capacity().checked_mul(std::mem::size_of::<NativeMountAllocationAdmission>())
                        .ok_or(crate::WorkError::Overflow)? as u64;
                    let checked = original.with_original_leaf(|work| work.with_resource_scope(None,
                        |work| work.with_native_capture_scope(None,
                            |work| work.reconcile_native_allocation(&mut claimed, actual))))
                        .and_then(std::convert::identity);
                    if let Err(error) = checked {
                        drop(replacement);
                        original.with_original_leaf(|work| work.with_resource_scope(None,
                            |work| work.release_native_owned(claimed, &None)))??;
                        return Err(error);
                    }
                }
                let mut retired = None;
                let published = original.with_original_leaf(|work| {
                    work.check()?;
                    let (admissions, _, counters, budget) = work.native_mount_admission_parts()?;
                    if admissions.first.is_some() && admissions.more.len() == admissions.more.capacity() {
                        if replacement.capacity() <= admissions.more.len() { return Ok(false); }
                        let copied = WorkCounters {
                            bytes_copied: admissions.more.len().checked_mul(std::mem::size_of::<NativeMountAllocationAdmission>())
                                .ok_or(crate::WorkError::Overflow)? as u64,
                            ..WorkCounters::UNCHARGED
                        };
                        counters.admit(&copied, &budget)?;
                        // Capacity was verified: append cannot allocate.
                        replacement.append(&mut admissions.more);
                        retired = Some(std::mem::replace(&mut admissions.more, std::mem::take(&mut replacement)));
                        counters.try_add_assign(&copied)?;
                    }
                    let admission = NativeMountAllocationAdmission {
                        live: Arc::downgrade(&live), adopted: metadata, known_control: None,
                    };
                    if admissions.first.is_none() { admissions.first = Some(admission); }
                    else { admissions.more.push(admission); }
                    Ok(true)
                }).and_then(std::convert::identity);
                let installed = retired.is_some();
                if let Some(old) = retired {
                    let bytes = old.capacity().checked_mul(std::mem::size_of::<NativeMountAllocationAdmission>())
                        .ok_or(crate::WorkError::Overflow)? as u64;
                    drop(old);
                    original.with_original_leaf(|work| work.with_resource_scope(None, |work|
                        work.release_native_owned(bytes, &None))).and_then(std::convert::identity)?;
                }
                if !installed {
                    drop(replacement);
                    original.with_original_leaf(|work| work.with_resource_scope(None, |work|
                        work.release_native_owned(claimed, &None))).and_then(std::convert::identity)?;
                }
                match published? {
                    true => return Ok(()),
                    false => {
                        // A concurrent registration consumed the planned slot.
                        original.with_original_leaf(|work| work.charge(WorkCounters {
                            items_examined: 1, ..WorkCounters::UNCHARGED
                        }))??;
                    }
                }
            }
        })();
        if let Err(error) = result {
            drop(live);
            original.with_original_leaf(|work| work.with_resource_scope(None, |work|
                work.release_native_owned(metadata, &None))).and_then(std::convert::identity)?;
            return Err(error);
        }
        Ok(Self { live })
    }

    pub(crate) fn new(work: &mut DemandWork) -> Result<Self, DemandError> {
        work.check()?;
        let (admissions, allocations, counters, budget) = work.native_mount_admission_parts()?;
        admissions.reserve_slot(allocations, counters, budget)?;
        let copied = crate::WorkCounters {
            bytes_copied: if admissions.first.is_some() {
                std::mem::size_of::<NativeMountAllocationAdmission>() as u64
            } else { 0 },
            ..crate::WorkCounters::UNCHARGED
        };
        counters.admit(&copied, &budget)?;
        let bytes = std::mem::size_of::<NativeMountAllocationCustodyData>() as u64;
        allocations.claim_bytes(bytes, 1, counters, budget)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        let live = Arc::new(NativeMountAllocationCustodyData {
            live: std::sync::atomic::AtomicU64::new(bytes),
            capture_claim_origin: None,
            original: None,
        });
        let admission = NativeMountAllocationAdmission {
            live: Arc::downgrade(&live), adopted: bytes, known_control: None,
        };
        if admissions.first.is_none() { admissions.first = Some(admission); }
        else { admissions.more.push(admission); }
        counters.charge(&copied, &budget)?;
        Ok(Self { live })
    }

    /// Attaches the real known constructor Arc after its payload and this
    /// registration's complete layout were admitted. On denial the constructor
    /// retains its actual owner for measured cleanup; no post-failure Box exists.
    pub(crate) fn attach_known_control(
        &self, owner: NativeMountControlAllocation, work: &mut crate::demand::DemandWork,
    ) -> Result<(), DemandError> {
        if !owner.has_strong_owner() { return Err(crate::DemandError::InvalidRequest.into()); }
        let (admissions, _, counters, budget) = work.native_mount_admission_parts()?;
        for entry in admissions.first.iter_mut().chain(admissions.more.iter_mut()) {
            let step = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
            counters.charge(&step, &budget)?;
            if entry.live.as_ptr() == Arc::as_ptr(&self.live) {
                if entry.known_control.is_some() { return Err(crate::DemandError::InvalidRequest.into()); }
                entry.known_control = Some(NativeMountKnownControl {
                    _owner: owner, _custody: Arc::downgrade(&self.live),
                });
                return Ok(());
            }
        }
        Err(crate::DemandError::InvalidRequest.into())
    }

    /// Transfers one exact already-admitted known payload claim. Construction
    /// must precede retaining the actual payload, or its caller must drop and
    /// release that payload on denial. No second claim or allocation is guessed.
    pub(crate) fn from_admitted(
        bytes: u64, work: &mut crate::demand::DemandWork,
    ) -> Result<Self, DemandError> {
        let custody = Self::new(work)?;
        custody.retain(bytes, work, false)?;
        Ok(custody)
    }

    /// Recognizes only this physical token's registration on the original
    /// invocation. It neither adopts borrowed bytes nor creates authority.
    pub(crate) fn is_admitted(
        &self, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, DemandError> {
        let (admissions, _, counters, budget) = work.native_mount_admission_parts()?;
        for entry in admissions.first.iter().chain(admissions.more.iter()) {
            let step = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
            counters.charge(&step, &budget)?;
            if entry.live.as_ptr() == Arc::as_ptr(&self.live) { return Ok(true); }
        }
        Ok(false)
    }

    /// Admits a concrete allocation before entering its actual leaf.
    pub(crate) fn claim(
        &self, bytes: u64, work: &mut crate::demand::DemandWork,
    ) -> Result<(), DemandError> {
        self.retain(bytes, work, true)
    }

    /// Transfers a producer's already-live known claim without adopting or
    /// allocating it again. Call only after moving that exact owned payload.
    pub(crate) fn retain_admitted(
        &self, bytes: u64, work: &mut crate::demand::DemandWork,
    ) -> Result<(), DemandError> {
        self.retain(bytes, work, false)
    }

    /// Attaches only the exact already-owned payload after its producer's work
    /// and adoption have reached this original ledger. Cancellation cannot
    /// abandon physical ownership. This terminal transfer claims no allocation
    /// and grants no permission for another operation.
    pub(crate) fn retain_completed_admitted(
        &self, bytes: u64, work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::WorkError> {
        let (admissions, _, _, _) = work.native_mount_completed_admission_parts();
        let entry = admissions.first.iter_mut().chain(admissions.more.iter_mut())
            .find(|entry| entry.live.as_ptr() == Arc::as_ptr(&self.live))
            .ok_or(crate::WorkError::Overflow)?;
        let adopted = entry.adopted.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        self.live.load(std::sync::atomic::Ordering::Acquire).checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        if let Some(capture) = self.live.capture_claim_origin.as_ref().and_then(std::sync::Weak::upgrade) {
            capture.transfer_retained_bytes(bytes)?;
        }
        self.live.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        entry.adopted = adopted;
        Ok(())
    }

    fn retain(
        &self, bytes: u64, work: &mut crate::demand::DemandWork, allocate: bool,
    ) -> Result<(), DemandError> {
        work.check()?;
        if !allocate {
            return self.retain_completed_admitted(bytes, work).map_err(DemandError::from);
        }
        let adopted = {
            let (admissions, _, _, _) = work.native_mount_admission_parts()?;
            let entry = admissions.first.iter().chain(admissions.more.iter())
                .find(|entry| entry.live.as_ptr() == Arc::as_ptr(&self.live))
                .ok_or(DemandError::InvalidRequest)?;
            entry.adopted.checked_add(bytes).ok_or(crate::WorkError::Overflow)?
        };
        self.live.load(std::sync::atomic::Ordering::Acquire).checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        work.claim_native_owned(bytes, u64::from(bytes != 0), &None)?;
        let (admissions, _, _, _) = work.native_mount_completed_admission_parts();
        let entry = admissions.first.iter_mut().chain(admissions.more.iter_mut())
            .find(|entry| entry.live.as_ptr() == Arc::as_ptr(&self.live))
            .ok_or(DemandError::InvalidRequest)?;
        self.live.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        entry.adopted = adopted;
        Ok(())
    }

    /// Marks only physically dropped payloads. The original ledger, if still
    /// retained, performs the release during its next reconciliation.
    pub(crate) fn retire(&self, bytes: u64) -> Result<(), crate::WorkError> {
        let metadata = std::mem::size_of::<NativeMountAllocationCustodyData>() as u64;
        self.live.fetch_update(
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
            |live| live.checked_sub(bytes).filter(|live| *live >= metadata),
        ).map_err(|_| crate::WorkError::Overflow)?;
        if let Some(original) = &self.live.original
            && let Some(resource) = original.resource_allowance()
            && let Err(error) = resource.retire(bytes) {
            let _recorded = original.with_original_leaf(|work| work.record_native_io_failure(error));
            return Err(crate::WorkError::Overflow);
        }
        Ok(())
    }
}
