//! Bounded profile-independent namespace paths.

use super::{LogicalName, NameEncoding, TreePageError};
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::path::PortablePath;
use super::allocation::{AllocationError, AllocationLedger};
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use thiserror::Error;
use super::codec::{CanonicalDecodeError, Decoder, Encoder};

/// Canonical absolute path represented as exact logical-name components.
///
/// An empty component vector denotes the volume root. Unlike
/// [`PortablePath`], this type preserves raw POSIX bytes and Windows UTF-16LE
/// names. A volume executor additionally checks that every component encoding
/// is admitted by that volume's filesystem profile.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NamespacePath {
    components: Vec<LogicalName>,
    encoded_bytes: u32,
}

impl NamespacePath {
    /// Constructs one bounded absolute namespace path.
    ///
    /// # Errors
    ///
    /// Rejects excessive depth, component bytes, total bytes, or arithmetic
    /// overflow before the path reaches a storage backend.
    pub fn new(
        components: Vec<LogicalName>,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        let encoded_bytes = namespace_encoded_bytes(components.iter(), limits)?;
        Ok(Self {
            components,
            encoded_bytes,
        })
    }

    /// Checks exact encoded component lengths without allocating names.
    /// Native adapters use this before producing their owned native buffers.
    pub(crate) fn validate_component_lengths(
        lengths: impl IntoIterator<Item = usize>,
        limits: VolumeLimits,
    ) -> Result<u32, NamespacePathError> {
        let mut depth = 0;
        let mut encoded_bytes = 1;
        for length in lengths {
            append_encoded_length(&mut depth, &mut encoded_bytes, length, limits)?;
        }
        finish_encoded_lengths(depth, encoded_bytes, limits)
    }

    /// Converts a strict UTF-8 portable path without losing information.
    ///
    /// # Errors
    ///
    /// Returns the same component and total bounds as [`Self::new`], plus a
    /// canonical-name failure if the portable input violates the name format.
    pub fn from_portable(
        path: &PortablePath,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        Self::from_portable_in_profile(path, FilesystemProfile::Portable, limits)
    }

    /// Converts a portable path to the canonical names of one volume profile.
    ///
    /// The string API can address names representable as Unicode; native
    /// capture APIs retain raw POSIX bytes or UTF-16 units outside that subset.
    ///
    /// # Errors
    ///
    /// Returns bounded-name or path errors when the encoded host names exceed
    /// the volume limits.
    pub fn from_portable_in_profile(
        path: &PortablePath,
        profile: FilesystemProfile,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        Self::parse_in_profile_measured(
            path.as_str(), profile, limits, WorkBudget::UNBOUNDED,
            &CancellationToken::new(), 0,
        ).map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Parses canonical Unicode through the portable validation rules and
    /// admits every owned capacity before allocating it.
    ///
    /// `live_owned_bytes` is the caller's currently live storage, not its peak.
    ///
    /// # Errors
    /// Returns validation, cancellation, allocation, or budget failures with
    /// the actual prefix of work performed.
    pub fn parse_in_profile_measured(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        live_owned_bytes: u64,
    ) -> MeasuredResult<OperationReceipt<Self>, NamespacePathError> {
        let mut work = WorkCounters::default();
        let mut ledger = AllocationLedger::default();
        let result = (|| {
            cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
            ledger.claim_bytes(live_owned_bytes, 0, &mut work, budget)
                .map_err(allocation_error)?;
            Self::parse_in_profile_with_ledger(
                path, profile, limits, &mut ledger, &mut work, budget, cancellation,
            )
        })();
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) fn parse_in_profile_with_ledger(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, NamespacePathError> {
        cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
        let depth = PortablePath::validate(path, limits).map_err(NamespacePathError::Path)?;
        let encoding = match profile {
            FilesystemProfile::Portable | FilesystemProfile::Browser => NameEncoding::Utf8,
            FilesystemProfile::Posix => NameEncoding::PosixBytes,
            FilesystemProfile::Windows => NameEncoding::WindowsUtf16Le,
        };
        let components = || path.strip_prefix('/').unwrap_or_default()
            .split('/').filter(|component| !component.is_empty());
        let mut encoded_bytes = 1_u32;
        let mut encoded_depth = 0;
        for component in components() {
            cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
            work.charge(&WorkCounters { source_path_components: 1, items_examined: 1,
                ..WorkCounters::default() }, &budget).map_err(NamespacePathError::Work)?;
            let length = LogicalName::validate_unicode(encoding, component, limits.maximum_component_bytes)
                .map_err(NamespacePathError::Name)?;
            append_encoded_length(&mut encoded_depth, &mut encoded_bytes, length, limits)?;
        }
        let encoded_bytes = finish_encoded_lengths(encoded_depth, encoded_bytes, limits)?;
        let initial_live = ledger.live_bytes();
        let result = (|| {
            let mut names = path_vec(depth, ledger, work, budget)?;
            for component in components() {
                cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
                let length = encoded_length(component, encoding)?;
                let delta = if encoding == NameEncoding::WindowsUtf16Le {
                    WorkCounters { bytes_encoded: length as u64, ..WorkCounters::default() }
                } else {
                    WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() }
                };
                work.admit(&delta, &budget).map_err(NamespacePathError::Work)?;
                let mut bytes = path_vec(length, ledger, work, budget)?;
                if encoding == NameEncoding::WindowsUtf16Le {
                    for unit in component.encode_utf16() { bytes.extend_from_slice(&unit.to_le_bytes()); }
                } else {
                    bytes.extend_from_slice(component.as_bytes());
                }
                work.charge(&delta, &budget).map_err(NamespacePathError::Work)?;
                names.push(LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                    .map_err(NamespacePathError::Name)?);
            }
            Ok(Self { components: names, encoded_bytes })
        })();
        if result.is_err() {
            ledger.release(ledger.live_bytes() - initial_live).map_err(allocation_error)?;
        }
        result
    }

    /// Copies exact canonical names, including opaque native code units,
    /// under the caller's remaining budget and current live allocation.
    ///
    /// # Errors
    /// Returns cancellation, allocation, or budget failures with actual work.
    pub fn clone_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        live_owned_bytes: u64,
    ) -> MeasuredResult<OperationReceipt<Self>, NamespacePathError> {
        Self::copy_names_measured(
            &self.components, None, self.encoded_bytes, budget, cancellation, live_owned_bytes,
        )
    }

    /// Appends one already-validated exact native name, checking all volume
    /// bounds before allocating the new path.
    ///
    /// # Errors
    /// Returns bounds, cancellation, allocation, or budget failures.
    pub fn child_measured(
        &self,
        name: &LogicalName,
        limits: VolumeLimits,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        live_owned_bytes: u64,
    ) -> MeasuredResult<OperationReceipt<Self>, NamespacePathError> {
        cancellation.check().map_err(|_| OperationFailure::before_work(NamespacePathError::Cancelled))?;
        let encoded_bytes = namespace_encoded_bytes(
            self.components.iter().chain(std::iter::once(name)), limits,
        ).map_err(OperationFailure::before_work)?;
        Self::copy_names_measured(
            &self.components, Some(name), encoded_bytes, budget, cancellation, live_owned_bytes,
        )
    }

    /// Appends with the containing operation's cumulative budget and live ledger.
    pub(crate) fn child_with_ledger(
        &self,
        name: &LogicalName,
        limits: VolumeLimits,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, NamespacePathError> {
        cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
        let encoded_bytes = namespace_encoded_bytes(
            self.components.iter().chain(std::iter::once(name)), limits,
        )?;
        Self::copy_names_with_ledger(
            &self.components, Some(name), encoded_bytes, ledger, work, budget, cancellation,
        )
    }

    pub(crate) fn clone_with_ledger(
        &self,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, NamespacePathError> {
        Self::copy_names_with_ledger(
            &self.components, None, self.encoded_bytes, ledger, work, budget, cancellation,
        )
    }

    fn copy_names_measured(
        names: &[LogicalName],
        tail: Option<&LogicalName>,
        encoded_bytes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        live_owned_bytes: u64,
    ) -> MeasuredResult<OperationReceipt<Self>, NamespacePathError> {
        let mut work = WorkCounters::default();
        let mut ledger = AllocationLedger::default();
        let result = (|| {
            cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
            ledger.claim_bytes(live_owned_bytes, 0, &mut work, budget).map_err(allocation_error)?;
            Self::copy_names_with_ledger(
                names, tail, encoded_bytes, &mut ledger, &mut work, budget, cancellation,
            )
        })();
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    fn copy_names_with_ledger(
        names: &[LogicalName],
        tail: Option<&LogicalName>,
        encoded_bytes: u32,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, NamespacePathError> {
        let initial_live = ledger.live_bytes();
        let result = (|| {
            cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
            let depth = names.len().checked_add(usize::from(tail.is_some()))
                .ok_or(NamespacePathError::TooDeep)?;
            for _ in names.iter().chain(tail) {
                cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
                work.charge(&WorkCounters { source_path_components: 1, items_examined: 1,
                    ..WorkCounters::default() }, &budget).map_err(NamespacePathError::Work)?;
            }
            let mut components = path_vec(depth, ledger, work, budget)?;
            for name in names.iter().chain(tail) {
                cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
                let length = name.as_bytes().len();
                let delta = WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() };
                work.admit(&delta, &budget).map_err(NamespacePathError::Work)?;
                let mut bytes = path_vec(length, ledger, work, budget)?;
                bytes.extend_from_slice(name.as_bytes());
                work.charge(&delta, &budget).map_err(NamespacePathError::Work)?;
                components.push(LogicalName::new(name.encoding(), bytes, u32::MAX)
                    .map_err(NamespacePathError::Name)?);
            }
            Ok(Self { components, encoded_bytes })
        })();
        if result.is_err() {
            ledger.release(ledger.live_bytes() - initial_live).map_err(allocation_error)?;
        }
        result
    }

    /// Exact logical capacity retained by this owned path.
    #[must_use]
    pub fn retained_bytes(&self) -> u64 {
        (self.components.capacity() * std::mem::size_of::<LogicalName>()) as u64
            + self.components.iter().map(|name| name.retained_bytes() as u64).sum::<u64>()
    }

    /// Returns whether this is the volume root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.components.is_empty()
    }

    /// Exact logical path depth.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.components.len()
    }

    /// Canonical encoded bytes including separators and the leading root mark.
    #[must_use]
    pub const fn encoded_bytes(&self) -> u32 {
        self.encoded_bytes
    }

    /// Borrows exact logical-name components in root-to-leaf order.
    #[must_use]
    pub fn components(&self) -> &[LogicalName] {
        &self.components
    }

    /// Returns the parent path and final name, or `None` for root.
    #[must_use]
    pub fn split_last(&self) -> Option<(&[LogicalName], &LogicalName)> {
        self.components
            .split_last()
            .map(|(name, parent)| (parent, name))
    }

    /// Returns the exact parent path, or `None` for the namespace root.
    ///
    /// This preserves the already-validated component representation and does
    /// not consult a filesystem or allocate beyond cloning the parent prefix.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        let (_, name) = self.split_last()?;
        let encoded_bytes = if self.components.len() == 1 {
            1
        } else {
            self.encoded_bytes
                .saturating_sub(u32::try_from(name.as_bytes().len()).unwrap_or(u32::MAX))
                .saturating_sub(1)
        };
        let components = self.components.get(..self.components.len() - 1)?.to_vec();
        Some(Self {
            components,
            encoded_bytes,
        })
    }

    pub(crate) fn cursor_encoded_len(&self) -> Result<usize, NamespacePathError> {
        self.components.iter().try_fold(4_usize, |length, name| {
            length.checked_add(name.cursor_encoded_len()?)
                .ok_or(NamespacePathError::Codec(CanonicalDecodeError::LengthOverflow))
        })
    }

    /// Appends only within the containing cursor's admitted output capacity.
    pub(crate) fn encode_cursor_into(
        &self,
        encoder: &mut Encoder,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), NamespacePathError> {
        cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
        let count = u32::try_from(self.components.len()).map_err(|_| NamespacePathError::TooDeep)?;
        if encoder.remaining_capacity() < self.cursor_encoded_len()? {
            return Err(NamespacePathError::AllocationFailed);
        }
        work.charge(&WorkCounters { bytes_encoded: 4, items_examined: 1,
            ..WorkCounters::default() }, &budget).map_err(NamespacePathError::Work)?;
        encoder.u32(count);
        for name in &self.components {
            name.encode_cursor_into(encoder, work, budget, cancellation)?;
        }
        Ok(())
    }

    /// Validates the entire borrowed shape before claiming owned backing.
    /// Successful claims remain in the caller's ledger; failures retire only
    /// this decode's claims and preserve its actual performed-work prefix.
    pub(crate) fn decode_cursor_with_ledger(
        decoder: &mut Decoder<'_>,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, NamespacePathError> {
        cancellation.check().map_err(|_| NamespacePathError::Cancelled)?;
        work.charge_items(1, &budget).map_err(NamespacePathError::Work)?;
        let count = decoder.u32().map_err(NamespacePathError::Codec)?;
        if count > u32::from(limits.maximum_path_depth) {
            return Err(NamespacePathError::TooDeep);
        }
        let mut preview = *decoder;
        let mut depth = 0;
        let mut encoded_bytes = 1;
        for _ in 0..count {
            let (_, bytes) = LogicalName::read_cursor_borrowed(
                &mut preview, profile, limits.maximum_component_bytes,
                work, budget, cancellation,
            )?;
            append_encoded_length(&mut depth, &mut encoded_bytes, bytes.len(), limits)?;
        }
        finish_encoded_lengths(depth, encoded_bytes, limits)?;
        let initial_live = ledger.live_bytes();
        let result = (|| {
            let capacity = usize::try_from(count).map_err(|_| NamespacePathError::TooDeep)?;
            let mut components = path_vec(capacity, ledger, work, budget)?;
            for _ in 0..count {
                components.push(LogicalName::decode_cursor_with_ledger(
                    decoder, profile, limits.maximum_component_bytes,
                    ledger, work, budget, cancellation,
                )?);
            }
            Self::new(components, limits)
        })();
        if result.is_err() {
            ledger.release(ledger.live_bytes() - initial_live).map_err(allocation_error)?;
        }
        result
    }

    /// Returns whether this path is equal to or below `ancestor`.
    #[must_use]
    pub fn is_within(&self, ancestor: &Self) -> bool {
        self.components.starts_with(&ancestor.components)
    }
}

/// Profile-independent namespace path failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NamespacePathError {
    /// Component count exceeds the volume bound.
    #[error("namespace path exceeds its depth bound")]
    TooDeep,
    /// One exact encoded component exceeds the volume bound.
    #[error("namespace path component exceeds its byte bound")]
    ComponentTooLong,
    /// Total canonical encoded path bytes exceed the volume bound.
    #[error("namespace path exceeds its byte bound")]
    PathTooLong,
    /// One portable component could not become a canonical logical name.
    #[error(transparent)]
    Name(TreePageError),
    /// The Unicode spelling violates the canonical portable rules.
    #[error(transparent)]
    Path(crate::path::PathError),
    /// An admitted work limit was exceeded.
    #[error(transparent)]
    Work(crate::WorkError),
    /// The caller cancelled before the next allocation.
    #[error("filesystem operation was cancelled")]
    Cancelled,
    /// A requested owned capacity could not be allocated.
    #[error("namespace path allocation failed")]
    AllocationFailed,
    /// A nested canonical cursor field is malformed.
    #[error(transparent)]
    Codec(CanonicalDecodeError),
}

pub(super) fn path_vec<T>(
    capacity: usize,
    ledger: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<Vec<T>, NamespacePathError> {
    let claimed = ledger.claim_elements::<T>(capacity, work, budget).map_err(allocation_error)?;
    let mut values = Vec::new();
    if values.try_reserve_exact(capacity).is_err() {
        ledger.release(claimed).map_err(allocation_error)?;
        return Err(NamespacePathError::AllocationFailed);
    }
    let actual = values.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or(NamespacePathError::AllocationFailed)? as u64;
    if actual > claimed {
        if let Err(error) = ledger.claim_bytes(actual - claimed, 0, work, WorkBudget::UNBOUNDED) {
            ledger.release(claimed).map_err(allocation_error)?;
            return Err(allocation_error(error));
        }
        if let Err(error) = work.verify_ref(&budget) {
            ledger.release(actual).map_err(allocation_error)?;
            return Err(NamespacePathError::Work(error));
        }
    }
    Ok(values)
}

fn namespace_encoded_bytes<'a>(
    components: impl Iterator<Item = &'a LogicalName>,
    limits: VolumeLimits,
) -> Result<u32, NamespacePathError> {
    NamespacePath::validate_component_lengths(
        components.map(|component| component.as_bytes().len()), limits,
    )
}

fn append_encoded_length(
    depth: &mut usize,
    encoded_bytes: &mut u32,
    length: usize,
    limits: VolumeLimits,
) -> Result<(), NamespacePathError> {
    *depth = depth.checked_add(1).ok_or(NamespacePathError::TooDeep)?;
    if *depth > usize::from(limits.maximum_path_depth) {
        return Err(NamespacePathError::TooDeep);
    }
    let length = u32::try_from(length).map_err(|_| NamespacePathError::ComponentTooLong)?;
    if length > limits.maximum_component_bytes {
        return Err(NamespacePathError::ComponentTooLong);
    }
    *encoded_bytes = encoded_bytes.checked_add(length)
        .and_then(|value| value.checked_add(1)).ok_or(NamespacePathError::PathTooLong)?;
    Ok(())
}

fn finish_encoded_lengths(
    depth: usize,
    mut encoded_bytes: u32,
    limits: VolumeLimits,
) -> Result<u32, NamespacePathError> {
    if depth != 0 { encoded_bytes -= 1; }
    if encoded_bytes > limits.maximum_path_bytes { return Err(NamespacePathError::PathTooLong); }
    Ok(encoded_bytes)
}

fn encoded_length(component: &str, encoding: NameEncoding) -> Result<usize, NamespacePathError> {
    if encoding == NameEncoding::WindowsUtf16Le {
        component.encode_utf16().count().checked_mul(2).ok_or(NamespacePathError::ComponentTooLong)
    } else {
        Ok(component.len())
    }
}

fn allocation_error(error: AllocationError) -> NamespacePathError {
    match error {
        AllocationError::Work(error) => NamespacePathError::Work(error),
        _ => NamespacePathError::AllocationFailed,
    }
}

#[cfg(test)]
#[path = "tests/namespace_path.rs"]
mod tests;
