//! Bounded profile-independent namespace paths.

use super::{LogicalName, NameEncoding, TreePageError};
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::path::PortablePath;
use super::allocation::{AllocationError, AllocationLedger, KernelAccounting, KernelOwnedAllocation};
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use thiserror::Error;
use super::codec::{CanonicalDecodeError, Decoder, Encoder, ReadAllocationStep};

/// Canonical absolute path represented as exact logical-name components.
#[derive(Debug)]
pub struct NamespacePath {
    components: Vec<LogicalName>,
    encoded_bytes: u32,
    logical_owned_bytes: u64,
}

impl Clone for NamespacePath {
    fn clone(&self) -> Self {
        Self::from_owned_components(self.components.clone(), self.encoded_bytes)
            .expect("simultaneously allocated path buffers fit the address space")
    }
}

impl PartialEq for NamespacePath {
    fn eq(&self, other: &Self) -> bool {
        self.components == other.components && self.encoded_bytes == other.encoded_bytes
    }
}

impl Eq for NamespacePath {}

impl Ord for NamespacePath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.components.cmp(&other.components)
            .then_with(|| self.encoded_bytes.cmp(&other.encoded_bytes))
    }
}

impl PartialOrd for NamespacePath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) enum NamespacePathStep<'a> {
    Allocation(ReadAllocationStep),
    Admit(&'a WorkCounters),
    Charge(&'a WorkCounters),
    Performed(&'a WorkCounters),
}

impl NamespacePath {
    fn from_owned_components(
        components: Vec<LogicalName>, encoded_bytes: u32,
    ) -> Result<Self, NamespacePathError> {
        let vector_bytes = components.capacity()
            .checked_mul(std::mem::size_of::<LogicalName>())
            .ok_or(NamespacePathError::Work(crate::WorkError::Overflow))?;
        let logical_owned_bytes = components.iter().try_fold(vector_bytes, |total, name| {
            total.checked_add(name.retained_bytes())
                .ok_or(NamespacePathError::Work(crate::WorkError::Overflow))
        })?;
        let logical_owned_bytes = u64::try_from(logical_owned_bytes)
            .map_err(|_| NamespacePathError::Work(crate::WorkError::Overflow))?;
        Ok(Self { components, encoded_bytes, logical_owned_bytes })
    }

    /// Constructs one bounded absolute namespace path.
    pub fn new(
        components: Vec<LogicalName>,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        let encoded_bytes = namespace_encoded_bytes(components.iter(), limits)?;
        Self::from_owned_components(components, encoded_bytes)
    }

    pub(crate) fn validate_component_lengths(
        lengths: impl IntoIterator<Item = usize>,
        limits: VolumeLimits,
    ) -> Result<u32, NamespacePathError> {
        Self::validate_component_lengths_with_steps(lengths, limits, || Ok(()))
    }

    fn validate_component_lengths_with_steps<E: From<NamespacePathError>>(
        lengths: impl IntoIterator<Item = usize>,
        limits: VolumeLimits,
        mut component_step: impl FnMut() -> Result<(), E>,
    ) -> Result<u32, E> {
        let mut depth = 0;
        let mut encoded_bytes = 1;
        for length in lengths {
            component_step()?;
            append_encoded_length(&mut depth, &mut encoded_bytes, length, limits).map_err(E::from)?;
        }
        finish_encoded_lengths(depth, encoded_bytes, limits).map_err(E::from)
    }

    /// Converts a strict UTF-8 portable path without losing information.
    pub fn from_portable(
        path: &PortablePath,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        Self::from_portable_in_profile(path, FilesystemProfile::Portable, limits)
    }

    /// Converts a portable path to the canonical names of one volume profile.
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

    /// Parses canonical Unicode through the portable validation rules and admits every owned capacity before allocating it.
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

    pub(crate) fn validate_in_profile_with_accounting(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        Self::validate_in_profile_with_steps(path, profile, limits, cancellation, || {
            accounting.charge(WorkCounters {
                source_path_components: 1, items_examined: 1,
                ..WorkCounters::UNCHARGED
            })
        }).map(|_| ())
    }

    fn validate_in_profile_with_steps<E: From<NamespacePathError>>(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        cancellation: &CancellationToken,
        mut component_step: impl FnMut() -> Result<(), E>,
    ) -> Result<(usize, NameEncoding, u32), E> {
        cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
        let depth = PortablePath::validate(path, limits)
            .map_err(|error| E::from(NamespacePathError::Path(error)))?;
        let encoding = match profile {
            FilesystemProfile::Portable | FilesystemProfile::Browser => NameEncoding::Utf8,
            FilesystemProfile::Posix => NameEncoding::PosixBytes,
            FilesystemProfile::Windows => NameEncoding::WindowsUtf16Le,
        };
        let mut encoded_bytes = 1_u32;
        let mut encoded_depth = 0;
        for component in path.strip_prefix('/').unwrap_or_default()
            .split('/').filter(|component| !component.is_empty()) {
            cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
            component_step()?;
            let length = LogicalName::validate_unicode(encoding, component, limits.maximum_component_bytes)
                .map_err(|error| E::from(NamespacePathError::Name(error)))?;
            append_encoded_length(&mut encoded_depth, &mut encoded_bytes, length, limits)
                .map_err(E::from)?;
        }
        Ok((depth, encoding,
            finish_encoded_lengths(encoded_depth, encoded_bytes, limits).map_err(E::from)?))
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
        let initial_live = ledger.live_bytes();
        let result = Self::parse_in_profile_with_steps(
            path, profile, limits, cancellation, &mut |step| {
                observe_path_ledger(step, ledger, work, budget)
            },
        );
        if result.is_err() {
            ledger.release(ledger.live_bytes() - initial_live).map_err(allocation_error)?;
        }
        result
    }

    pub(crate) fn parse_in_profile_with_accounting(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        let mut owned = NamespacePathAccounting::new(accounting)?;
        let result = Self::parse_in_profile_with_steps(
            path, profile, limits, cancellation, &mut |step| owned.observe(step),
        );
        owned.finish(result)
    }

    pub(crate) fn parse_in_profile_with_steps<E: From<NamespacePathError>>(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<Self, E> {
        let (depth, encoding, encoded_bytes) = Self::validate_in_profile_with_steps(
            path, profile, limits, cancellation, || {
                steps(NamespacePathStep::Charge(&WorkCounters {
                    source_path_components: 1, items_examined: 1,
                    ..WorkCounters::default()
                }))
            },
        )?;
        let mut names = namespace_path_vec_with_steps(depth, cancellation, steps)?;
        for component in path.strip_prefix('/').unwrap_or_default()
            .split('/').filter(|component| !component.is_empty()) {
            cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
            let length = encoded_length(component, encoding).map_err(E::from)?;
            let delta = if encoding == NameEncoding::WindowsUtf16Le {
                WorkCounters { bytes_encoded: length as u64, ..WorkCounters::default() }
            } else {
                WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() }
            };
            steps(NamespacePathStep::Admit(&delta))?;
            let mut bytes = namespace_path_vec_with_steps(length, cancellation, steps)?;
            if encoding == NameEncoding::WindowsUtf16Le {
                for unit in component.encode_utf16() { bytes.extend_from_slice(&unit.to_le_bytes()); }
            } else {
                bytes.extend_from_slice(component.as_bytes());
            }
            steps(NamespacePathStep::Performed(&delta))?;
            names.push(LogicalName::from_validated_bytes(encoding, bytes));
        }
        Self::from_owned_components(names, encoded_bytes).map_err(E::from)
    }

    /// Appends one already-validated exact native name, checking all volume bounds before allocating the new path.
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

    pub(crate) fn child_with_accounting(
        &self,
        name: &LogicalName,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        let mut owned = NamespacePathAccounting::new(accounting)?;
        cancellation.check()?;
        let encoded_bytes = namespace_encoded_bytes(
            self.components.iter().chain(std::iter::once(name)), limits,
        )?;
        let result = Self::copy_names_with_steps(
            &self.components, Some(name), encoded_bytes, cancellation,
            &mut |step| owned.observe(step),
        );
        owned.finish(result)
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

    pub(crate) fn clone_with_steps<E: From<NamespacePathError>>(
        &self,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::copy_names_with_steps(
            &self.components, None, self.encoded_bytes, cancellation, steps,
        )
    }

    pub(crate) fn clone_with_accounting(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        let mut owned = NamespacePathAccounting::new(accounting)?;
        let result = self.clone_with_steps(cancellation, &mut |step| owned.observe(step));
        owned.finish(result)
    }

    pub(crate) fn prefix_with_accounting(
        &self,
        depth: usize,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        let mut owned = NamespacePathAccounting::new(accounting)?;
        cancellation.check()?;
        let names = self.components.get(..depth).ok_or(NamespacePathError::TooDeep)?;
        let encoded_bytes = namespace_encoded_bytes(names.iter(), limits)?;
        let result = Self::copy_names_with_steps(
            names, None, encoded_bytes, cancellation, &mut |step| owned.observe(step),
        );
        owned.finish(result)
    }

    pub(crate) fn unicode_text_with_accounting(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(String, KernelOwnedAllocation), crate::FsError> {
        let mut owned = NamespacePathAccounting::new(accounting)?;
        let result = self.unicode_text_with_steps(cancellation, &mut |step| owned.observe(step));
        owned.finish(result)
    }

    fn unicode_text_with_steps<E: From<NamespacePathError>>(
        &self,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<String, E> {
        cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
        let mut length = usize::from(self.is_root());
        for name in &self.components {
            cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
            steps(NamespacePathStep::Charge(&WorkCounters {
                items_examined: 1, ..WorkCounters::UNCHARGED
            }))?;
            let bytes = name.unicode_utf8_bytes()
                .ok_or_else(|| E::from(NamespacePathError::NonUnicodeProjection))?;
            length = length.checked_add(1).and_then(|value| value.checked_add(bytes))
                .ok_or_else(|| E::from(NamespacePathError::Work(crate::WorkError::Overflow)))?;
        }
        let total = WorkCounters { bytes_encoded: length as u64,
            output_bytes: length as u64, ..WorkCounters::UNCHARGED };
        steps(NamespacePathStep::Admit(&total))?;
        cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
        steps(NamespacePathStep::Allocation(ReadAllocationStep::Allocate {
            bytes: length as u64, operations: u64::from(length != 0),
        }))?;
        let mut output = String::new();
        if let Err(error) = output.try_reserve_exact(length) {
            steps(NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)))?;
            return Err(E::from(NamespacePathError::AllocationFailed));
        }
        steps(NamespacePathStep::Allocation(ReadAllocationStep::Capacity {
            admitted_bytes: length as u64, actual_bytes: output.capacity() as u64,
        }))?;
        if self.is_root() {
            output.push('/');
            steps(NamespacePathStep::Performed(&WorkCounters {
                bytes_encoded: 1, output_bytes: 1, ..WorkCounters::UNCHARGED
            }))?;
        }
        for name in &self.components {
            cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
            let before = output.len();
            output.push('/');
            name.append_unicode(&mut output)
                .map_err(|error| E::from(NamespacePathError::Name(error)))?;
            let encoded = (output.len() - before) as u64;
            steps(NamespacePathStep::Performed(&WorkCounters {
                bytes_encoded: encoded, output_bytes: encoded, ..WorkCounters::UNCHARGED
            }))?;
        }
        Ok(output)
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
        let result = Self::copy_names_with_steps(
            names, tail, encoded_bytes, cancellation,
            &mut |step| observe_path_ledger(step, ledger, work, budget),
        );
        if result.is_err() {
            ledger.release(ledger.live_bytes() - initial_live).map_err(allocation_error)?;
        }
        result
    }

    fn copy_names_with_steps<E: From<NamespacePathError>>(
        names: &[LogicalName],
        tail: Option<&LogicalName>,
        encoded_bytes: u32,
        cancellation: &CancellationToken,
        steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
    ) -> Result<Self, E> {
        cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
        let depth = names.len().checked_add(usize::from(tail.is_some()))
            .ok_or_else(|| E::from(NamespacePathError::TooDeep))?;
        for _ in names.iter().chain(tail) {
            cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
            steps(NamespacePathStep::Charge(&WorkCounters {
                source_path_components: 1, items_examined: 1,
                ..WorkCounters::default()
            }))?;
        }
        let mut components = namespace_path_vec_with_steps(depth, cancellation, steps)?;
        for name in names.iter().chain(tail) {
            cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
            let delta = WorkCounters { bytes_copied: name.as_bytes().len() as u64,
                ..WorkCounters::default() };
            steps(NamespacePathStep::Admit(&delta))?;
            let mut bytes = namespace_path_vec_with_steps(name.as_bytes().len(), cancellation, steps)?;
            bytes.extend_from_slice(name.as_bytes());
            steps(NamespacePathStep::Performed(&delta))?;
            components.push(LogicalName::from_validated_bytes(name.encoding(), bytes));
        }
        Self::from_owned_components(components, encoded_bytes).map_err(E::from)
    }

    /// Exact cached backing capacity of the component vector and owned names. Allocator bookkeeping and the inline path value are excluded.
    #[must_use]
    pub const fn logical_owned_bytes(&self) -> u64 {
        self.logical_owned_bytes
    }

    /// Exact logical capacity retained by this owned path.
    #[must_use]
    pub const fn retained_bytes(&self) -> u64 {
        self.logical_owned_bytes
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
        Some(Self::from_owned_components(components, encoded_bytes)
            .expect("simultaneously allocated path buffers fit the address space"))
    }

    pub(crate) fn cursor_encoded_len(&self) -> Result<usize, NamespacePathError> {
        self.components.iter().try_fold(4_usize, |length, name| {
            length.checked_add(name.cursor_encoded_len()?)
                .ok_or(NamespacePathError::Codec(CanonicalDecodeError::LengthOverflow))
        })
    }

    pub(crate) fn encode_cursor_into_with_accounting(
        &self,
        encoder: &mut Encoder,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        cancellation.check()?;
        let count = u32::try_from(self.components.len())
            .map_err(|_| NamespacePathError::TooDeep)?;
        if encoder.remaining_capacity() < self.cursor_encoded_len()? {
            return Err(NamespacePathError::AllocationFailed.into());
        }
        let delta = WorkCounters { bytes_encoded: 4, items_examined: 1,
            ..WorkCounters::UNCHARGED };
        accounting.admit(delta)?;
        encoder.u32(count);
        accounting.record_completed(delta)?;
        for name in &self.components {
            name.encode_cursor_into_with_accounting(encoder, accounting, cancellation)?;
        }
        Ok(())
    }

    pub(crate) fn decode_cursor_with_accounting(
        decoder: &mut Decoder<'_>,
        profile: FilesystemProfile,
        limits: VolumeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), crate::FsError> {
        let mut owned = NamespacePathAccounting::new(accounting)?;
        let result = (|| -> Result<Self, crate::FsError> {
            cancellation.check()?;
            owned.observe(NamespacePathStep::Charge(&WorkCounters {
                items_examined: 1, ..WorkCounters::UNCHARGED
            }))?;
            let count = decoder.u32(&mut |_| Ok::<_, CanonicalDecodeError>(())).map_err(NamespacePathError::Codec)?;
            if count > u32::from(limits.maximum_path_depth) {
                return Err(NamespacePathError::TooDeep.into());
            }
            let mut preview = *decoder;
            let mut depth = 0;
            let mut encoded_bytes = 1;
            for _ in 0..count {
                let (_, bytes) = LogicalName::read_cursor_borrowed(
                    &mut preview, profile, limits.maximum_component_bytes, cancellation, &mut owned,
                )?;
                append_encoded_length(&mut depth, &mut encoded_bytes, bytes.len(), limits)?;
            }
            let encoded_bytes = finish_encoded_lengths(depth, encoded_bytes, limits)?;
            let capacity = usize::try_from(count).map_err(|_| NamespacePathError::TooDeep)?;
            let mut components = namespace_path_vec_with_steps(
                capacity, cancellation, &mut |step| owned.observe(step),
            )?;
            for _ in 0..count {
                components.push(LogicalName::decode_cursor(
                    decoder, profile, limits.maximum_component_bytes, cancellation, &mut owned,
                )?);
            }
            Self::from_owned_components(components, encoded_bytes).map_err(Into::into)
        })();
        owned.finish(result)
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
    /// A valid opaque native name cannot be represented by the Unicode API.
    #[error("non-Unicode path cannot be projected as a portable string")]
    NonUnicodeProjection,
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

impl From<NamespacePathError> for crate::FsError {
    fn from(error: NamespacePathError) -> Self {
        match error {
            NamespacePathError::Work(error) => Self::Work(error),
            NamespacePathError::Cancelled => Self::Cancelled(crate::CancellationError),
            error => Self::Path(super::PathLookupError::NamespacePath(error)),
        }
    }
}

pub(super) struct NamespacePathAccounting<'a, 'b> {
    accounting: &'a mut KernelAccounting<'b>,
    allocation: KernelOwnedAllocation,
    owned_bytes: u64,
}

impl<'a, 'b> NamespacePathAccounting<'a, 'b> {
    pub(super) fn new(accounting: &'a mut KernelAccounting<'b>) -> Result<Self, crate::FsError> {
        let allocation = accounting.claim_owned(0, 0)?;
        Ok(Self { accounting, allocation, owned_bytes: 0 })
    }

    pub(super) fn observe(&mut self, step: NamespacePathStep<'_>) -> Result<(), crate::FsError> {
        match step {
            NamespacePathStep::Admit(delta) => self.accounting.admit(*delta),
            NamespacePathStep::Charge(delta) => self.accounting.charge(*delta),
            NamespacePathStep::Performed(delta) => self.accounting.record_completed(*delta),
            NamespacePathStep::Allocation(step) => match step {
                ReadAllocationStep::Allocate { bytes, operations } => {
                    let next = self.owned_bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                    let mut incoming = self.accounting.claim_owned(bytes, operations)?;
                    if let Err(error) = self.allocation.absorb(&mut incoming) {
                        super::persistent_point::retire_allocation(incoming, bytes, self.accounting)?;
                        return Err(error);
                    }
                    self.owned_bytes = next;
                    Ok(())
                }
                ReadAllocationStep::Capacity { admitted_bytes, actual_bytes } => {
                    self.owned_bytes = self.owned_bytes.checked_sub(admitted_bytes)
                        .and_then(|bytes| bytes.checked_add(actual_bytes))
                        .ok_or(crate::WorkError::Overflow)?;
                    self.accounting.reconcile_owned(&mut self.allocation, admitted_bytes, actual_bytes)
                }
                ReadAllocationStep::ReserveFailed(error) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    if self.accounting.native_work().is_some() {
                        return Err(crate::NativeTreeMaterializationError::AllocationFailed(error).into());
                    }
                    drop(error);
                    Err(NamespacePathError::AllocationFailed.into())
                }
                ReadAllocationStep::BeforeCopy(bytes) => self.accounting.admit(
                    WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED },
                ),
                ReadAllocationStep::Copied(bytes) => self.accounting.record_completed(
                    WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED },
                ),
                ReadAllocationStep::BeforeHash(bytes) => {
                    self.accounting.check()?;
                    self.accounting.admit(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED })
                }
                ReadAllocationStep::Hashed(bytes) => self.accounting.record_completed(
                    WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED },
                ),
                ReadAllocationStep::BeforeItem => self.accounting.admit(
                    WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
                ),
                ReadAllocationStep::Item => self.accounting.record_completed(
                    WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
                ),
            },
        }
    }

    pub(super) fn finish<T>(
        self,
        result: Result<T, crate::FsError>,
    ) -> Result<(T, KernelOwnedAllocation), crate::FsError> {
        let Self { accounting, allocation, owned_bytes } = self;
        match result {
            Ok(path) => Ok((path, allocation)),
            Err(error) => {
                // The producer has already destroyed every partial buffer.
                super::persistent_point::retire_allocation(allocation, owned_bytes, accounting)?;
                Err(error)
            }
        }
    }
}

pub(super) fn observe_path_ledger(
    step: NamespacePathStep<'_>,
    ledger: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), NamespacePathError> {
    match step {
        NamespacePathStep::Admit(delta) =>
            work.admit(delta, &budget).map_err(NamespacePathError::Work),
        NamespacePathStep::Charge(delta) =>
            work.charge(delta, &budget).map_err(NamespacePathError::Work),
        NamespacePathStep::Performed(delta) => {
            work.try_add_assign(delta).map_err(NamespacePathError::Work)?;
            work.verify_ref(&budget).map_err(NamespacePathError::Work)
        }
        NamespacePathStep::Allocation(step) => match step {
            ReadAllocationStep::Allocate { bytes, operations } =>
                ledger.claim_bytes(bytes, operations, work, budget).map_err(allocation_error),
            ReadAllocationStep::Capacity { admitted_bytes, actual_bytes } => {
                if actual_bytes > admitted_bytes {
                    ledger.record_owned_bytes(actual_bytes - admitted_bytes, work)
                        .map_err(allocation_error)?;
                    work.verify_ref(&budget).map_err(NamespacePathError::Work)?;
                }
                Ok(())
            }
            ReadAllocationStep::ReserveFailed(_) => Ok(()),
            ReadAllocationStep::BeforeCopy(bytes) =>
                work.admit(&WorkCounters { bytes_copied: bytes,
                    ..WorkCounters::default() }, &budget).map_err(NamespacePathError::Work),
            ReadAllocationStep::Copied(bytes) => {
                work.try_add_assign(&WorkCounters { bytes_copied: bytes,
                    ..WorkCounters::default() }).map_err(NamespacePathError::Work)?;
                work.verify_ref(&budget).map_err(NamespacePathError::Work)
            }
            ReadAllocationStep::BeforeHash(bytes) =>
                work.admit(&WorkCounters { bytes_hashed: bytes,
                    ..WorkCounters::default() }, &budget).map_err(NamespacePathError::Work),
            ReadAllocationStep::Hashed(bytes) => {
                work.try_add_assign(&WorkCounters { bytes_hashed: bytes,
                    ..WorkCounters::default() }).map_err(NamespacePathError::Work)?;
                work.verify_ref(&budget).map_err(NamespacePathError::Work)
            }
            ReadAllocationStep::BeforeItem =>
                work.admit(&WorkCounters { items_examined: 1,
                    ..WorkCounters::default() }, &budget).map_err(NamespacePathError::Work),
            ReadAllocationStep::Item =>
                work.charge_items(1, &budget).map_err(NamespacePathError::Work),
        },
    }
}

pub(super) fn namespace_path_vec_with_steps<T, E: From<NamespacePathError>>(
    capacity: usize,
    cancellation: &CancellationToken,
    steps: &mut impl FnMut(NamespacePathStep<'_>) -> Result<(), E>,
) -> Result<Vec<T>, E> {
    cancellation.check().map_err(|_| E::from(NamespacePathError::Cancelled))?;
    let admitted_bytes = capacity.checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| E::from(NamespacePathError::AllocationFailed))?;
    steps(NamespacePathStep::Allocation(ReadAllocationStep::Allocate {
        bytes: admitted_bytes, operations: u64::from(capacity != 0),
    }))?;
    let mut values = Vec::new();
    if let Err(error) = values.try_reserve_exact(capacity) {
        steps(NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)))?;
        return Err(E::from(NamespacePathError::AllocationFailed));
    }
    let actual_bytes = values.capacity().checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| E::from(NamespacePathError::AllocationFailed))?;
    steps(NamespacePathStep::Allocation(ReadAllocationStep::Capacity {
        admitted_bytes, actual_bytes,
    }))?;
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
