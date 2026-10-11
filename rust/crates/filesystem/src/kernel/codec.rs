//! Small fail-closed codec used by every hash-bearing filesystem object.

use crate::foundation::usize_to_u64;
use crate::model::VolumeConfig;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodedPageKind {
    Leaf,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodedPageShape {
    pub(crate) kind: DecodedPageKind,
    pub(crate) items: usize,
    pub(crate) nested_bytes: u64,
}

/// Admission, actual reserve failures, and completion points of canonical owned-buffer readers.
pub(crate) enum ReadAllocationStep {
    Allocate { bytes: u64, operations: u64 },
    /// Emitted only after a failed reserve; no-op observers retain the canonical error.
    ReserveFailed(std::collections::TryReserveError),
    Capacity { admitted_bytes: u64, actual_bytes: u64 },
    BeforeCopy(u64),
    Copied(u64),
    BeforeHash(u64),
    Hashed(u64),
    BeforeItem,
    Item,
}

/// Allocation and collection bounds for one canonical object decode.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DecodeLimits {
    /// Maximum complete canonical object bytes.
    pub maximum_object_bytes: u64,
    /// Maximum bytes in one logical name.
    pub maximum_name_bytes: u32,
    /// Maximum entries or children in one page.
    pub maximum_page_items: u32,
    /// Maximum canonical bytes in one authenticated index/page object.
    pub maximum_page_bytes: u32,
    /// Maximum authenticated page levels followed by one operation.
    pub maximum_page_height: u16,
    /// Maximum distinct authenticated pages retained for cycle/alias checks.
    pub maximum_visited_pages: u32,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            maximum_object_bytes: 64 * 1024 * 1024,
            maximum_name_bytes: 255,
            maximum_page_items: 1_024,
            maximum_page_bytes: 256 * 1024,
            maximum_page_height: 64,
            maximum_visited_pages: 4_096,
        }
    }
}

impl DecodeLimits {
    /// Bounds every page a volume with `config` may have written.
    pub(crate) fn for_volume(config: VolumeConfig) -> Self {
        Self {
            maximum_object_bytes: config.limits.maximum_object_bytes,
            maximum_name_bytes: config.limits.maximum_component_bytes,
            maximum_page_items: config.limits.maximum_directory_page_entries,
            maximum_page_bytes: u32::try_from(config.limits.maximum_object_bytes)
                .unwrap_or(u32::MAX),
            maximum_page_height: config.limits.maximum_page_height,
            maximum_visited_pages: u32::try_from(config.limits.maximum_objects_per_generation)
                .unwrap_or(u32::MAX),
        }
    }

    pub(crate) fn maximum_page_object_bytes(self) -> u64 {
        self.maximum_object_bytes
            .min(u64::from(self.maximum_page_bytes))
    }

    pub(crate) fn page_limits_valid(self, minimum_items: u32) -> bool {
        self.maximum_page_items >= minimum_items
            && self.maximum_page_bytes != 0
            && self.maximum_page_height != 0
            && self.maximum_visited_pages != 0
    }
}

/// Actual reservation and header-write boundaries of the one canonical writer.
pub(crate) enum EncodeAllocationStep {
    Reserve { capacity: usize },
    Capacity { admitted_bytes: u64, actual_bytes: u64 },
    BeforeHeader { bytes: u64 },
    HeaderWritten { bytes: u64 },
}

/// Work boundaries of the canonical writer body, outside every original-root loan.
pub(crate) enum EncodeWorkStep {
    Check,
    Admit(crate::WorkCounters),
    Completed(crate::WorkCounters),
    Allocation(EncodeAllocationStep),
}

/// Funds a canonical writer through short scalar original-ledger operations.
///
/// Allocators, byte writes, validation, and callbacks run outside the root loan.
/// `finish` transfers the buffer and its guard together, or destroys the buffer
/// before retiring its failed-operation capacity.
pub(crate) struct EncodeAccounting<'a, 'root> {
    accounting: &'a mut super::allocation::KernelAccounting<'root>,
    owned_bytes: u64,
    allocation: Option<super::allocation::KernelOwnedAllocation>,
}

impl<'a, 'root> EncodeAccounting<'a, 'root> {
    pub(crate) fn new(accounting: &'a mut super::allocation::KernelAccounting<'root>) -> Self {
        Self { accounting, owned_bytes: 0, allocation: None }
    }

    pub(crate) fn observe(&mut self, step: EncodeWorkStep) -> Result<(), crate::FsError> {
        match step {
            EncodeWorkStep::Check => self.accounting.check(),
            EncodeWorkStep::Admit(delta) => self.accounting.admit(delta),
            EncodeWorkStep::Completed(delta) => self.accounting.record_completed(delta),
            EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
                let requested = usize_to_u64(capacity);
                let next_owned = self.owned_bytes.checked_add(requested)
                    .ok_or(crate::WorkError::Overflow)?;
                let mut allocation = self.accounting.claim_owned(requested, u64::from(capacity != 0))?;
                self.owned_bytes = next_owned;
                if let Some(owned) = self.allocation.as_mut() {
                    owned.absorb(&mut allocation)?;
                } else {
                    self.allocation = Some(allocation);
                }
                Ok(())
            }
            EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                self.owned_bytes = self.owned_bytes.checked_sub(admitted_bytes)
                    .and_then(|other| other.checked_add(actual_bytes)).ok_or(crate::WorkError::Overflow)?;
                let allocation = self.allocation.as_mut().ok_or(crate::WorkError::Overflow)?;
                self.accounting.reconcile_owned(allocation, admitted_bytes, actual_bytes)
            }
            EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) =>
                self.accounting.admit(crate::WorkCounters { bytes_encoded: bytes, ..crate::WorkCounters::default() }),
            EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) =>
                self.accounting.record_completed(crate::WorkCounters { bytes_encoded: bytes, ..crate::WorkCounters::default() }),
        }
    }

    pub(crate) fn finish(
        mut self,
        result: Result<Vec<u8>, crate::FsError>,
    ) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
        match result {
            Ok(bytes) => {
                if let Err(error) = self.accounting.check() {
                    drop(bytes);
                    self.retire()?;
                    return Err(error);
                }
                let Some(allocation) = self.allocation.take() else {
                    drop(bytes);
                    return Err(crate::WorkError::Overflow.into());
                };
                self.owned_bytes = 0;
                Ok((bytes, allocation))
            }
            Err(error) => {
                self.retire()?;
                Err(error)
            }
        }
    }

    fn retire(&mut self) -> Result<(), crate::FsError> {
        let Some(allocation) = self.allocation.take() else { return Ok(()); };
        let bytes = std::mem::take(&mut self.owned_bytes);
        match allocation {
            super::allocation::KernelOwnedAllocation::Standalone => {
                match self.accounting {
                    super::allocation::KernelAccounting::Standalone { allocations, .. } =>
                        allocations.release(bytes),
                    super::allocation::KernelAccounting::BorrowedStandalone { allocations, .. } =>
                        allocations.release(bytes),
                    #[cfg(not(target_arch = "wasm32"))]
                    super::allocation::KernelAccounting::NativeOriginal { .. } =>
                        return Err(crate::WorkError::Overflow.into()),
                }.map_err(|error| error.into_work_or(crate::FsError::PendingMutationAllocationFailed))
            }
            #[cfg(not(target_arch = "wasm32"))]
            super::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
                drop(allocation);
                Ok(())
            }
        }
    }
}

impl Drop for EncodeAccounting<'_, '_> {
    fn drop(&mut self) {
        let retired = self.retire();
        debug_assert!(retired.is_ok(), "canonical encoder funding must retire its exact live capacity");
    }
}

pub(crate) struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    pub(crate) fn new(domain: &[u8], version: u16) -> Self {
        let mut bytes = Vec::with_capacity(domain.len() + 2);
        bytes.extend_from_slice(domain);
        bytes.extend_from_slice(&version.to_le_bytes());
        Self { bytes }
    }

    pub(crate) fn with_exact_capacity(
        domain: &[u8],
        version: u16,
        capacity: usize,
    ) -> Result<Self, CanonicalDecodeError> {
        Self::with_exact_capacity_with_steps(
            domain, version, capacity, |_| Ok(()),
            |_| CanonicalDecodeError::AllocationFailed,
        )
    }

    /// The same exact-capacity producer with original-ledger admission before
    /// reservation and actual-capacity reconciliation before any header write.
    pub(crate) fn with_exact_capacity_with_steps<E: From<CanonicalDecodeError>>(
        domain: &[u8],
        version: u16,
        capacity: usize,
        mut observe: impl FnMut(EncodeAllocationStep) -> Result<(), E>,
        allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
    ) -> Result<Self, E> {
        let minimum = domain.len().checked_add(2)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        if capacity < minimum {
            return Err(CanonicalDecodeError::LengthOverflow.into());
        }
        observe(EncodeAllocationStep::Reserve { capacity })?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(capacity).map_err(allocation_error)?;
        observe(EncodeAllocationStep::Capacity {
            admitted_bytes: usize_to_u64(capacity),
            actual_bytes: usize_to_u64(bytes.capacity()),
        })?;
        observe(EncodeAllocationStep::BeforeHeader { bytes: usize_to_u64(minimum) })?;
        bytes.extend_from_slice(domain);
        bytes.extend_from_slice(&version.to_le_bytes());
        observe(EncodeAllocationStep::HeaderWritten { bytes: usize_to_u64(minimum) })?;
        Ok(Self { bytes })
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn fixed(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub(crate) fn bounded_bytes(&mut self, value: &[u8]) -> Result<(), CanonicalDecodeError> {
        let length =
            u32::try_from(value.len()).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
        self.u32(length);
        self.fixed(value);
        Ok(())
    }

    pub(crate) fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    pub(crate) fn remaining_capacity(&self) -> usize {
        self.bytes.capacity() - self.bytes.len()
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(
        bytes: &'a [u8],
        domain: &[u8],
        expected_version: u16,
        maximum_object_bytes: u64,
    ) -> Result<Self, CanonicalDecodeError> {
        Self::new_with_steps(bytes, domain, expected_version, maximum_object_bytes, &mut |_| Ok(()))
    }

    pub(crate) fn new_with_steps<E: From<CanonicalDecodeError>>(
        bytes: &'a [u8],
        domain: &[u8],
        expected_version: u16,
        maximum_object_bytes: u64,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::new_recognized_versions_with_steps(
            bytes, domain, &[expected_version], maximum_object_bytes, observe,
        ).map(|(decoder, _)| decoder)
    }

    pub(crate) fn new_recognized_versions_with_steps<E: From<CanonicalDecodeError>>(
        bytes: &'a [u8],
        domain: &[u8],
        supported_versions: &[u16],
        maximum_object_bytes: u64,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<(Self, u16), E> {
        let observed = usize_to_u64(bytes.len());
        if observed > maximum_object_bytes {
            return Err(CanonicalDecodeError::ObjectTooLarge {
                observed,
                maximum: maximum_object_bytes,
            }.into());
        }
        let header_length = domain
            .len()
            .checked_add(2)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        let domain_prefix = bytes
            .get(..domain.len())
            .ok_or(CanonicalDecodeError::WrongDomain)?;
        if bytes.len() < header_length || domain_prefix != domain {
            return Err(CanonicalDecodeError::WrongDomain.into());
        }
        observe(ReadAllocationStep::BeforeCopy(2))?;
        let version_bytes: [u8; 2] = bytes
            .get(domain.len()..header_length)
            .ok_or(CanonicalDecodeError::Truncated)?
            .try_into()
            .map_err(|_| CanonicalDecodeError::Truncated)?;
        observe(ReadAllocationStep::Copied(2))?;
        let version = u16::from_le_bytes(version_bytes);
        if !supported_versions.contains(&version) {
            return Err(CanonicalDecodeError::UnsupportedVersion(version).into());
        }
        Ok((Self {
            bytes,
            cursor: header_length,
        }, version))
    }

    pub(crate) fn u8(&mut self) -> Result<u8, CanonicalDecodeError> {
        self.u8_with_steps(&mut |_| Ok(()))
    }

    pub(crate) fn u8_with_steps<E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<u8, E> {
        let source = self.bytes.get(self.cursor).ok_or(CanonicalDecodeError::Truncated)?;
        let next = self.cursor.checked_add(1).ok_or(CanonicalDecodeError::LengthOverflow)?;
        observe(ReadAllocationStep::BeforeCopy(1))?;
        let value = *source;
        self.cursor = next;
        observe(ReadAllocationStep::Copied(1))?;
        Ok(value)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, CanonicalDecodeError> {
        self.u32_with_steps(&mut |_| Ok(()))
    }

    pub(crate) fn u32_with_steps<E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<u32, E> {
        Ok(u32::from_le_bytes(self.fixed_with_steps::<4, E>(observe)?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, CanonicalDecodeError> {
        self.u64_with_steps(&mut |_| Ok(()))
    }

    pub(crate) fn u64_with_steps<E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<u64, E> {
        Ok(u64::from_le_bytes(self.fixed_with_steps::<8, E>(observe)?))
    }

    pub(crate) fn i64(&mut self) -> Result<i64, CanonicalDecodeError> {
        self.i64_with_steps(&mut |_| Ok(()))
    }

    pub(crate) fn i64_with_steps<E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<i64, E> {
        Ok(i64::from_le_bytes(self.fixed_with_steps::<8, E>(observe)?))
    }

    pub(crate) fn fixed<const N: usize>(&mut self) -> Result<[u8; N], CanonicalDecodeError> {
        self.fixed_with_steps::<N, CanonicalDecodeError>(&mut |_| Ok(()))
    }

    pub(crate) fn fixed_with_steps<const N: usize, E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<[u8; N], E> {
        let source = self.take(N)?;
        let copied = usize_to_u64(N);
        observe(ReadAllocationStep::BeforeCopy(copied))?;
        let value = source.try_into().map_err(|_| CanonicalDecodeError::Truncated)?;
        observe(ReadAllocationStep::Copied(copied))?;
        Ok(value)
    }

    pub(crate) fn bounded_bytes(&mut self, maximum: u32) -> Result<Vec<u8>, CanonicalDecodeError> {
        self.bounded_bytes_with_steps(maximum, &mut |_| Ok(()))
    }

    pub(crate) fn bounded_bytes_with_steps<E>(
        &mut self,
        maximum: u32,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Vec<u8>, E>
    where
        E: From<CanonicalDecodeError>,
    {
        let length = self.u32_with_steps(observe)?;
        if length > maximum {
            return Err(CanonicalDecodeError::FieldTooLarge {
                observed: length,
                maximum,
            }.into());
        }
        let length = usize::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
        let source = self.take(length)?;
        let mut value = Vec::new();
        let admitted_bytes = usize_to_u64(length);
        observe(ReadAllocationStep::Allocate {
            bytes: admitted_bytes, operations: u64::from(length != 0),
        })?;
        if let Err(error) = value.try_reserve_exact(length) {
            observe(ReadAllocationStep::ReserveFailed(error))?;
            return Err(CanonicalDecodeError::AllocationFailed.into());
        }
        observe(ReadAllocationStep::Capacity {
            admitted_bytes, actual_bytes: usize_to_u64(value.capacity()),
        })?;
        observe(ReadAllocationStep::BeforeCopy(admitted_bytes))?;
        value.extend_from_slice(source);
        observe(ReadAllocationStep::Copied(admitted_bytes))?;
        Ok(value)
    }

    pub(crate) fn skip_bounded_bytes(
        &mut self,
        maximum: u32,
    ) -> Result<usize, CanonicalDecodeError> {
        let length = self.u32()?;
        if length > maximum {
            return Err(CanonicalDecodeError::FieldTooLarge {
                observed: length,
                maximum,
            });
        }
        let length = usize::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
        self.take(length)?;
        Ok(length)
    }

    pub(crate) fn take_exact(&mut self, length: usize) -> Result<&'a [u8], CanonicalDecodeError> {
        self.take(length)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CanonicalDecodeError> {
        let end = self
            .cursor
            .checked_add(length)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or(CanonicalDecodeError::Truncated)?;
        self.cursor = end;
        Ok(value)
    }

    pub(crate) fn finish(self) -> Result<(), CanonicalDecodeError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(CanonicalDecodeError::TrailingBytes)
        }
    }
}

/// Allocation-free semantic causes retained by canonical codecs and name clones.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CanonicalInvariant {
    /// A fixed canonical-domain diagnostic.
    #[error("{0}")]
    Static(&'static str),
    /// Tree-page validation failed.
    #[error(transparent)]
    Tree(#[from] super::types::TreePageError),
    /// Logical-name validation failed using its actual validation error type.
    #[error(transparent)]
    Name(super::types::TreePageError),
    /// Attribute-page or attribute-name validation failed.
    #[error(transparent)]
    Attribute(#[from] super::attribute::AttributeError),
    /// File-table validation failed.
    #[error(transparent)]
    FileTable(#[from] super::file_table::FileTableError),
    /// Inline regular-file data validation failed.
    #[error(transparent)]
    InlineFileData(#[from] super::file_table::InlineFileDataError),
    /// Extent-page validation failed.
    #[error(transparent)]
    Extent(#[from] super::types::ExtentPageError),
    /// Canonical work accounting failed.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// A metadata reference names an object in the wrong domain.
    #[error("{field} has the wrong object kind")]
    MetadataReferenceKind {
        /// Stable metadata field name.
        field: &'static str,
    },
}

/// Fail-closed canonical object decoding failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CanonicalDecodeError {
    /// Object belongs to another canonical domain.
    #[error("canonical object domain is invalid")]
    WrongDomain,
    /// Object format version is unsupported.
    #[error("unsupported canonical object version {0}")]
    UnsupportedVersion(u16),
    /// Object ended before a declared field was complete.
    #[error("canonical object is truncated")]
    Truncated,
    /// Complete object exceeds the admitted allocation/work bound.
    #[error("canonical object has {observed} bytes; maximum is {maximum}")]
    ObjectTooLarge {
        /// Observed bytes.
        observed: u64,
        /// Admitted maximum.
        maximum: u64,
    },
    /// One bounded field exceeds its admitted bytes.
    #[error("canonical field has {observed} bytes; maximum is {maximum}")]
    FieldTooLarge {
        /// Observed bytes or items.
        observed: u32,
        /// Admitted maximum.
        maximum: u32,
    },
    /// Length arithmetic cannot be represented safely.
    #[error("canonical object length overflowed")]
    LengthOverflow,
    /// A bounded canonical encoding or decoding allocation failed.
    #[error("canonical codec allocation failed")]
    AllocationFailed,
    /// Canonical object contains bytes after its final field.
    #[error("canonical object has trailing bytes")]
    TrailingBytes,
    /// A field tag is not recognized by this version.
    #[error("unknown {field} tag {tag}")]
    UnknownTag {
        /// Stable field description.
        field: &'static str,
        /// Unknown tag.
        tag: u8,
    },
    /// Decoded fields violate a semantic page invariant.
    #[error("canonical object invariant failed: {0}")]
    Invariant(#[source] CanonicalInvariant),
}

#[cfg(test)]
#[path = "tests/codec.rs"]
mod tests;
