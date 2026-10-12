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
pub enum ReadAllocationStep {
    /// Admit owned capacity before reservation.
    Allocate { /// Requested bytes.
        bytes: u64, /// Allocation operations.
        operations: u64 },
    /// Preserve the actual failed reserve cause.
    ReserveFailed(std::collections::TryReserveError),
    /// Reconcile owned physical capacity.
    Capacity { /// Requested bytes.
        admitted_bytes: u64, /// Physical bytes.
        actual_bytes: u64 },
    /// Admit a byte copy.
    BeforeCopy(u64),
    /// Record a completed byte copy.
    Copied(u64),
    /// Admit hashing.
    BeforeHash(u64),
    /// Record completed hashing.
    Hashed(u64),
    /// Admit one decoded item.
    BeforeItem,
    /// Record one completed decoded item.
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
pub enum EncodeAllocationStep {
    /// Admit output capacity before reservation.
    Reserve { /// Requested byte capacity.
        capacity: usize },
    /// Reconcile physical capacity before writing.
    Capacity { /// Requested byte capacity.
        admitted_bytes: u64, /// Physical byte capacity.
        actual_bytes: u64 },
    /// Admit the canonical header copy.
    BeforeHeader { /// Header bytes.
        bytes: u64 },
    /// Record the completed header copy.
    HeaderWritten { /// Header bytes.
        bytes: u64 },
}

/// Work boundaries of the canonical writer body, outside every original-root loan.
pub enum EncodeWorkStep {
    /// Check cancellation before effect.
    Check,
    /// Admit the next effect.
    Admit(crate::WorkCounters),
    /// Record completed physical work.
    Completed(crate::WorkCounters),
    /// Observe the output buffer.
    Allocation(EncodeAllocationStep),
}

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

    pub(crate) fn with_exact_capacity<E: From<CanonicalDecodeError>>(
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
    pub(crate) fn new<E: From<CanonicalDecodeError>>(
        bytes: &'a [u8],
        domain: &[u8],
        expected_version: u16,
        maximum_object_bytes: u64,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::new_recognized_versions(
            bytes, domain, &[expected_version], maximum_object_bytes, observe,
        ).map(|(decoder, _)| decoder)
    }

    pub(crate) fn new_recognized_versions<E: From<CanonicalDecodeError>>(
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

    pub(crate) fn u8<E: From<CanonicalDecodeError>>(
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

    pub(crate) fn u32<E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<u32, E> {
        Ok(u32::from_le_bytes(self.fixed::<4, E>(observe)?))
    }

    pub(crate) fn u64<E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<u64, E> {
        Ok(u64::from_le_bytes(self.fixed::<8, E>(observe)?))
    }

    pub(crate) fn i64<E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<i64, E> {
        Ok(i64::from_le_bytes(self.fixed::<8, E>(observe)?))
    }

    pub(crate) fn fixed<const N: usize, E: From<CanonicalDecodeError>>(
        &mut self, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<[u8; N], E> {
        let source = self.take(N)?;
        let copied = usize_to_u64(N);
        observe(ReadAllocationStep::BeforeCopy(copied))?;
        let value = source.try_into().map_err(|_| CanonicalDecodeError::Truncated)?;
        observe(ReadAllocationStep::Copied(copied))?;
        Ok(value)
    }

    pub(crate) fn bounded_slice<E: From<CanonicalDecodeError>>(
        &mut self,
        maximum: u32,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<&'a [u8], E> {
        let length = self.u32(observe)?;
        if length > maximum {
            return Err(CanonicalDecodeError::FieldTooLarge {
                observed: length,
                maximum,
            }.into());
        }
        let length = usize::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
        self.take(length).map_err(Into::into)
    }

    pub(crate) fn bounded_bytes<E: From<CanonicalDecodeError>>(
        &mut self,
        maximum: u32,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Vec<u8>, E> {
        let source = self.bounded_slice(maximum, observe)?;
        let length = source.len();
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
        let length = self.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
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

/// Canonical encoding admission or reservation failure.
#[derive(Debug, Error)]
pub enum EncodeError {
    /// Invalid canonical object.
    #[error(transparent)]
    Canonical(#[from] CanonicalDecodeError),
    /// Work admission failed.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Encoding was cancelled.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
    /// The actual output reservation failed.
    #[error("canonical output allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
}

impl EncodeError {
    pub(crate) fn into_fs_error(self, original: bool) -> crate::FsError {
        #[cfg(target_arch = "wasm32")]
        let _ = original;
        match self {
            Self::Canonical(error) => error.into(),
            Self::Work(error) => error.into(),
            Self::Cancelled(error) => error.into(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::AllocationFailed(error) if original => crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::AllocationFailed(error)),
            Self::AllocationFailed(_) => CanonicalDecodeError::AllocationFailed.into(),
        }
    }
}

impl From<crate::WorkError> for CanonicalDecodeError {
    fn from(error: crate::WorkError) -> Self { Self::Invariant(CanonicalInvariant::Work(error)) }
}

pub(crate) fn encode_owned(
    accounting: &mut super::allocation::KernelAccounting<'_>,
    operation: impl FnOnce(&mut dyn FnMut(EncodeWorkStep) -> Result<(), crate::FsError>,
        &mut dyn FnMut(std::collections::TryReserveError) -> crate::FsError) -> Result<Vec<u8>, crate::FsError>,
) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let original = accounting.native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    let original = false;
    let mut encoding = EncodeAccounting::new(accounting);
    let result = operation(&mut |step| encoding.observe(step),
        &mut |error| EncodeError::AllocationFailed(error).into_fs_error(original));
    encoding.finish(result)
}

/// Runs a canonical producer with a finite standalone permit and completed work.
pub fn encode<E>(
    budget: crate::WorkBudget, cancellation: Option<&crate::CancellationToken>, live_owned_bytes: u64,
    operation: impl FnOnce(&mut dyn FnMut(EncodeWorkStep) -> Result<(), E>) -> Result<Vec<u8>, E>,
) -> crate::MeasuredResult<crate::OperationReceipt<Vec<u8>>, E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> + From<crate::CancellationError> {
    use crate::WorkCounters;
    let mut work = WorkCounters::UNCHARGED;
    let mut allocations = super::allocation::AllocationLedger::default();
    let result = (|| {
        if let Some(token) = cancellation { token.check()?; }
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| error.into_work_or(E::from(CanonicalDecodeError::AllocationFailed)))?;
        operation(&mut |step| {
            match step {
                EncodeWorkStep::Check => { if let Some(token) = cancellation { token.check()?; } }
                EncodeWorkStep::Admit(delta) => work.admit(&delta, &budget)?,
                EncodeWorkStep::Completed(delta) => {
                    work.try_add_assign(&delta)?;
                    work.verify(budget)?;
                    if let Some(token) = cancellation { token.check()?; }
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
                    allocations.claim_elements::<u8>(capacity, &mut work, budget)
                        .map_err(|error| error.into_work_or(E::from(CanonicalDecodeError::AllocationFailed)))?;
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                    if actual_bytes >= admitted_bytes {
                        allocations.record_owned_bytes(actual_bytes - admitted_bytes, &mut work)
                            .map_err(|error| error.into_work_or(E::from(CanonicalDecodeError::AllocationFailed)))?;
                    } else {
                        allocations.release(admitted_bytes - actual_bytes)
                            .map_err(|error| error.into_work_or(E::from(crate::WorkError::Overflow)))?;
                    }
                    work.verify(budget)?;
                }
                EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) =>
                    work.admit(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::UNCHARGED }, &budget)?,
                EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) => {
                    work.try_add_assign(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::UNCHARGED })?;
                    work.verify(budget)?;
                    if let Some(token) = cancellation { token.check()?; }
                }
            }
            Ok(())
        })
    })();
    result.map(|value| crate::OperationReceipt { value, work })
        .map_err(|error| crate::OperationFailure::new(error, work))
}

pub(crate) fn observe_encoding<E: From<crate::WorkError>>(
    observe: &mut impl FnMut(EncodeWorkStep) -> Result<(), E>,
    step: EncodeWorkStep,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<(), E> {
    let completed = data_custody.and_then(|_| match &step {
        EncodeWorkStep::Completed(delta) => Some(*delta),
        EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { actual_bytes, .. }) => Some(crate::WorkCounters {
            allocation_operations: u64::from(*actual_bytes != 0),
            peak_allocation_bytes: *actual_bytes, ..crate::WorkCounters::default()
        }),
        EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) => Some(crate::WorkCounters {
            bytes_encoded: *bytes, ..crate::WorkCounters::default()
        }),
        _ => None,
    });
    let observed = observe(step);
    let attributed = match (data_custody, completed) {
        (Some(owner), Some(delta)) => owner.record_completed_leaf_work(delta),
        _ => Ok(()),
    };
    observed?;
    attributed.map_err(Into::into)
}
