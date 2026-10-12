//! Authenticated scalable named attributes, alternate streams, and resource forks.

use super::codec::{
    CanonicalDecodeError, DecodeLimits, DecodedPageKind, DecodedPageShape, Decoder, Encoder, ReadAllocationStep,
};
use super::types::digest_object;
use crate::foundation::usize_to_u64;
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{CancellationToken, WorkBudget, WorkCounters, WorkError};
use thiserror::Error;

const DOMAIN: &[u8; 8] = b"ACYFSATR";
const VERSION: u16 = 1;

/// Source semantics of one named metadata payload.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AttributeClass {
    /// POSIX extended attribute.
    PosixXattr,
    /// Windows alternate data stream.
    WindowsStream,
    /// macOS resource fork or Finder metadata stream.
    MacResourceFork,
}

impl AttributeClass {
    const fn tag(self) -> u8 {
        match self {
            Self::PosixXattr => 1,
            Self::WindowsStream => 2,
            Self::MacResourceFork => 3,
        }
    }

    const fn from_tag(tag: u8) -> Result<Self, AttributeError> {
        match tag {
            1 => Ok(Self::PosixXattr),
            2 => Ok(Self::WindowsStream),
            3 => Ok(Self::MacResourceFork),
            value => Err(AttributeError::UnknownClass(value)),
        }
    }
}

/// Canonical namespace plus raw, separator-independent attribute name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AttributeName {
    class: AttributeClass,
    bytes: Vec<u8>,
}

impl AttributeName {
    /// Validates an exact bounded attribute name.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, or NUL-bearing names.
    pub fn new(
        class: AttributeClass,
        bytes: Vec<u8>,
        maximum_bytes: u32,
    ) -> Result<Self, AttributeError> {
        if bytes.is_empty()
            || bytes.contains(&0)
            || u32::try_from(bytes.len()).unwrap_or(u32::MAX) > maximum_bytes
        {
            return Err(AttributeError::InvalidName);
        }
        Ok(Self { class, bytes })
    }

    pub(crate) fn try_clone_with_steps<E: From<CanonicalDecodeError>>(
        &self,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self, E> {
        use super::codec::ReadAllocationStep;
        let length = self.bytes.len();
        let mut bytes = Vec::new();
        steps(ReadAllocationStep::Allocate { bytes: length as u64, operations: 1 })?;
        if let Err(error) = bytes.try_reserve_exact(length) {
            steps(ReadAllocationStep::ReserveFailed(error))?;
            return Err(CanonicalDecodeError::AllocationFailed.into());
        }
        steps(ReadAllocationStep::Capacity {
            admitted_bytes: length as u64,
            actual_bytes: bytes.capacity() as u64,
        })?;
        steps(ReadAllocationStep::BeforeCopy(length as u64))?;
        bytes.extend_from_slice(&self.bytes);
        steps(ReadAllocationStep::Copied(length as u64))?;
        Self::new(self.class, bytes, maximum_bytes)
            .map_err(|error| E::from(invariant(error)))
    }

    /// Returns source semantics.
    #[must_use]
    pub const fn class(&self) -> AttributeClass {
        self.class
    }

    /// Borrows exact canonical name bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.bytes.capacity()
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// One named immutable payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeEntry {
    /// Canonical attribute name.
    pub name: AttributeName,
    /// Exact payload bytes.
    pub value_bytes: u64,
    /// Authenticated blob-index root.
    pub value: ObjectId,
}

/// One lower-bound-to-page attribute-tree child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeChild {
    /// Exact first name in the child subtree.
    pub first_name: AttributeName,
    /// Authenticated attribute page.
    pub page: ObjectId,
}

/// One authenticated named-attribute B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttributePage {
    /// Strictly ordered named values.
    Leaf(Vec<AttributeEntry>),
    /// Strictly ordered child lower bounds.
    Internal(Vec<AttributeChild>),
}

impl AttributePage {
    fn validate(&self, maximum_items: u32) -> Result<(), AttributeError> {
        let count = match self {
            Self::Leaf(entries) => {
                if entries
                    .windows(2)
                    .any(|pair| matches!(pair, [left, right] if left.name >= right.name))
                    || entries
                        .iter()
                        .any(|entry| entry.value.kind != ObjectKind::Blob)
                {
                    return Err(AttributeError::InvalidPage);
                }
                entries.len()
            }
            Self::Internal(children) => {
                if children.is_empty()
                    || children.windows(2).any(
                        |pair| matches!(pair, [left, right] if left.first_name >= right.first_name),
                    )
                    || children
                        .iter()
                        .any(|child| child.page.kind != ObjectKind::AttributePage)
                {
                    return Err(AttributeError::InvalidPage);
                }
                children.len()
            }
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(AttributeError::TooManyItems);
        }
        Ok(())
    }
}

/// Encodes one canonical attribute page.
///
/// # Errors
///
/// Rejects invalid ordering, object classes, names, or item bounds.
pub fn encode_attribute_page(
    page: &AttributePage,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    match page {
        AttributePage::Leaf(entries) => encode_attribute_leaf_entries(entries, maximum_items),
        AttributePage::Internal(children) => encode_attribute_internal_children(
            children.iter().map(|child| (&child.first_name, child.page)), maximum_items,
        ),
    }
}

pub(crate) fn encode_attribute_leaf_entries(
    entries: &[AttributeEntry],
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    let mut work = WorkCounters::default();
    let mut allocations = super::allocation::AllocationLedger::default();
    encode_attribute_leaf_entries_with_allocations(
        entries, maximum_items, &mut allocations, &mut work, WorkBudget::UNBOUNDED, None,
    ).map_err(|error| error.into_canonical())
}

pub(crate) fn encode_attribute_internal_children<'a, I>(
    children: I,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError>
where
    I: Clone + ExactSizeIterator<Item = (&'a AttributeName, ObjectId)>,
{
    let mut work = WorkCounters::default();
    let mut allocations = super::allocation::AllocationLedger::default();
    encode_attribute_internal_children_with_allocations(
        children, maximum_items, &mut allocations, &mut work, WorkBudget::UNBOUNDED, None,
    ).map_err(|error| error.into_canonical())
}

/// Uses the original invocation ledger; destroy bytes before their returned guard.
pub(crate) fn encode_attribute_page_with_accounting(
    page: &AttributePage,
    maximum_items: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    match page {
        AttributePage::Leaf(entries) => encode_attribute_leaf_entries_with_accounting(entries, maximum_items, accounting),
        AttributePage::Internal(children) => encode_attribute_internal_children_with_accounting(
            children.iter().map(|child| (&child.first_name, child.page)), maximum_items, accounting,
        ),
    }
}

pub(super) fn encode_attribute_leaf_entries_with_accounting(
    entries: &[AttributeEntry],
    maximum_items: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let original = accounting.native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    let original = false;
    let mut funding = super::codec::EncodeAccounting::new(accounting);
    let result = encode_attribute_leaf_entries_with_steps(
        entries, maximum_items, |step| funding.observe(step),
        |source| attribute_encode_error(AttributeEncodeError::AllocationFailed(source), original),
    );
    funding.finish(result)
}

pub(super) fn encode_attribute_internal_children_with_accounting<'a, I>(
    children: I,
    maximum_items: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError>
where
    I: Clone + ExactSizeIterator<Item = (&'a AttributeName, ObjectId)>,
{
    #[cfg(not(target_arch = "wasm32"))]
    let original = accounting.native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    let original = false;
    let mut funding = super::codec::EncodeAccounting::new(accounting);
    let result = encode_attribute_internal_children_with_steps(
        children, maximum_items, |step| funding.observe(step),
        |source| attribute_encode_error(AttributeEncodeError::AllocationFailed(source), original),
    );
    funding.finish(result)
}

pub(super) fn attribute_encode_error(error: AttributeEncodeError, original: bool) -> crate::FsError {
    #[cfg(target_arch = "wasm32")]
    let _ = original;
    match error {
        AttributeEncodeError::Canonical(error) => error.into(),
        AttributeEncodeError::Work(error) => error.into(),
        AttributeEncodeError::Cancelled(error) => error.into(),
        #[cfg(not(target_arch = "wasm32"))]
        AttributeEncodeError::AllocationFailed(error) if original => crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::AllocationFailed(error),
        ),
        AttributeEncodeError::AllocationFailed(_) => CanonicalDecodeError::AllocationFailed.into(),
    }
}

/// Failure from the actual bounded canonical attribute encoder.
#[derive(Debug, Error)]
pub enum AttributeEncodeError {
    /// Canonical validation or allocation failed.
    #[error(transparent)]
    Canonical(#[from] CanonicalDecodeError),
    /// A work admission failed.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// Encoding was cancelled.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
    /// The actual fallible canonical-buffer reservation failed.
    #[error("canonical attribute allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
}

impl AttributeEncodeError {
    fn into_canonical(self) -> CanonicalDecodeError {
        match self {
            Self::Canonical(error) => error,
            Self::AllocationFailed(_) => CanonicalDecodeError::AllocationFailed,
            Self::Work(error) => CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Work(error)),
            Self::Cancelled(_) => CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static("filesystem operation was cancelled")),
        }
    }
}

pub(super) fn encode_attribute_leaf_entries_with_allocations(
    entries: &[AttributeEntry],
    maximum_items: u32,
    allocations: &mut super::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, AttributeEncodeError> {
    let before = allocations.live_bytes();
    let result = encode_attribute_leaf_entries_with_steps(
        entries, maximum_items,
        |step| observe_attribute_encoding(step, allocations, work, budget, cancellation),
        AttributeEncodeError::AllocationFailed,
    );
    if result.is_err() {
        let owned = allocations.live_bytes().checked_sub(before).ok_or(WorkError::Overflow)?;
        allocations.release(owned)
            .map_err(|error| error.into_work_or(AttributeEncodeError::Work(WorkError::Overflow)))?;
    }
    result
}

pub(super) fn encode_attribute_leaf_entries_with_steps<E>(
    entries: &[AttributeEntry],
    maximum_items: u32,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<WorkError>,
{
    use super::codec::EncodeWorkStep;
    observe(EncodeWorkStep::Check)?;
    if count(entries.len())? > maximum_items {
        return Err(invariant(AttributeError::InvalidPage).into());
    }
    let mut length = DOMAIN.len() + 2 + 1 + 4;
    let mut copied = 0_u64;
    let mut prior = None;
    for entry in entries {
        observe(EncodeWorkStep::Check)?;
        let examined = WorkCounters { items_examined: 1, ..WorkCounters::default() };
        observe(EncodeWorkStep::Admit(examined))?;
        let valid = !prior.is_some_and(|prior| prior >= &entry.name) && entry.value.kind == ObjectKind::Blob;
        observe(EncodeWorkStep::Completed(examined))?;
        if !valid {
            return Err(invariant(AttributeError::InvalidPage).into());
        }
        length = length.checked_add(attribute_leaf_item_encoded_length(entry)?)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        copied = copied.checked_add(usize_to_u64(entry.name.as_bytes().len()))
            .and_then(|bytes| bytes.checked_add(32)).ok_or(WorkError::Overflow)?;
        prior = Some(&entry.name);
    }
    observe(EncodeWorkStep::Admit(WorkCounters {
        items_examined: usize_to_u64(entries.len()), bytes_encoded: usize_to_u64(length),
        bytes_copied: copied, ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity_with_steps(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    let header = WorkCounters { bytes_encoded: 5, ..WorkCounters::default() };
    observe(EncodeWorkStep::Admit(header))?;
    encoder.u8(1);
    encoder.u32(count(entries.len())?);
    observe(EncodeWorkStep::Completed(header))?;
    for entry in entries {
        observe(EncodeWorkStep::Check)?;
        let encoded = WorkCounters {
            items_examined: 1, bytes_encoded: usize_to_u64(attribute_leaf_item_encoded_length(entry)?),
            bytes_copied: usize_to_u64(entry.name.as_bytes().len()) + 32, ..WorkCounters::default()
        };
        observe(EncodeWorkStep::Admit(encoded))?;
        encode_name(&mut encoder, &entry.name)?;
        encoder.u64(entry.value_bytes);
        encoder.fixed(entry.value.digest.as_bytes());
        observe(EncodeWorkStep::Completed(encoded))?;
    }
    Ok(encoder.finish())
}

pub(super) fn encode_attribute_internal_children_with_allocations<'a, I>(
    children: I,
    maximum_items: u32,
    allocations: &mut super::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, AttributeEncodeError>
where
    I: Clone + ExactSizeIterator<Item = (&'a AttributeName, ObjectId)>,
{
    let before = allocations.live_bytes();
    let result = encode_attribute_internal_children_with_steps(
        children, maximum_items,
        |step| observe_attribute_encoding(step, allocations, work, budget, cancellation),
        AttributeEncodeError::AllocationFailed,
    );
    if result.is_err() {
        let owned = allocations.live_bytes().checked_sub(before).ok_or(WorkError::Overflow)?;
        allocations.release(owned)
            .map_err(|error| error.into_work_or(AttributeEncodeError::Work(WorkError::Overflow)))?;
    }
    result
}

pub(super) fn encode_attribute_internal_children_with_steps<'a, E, I>(
    children: I,
    maximum_items: u32,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<WorkError>,
    I: Clone + ExactSizeIterator<Item = (&'a AttributeName, ObjectId)>,
{
    use super::codec::EncodeWorkStep;
    observe(EncodeWorkStep::Check)?;
    let item_count = count(children.len())?;
    if item_count == 0 || item_count > maximum_items {
        return Err(invariant(AttributeError::InvalidPage).into());
    }
    let mut length = DOMAIN.len() + 2 + 1 + 4;
    let mut copied = 0_u64;
    let mut prior = None;
    for (name, page) in children.clone() {
        observe(EncodeWorkStep::Check)?;
        let examined = WorkCounters { items_examined: 1, ..WorkCounters::default() };
        observe(EncodeWorkStep::Admit(examined))?;
        let valid = !prior.is_some_and(|prior| prior >= name) && page.kind == ObjectKind::AttributePage;
        observe(EncodeWorkStep::Completed(examined))?;
        if !valid {
            return Err(invariant(AttributeError::InvalidPage).into());
        }
        length = length.checked_add(attribute_internal_item_encoded_length(name)?)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        copied = copied.checked_add(usize_to_u64(name.as_bytes().len()))
            .and_then(|bytes| bytes.checked_add(32)).ok_or(WorkError::Overflow)?;
        prior = Some(name);
    }
    observe(EncodeWorkStep::Admit(WorkCounters {
        items_examined: u64::from(item_count), bytes_encoded: usize_to_u64(length),
        bytes_copied: copied, ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity_with_steps(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    let header = WorkCounters { bytes_encoded: 5, ..WorkCounters::default() };
    observe(EncodeWorkStep::Admit(header))?;
    encoder.u8(2);
    encoder.u32(item_count);
    observe(EncodeWorkStep::Completed(header))?;
    for (name, page) in children {
        observe(EncodeWorkStep::Check)?;
        let encoded = WorkCounters {
            items_examined: 1, bytes_encoded: usize_to_u64(attribute_internal_item_encoded_length(name)?),
            bytes_copied: usize_to_u64(name.as_bytes().len()) + 32, ..WorkCounters::default()
        };
        observe(EncodeWorkStep::Admit(encoded))?;
        encode_name(&mut encoder, name)?;
        encoder.fixed(page.digest.as_bytes());
        observe(EncodeWorkStep::Completed(encoded))?;
    }
    Ok(encoder.finish())
}

fn observe_attribute_encoding(
    step: super::codec::EncodeWorkStep,
    allocations: &mut super::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<(), AttributeEncodeError> {
    use super::codec::{EncodeAllocationStep, EncodeWorkStep};
    let completed = match step {
        EncodeWorkStep::Check => {
            cancellation.map_or(Ok(()), CancellationToken::check)?;
            return Ok(());
        }
        EncodeWorkStep::Admit(delta) => {
            work.admit(&delta, &budget)?;
            return Ok(());
        }
        EncodeWorkStep::Completed(delta) => delta,
        EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
            allocations.claim_elements::<u8>(capacity, work, budget)
                .map_err(|error| error.into_work_or(AttributeEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
            return Ok(());
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
            if actual_bytes > admitted_bytes {
                allocations.record_owned_bytes(actual_bytes - admitted_bytes, work)
                    .map_err(|error| error.into_work_or(AttributeEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
            }
            work.verify(budget)?;
            return Ok(());
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) => {
            work.admit(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() }, &budget)?;
            return Ok(());
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) =>
            WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() },
    };
    work.try_add_assign(&completed)?;
    work.verify(budget)?;
    cancellation.map_or(Ok(()), CancellationToken::check)?;
    Ok(())
}

pub(crate) fn attribute_leaf_page_encoded_length(
    entries: &[AttributeEntry],
    maximum_items: u32,
) -> Result<usize, CanonicalDecodeError> {
    if count(entries.len())? > maximum_items {
        return Err(CanonicalDecodeError::LengthOverflow);
    }
    entries
        .iter()
        .try_fold(DOMAIN.len() + 2 + 1 + 4, |bytes, entry| {
            bytes
                .checked_add(attribute_leaf_item_encoded_length(entry)?)
                .ok_or(CanonicalDecodeError::LengthOverflow)
        })
}

pub(crate) fn attribute_internal_page_encoded_length<'a, I>(
    mut children: I,
    maximum_items: u32,
) -> Result<usize, CanonicalDecodeError>
where
    I: ExactSizeIterator<Item = (&'a AttributeName, ObjectId)>,
{
    if count(children.len())? > maximum_items {
        return Err(CanonicalDecodeError::LengthOverflow);
    }
    children.try_fold(DOMAIN.len() + 2 + 1 + 4, |bytes, (name, _)| {
        bytes
            .checked_add(attribute_internal_item_encoded_length(name)?)
            .ok_or(CanonicalDecodeError::LengthOverflow)
    })
}

pub(crate) fn attribute_leaf_item_encoded_length(entry: &AttributeEntry) -> Result<usize, CanonicalDecodeError> {
    count(entry.name.as_bytes().len())?;
    entry.name.as_bytes().len().checked_add(1 + 4 + 8 + 32)
        .ok_or(CanonicalDecodeError::LengthOverflow)
}

pub(crate) fn attribute_internal_item_encoded_length(name: &AttributeName) -> Result<usize, CanonicalDecodeError> {
    count(name.as_bytes().len())?;
    name.as_bytes().len().checked_add(1 + 4 + 32)
        .ok_or(CanonicalDecodeError::LengthOverflow)
}


pub(crate) fn attribute_page_decode_shape(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<DecodedPageShape, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let mut nested_bytes = 0_u64;
    let kind = match tag {
        1 | 2 => {
            for _ in 0..item_count {
                decoder.u8()?;
                let name_bytes = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
                nested_bytes = nested_bytes
                    .checked_add(usize_to_u64(name_bytes))
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
                if tag == 1 {
                    decoder.u64()?;
                }
                let _: [u8; 32] = decoder.fixed()?;
            }
            if tag == 1 {
                DecodedPageKind::Leaf
            } else {
                DecodedPageKind::Internal
            }
        }
        value => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "attribute_page",
                tag: value,
            });
        }
    };
    decoder.finish()?;
    Ok(DecodedPageShape {
        kind,
        items: usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?,
        nested_bytes,
    })
}

/// Decodes one bounded canonical attribute page.
///
/// # Errors
///
/// Fails closed on malformed bytes, names, tags, ordering, bounds, or trailing data.
pub fn decode_attribute_page(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<AttributePage, CanonicalDecodeError> {
    decode_attribute_page_with_steps(bytes, limits, &mut |_| Ok(()))
}

/// Observes allocation, copy, and completed-item boundaries in the canonical decoder.
pub(crate) fn decode_attribute_page_with_steps<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<AttributePage, E> {
    decode_attribute_page_mapped_with_steps(
        bytes, limits, observe, AttributePage::Leaf, AttributePage::Internal,
        |first_name, page| AttributeChild { first_name, page },
        |child| &child.first_name,
    )
}

// Build the caller's child representation directly in the admitted vector.
pub(crate) fn decode_attribute_page_mapped_with_steps<E, C, P>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    leaf: impl FnOnce(Vec<AttributeEntry>) -> P,
    internal: impl FnOnce(Vec<C>) -> P,
    child: impl Fn(AttributeName, ObjectId) -> C,
    child_name: impl Fn(&C) -> &AttributeName,
) -> Result<P, E>
where
    E: From<CanonicalDecodeError>,
{
    let mut decoder = Decoder::new_with_steps(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), observe)?;
    let tag = decoder.u8_with_steps(observe)?;
    let item_count = decoder.u32_with_steps(observe)?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        }.into());
    }
    let capacity = usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    match tag {
        1 => {
            let mut entries = Vec::new();
            let admitted_bytes = usize_to_u64(capacity.checked_mul(std::mem::size_of::<AttributeEntry>())
                .ok_or(CanonicalDecodeError::LengthOverflow)?);
            observe(ReadAllocationStep::Allocate { bytes: admitted_bytes, operations: u64::from(capacity != 0) })?;
            if let Err(error) = entries.try_reserve_exact(capacity) {
                observe(ReadAllocationStep::ReserveFailed(error))?;
                return Err(CanonicalDecodeError::AllocationFailed.into());
            }
            observe(ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes: usize_to_u64(entries.capacity() * std::mem::size_of::<AttributeEntry>()),
            })?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                entries.push(AttributeEntry {
                    name: decode_name_with_steps(&mut decoder, limits.maximum_name_bytes, observe)?,
                    value_bytes: decoder.u64_with_steps(observe)?,
                    value: digest_object(ObjectKind::Blob, decoder.fixed_with_steps::<32, E>(observe)?),
                });
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            let page = AttributePage::Leaf(entries);
            page.validate(limits.maximum_page_items).map_err(invariant)?;
            let AttributePage::Leaf(entries) = page else { unreachable!() };
            Ok(leaf(entries))
        }
        2 => {
            let mut children = Vec::new();
            let admitted_bytes = usize_to_u64(capacity.checked_mul(std::mem::size_of::<C>())
                .ok_or(CanonicalDecodeError::LengthOverflow)?);
            observe(ReadAllocationStep::Allocate { bytes: admitted_bytes, operations: u64::from(capacity != 0 && std::mem::size_of::<C>() != 0) })?;
            if let Err(error) = children.try_reserve_exact(capacity) {
                observe(ReadAllocationStep::ReserveFailed(error))?;
                return Err(CanonicalDecodeError::AllocationFailed.into());
            }
            observe(ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes: usize_to_u64(children.capacity() * std::mem::size_of::<C>()),
            })?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                let first_name = decode_name_with_steps(&mut decoder, limits.maximum_name_bytes, observe)?;
                let page = digest_object(ObjectKind::AttributePage, decoder.fixed_with_steps::<32, E>(observe)?);
                children.push(child(first_name, page));
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            if children.is_empty()
                || children.windows(2).any(|pair| matches!(pair, [left, right] if child_name(left) >= child_name(right)))
            {
                return Err(invariant(AttributeError::InvalidPage).into());
            }
            // Child object kinds are fixed by the canonical constructor above.
            Ok(internal(children))
        }
        tag => Err(CanonicalDecodeError::UnknownTag { field: "attribute_page", tag }.into()),
    }
}

/// Computes one typed authenticated attribute-page identity.
///
/// # Errors
///
/// Returns the same validation errors as [`encode_attribute_page`].
pub fn attribute_page_id(
    page: &AttributePage,
    maximum_items: u32,
) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_attribute_page(page, maximum_items)?;
    Ok(ObjectId {
        kind: ObjectKind::AttributePage,
        digest: object_digest(ObjectKind::AttributePage, &bytes),
    })
}

/// Named-attribute invariant failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AttributeError {
    /// Attribute class tag is unknown.
    #[error("unknown attribute class {0}")]
    UnknownClass(u8),
    /// Name is empty, oversized, or contains NUL.
    #[error("attribute name is invalid")]
    InvalidName,
    /// Page ordering or referenced object class is invalid.
    #[error("attribute page is invalid")]
    InvalidPage,
    /// Page exceeds its admitted item bound.
    #[error("attribute page exceeds its item bound")]
    TooManyItems,
}

fn encode_name(encoder: &mut Encoder, name: &AttributeName) -> Result<(), CanonicalDecodeError> {
    encoder.u8(name.class.tag());
    encoder.bounded_bytes(name.as_bytes())
}

fn decode_name_with_steps<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    maximum_bytes: u32,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<AttributeName, E> {
    let class = AttributeClass::from_tag(decoder.u8_with_steps(observe)?).map_err(invariant)?;
    let bytes = decoder.bounded_bytes_with_steps(maximum_bytes, observe)?;
    AttributeName::new(class, bytes, maximum_bytes).map_err(|error| invariant(error).into())
}

fn count(value: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(error: AttributeError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Attribute(error))
}

#[cfg(test)]
#[path = "tests/attribute.rs"]
mod tests;
