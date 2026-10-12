//! Authenticated scalable named attributes, alternate streams, and resource forks.

use crate::kernel::Child;
use super::codec::{
    CanonicalDecodeError, DecodeLimits, DecodedPageKind, DecodedPageShape, Decoder, Encoder, ReadAllocationStep};
use super::types::digest_object;
use crate::foundation::usize_to_u64;
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{WorkCounters, WorkError};
use thiserror::Error;

/// Encodes one canonical page with before-effect admission.
pub fn encode_attribute_page<E>(
    page: &AttributePage, maximum_items: u32,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> {
    match page {
        AttributePage::Leaf(entries) => encode_attribute_leaf_entries(entries, maximum_items, &mut observe, allocation_error),
        AttributePage::Internal(children) => encode_attribute_internal_children(children.iter().map(|child| (&child.first, child.page)), maximum_items, observe, allocation_error),
    }
}

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

    pub(crate) fn try_clone<E: From<CanonicalDecodeError>>(
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

/// One authenticated named-attribute B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttributePage {
    /// Strictly ordered named values.
    Leaf(Vec<AttributeEntry>),
    /// Strictly ordered child lower bounds.
    Internal(Vec<Child<AttributeName>>),
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
                        |pair| matches!(pair, [left, right] if left.first >= right.first),
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

pub(crate) fn encode_attribute_leaf_entries<E>(
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
    let mut encoder = Encoder::with_exact_capacity(
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

pub(crate) fn encode_attribute_internal_children<'a, E, I>(
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
    let mut encoder = Encoder::with_exact_capacity(
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
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    let tag = decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    let item_count = decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
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
                decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                let name_bytes = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
                nested_bytes = nested_bytes
                    .checked_add(usize_to_u64(name_bytes))
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
                if tag == 1 {
                    decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                }
                let _: [u8; 32] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
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

/// Decodes one canonical object with before-effect admission.
pub fn decode_attribute_page<E>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<AttributePage, E>
where
    E: From<CanonicalDecodeError>,
{
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), observe)?;
    let tag = decoder.u8(observe)?;
    let item_count = decoder.u32(observe)?;
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
                    name: decode_name(&mut decoder, limits.maximum_name_bytes, observe)?,
                    value_bytes: decoder.u64(observe)?,
                    value: digest_object(ObjectKind::Blob, decoder.fixed::<32, E>(observe)?),
                });
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            let page = AttributePage::Leaf(entries);
            page.validate(limits.maximum_page_items).map_err(invariant)?;
            Ok(page)
        }
        2 => {
            let mut children = Vec::new();
            let admitted_bytes = usize_to_u64(capacity.checked_mul(std::mem::size_of::<Child<AttributeName>>())
                .ok_or(CanonicalDecodeError::LengthOverflow)?);
            observe(ReadAllocationStep::Allocate { bytes: admitted_bytes, operations: u64::from(capacity != 0) })?;
            if let Err(error) = children.try_reserve_exact(capacity) {
                observe(ReadAllocationStep::ReserveFailed(error))?;
                return Err(CanonicalDecodeError::AllocationFailed.into());
            }
            observe(ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes: usize_to_u64(children.capacity() * std::mem::size_of::<Child<AttributeName>>()),
            })?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                let first = decode_name(&mut decoder, limits.maximum_name_bytes, observe)?;
                let page = digest_object(ObjectKind::AttributePage, decoder.fixed::<32, E>(observe)?);
                children.push(Child { first, page });
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            if children.is_empty()
                || children.windows(2).any(|pair| matches!(pair, [left, right] if left.first >= right.first))
            {
                return Err(invariant(AttributeError::InvalidPage).into());
            }
            // Child object kinds are fixed by the canonical constructor above.
            Ok(AttributePage::Internal(children))
        }
        tag => Err(CanonicalDecodeError::UnknownTag { field: "attribute_page", tag }.into()),
    }
}

/// Computes one typed authenticated attribute-page identity.
pub fn attribute_page_id(
    page: &AttributePage,
    maximum_items: u32,
) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_attribute_page(page, maximum_items, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
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

fn decode_name<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    maximum_bytes: u32,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<AttributeName, E> {
    let class = AttributeClass::from_tag(decoder.u8(observe)?).map_err(invariant)?;
    let bytes = decoder.bounded_bytes(maximum_bytes, observe)?;
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
