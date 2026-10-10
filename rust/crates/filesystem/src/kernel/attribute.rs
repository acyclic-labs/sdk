//! Authenticated scalable named attributes, alternate streams, and resource forks.

use super::codec::{
    CanonicalDecodeError, DecodeLimits, DecodedPageKind, DecodedPageShape, Decoder, Encoder,
};
use super::types::digest_object;
use crate::foundation::usize_to_u64;
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
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
    encode_attribute_leaf_entries_engine(entries, maximum_items, WorkBudget::UNBOUNDED, None)
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error.into_canonical())
}

pub(crate) fn encode_attribute_internal_children<'a, I>(
    children: I,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError>
where
    I: Clone + ExactSizeIterator<Item = (&'a AttributeName, ObjectId)>,
{
    encode_attribute_internal_children_engine(children, maximum_items, WorkBudget::UNBOUNDED, None)
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error.into_canonical())
}

/// Encodes one canonical page using actual bounded validation and buffer writes.
///
/// Borrowed names and page storage are caller-owned. Newly authored buffers
/// retained by the caller must be reserved in the nested permit beforehand.
///
/// # Errors
///
/// Returns canonical, cancellation, allocation, or budget failures with spent work.
pub fn encode_attribute_page_measured(
    page: &AttributePage,
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, AttributeEncodeError> {
    match page {
        AttributePage::Leaf(entries) => encode_attribute_leaf_entries_measured(entries, maximum_items, budget, cancellation),
        AttributePage::Internal(children) => encode_attribute_internal_children_measured(
            children.iter().map(|child| (&child.first_name, child.page)),
            maximum_items, budget, cancellation,
        ),
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
}

impl AttributeEncodeError {
    fn into_canonical(self) -> CanonicalDecodeError {
        match self {
            Self::Canonical(error) => error,
            error => CanonicalDecodeError::Invariant(error.to_string()),
        }
    }
}

pub(crate) fn encode_attribute_leaf_entries_measured(
    entries: &[AttributeEntry],
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, AttributeEncodeError> {
    encode_attribute_leaf_entries_engine(entries, maximum_items, budget, Some(cancellation))
}

fn encode_attribute_leaf_entries_engine(
    entries: &[AttributeEntry],
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, AttributeEncodeError> {
    let mut work = WorkCounters::default();
    let result = (|| {
        cancellation.map_or(Ok(()), CancellationToken::check)?;
        if count(entries.len())? > maximum_items {
            return Err(invariant(AttributeError::InvalidPage).into());
        }
        let mut length = DOMAIN.len() + 2 + 1 + 4;
        let mut copied = 0_u64;
        let mut prior = None;
        for entry in entries {
            cancellation.map_or(Ok(()), CancellationToken::check)?;
            work.charge_items(1, &budget)?;
            if prior.is_some_and(|prior| prior >= &entry.name) || entry.value.kind != ObjectKind::Blob {
                return Err(invariant(AttributeError::InvalidPage).into());
            }
            length = length.checked_add(attribute_leaf_item_encoded_length(entry)?)
                .ok_or(CanonicalDecodeError::LengthOverflow)?;
            copied = copied.checked_add(usize_to_u64(entry.name.as_bytes().len()))
                .and_then(|bytes| bytes.checked_add(32)).ok_or(WorkError::Overflow)?;
            prior = Some(&entry.name);
        }
        cancellation.map_or(Ok(()), CancellationToken::check)?;
        work.admit(&WorkCounters { items_examined: usize_to_u64(entries.len()), bytes_encoded: usize_to_u64(length), bytes_copied: copied, ..WorkCounters::default() }, &budget)?;
        let mut encoder = start_attribute_encoder(length, &mut work, budget)?;
        charge_attribute_encoding(&mut work, budget, 1 + 4, 0)?;
        encoder.u8(1);
        encoder.u32(count(entries.len())?);
        for entry in entries {
            cancellation.map_or(Ok(()), CancellationToken::check)?;
            work.charge_items(1, &budget)?;
            let copied = usize_to_u64(entry.name.as_bytes().len()).checked_add(32).ok_or(WorkError::Overflow)?;
            charge_attribute_encoding(&mut work, budget, attribute_leaf_item_encoded_length(entry)?, copied)?;
            encode_name(&mut encoder, &entry.name)?;
            encoder.u64(entry.value_bytes);
            encoder.fixed(entry.value.digest.as_bytes());
        }
        Ok(encoder.finish())
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

pub(crate) fn encode_attribute_internal_children_measured<'a, I>(
    children: I,
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, AttributeEncodeError>
where
    I: Clone + ExactSizeIterator<Item = (&'a AttributeName, ObjectId)>,
{
    encode_attribute_internal_children_engine(children, maximum_items, budget, Some(cancellation))
}

fn encode_attribute_internal_children_engine<'a, I>(
    children: I,
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, AttributeEncodeError>
where
    I: Clone + ExactSizeIterator<Item = (&'a AttributeName, ObjectId)>,
{
    let mut work = WorkCounters::default();
    let result = (|| {
        cancellation.map_or(Ok(()), CancellationToken::check)?;
        let item_count = count(children.len())?;
        if item_count == 0 || item_count > maximum_items {
            return Err(invariant(AttributeError::InvalidPage).into());
        }
        let mut length = DOMAIN.len() + 2 + 1 + 4;
        let mut copied = 0_u64;
        let mut prior = None;
        for (name, page) in children.clone() {
            cancellation.map_or(Ok(()), CancellationToken::check)?;
            work.charge_items(1, &budget)?;
            if prior.is_some_and(|prior| prior >= name) || page.kind != ObjectKind::AttributePage {
                return Err(invariant(AttributeError::InvalidPage).into());
            }
            length = length.checked_add(attribute_internal_item_encoded_length(name)?)
                .ok_or(CanonicalDecodeError::LengthOverflow)?;
            copied = copied.checked_add(usize_to_u64(name.as_bytes().len()))
                .and_then(|bytes| bytes.checked_add(32)).ok_or(WorkError::Overflow)?;
            prior = Some(name);
        }
        cancellation.map_or(Ok(()), CancellationToken::check)?;
        work.admit(&WorkCounters { items_examined: u64::from(item_count), bytes_encoded: usize_to_u64(length), bytes_copied: copied, ..WorkCounters::default() }, &budget)?;
        let mut encoder = start_attribute_encoder(length, &mut work, budget)?;
        charge_attribute_encoding(&mut work, budget, 1 + 4, 0)?;
        encoder.u8(2);
        encoder.u32(item_count);
        for (name, page) in children {
            cancellation.map_or(Ok(()), CancellationToken::check)?;
            work.charge_items(1, &budget)?;
            let copied = usize_to_u64(name.as_bytes().len()).checked_add(32).ok_or(WorkError::Overflow)?;
            charge_attribute_encoding(&mut work, budget, attribute_internal_item_encoded_length(name)?, copied)?;
            encode_name(&mut encoder, name)?;
            encoder.fixed(page.digest.as_bytes());
        }
        Ok(encoder.finish())
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

fn charge_attribute_encoding(
    work: &mut WorkCounters,
    budget: WorkBudget,
    encoded: usize,
    copied: u64,
) -> Result<(), WorkError> {
    work.charge(&WorkCounters { bytes_encoded: usize_to_u64(encoded), bytes_copied: copied, ..WorkCounters::default() }, &budget)
}

fn start_attribute_encoder(
    length: usize,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<Encoder, AttributeEncodeError> {
    let mut allocations = super::allocation::AllocationLedger::default();
    let header = DOMAIN.len() + 2;
    work.admit(&WorkCounters { bytes_encoded: usize_to_u64(header), ..WorkCounters::default() }, &budget)?;
    let admitted = allocations.claim_elements::<u8>(length, work, budget)
        .map_err(|error| error.into_work_or(AttributeEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
    let encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, length)?;
    charge_attribute_encoding(work, budget, header, 0)?;
    let actual = usize_to_u64(encoder.capacity());
    if actual > admitted {
        allocations.claim_bytes(actual - admitted, 0, work, WorkBudget::UNBOUNDED)
            .map_err(|error| error.into_work_or(AttributeEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
        work.verify(budget)?;
    }
    Ok(encoder)
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
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let capacity = usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    let page = match tag {
        1 => {
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(capacity)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                entries.push(AttributeEntry {
                    name: decode_name(&mut decoder, limits.maximum_name_bytes)?,
                    value_bytes: decoder.u64()?,
                    value: digest_object(ObjectKind::Blob, decoder.fixed()?),
                });
            }
            AttributePage::Leaf(entries)
        }
        2 => {
            let mut children = Vec::new();
            children
                .try_reserve_exact(capacity)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                children.push(AttributeChild {
                    first_name: decode_name(&mut decoder, limits.maximum_name_bytes)?,
                    page: digest_object(ObjectKind::AttributePage, decoder.fixed()?),
                });
            }
            AttributePage::Internal(children)
        }
        value => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "attribute_page",
                tag: value,
            });
        }
    };
    decoder.finish()?;
    page.validate(limits.maximum_page_items)
        .map_err(invariant)?;
    Ok(page)
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

fn decode_name(
    decoder: &mut Decoder<'_>,
    maximum_bytes: u32,
) -> Result<AttributeName, CanonicalDecodeError> {
    let class = AttributeClass::from_tag(decoder.u8()?).map_err(invariant)?;
    let bytes = decoder.bounded_bytes(maximum_bytes)?;
    AttributeName::new(class, bytes, maximum_bytes).map_err(invariant)
}

fn count(value: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(error: AttributeError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(error.to_string())
}

#[cfg(test)]
#[path = "tests/attribute.rs"]
mod tests;
