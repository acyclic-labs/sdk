//! Canonical authenticated directory-page encoding.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder, ReadAllocationStep, EncodeAllocationStep, EncodeWorkStep};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::types::{
    FileKind, LogicalName, NameEncoding, TreeChild, TreeEntry, TreePage, TreePageError,
    digest_object};
use crate::foundation::{FileId, usize_to_u64};
use crate::storage::{ObjectId, ObjectKind, object_digest};

use crate::{WorkCounters};

/// Encodes one canonical page with before-effect admission.
pub fn encode_tree_page<E>(
    page: &TreePage, maximum_items: u32,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> {
    match page {
        TreePage::Leaf(entries) => encode_tree_leaf_entries(entries, maximum_items, None, None, &mut observe, allocation_error),
        TreePage::Internal(children) => encode_tree_internal_children(children.iter().map(|child| (&child.first_name, child.page)), maximum_items, observe, allocation_error),
    }
}

const DOMAIN: &[u8; 8] = b"ACYFSTRE";
const VERSION: u16 = 1;
const LEAF_TAG: u8 = 1;
const INTERNAL_TAG: u8 = 2;

pub(crate) fn encode_tree_leaf_entries<E>(
    entries: &[TreeEntry],
    maximum_items: u32,
    semantic_tail: Option<u8>,
    data_custody: Option<&super::ProofDataCustody>,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> {
    let mut emit = |step| super::codec::observe_encoding(&mut observe, step, data_custody);
    emit(EncodeWorkStep::Check)?;
    let page_length = tree_leaf_encoded_length(entries, maximum_items, &mut emit)?;
    let copied = (page_length - DOMAIN.len() - 2 - 1 - 4 - entries.len() * 6) as u64;
    let length = page_length.checked_add(usize::from(semantic_tail.is_some()))
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    emit(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: length as u64, bytes_copied: copied, ..WorkCounters::default()
    }))?;
    emit(EncodeWorkStep::Check)?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN, VERSION, length, |step| emit(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    encoder.u8(LEAF_TAG);
    encoder.u32(entries.len() as u32);
    for entry in entries {
        encode_name(&mut encoder, &entry.name);
        encoder.fixed(&entry.file_id.into_bytes());
        encoder.u8(entry.kind.tag());
    }
    if let Some(tail) = semantic_tail { encoder.u8(tail); }
    emit(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: (length - DOMAIN.len() - 2) as u64,
        bytes_copied: copied, ..WorkCounters::default()
    }))?;
    Ok(encoder.finish())
}

pub(crate) fn encode_tree_internal_children<'a, I, E>(
    children: I,
    maximum_items: u32,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    I: Clone + ExactSizeIterator<Item = (&'a LogicalName, ObjectId)>,
    E: From<CanonicalDecodeError>,
{
    observe(EncodeWorkStep::Check)?;
    let length = tree_internal_encoded_length(children.clone(), maximum_items, &mut observe)?;
    let copied = (length - DOMAIN.len() - 2 - 1 - 4 - children.len() * 5) as u64;
    observe(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: length as u64, bytes_copied: copied, ..WorkCounters::default()
    }))?;
    observe(EncodeWorkStep::Check)?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN, VERSION, length, |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    encoder.u8(INTERNAL_TAG);
    encoder.u32(children.len() as u32);
    for (name, page) in children {
        encode_name(&mut encoder, name);
        encoder.fixed(page.digest.as_bytes());
    }
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: (length - DOMAIN.len() - 2) as u64,
        bytes_copied: copied, ..WorkCounters::default()
    }))?;
    Ok(encoder.finish())
}

fn tree_leaf_encoded_length<E: From<CanonicalDecodeError>>(
    entries: &[TreeEntry],
    maximum_items: u32,
    observe: &mut impl FnMut(EncodeWorkStep) -> Result<(), E>,
) -> Result<usize, E> {
    if count(entries.len())? > maximum_items {
        return Err(invariant(TreePageError::TooManyItems).into());
    }
    let mut length = DOMAIN.len() + 2 + 1 + 4;
    let mut prior: Option<&LogicalName> = None;
    for entry in entries {
        observe(EncodeWorkStep::Check)?;
        let item = WorkCounters { items_examined: 1, ..WorkCounters::default() };
        observe(EncodeWorkStep::Admit(item))?;
        let next = (|| {
            if prior.is_some_and(|name| name >= &entry.name) {
                return Err(invariant(TreePageError::NamesNotStrictlyOrdered));
            }
            count(entry.name.as_bytes().len())?;
            length.checked_add(1 + 4 + 16 + 1)
                .and_then(|value| value.checked_add(entry.name.as_bytes().len()))
                .ok_or(CanonicalDecodeError::LengthOverflow)
        })();
        observe(EncodeWorkStep::Completed(item))?;
        length = next?;
        prior = Some(&entry.name);
    }
    Ok(length)
}

fn tree_internal_encoded_length<'a, I, E>(
    children: I,
    maximum_items: u32,
    observe: &mut impl FnMut(EncodeWorkStep) -> Result<(), E>,
) -> Result<usize, E>
where
    I: ExactSizeIterator<Item = (&'a LogicalName, ObjectId)>,
    E: From<CanonicalDecodeError>,
{
    if children.len() == 0 {
        return Err(invariant(TreePageError::EmptyInternalPage).into());
    }
    if count(children.len())? > maximum_items {
        return Err(invariant(TreePageError::TooManyItems).into());
    }
    let mut length = DOMAIN.len() + 2 + 1 + 4;
    let mut prior: Option<&LogicalName> = None;
    for (name, page) in children {
        observe(EncodeWorkStep::Check)?;
        let item = WorkCounters { items_examined: 1, ..WorkCounters::default() };
        observe(EncodeWorkStep::Admit(item))?;
        let next = (|| {
            if prior.is_some_and(|value| value >= name) {
                return Err(invariant(TreePageError::NamesNotStrictlyOrdered));
            }
            if page.kind != ObjectKind::TreePage {
                return Err(invariant(TreePageError::WrongChildKind));
            }
            count(name.as_bytes().len())?;
            length.checked_add(1 + 4 + 32)
                .and_then(|value| value.checked_add(name.as_bytes().len()))
                .ok_or(CanonicalDecodeError::LengthOverflow)
        })();
        observe(EncodeWorkStep::Completed(item))?;
        length = next?;
        prior = Some(name);
    }
    Ok(length)
}

pub(crate) fn tree_page_decode_shape(
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
        LEAF_TAG => {
            for _ in 0..item_count {
                decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                let name_bytes = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
                nested_bytes = nested_bytes
                    .checked_add(usize_to_u64(name_bytes))
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
                let _: [u8; 16] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
            }
            DecodedPageKind::Leaf
        }
        INTERNAL_TAG => {
            for _ in 0..item_count {
                decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                let name_bytes = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
                nested_bytes = nested_bytes
                    .checked_add(usize_to_u64(name_bytes))
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
                let _: [u8; 32] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
            }
            DecodedPageKind::Internal
        }
        value => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "tree_page",
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

// Construct destination children in the original parser's reserved vector. In
// particular, persistent reads never allocate a second conversion vector.
/// Decodes one canonical object with before-effect admission.
pub fn decode_tree_page<E, C, P>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    leaf: impl FnOnce(Vec<TreeEntry>) -> P,
    internal: impl FnOnce(Vec<C>) -> P,
    child: impl Fn(LogicalName, ObjectId) -> C,
    child_name: impl Fn(&C) -> &LogicalName,
) -> Result<P, E>
where
    E: From<CanonicalDecodeError>,
{
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), observe)?;
    let page_tag = decoder.u8(observe)?;
    let item_count = decoder.u32(observe)?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        }.into());
    }
    let capacity = usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    match page_tag {
        LEAF_TAG => {
            let mut entries = Vec::new();
            let admitted_bytes = usize_to_u64(capacity.checked_mul(std::mem::size_of::<TreeEntry>())
                .ok_or(CanonicalDecodeError::LengthOverflow)?);
            observe(ReadAllocationStep::Allocate { bytes: admitted_bytes, operations: u64::from(capacity != 0) })?;
            if let Err(error) = entries.try_reserve_exact(capacity) {
                observe(ReadAllocationStep::ReserveFailed(error))?;
                return Err(CanonicalDecodeError::AllocationFailed.into());
            }
            observe(ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes: usize_to_u64(entries.capacity() * std::mem::size_of::<TreeEntry>()),
            })?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                let name = decode_name(&mut decoder, limits.maximum_name_bytes, observe)?;
                let file_id = FileId::from_bytes(decoder.fixed::<16, E>(observe)?);
                let kind = FileKind::from_tag(decoder.u8(observe)?).map_err(invariant)?;
                entries.push(TreeEntry { name, file_id, kind });
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            let page = TreePage::Leaf(entries);
            page.validate(limits.maximum_page_items).map_err(invariant)?;
            let TreePage::Leaf(entries) = page else { unreachable!() };
            Ok(leaf(entries))
        }
        INTERNAL_TAG => {
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
                let first_name = decode_name(&mut decoder, limits.maximum_name_bytes, observe)?;
                let page = digest_object(ObjectKind::TreePage, decoder.fixed::<32, E>(observe)?);
                children.push(child(first_name, page));
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            if children.is_empty() {
                return Err(invariant(TreePageError::EmptyInternalPage).into());
            }
            if children.windows(2).any(|pair| matches!(pair, [left, right] if child_name(left) >= child_name(right))) {
                return Err(invariant(TreePageError::NamesNotStrictlyOrdered).into());
            }
            // Every child identity above is constructed with ObjectKind::TreePage.
            Ok(internal(children))
        }
        tag => Err(CanonicalDecodeError::UnknownTag { field: "tree_page", tag }.into()),
    }
}

/// Computes the typed authenticated identity of one validated tree page.
pub fn tree_page_id(page: &TreePage, maximum_items: u32) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_tree_page(page, maximum_items, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    Ok(ObjectId {
        kind: ObjectKind::TreePage,
        digest: object_digest(ObjectKind::TreePage, &bytes),
    })
}

fn encode_name(encoder: &mut Encoder, name: &LogicalName) {
    encoder.u8(name.encoding().tag());
    encoder.u32(name.as_bytes().len() as u32);
    encoder.fixed(name.as_bytes());
}

fn decode_name<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    maximum_name_bytes: u32,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<LogicalName, E> {
    let encoding = NameEncoding::from_tag(decoder.u8(observe)?).map_err(invariant)?;
    let bytes = decoder.bounded_bytes(maximum_name_bytes, observe)?;
    LogicalName::new(encoding, bytes, maximum_name_bytes).map_err(|error| invariant(error).into())
}

fn count(length: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(error: TreePageError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Tree(error))
}

#[cfg(test)]
#[path = "tests/tree.rs"]
mod tests;
