//! Canonical authenticated directory-page encoding.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::types::{
    FileKind, LogicalName, NameEncoding, TreeChild, TreeEntry, TreePage, TreePageError,
    digest_object,
};
use crate::foundation::{FileId, usize_to_u64};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use super::allocation::AllocationLedger;
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use thiserror::Error;

const DOMAIN: &[u8; 8] = b"ACYFSTRE";
const VERSION: u16 = 1;
const LEAF_TAG: u8 = 1;
const INTERNAL_TAG: u8 = 2;

/// Encodes one validated tree page into its stable versioned bytes.
///
/// # Errors
///
/// Rejects invalid ordering, bounds, or unrepresentable lengths.
pub fn encode_tree_page(
    page: &TreePage,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    match page {
        TreePage::Leaf(entries) => encode_tree_leaf_entries(entries, maximum_items),
        TreePage::Internal(children) => encode_tree_internal_children(
            children.iter().map(|child| (&child.first_name, child.page)), maximum_items,
        ),
    }
}

/// Encodes a canonical tree page with finite admission and completed-work receipts.
/// `live_owned_bytes` is the capacity retained by the containing operation.
/// Borrowed entries and names are not cloned. Copies count name payloads and
/// identities/digests, while tags, lengths, and the static domain count as encoding.
/// The ordinary encoder projects this same engine with an unbounded permit.
///
/// # Errors
/// Returns validation, cancellation, allocation, or budget failures with their prefix.
pub fn encode_tree_page_measured(
    page: &TreePage,
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    live_owned_bytes: u64,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, TreePageEncodeError> {
    match page {
        TreePage::Leaf(entries) => encode_tree_leaf_entries_measured(entries, maximum_items, budget, Some(cancellation), live_owned_bytes),
        TreePage::Internal(children) => encode_tree_internal_children_measured(
            children.iter().map(|child| (&child.first_name, child.page)),
            maximum_items, budget, Some(cancellation), live_owned_bytes,
        ),
    }
}

/// Failure from finite canonical tree encoding.
#[derive(Debug, Error)]
pub enum TreePageEncodeError {
    /// Invalid canonical page or failed owned-buffer allocation.
    #[error(transparent)]
    Canonical(#[from] CanonicalDecodeError),
    /// The actual operation exceeded its permit.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// The operation was cancelled.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
}

fn project_encoding(result: MeasuredResult<OperationReceipt<Vec<u8>>, TreePageEncodeError>) -> Result<Vec<u8>, CanonicalDecodeError> {
    result.map(|receipt| receipt.value).map_err(|failure| match failure.error {
        TreePageEncodeError::Canonical(error) => error,
        TreePageEncodeError::Work(_) => CanonicalDecodeError::LengthOverflow,
        TreePageEncodeError::Cancelled(_) => unreachable!("unobserved cancellation"),
    })
}

pub(crate) fn encode_tree_leaf_entries(entries: &[TreeEntry], maximum_items: u32) -> Result<Vec<u8>, CanonicalDecodeError> {
    project_encoding(encode_tree_leaf_entries_measured(entries, maximum_items, WorkBudget::UNBOUNDED, None, 0))
}

fn encode_tree_leaf_entries_measured(
    entries: &[TreeEntry],
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
    live_owned_bytes: u64,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, TreePageEncodeError> {
    let mut work = WorkCounters::default();
    let mut allocations = AllocationLedger::default();
    let result = (|| {
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| error.into_work_or(TreePageEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
        let length = tree_leaf_encoded_length(entries, maximum_items, &mut work, budget, cancellation)?;
        let copied = (length - DOMAIN.len() - 2 - 1 - 4 - entries.len() * 6) as u64;
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        let mut encoder = admit_tree_encoder(length, copied, &mut allocations, &mut work, budget)?;
        encoder.u8(LEAF_TAG);
        encoder.u32(entries.len() as u32);
        for entry in entries {
            encode_name(&mut encoder, &entry.name);
            encoder.fixed(&entry.file_id.into_bytes());
            encoder.u8(entry.kind.tag());
        }
        finish_tree_encoder(encoder, length, copied, &mut work, budget)
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

pub(crate) fn encode_tree_internal_children<'a, I>(children: I, maximum_items: u32) -> Result<Vec<u8>, CanonicalDecodeError>
where I: Clone + ExactSizeIterator<Item = (&'a LogicalName, ObjectId)> {
    project_encoding(encode_tree_internal_children_measured(children, maximum_items, WorkBudget::UNBOUNDED, None, 0))
}

fn encode_tree_internal_children_measured<'a, I>(
    children: I,
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
    live_owned_bytes: u64,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, TreePageEncodeError>
where I: Clone + ExactSizeIterator<Item = (&'a LogicalName, ObjectId)> {
    let mut work = WorkCounters::default();
    let mut allocations = AllocationLedger::default();
    let result = (|| {
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| error.into_work_or(TreePageEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
        let length = tree_internal_encoded_length(children.clone(), maximum_items, &mut work, budget, cancellation)?;
        let copied = (length - DOMAIN.len() - 2 - 1 - 4 - children.len() * 5) as u64;
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        let mut encoder = admit_tree_encoder(length, copied, &mut allocations, &mut work, budget)?;
        encoder.u8(INTERNAL_TAG);
        encoder.u32(children.len() as u32);
        for (name, page) in children {
            encode_name(&mut encoder, name);
            encoder.fixed(page.digest.as_bytes());
        }
        finish_tree_encoder(encoder, length, copied, &mut work, budget)
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

fn tree_leaf_encoded_length(
    entries: &[TreeEntry],
    maximum_items: u32,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<usize, TreePageEncodeError> {
    if count(entries.len())? > maximum_items {
        return Err(invariant(TreePageError::TooManyItems).into());
    }
    let mut length = DOMAIN.len() + 2 + 1 + 4;
    let mut prior: Option<&LogicalName> = None;
    for entry in entries {
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        work.charge_items(1, &budget)?;
        if prior.is_some_and(|name| name >= &entry.name) {
            return Err(invariant(TreePageError::NamesNotStrictlyOrdered).into());
        }
        count(entry.name.as_bytes().len())?;
        length = length.checked_add(1 + 4 + 16 + 1)
            .and_then(|value| value.checked_add(entry.name.as_bytes().len()))
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        prior = Some(&entry.name);
    }
    Ok(length)
}

fn tree_internal_encoded_length<'a, I>(
    children: I,
    maximum_items: u32,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<usize, TreePageEncodeError>
where I: ExactSizeIterator<Item = (&'a LogicalName, ObjectId)> {
    if children.len() == 0 {
        return Err(invariant(TreePageError::EmptyInternalPage).into());
    }
    if count(children.len())? > maximum_items {
        return Err(invariant(TreePageError::TooManyItems).into());
    }
    let mut length = DOMAIN.len() + 2 + 1 + 4;
    let mut prior: Option<&LogicalName> = None;
    for (name, page) in children {
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        work.charge_items(1, &budget)?;
        if prior.is_some_and(|value| value >= name) {
            return Err(invariant(TreePageError::NamesNotStrictlyOrdered).into());
        }
        if page.kind != ObjectKind::TreePage {
            return Err(invariant(TreePageError::WrongChildKind).into());
        }
        count(name.as_bytes().len())?;
        length = length.checked_add(1 + 4 + 32)
            .and_then(|value| value.checked_add(name.as_bytes().len()))
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        prior = Some(name);
    }
    Ok(length)
}

fn admit_tree_encoder(length: usize, copied: u64, allocations: &mut AllocationLedger, work: &mut WorkCounters, budget: WorkBudget) -> Result<Encoder, TreePageEncodeError> {
    work.admit(&WorkCounters { bytes_encoded: length as u64, bytes_copied: copied, ..WorkCounters::default() }, &budget)?;
    let admitted = allocations.claim_elements::<u8>(length, work, budget)
        .map_err(|error| error.into_work_or(TreePageEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
    let encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, length)?;
    let header_bytes = (DOMAIN.len() + 2) as u64;
    work.charge(&WorkCounters { bytes_encoded: header_bytes, ..WorkCounters::default() }, &budget)?;
    if encoder.capacity() as u64 > admitted {
        // Allocator excess is already physically live; record it before refusal.
        allocations.claim_bytes(encoder.capacity() as u64 - admitted, 0, work, WorkBudget::UNBOUNDED)
            .map_err(|error| error.into_work_or(TreePageEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
        work.verify(budget)?;
    }
    Ok(encoder)
}

fn finish_tree_encoder(encoder: Encoder, length: usize, copied: u64, work: &mut WorkCounters, budget: WorkBudget) -> Result<Vec<u8>, TreePageEncodeError> {
    let body_bytes = (length - DOMAIN.len() - 2) as u64;
    work.charge(&WorkCounters { bytes_encoded: body_bytes, bytes_copied: copied, ..WorkCounters::default() }, &budget)?;
    Ok(encoder.finish())
}

pub(crate) fn tree_page_decode_shape(
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
        LEAF_TAG => {
            for _ in 0..item_count {
                decoder.u8()?;
                let name_bytes = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
                nested_bytes = nested_bytes
                    .checked_add(usize_to_u64(name_bytes))
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
                let _: [u8; 16] = decoder.fixed()?;
                decoder.u8()?;
            }
            DecodedPageKind::Leaf
        }
        INTERNAL_TAG => {
            for _ in 0..item_count {
                decoder.u8()?;
                let name_bytes = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
                nested_bytes = nested_bytes
                    .checked_add(usize_to_u64(name_bytes))
                    .ok_or(CanonicalDecodeError::LengthOverflow)?;
                let _: [u8; 32] = decoder.fixed()?;
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

/// Decodes and semantically validates one canonical tree page.
///
/// # Errors
///
/// Fails closed on unknown versions/tags, trailing/truncated bytes, allocation
/// bounds, malformed names, ordering, duplicate names, or invalid children.
pub fn decode_tree_page(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<TreePage, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let page_tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let capacity = usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    let page = match page_tag {
        LEAF_TAG => {
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(capacity)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                let name = decode_name(&mut decoder, limits.maximum_name_bytes)?;
                let file_id = FileId::from_bytes(decoder.fixed()?);
                let kind = FileKind::from_tag(decoder.u8()?).map_err(invariant)?;
                entries.push(TreeEntry {
                    name,
                    file_id,
                    kind,
                });
            }
            TreePage::Leaf(entries)
        }
        INTERNAL_TAG => {
            let mut children = Vec::new();
            children
                .try_reserve_exact(capacity)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                let first_name = decode_name(&mut decoder, limits.maximum_name_bytes)?;
                let page = digest_object(ObjectKind::TreePage, decoder.fixed()?);
                children.push(TreeChild { first_name, page });
            }
            TreePage::Internal(children)
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "tree_page",
                tag,
            });
        }
    };
    decoder.finish()?;
    page.validate(limits.maximum_page_items)
        .map_err(invariant)?;
    Ok(page)
}

/// Computes the typed authenticated identity of one validated tree page.
///
/// # Errors
///
/// Returns the same validation failures as [`encode_tree_page`].
pub fn tree_page_id(page: &TreePage, maximum_items: u32) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_tree_page(page, maximum_items)?;
    Ok(ObjectId {
        kind: ObjectKind::TreePage,
        digest: object_digest(ObjectKind::TreePage, &bytes),
    })
}

/// The shared validation/length pass has already checked the name's u32 bound.
fn encode_name(encoder: &mut Encoder, name: &LogicalName) {
    encoder.u8(name.encoding().tag());
    encoder.u32(name.as_bytes().len() as u32);
    encoder.fixed(name.as_bytes());
}

fn decode_name(
    decoder: &mut Decoder<'_>,
    maximum_name_bytes: u32,
) -> Result<LogicalName, CanonicalDecodeError> {
    let encoding = NameEncoding::from_tag(decoder.u8()?).map_err(invariant)?;
    let bytes = decoder.bounded_bytes(maximum_name_bytes)?;
    LogicalName::new(encoding, bytes, maximum_name_bytes).map_err(invariant)
}

fn count(length: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(error: TreePageError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(error.to_string())
}

#[cfg(test)]
#[path = "tests/tree.rs"]
mod tests;
