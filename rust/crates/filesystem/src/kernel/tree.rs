//! Canonical authenticated directory-page encoding.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder, ReadAllocationStep, EncodeAllocationStep, EncodeWorkStep, EncodeAccounting};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::types::{
    FileKind, LogicalName, NameEncoding, TreeChild, TreeEntry, TreePage, TreePageError,
    digest_object,
};
use crate::foundation::{FileId, usize_to_u64};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use super::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};
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
/// Returns validation, cancellation, allocation, or budget failures with their prefix;
/// output-reservation failures retain the allocator's original error as their source.
pub fn encode_tree_page_measured(
    page: &TreePage,
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    live_owned_bytes: u64,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, TreePageEncodeError> {
    match page {
        TreePage::Leaf(entries) => encode_tree_leaf_entries_measured(entries, maximum_items, budget, Some(cancellation), live_owned_bytes, None),
        TreePage::Internal(children) => encode_tree_internal_children_measured(
            children.iter().map(|child| (&child.first_name, child.page)),
            maximum_items, budget, Some(cancellation), live_owned_bytes,
        ),
    }
}

/// Failure from finite canonical tree encoding.
#[derive(Debug, Error)]
pub enum TreePageEncodeError {
    /// Invalid canonical page.
    #[error(transparent)]
    Canonical(#[from] CanonicalDecodeError),
    /// The canonical output reservation failed with its actual allocator cause.
    #[error("canonical tree-page output allocation failed")]
    AllocationFailed(#[source] std::collections::TryReserveError),
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
        TreePageEncodeError::AllocationFailed(_) => CanonicalDecodeError::AllocationFailed,
        TreePageEncodeError::Work(error) => CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Work(error)),
        TreePageEncodeError::Cancelled(_) => CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static("filesystem operation was cancelled")),
    })
}

pub(crate) fn encode_tree_leaf_entries(entries: &[TreeEntry], maximum_items: u32) -> Result<Vec<u8>, CanonicalDecodeError> {
    project_encoding(encode_tree_leaf_entries_measured(entries, maximum_items, WorkBudget::UNBOUNDED, None, 0, None))
}

/// Runs the canonical writer on the caller's ledger and returns its actual
/// encoded-buffer capacity custody for native original-work callers.
pub(crate) fn encode_tree_page_with_accounting(
    page: &TreePage,
    maximum_items: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    cancellation.check()?;
    match page {
        TreePage::Leaf(entries) => encode_tree_leaf_entries_with_accounting(
            entries, maximum_items, accounting, None, None,
        ),
        TreePage::Internal(children) => encode_tree_internal_children_with_accounting(
            children.iter().map(|child| (&child.first_name, child.page)), maximum_items, accounting,
        ),
    }
}

/// Borrows leaf entries and encodes outside the original-root mutex.
pub(super) fn encode_tree_leaf_entries_with_accounting(
    entries: &[TreeEntry],
    maximum_items: u32,
    accounting: &mut KernelAccounting<'_>,
    semantic_tail: Option<u8>,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError> {
    let mut encoding = EncodeAccounting::new(accounting);
    let result = encode_tree_leaf_entries_with_steps(
        entries, maximum_items, semantic_tail, data_custody,
        |step| encoding.observe(step),
        crate::FsError::EncodedAllocationFailed,
    );
    encoding.finish(result)
}

/// Borrows internal children; reservation and copies are outside Root loans.
pub(super) fn encode_tree_internal_children_with_accounting<'a, I>(
    children: I,
    maximum_items: u32,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError>
where I: Clone + ExactSizeIterator<Item = (&'a LogicalName, ObjectId)> {
    let mut encoding = EncodeAccounting::new(accounting);
    let result = encode_tree_internal_children_with_steps(
        children, maximum_items, |step| encoding.observe(step),
        crate::FsError::EncodedAllocationFailed,
    );
    encoding.finish(result)
}

pub(super) fn encode_tree_leaf_entries_measured(
    entries: &[TreeEntry],
    maximum_items: u32,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
    live_owned_bytes: u64,
    semantic_tail: Option<u8>,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, TreePageEncodeError> {
    let mut work = WorkCounters::default();
    let mut allocations = AllocationLedger::default();
    let result = (|| {
        if let Some(cancellation) = cancellation { cancellation.check()?; }
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| error.into_work_or(TreePageEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
        encode_tree_leaf_entries_with_allocations(
            entries, maximum_items, &mut allocations, &mut work, budget,
            cancellation, semantic_tail, None,
            TreePageEncodeError::AllocationFailed,
        )
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

/// The canonical leaf encoder on the caller's actual live allocation ledger.
pub(super) fn encode_tree_leaf_entries_with_allocations<E>(
    entries: &[TreeEntry], maximum_items: u32,
    allocations: &mut AllocationLedger, work: &mut WorkCounters, budget: WorkBudget,
    cancellation: Option<&CancellationToken>, semantic_tail: Option<u8>,
    data_custody: Option<&super::ProofDataCustody>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> + From<crate::CancellationError> {
    let before = allocations.live_bytes();
    let result = encode_tree_leaf_entries_with_steps(
        entries, maximum_items, semantic_tail, data_custody,
        |step| observe_tree_standalone(step, allocations, work, budget, cancellation),
        allocation_error,
    );
    if result.is_err() {
        // Any encoder/output has physically dropped before releasing its claim.
        let owned = allocations.live_bytes().checked_sub(before).ok_or(crate::WorkError::Overflow)?;
        allocations.release(owned)
            .map_err(|error| error.into_work_or(E::from(crate::WorkError::Overflow)))?;
    }
    result
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
        encode_tree_internal_children_with_allocations(
            children, maximum_items, &mut allocations, &mut work, budget, cancellation,
            TreePageEncodeError::AllocationFailed,
        )
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

pub(super) fn encode_tree_internal_children_with_allocations<'a, I, E>(
    children: I,
    maximum_items: u32,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    I: Clone + ExactSizeIterator<Item = (&'a LogicalName, ObjectId)>,
    E: From<CanonicalDecodeError> + From<crate::WorkError> + From<crate::CancellationError>,
{
    let before = allocations.live_bytes();
    let result = encode_tree_internal_children_with_steps(
        children, maximum_items,
        |step| observe_tree_standalone(step, allocations, work, budget, cancellation),
        allocation_error,
    );
    if result.is_err() {
        let owned = allocations.live_bytes().checked_sub(before).ok_or(crate::WorkError::Overflow)?;
        allocations.release(owned)
            .map_err(|error| error.into_work_or(E::from(crate::WorkError::Overflow)))?;
    }
    result
}

fn observe_tree_standalone<E>(
    step: EncodeWorkStep,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<(), E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> + From<crate::CancellationError> {
    match step {
        EncodeWorkStep::Check => {
            if let Some(cancellation) = cancellation { cancellation.check()?; }
        }
        EncodeWorkStep::Admit(delta) => work.admit(&delta, &budget)?,
        EncodeWorkStep::Completed(delta) => {
            work.try_add_assign(&delta)?;
            work.verify(budget)?;
            if let Some(cancellation) = cancellation { cancellation.check()?; }
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
            allocations.claim_elements::<u8>(capacity, work, budget)
                .map_err(|error| error.into_work_or(E::from(CanonicalDecodeError::AllocationFailed)))?;
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
            if actual_bytes > admitted_bytes {
                allocations.record_owned_bytes(actual_bytes - admitted_bytes, work)
                    .map_err(|error| error.into_work_or(E::from(CanonicalDecodeError::AllocationFailed)))?;
                work.verify(budget)?;
            }
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) => {
            work.admit(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() }, &budget)?;
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) => {
            work.try_add_assign(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() })?;
            work.verify(budget)?;
            if let Some(cancellation) = cancellation { cancellation.check()?; }
        }
    }
    Ok(())
}

/// Attributes only reached physical completions, including a late refusal.
fn observe_tree_step<E: From<crate::WorkError>>(
    observe: &mut impl FnMut(EncodeWorkStep) -> Result<(), E>,
    step: EncodeWorkStep,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<(), E> {
    let completed = data_custody.and_then(|_| match &step {
        EncodeWorkStep::Completed(delta) => Some(*delta),
        EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { actual_bytes, .. }) => Some(WorkCounters {
            allocation_operations: u64::from(*actual_bytes != 0),
            peak_allocation_bytes: *actual_bytes, ..WorkCounters::default()
        }),
        EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) => Some(WorkCounters {
            bytes_encoded: *bytes, ..WorkCounters::default()
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

fn encode_tree_leaf_entries_with_steps<E>(
    entries: &[TreeEntry],
    maximum_items: u32,
    semantic_tail: Option<u8>,
    data_custody: Option<&super::ProofDataCustody>,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> {
    let mut emit = |step| observe_tree_step(&mut observe, step, data_custody);
    emit(EncodeWorkStep::Check)?;
    let page_length = tree_leaf_encoded_length(entries, maximum_items, &mut emit)?;
    let copied = (page_length - DOMAIN.len() - 2 - 1 - 4 - entries.len() * 6) as u64;
    let length = page_length.checked_add(usize::from(semantic_tail.is_some()))
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    emit(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: length as u64, bytes_copied: copied, ..WorkCounters::default()
    }))?;
    emit(EncodeWorkStep::Check)?;
    let mut encoder = Encoder::with_exact_capacity_with_steps(
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

fn encode_tree_internal_children_with_steps<'a, I, E>(
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
    let mut encoder = Encoder::with_exact_capacity_with_steps(
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
    decode_tree_page_with_steps(bytes, limits, &mut |_| Ok(()))
}

/// Decodes the canonical page while observing actual allocation, copy, and item boundaries.
pub(crate) fn decode_tree_page_with_steps<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<TreePage, E> {
    decode_tree_page_mapped_with_steps(
        bytes, limits, observe, TreePage::Leaf, TreePage::Internal,
        |first_name, page| TreeChild { first_name, page },
        |child| &child.first_name,
    )
}

// Construct destination children in the original parser's reserved vector. In
// particular, persistent reads never allocate a second conversion vector.
pub(crate) fn decode_tree_page_mapped_with_steps<E, C, P>(
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
    let mut decoder = Decoder::new_with_steps(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), observe)?;
    let page_tag = decoder.u8_with_steps(observe)?;
    let item_count = decoder.u32_with_steps(observe)?;
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
                let name = decode_name_with_steps(&mut decoder, limits.maximum_name_bytes, observe)?;
                let file_id = FileId::from_bytes(decoder.fixed_with_steps::<16, E>(observe)?);
                let kind = FileKind::from_tag(decoder.u8_with_steps(observe)?).map_err(invariant)?;
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
                let first_name = decode_name_with_steps(&mut decoder, limits.maximum_name_bytes, observe)?;
                let page = digest_object(ObjectKind::TreePage, decoder.fixed_with_steps::<32, E>(observe)?);
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

fn decode_name_with_steps<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    maximum_name_bytes: u32,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<LogicalName, E> {
    let encoding = NameEncoding::from_tag(decoder.u8_with_steps(observe)?).map_err(invariant)?;
    let bytes = decoder.bounded_bytes_with_steps(maximum_name_bytes, observe)?;
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
