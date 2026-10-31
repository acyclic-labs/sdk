//! Direct namespace edges, ordered by parent identity and exact canonical name.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::codec::{
    CanonicalDecodeError, CanonicalInvariant, DecodeLimits, DecodedPageKind, DecodedPageShape,
    Decoder, EncodeWorkStep, Encoder, ReadAllocationStep,
};
use super::persistent_btree::{Child, Format, Page, PageRef};
use super::types::LogicalName;
use crate::foundation::{FileId, usize_to_u64};
use crate::storage::{ObjectId, ObjectKind};
use crate::{FsError, WorkCounters};

const DOMAIN: &[u8; 8] = b"ACYFSBND";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 8 + 2 + 1 + 4;

/// One direct namespace edge; it never contains an expanded descendant path.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Binding {
    pub(crate) parent: FileId,
    pub(crate) name: LogicalName,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BindingFormat;
pub(crate) type BindingPage = Page<BindingFormat>;

/// A direct edge edit in the original namespace mutation order.
pub(crate) struct BindingMutation {
    pub(crate) binding: Binding,
    pub(crate) present: bool,
}

impl super::persistent_btree::Mutation<BindingFormat> for BindingMutation {
    type Error = super::TreeMutationError;

    fn key(&self) -> &Binding {
        &self.binding
    }

    fn changes_cardinality(&self) -> bool {
        true
    }

    fn replacement<'a>(
        &'a self,
        current: Option<&Binding>,
    ) -> Result<Option<&'a Binding>, Self::Error> {
        match (self.present, current.is_some()) {
            (true, false) => Ok(Some(&self.binding)),
            (false, true) => Ok(None),
            (true, true) => Err(Self::Error::AlreadyExists),
            (false, false) => Err(Self::Error::Missing),
        }
    }

    fn map_error(error: super::persistent_btree::Error<Self::Error>) -> FsError {
        super::tree_mutation::map_error(error).into()
    }
}

pub(crate) struct FileBindingEdit {
    pub(crate) file_id: FileId,
    order: u32,
    mutation: BindingMutation,
}

impl super::persistent_btree::Mutation<BindingFormat> for FileBindingEdit {
    type Error = super::TreeMutationError;

    fn key(&self) -> &Binding {
        &self.mutation.binding
    }

    fn changes_cardinality(&self) -> bool {
        true
    }

    fn replacement<'a>(
        &'a self,
        current: Option<&Binding>,
    ) -> Result<Option<&'a Binding>, Self::Error> {
        super::persistent_btree::Mutation::replacement(&self.mutation, current)
    }

    fn map_error(error: super::persistent_btree::Error<Self::Error>) -> crate::FsError {
        super::tree_mutation::map_error(error).into()
    }
}

pub(crate) fn push_edit(
    edits: &mut super::persistent_point::Owned<Vec<FileBindingEdit>>,
    file_id: FileId,
    order: u32,
    parent: FileId,
    name: &LogicalName,
    present: bool,
    maximum_name_bytes: u32,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    edits.ensure_for_push(usize::MAX, accounting)?;
    let copied = WorkCounters {
        bytes_copied: 16,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let (name, mut allocation) = super::persistent_io::clone_key::<super::tree_mutation::TreeFormat>(
        name,
        maximum_name_bytes,
        accounting,
        None,
        0,
    )?;
    let bytes = name.logical_owned_bytes();
    edits.value.push(FileBindingEdit {
        file_id,
        order,
        mutation: BindingMutation {
            binding: Binding { parent, name },
            present,
        },
    });
    edits.allocation.absorb(&mut allocation)?;
    edits.bytes = edits
        .bytes
        .checked_add(bytes)
        .ok_or(crate::WorkError::Overflow)?;
    accounting.record_completed(copied)?;
    Ok(())
}

pub(crate) fn sort_edits(
    edits: &mut super::persistent_point::Owned<Vec<FileBindingEdit>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    super::sort::heap_sort(
        &mut edits.value,
        accounting,
        |left, right, accounting| {
            let work = WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(work)?;
            let order = left
                .file_id
                .cmp(&right.file_id)
                .then_with(|| left.order.cmp(&right.order))
                .then_with(|| left.mutation.present.cmp(&right.mutation.present));
            accounting.record_completed(work)?;
            Ok::<_, FsError>(order)
        },
        |values, left, right, _| {
            values.swap(left, right);
            Ok(())
        },
        |accounting| accounting.check(),
    )
}

/// Updates exactly one file's edge set without traversing any other file or directory.
pub(crate) async fn apply_with_accounting<S, M>(
    store: &S,
    root: Option<ObjectId>,
    mutations: &[M],
    maximum_mutations: u32,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<ObjectId, FsError>
where
    S: crate::AsyncObjectStore,
    M: super::persistent_btree::Mutation<BindingFormat>,
{
    Ok(
        super::persistent_btree::apply_optional_root_with_accounting::<S, BindingFormat, M>(
            store,
            root,
            mutations,
            maximum_mutations,
            limits,
            accounting,
            cancellation,
        )
        .await?
        .value,
    )
}

impl acyclic_collections::page::Format for BindingFormat {
    type Key = Binding;
    type Value = Binding;
    type Reference = ObjectId;

    fn key(value: &Binding) -> &Binding {
        value
    }
}

impl Format for BindingFormat {
    fn key_hash(key: &Binding) -> u64 {
        super::persistent_btree::stable_hash(key.parent.as_bytes()).rotate_left(1)
            ^ super::persistent_btree::stable_hash(key.name.as_bytes())
    }

    fn kind() -> ObjectKind {
        ObjectKind::BindingPage
    }

    fn decode<E: From<CanonicalDecodeError>>(
        bytes: &[u8],
        limits: DecodeLimits,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<BindingPage, E> {
        decode_binding_page(bytes, limits, observe)
    }

    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError> {
        let mut observe = |_| Ok::<_, CanonicalDecodeError>(());
        let mut decoder = Decoder::new(
            bytes,
            DOMAIN,
            VERSION,
            limits.maximum_page_object_bytes(),
            &mut observe,
        )?;
        let (tag, count) = decoder.page_header(limits.maximum_page_items, &mut observe)?;
        let kind = page_kind(tag, count)?;
        let mut nested_bytes = 0_u64;
        for _ in 0..count {
            let _: [u8; 16] = decoder.fixed(&mut observe)?;
            let encoding = decoder.u8(&mut observe)?;
            super::types::NameEncoding::from_tag(encoding).map_err(|error| {
                CanonicalDecodeError::Invariant(CanonicalInvariant::Name(error))
            })?;
            let length = decoder.skip_bounded_bytes(limits.maximum_name_bytes)?;
            nested_bytes = nested_bytes
                .checked_add(usize_to_u64(length))
                .ok_or(CanonicalDecodeError::LengthOverflow)?;
            if kind == DecodedPageKind::Internal {
                decoder.object(ObjectKind::BindingPage, &mut observe)?;
            }
        }
        decoder.finish()?;
        Ok(DecodedPageShape {
            kind,
            items: usize::try_from(count).map_err(|_| CanonicalDecodeError::LengthOverflow)?,
            nested_bytes,
        })
    }

    fn leaf_item_encoded_length(value: &Binding) -> Result<usize, CanonicalDecodeError> {
        (16_usize + 1 + 4)
            .checked_add(value.name.as_bytes().len())
            .ok_or(CanonicalDecodeError::LengthOverflow)
    }

    fn internal_item_encoded_length(
        key: &Binding,
        page: ObjectId,
    ) -> Result<usize, CanonicalDecodeError> {
        Self::leaf_item_encoded_length(key)?
            .checked_add(page.encoded_len())
            .ok_or(CanonicalDecodeError::LengthOverflow)
    }

    fn key_nested_bytes(key: &Binding) -> u64 {
        usize_to_u64(key.name.as_bytes().len())
    }

    fn value_nested_bytes(value: &Binding) -> u64 {
        Self::key_nested_bytes(value)
    }

    fn key_owned_bytes(key: &Binding) -> u64 {
        key.name.logical_owned_bytes()
    }

    fn value_owned_bytes(value: &Binding) -> u64 {
        Self::key_owned_bytes(value)
    }

    fn try_clone_key<E: From<CanonicalDecodeError>>(
        key: &Binding,
        maximum_bytes: u32,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Binding, E> {
        let bytes = 16;
        observe(ReadAllocationStep::BeforeCopy(bytes))?;
        let parent = key.parent;
        observe(ReadAllocationStep::Copied(bytes))?;
        Ok(Binding {
            parent,
            name: key.name.try_clone(maximum_bytes, observe)?,
        })
    }

    fn try_clone_value<E: From<CanonicalDecodeError>>(
        value: &Binding,
        maximum_bytes: u32,
        observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Binding, E> {
        Self::try_clone_key(value, maximum_bytes, observe)
    }

    fn encode_with_accounting(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, KernelOwnedAllocation), FsError> {
        super::codec::encode_owned(accounting, |observe, reserve| {
            encode_binding_page(page, maximum_items, observe, reserve)
        })
    }
}

fn page_items<'a>(
    page: &'a PageRef<'_, BindingFormat>,
) -> impl Iterator<Item = (&'a Binding, Option<ObjectId>)> {
    let (leaves, children) = match page {
        PageRef::Leaf(entries) => (Some(*entries), None),
        PageRef::Internal(children) => (None, Some(*children)),
    };
    leaves.into_iter().flatten().map(|key| (key, None)).chain(
        children
            .into_iter()
            .flatten()
            .map(|child| (&child.first, Some(child.page))),
    )
}

fn encode_binding_page<E: From<CanonicalDecodeError>>(
    page: &PageRef<'_, BindingFormat>,
    maximum_items: u32,
    mut observe: impl FnMut(EncodeWorkStep) -> Result<(), E>,
    reserve: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E> {
    observe(EncodeWorkStep::Check)?;
    let (tag, count) = match page {
        PageRef::Leaf(entries) => (1, entries.len()),
        PageRef::Internal(children) => (2, children.len()),
    };
    let count = u32::try_from(count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    page_kind(tag, count)?;
    if count > maximum_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: count,
            maximum: maximum_items,
        }
        .into());
    }
    let mut prior = None;
    let mut length = HEADER_BYTES;
    for (key, child) in page_items(page) {
        observe(EncodeWorkStep::Check)?;
        let examined = WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        };
        observe(EncodeWorkStep::Admit(examined))?;
        let valid = validate_item(prior, key, child);
        observe(EncodeWorkStep::Completed(examined))?;
        valid?;
        let bytes = match child {
            Some(child) => BindingFormat::internal_item_encoded_length(key, child)?,
            None => BindingFormat::leaf_item_encoded_length(key)?,
        };
        length = length
            .checked_add(bytes)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        prior = Some(key);
    }
    let copied = length
        .checked_sub(HEADER_BYTES)
        .and_then(|body| body.checked_sub(usize::try_from(count).ok()?.checked_mul(5)?))
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    observe(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: usize_to_u64(length),
        bytes_copied: usize_to_u64(copied),
        ..WorkCounters::UNCHARGED
    }))?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN,
        VERSION,
        length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        reserve,
    )?;
    encoder.u8(tag);
    encoder.u32(count);
    for (key, child) in page_items(page) {
        encoder.fixed(&key.parent.into_bytes());
        super::tree::encode_name(&mut encoder, &key.name)?;
        if let Some(child) = child {
            encoder.object(child);
        }
    }
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: usize_to_u64(length - 10),
        bytes_copied: usize_to_u64(copied),
        ..WorkCounters::UNCHARGED
    }))?;
    Ok(encoder.finish())
}

fn validate_item(
    prior: Option<&Binding>,
    key: &Binding,
    child: Option<ObjectId>,
) -> Result<(), CanonicalDecodeError> {
    if prior.is_some_and(|prior| prior >= key) {
        return Err(invariant("bindings are not strictly ordered"));
    }
    if child.is_some_and(|child| child.kind != ObjectKind::BindingPage) {
        return Err(invariant("binding child has the wrong object kind"));
    }
    Ok(())
}

fn page_kind(tag: u8, count: u32) -> Result<DecodedPageKind, CanonicalDecodeError> {
    match tag {
        1 => Ok(DecodedPageKind::Leaf),
        2 if count != 0 => Ok(DecodedPageKind::Internal),
        2 => Err(invariant("binding internal page is empty")),
        tag => Err(CanonicalDecodeError::UnknownTag {
            field: "binding_page",
            tag,
        }),
    }
}

fn invariant(reason: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(CanonicalInvariant::Static(reason))
}

pub(super) fn decode_binding_page<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<BindingPage, E> {
    let mut decoder = Decoder::new(
        bytes,
        DOMAIN,
        VERSION,
        limits.maximum_page_object_bytes(),
        observe,
    )?;
    let (tag, count) = decoder.page_header(limits.maximum_page_items, observe)?;
    let kind = page_kind(tag, count)?;
    let capacity = usize::try_from(count).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    let mut leaves = Vec::<Binding>::new();
    let mut children = Vec::<Child<Binding>>::new();
    match kind {
        DecodedPageKind::Leaf => {
            super::file_read::reserve_decoded_items(&mut leaves, capacity, observe)?
        }
        DecodedPageKind::Internal => {
            super::file_read::reserve_decoded_items(&mut children, capacity, observe)?
        }
    }
    for _ in 0..count {
        observe(ReadAllocationStep::BeforeItem)?;
        let key = Binding {
            parent: FileId::from_bytes(decoder.fixed(observe)?),
            name: super::tree::decode_name(&mut decoder, limits.maximum_name_bytes, observe)?,
        };
        match kind {
            DecodedPageKind::Leaf => {
                validate_item(leaves.last(), &key, None)?;
                leaves.push(key);
            }
            DecodedPageKind::Internal => {
                let page = decoder.object(ObjectKind::BindingPage, observe)?;
                validate_item(children.last().map(|child| &child.first), &key, Some(page))?;
                children.push(Child { first: key, page });
            }
        }
        observe(ReadAllocationStep::Item)?;
    }
    decoder.finish()?;
    Ok(match kind {
        DecodedPageKind::Leaf => Page::Leaf(leaves),
        DecodedPageKind::Internal => Page::Internal(children),
    })
}

#[cfg(test)]
#[path = "tests/bindings.rs"]
mod tests;
