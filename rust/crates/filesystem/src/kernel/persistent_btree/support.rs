//! Formats, mutation ordering and page packing for the shared B+tree engine.
use super::{
    AllocationError, CanonicalDecodeError, DecodeLimits, DecodedPageShape, KernelAccounting,
    KernelOwnedAllocation, ObjectId, ObjectKind, ObjectStoreError, Owned, WorkCounters, WorkError,
    retire_allocation, split_allocation, usize_to_u64,
};
use thiserror::Error;
pub(crate) trait Format: acyclic_collections::page::Format<Reference = ObjectId> {
    fn kind() -> ObjectKind;
    /// A stable hash of `key`, which places page boundaries.
    fn key_hash(key: &Self::Key) -> u64;
    fn decode<E: From<CanonicalDecodeError>>(
        bytes: &[u8],
        limits: DecodeLimits,
        observe: &mut impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Page<Self>, E>
    where
        Self: Sized;
    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError>;
    fn page_encoded_length(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
    ) -> Result<usize, CanonicalDecodeError>
    where
        Self: Sized,
    {
        let count = match page {
            PageRef::Leaf(entries) => entries.len(),
            PageRef::Internal(children) => children.len(),
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(CanonicalDecodeError::LengthOverflow);
        }
        let mut bytes = 8_usize + 2 + 1 + 4;
        match page {
            PageRef::Leaf(entries) => {
                for entry in *entries {
                    bytes = bytes
                        .checked_add(Self::leaf_item_encoded_length(entry)?)
                        .ok_or(CanonicalDecodeError::LengthOverflow)?;
                }
            }
            PageRef::Internal(children) => {
                for child in *children {
                    bytes = bytes
                        .checked_add(Self::internal_item_encoded_length(
                            &child.first,
                            child.page,
                        )?)
                        .ok_or(CanonicalDecodeError::LengthOverflow)?;
                }
            }
        }
        Ok(bytes)
    }
    fn leaf_item_encoded_length(value: &Self::Value) -> Result<usize, CanonicalDecodeError>;
    fn internal_item_encoded_length(
        key: &Self::Key,
        page: ObjectId,
    ) -> Result<usize, CanonicalDecodeError>;
    fn key_nested_bytes(key: &Self::Key) -> u64;
    fn value_nested_bytes(value: &Self::Value) -> u64;
    fn key_owned_bytes(key: &Self::Key) -> u64;
    fn value_owned_bytes(value: &Self::Value) -> u64;
    fn try_clone_key<E: From<CanonicalDecodeError>>(
        key: &Self::Key,
        steps: &mut impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Key, E>;
    fn try_clone_value<E: From<CanonicalDecodeError>>(
        value: &Self::Value,
        steps: &mut impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Value, E>;
    fn encode_with_accounting(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError>
    where
        Self: Sized;
}

/// One authenticated child page and its exact lower-bound key.
pub type Child<K> = acyclic_collections::page::Child<K, ObjectId>;

pub(crate) use acyclic_collections::page::{Page, PageRef};

pub(crate) trait Mutation<F: Format> {
    type Error: std::error::Error;

    fn key(&self) -> &F::Key;
    fn changes_cardinality(&self) -> bool;
    /// Whether the original expected/replacement can move an encoded cut.
    fn changes_boundary(&self) -> Result<bool, CanonicalDecodeError> {
        Ok(self.changes_cardinality())
    }
    fn replacement<'a>(
        &'a self,
        current: Option<&F::Value>,
    ) -> Result<Option<&'a F::Value>, Self::Error>;
    fn map_error(error: Error<Self::Error>) -> crate::FsError;

    /// Whether this format's mutations rewrite key ranges through
    /// [`Self::rewrite_range`] rather than replacing single keys.
    const RANGED: bool = false;

    /// The last key this mutation reaches, inclusive, or `None` for every
    /// key after its own. Every page holding a reached key is visited.
    fn last(&self) -> Option<&F::Key> {
        Some(self.key())
    }

    /// The first key of a suffix this mutation removes entirely. Pages wholly
    /// inside that suffix are dropped without being read.
    fn removes_from(&self) -> Option<&F::Key> {
        None
    }

    /// For ranged formats: rewrites one leaf holding `entries`, which tile
    /// `[lower, upper)`, under the mutations routed to it. Items carried in
    /// from the left come first in `output`, so neighbours across a page
    /// boundary can join. Each step admits original work before rewriting. `output` has room for
    /// every carried item, every entry, and three per mutation.
    /// Bounds are the routing lower/upper keys and the original root's optional
    /// exclusive end. That final end validates input without clipping growth.
    fn rewrite_range(
        _carried: &[F::Value],
        _entries: &[F::Value],
        _mutations: &[(usize, &Self)],
        _bounds: (Option<&F::Key>, Option<&F::Key>, Option<&F::Key>),
        _output: &mut Vec<F::Value>,
        _accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError>
    where
        Self: Sized,
    {
        Err(Self::map_error(Error::MutationContract))
    }
}

/// Whether a mutation reaching `last` reaches `key` too.
pub(super) fn reaches<K: Ord>(last: Option<&K>, key: &K) -> bool {
    last.is_none_or(|last| last >= key)
}

#[derive(Debug, Error)]
pub(crate) enum Error<E: std::error::Error> {
    #[error("persistent B+tree mutation batch is empty")]
    Empty,
    #[error("persistent B+tree root has the wrong object kind")]
    WrongRootKind,
    #[error("persistent B+tree limits are invalid")]
    InvalidLimits,
    #[error("one persistent B+tree item cannot fit in an admitted page")]
    PageItemTooLarge,
    #[error("persistent B+tree exceeds its height bound")]
    HeightExceeded,
    #[error("persistent B+tree contains a cycle or alias")]
    CycleOrAlias,
    #[error("persistent B+tree child bounds mismatch")]
    ChildBoundsMismatch,
    #[error("persistent B+tree mutation violated its physical-plan contract")]
    MutationContract,
    #[error("persistent B+tree scratch allocation failed")]
    AllocationFailed,
    #[error(transparent)]
    Allocation(#[from] AllocationError),
    #[error("persistent B+tree semantic mutation failed: {0}")]
    Semantic(E),
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    #[error(transparent)]
    Work(#[from] WorkError),
}

pub(super) fn sort_indexed<F, M>(
    mutations: &mut [(usize, &M)],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError>
where
    F: Format,
    M: Mutation<F>,
{
    let count = mutations.len();
    if count < 2 {
        return Ok(());
    }
    let scan_bound = usize_to_u64(count - 1);
    accounting.admit(WorkCounters {
        items_examined: scan_bound,
        ..WorkCounters::default()
    })?;
    let mut scan_comparisons = 0_u64;
    let mut ordered = true;
    for adjacent in mutations.windows(2) {
        scan_comparisons = scan_comparisons.checked_add(1).ok_or(WorkError::Overflow)?;
        if let [left, right] = adjacent
            && indexed_order::<F, M>(left, right).is_gt()
        {
            ordered = false;
            break;
        }
    }
    accounting.record_completed(WorkCounters {
        items_examined: scan_comparisons,
        ..WorkCounters::default()
    })?;
    if ordered {
        return Ok(());
    }

    let levels = usize::BITS - count.leading_zeros();
    let maximum_comparisons = usize_to_u64(count)
        .checked_mul(u64::from(levels))
        .and_then(|value| value.checked_mul(3))
        .ok_or(WorkError::Overflow)?;
    accounting.admit(WorkCounters {
        items_examined: maximum_comparisons,
        ..WorkCounters::default()
    })?;

    let mut comparisons = 0_u64;
    crate::kernel::sort::heap_sort(
        mutations,
        &mut comparisons,
        |left, right, comparisons| {
            *comparisons = comparisons.checked_add(1).ok_or(WorkError::Overflow)?;
            Ok::<_, WorkError>(indexed_order::<F, M>(left, right))
        },
        |values, left, right, _| {
            values.swap(left, right);
            Ok(())
        },
        |_| Ok(()),
    )?;
    accounting.record_completed(WorkCounters {
        items_examined: comparisons,
        ..WorkCounters::default()
    })?;
    Ok(())
}

pub(super) fn indexed_order<F, M>(left: &(usize, &M), right: &(usize, &M)) -> std::cmp::Ordering
where
    F: Format,
    M: Mutation<F>,
{
    left.1
        .key()
        .cmp(right.1.key())
        .then_with(|| left.0.cmp(&right.0))
}

pub(super) fn validate<F, M>(
    root: Option<ObjectId>,
    mutations: &[M],
    limits: DecodeLimits,
) -> Result<(), Error<M::Error>>
where
    F: Format,
    M: Mutation<F>,
{
    let error = if root.is_some_and(|root| root.kind != F::kind()) {
        Some(Error::WrongRootKind)
    } else if mutations.is_empty() {
        Some(Error::Empty)
    } else if !limits.page_limits_valid(2) {
        Some(Error::InvalidLimits)
    } else {
        None
    };
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) use crate::kernel::file_read::read_store_error as storage_error;

pub(super) fn push_summary<K>(
    output: &mut Owned<Vec<Child<K>>>,
    first: Owned<K>,
    page: ObjectId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    output.ensure_for_push(usize::MAX, accounting)?;
    let Owned {
        value,
        mut allocation,
        bytes,
    } = first;
    output.value.push(Child { first: value, page });
    output.allocation.absorb(&mut allocation)?;
    output.bytes = output.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
    Ok(())
}

pub(super) fn push_value<V>(
    output: &mut Owned<Vec<V>>,
    mut value: Owned<Option<V>>,
) -> Result<(), crate::FsError> {
    let entry = value
        .value
        .take()
        .ok_or(CanonicalDecodeError::AllocationFailed)?;
    output.value.push(entry);
    output.allocation.absorb(&mut value.allocation)?;
    output.bytes = output
        .bytes
        .checked_add(value.bytes)
        .ok_or(WorkError::Overflow)?;
    Ok(())
}

pub(super) fn append_owned<T>(
    output: &mut Owned<Vec<T>>,
    mut source: Owned<Vec<T>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let buffer_bytes = usize_to_u64(
        source
            .value
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(WorkError::Overflow)?,
    );
    while !source.value.is_empty() {
        output.ensure_for_push(usize::MAX, accounting)?;
        let spare = output.value.capacity() - output.value.len();
        let count = spare.min(source.value.len());
        output.value.extend(source.value.drain(..count));
    }
    let buffer_allocation = split_allocation(&mut source.allocation, buffer_bytes)?;
    drop(source.value);
    retire_allocation(buffer_allocation, buffer_bytes, accounting)?;
    output.allocation.absorb(&mut source.allocation)?;
    output.bytes = output
        .bytes
        .checked_add(
            source
                .bytes
                .checked_sub(buffer_bytes)
                .ok_or(WorkError::Overflow)?,
        )
        .ok_or(WorkError::Overflow)?;
    Ok(())
}

/// One authenticated page written, as charged before the backend's work.
pub(super) const PAGE_WRITE: WorkCounters = WorkCounters {
    page_writes: 1,
    ..WorkCounters::UNCHARGED
};

pub(crate) const TARGET_PAGE_BYTES: usize = 2 * 1024;
pub(super) const PAGE_HEADER_BYTES: usize = 8 + 2 + 1 + 4;

pub(crate) use acyclic_collections::page::stable_hash;

#[cfg(test)]
pub(crate) struct PageChunks<'a, T, L, H> {
    inner: acyclic_collections::page::PageChunks<'a, T, L, H, PAGE_HEADER_BYTES, TARGET_PAGE_BYTES>,
}

#[cfg(test)]
impl<'a, T, L, H> PageChunks<'a, T, L, H>
where
    L: Fn(&T) -> Result<usize, CanonicalDecodeError>,
    H: Fn(&T) -> u64,
{
    pub(super) fn new<E: std::error::Error>(
        items: &'a [T],
        limits: DecodeLimits,
        level: u16,
        encoded_length: L,
        key_hash: H,
    ) -> Result<Self, Error<E>> {
        let limits = acyclic_collections::page::PackingLimits {
            maximum_items: usize::try_from(limits.maximum_page_items)
                .map_err(|_| Error::InvalidLimits)?,
            maximum_object_bytes: usize::try_from(limits.maximum_object_bytes)
                .unwrap_or(usize::MAX),
            maximum_bytes: usize::try_from(limits.maximum_page_bytes)
                .map_err(|_| Error::InvalidLimits)?,
        };
        let inner = acyclic_collections::page::PageChunks::new(
            items,
            limits,
            level,
            encoded_length,
            key_hash,
        )
        .map_err(packing_error)?;
        Ok(Self { inner })
    }

    pub(crate) fn next<E: std::error::Error>(
        &mut self,
    ) -> Result<Option<(&'a [T], u64, bool)>, Error<E>> {
        self.inner.next_page(|| Ok(())).map_err(packing_error)
    }
}

fn packing_error<E: std::error::Error>(
    error: acyclic_collections::page::PackingError<CanonicalDecodeError>,
) -> Error<E> {
    match error {
        acyclic_collections::page::PackingError::InvalidLimits => Error::InvalidLimits,
        acyclic_collections::page::PackingError::ItemTooLarge => Error::PageItemTooLarge,
        acyclic_collections::page::PackingError::Encode(error) => Error::Decode(error),
    }
}

pub(crate) fn leaf_within<F: Format>(
    values: &[F::Value],
    lower: Option<&F::Key>,
    upper: Option<&F::Key>,
) -> bool {
    acyclic_collections::page::within_bounds(
        values.first().map(F::key),
        values.last().map(F::key),
        lower,
        upper,
    )
}

pub(crate) fn children_within<K: Ord>(
    children: &[Child<K>],
    lower: Option<&K>,
    upper: Option<&K>,
) -> bool {
    acyclic_collections::page::children_within(children, lower, upper)
}
