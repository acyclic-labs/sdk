//! Immutable ordered page shapes and bounded packing shared by storage engines.
//!
//! Page references, codecs, admission and publication belong to the caller.
//! Packing borrows the original admitted entries, allocates nothing and returns
//! only contiguous nonempty slices. It never reads another page or a backend.

pub mod merge;
pub mod publish;
pub mod rewrite;
pub mod selection;

/// One child page and the exact first key in its subtree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Child<K, P> {
    /// Exact first key represented by the child subtree.
    pub first: K,
    /// The caller's complete page reference.
    pub page: P,
}

/// An immutable decoded ordered page, retaining the caller's value types.
#[derive(Debug, Eq, PartialEq)]
pub enum Page<F: Format> {
    /// Ordered leaf values.
    Leaf(Vec<F::Value>),
    /// Ordered children with exact subtree lower bounds.
    Internal(Vec<Child<F::Key, F::Reference>>),
}

impl<F: Format> Clone for Page<F> {
    fn clone(&self) -> Self {
        match self {
            Self::Leaf(values) => Self::Leaf(values.clone()),
            Self::Internal(children) => Self::Internal(children.clone()),
        }
    }
}

/// The caller's ordered values and complete page-reference identity.
/// Codecs and original admission remain with the storage engine.
pub trait Format: Send + Sync + 'static {
    /// Ordered leaf and child-bound key.
    type Key: Clone + Eq + Ord + Send + Sync + 'static;
    /// One original leaf value.
    type Value: Clone + Eq + Send + Sync + 'static;
    /// The original complete reference, never a truncated or reconstructed ID.
    type Reference: Copy + Eq + Send + Sync + 'static;
    /// Borrows the ordering key from an original leaf value.
    fn key(value: &Self::Value) -> &Self::Key;
}

/// A borrowed page to encode without copying its entries.
pub enum PageRef<'a, F: Format> {
    /// Ordered leaf values.
    Leaf(&'a [F::Value]),
    /// Ordered children with exact subtree lower bounds.
    Internal(&'a [Child<F::Key, F::Reference>]),
}

/// The existing caller's page admission bounds.
#[derive(Clone, Copy)]
pub struct PackingLimits {
    /// Maximum entries admitted in one page.
    pub maximum_items: usize,
    /// Maximum complete encoded page size, including its header.
    pub maximum_bytes: usize,
}

/// A packing refusal before a page is handed to its encoder.
#[derive(Debug, Eq, PartialEq)]
pub enum PackingError<E> {
    /// A zero packing target cannot define a partition.
    InvalidLimits,
    /// One item or the accumulated encoded length exceeds the page bound.
    ItemTooLarge,
    /// The caller's original encoder length error.
    Encode(E),
}

impl<E: std::fmt::Display> std::fmt::Display for PackingError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimits => f.write_str("page packing target must be nonzero"),
            Self::ItemTooLarge => f.write_str("item cannot fit in an admitted page"),
            Self::Encode(error) => error.fmt(f),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for PackingError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::InvalidLimits | Self::ItemTooLarge => None,
        }
    }
}

/// A contiguous page partition over the original admitted entries.
///
/// Construction measures each entry once. Each successful step consumes at
/// least one entry; all steps together examine at most twice the entries plus
/// one boundary candidate per page. Returned visit counts let the original
/// caller account for its work without copying an allowance into this engine.
pub struct PageChunks<'a, T, L, const HEADER_BYTES: usize, const TARGET_BYTES: usize> {
    items: &'a [T],
    encoded_length: L,
    start: usize,
    remaining_bytes: usize,
    limits: PackingLimits,
}

impl<'a, T, L, E, const HEADER_BYTES: usize, const TARGET_BYTES: usize>
    PageChunks<'a, T, L, HEADER_BYTES, TARGET_BYTES>
where
    L: Fn(&T) -> Result<usize, E>,
{
    /// Measures the original entries and returns the number examined.
    /// The codec's header size and nonzero soft target are compile-time
    /// parameters, so the cursor retains no per-operation layout metadata.
    pub fn new(
        items: &'a [T],
        limits: PackingLimits,
        encoded_length: L,
    ) -> Result<(Self, u64), PackingError<E>> {
        if TARGET_BYTES == 0 {
            return Err(PackingError::InvalidLimits);
        }
        let mut remaining_bytes = 0_usize;
        for item in items {
            remaining_bytes = remaining_bytes
                .checked_add(encoded_length(item).map_err(PackingError::Encode)?)
                .ok_or(PackingError::ItemTooLarge)?;
        }
        Ok((
            Self {
                items,
                encoded_length,
                start: 0,
                remaining_bytes,
                limits,
            },
            items.len() as u64,
        ))
    }

    /// Returns the next nonempty page and its entry count, or a typed refusal.
    /// Exhaustion performs no encoder work and returns `None` permanently.
    pub fn next_page(&mut self) -> Result<Option<(&'a [T], u64)>, PackingError<E>> {
        let Some(rest) = self.items.get(self.start..).filter(|rest| !rest.is_empty()) else {
            return Ok(None);
        };
        let pages = self
            .remaining_bytes
            .div_ceil(TARGET_BYTES)
            .max(rest.len().div_ceil(self.limits.maximum_items.max(1)))
            .max(1);
        let share = self.remaining_bytes.div_ceil(pages);
        let mut count = 0_usize;
        let mut payload = 0_usize;
        for item in rest {
            if count == self.limits.maximum_items || (count >= 2 && payload >= share) {
                break;
            }
            let item_bytes = (self.encoded_length)(item).map_err(PackingError::Encode)?;
            let next = payload
                .checked_add(item_bytes)
                .ok_or(PackingError::ItemTooLarge)?;
            if next
                .checked_add(HEADER_BYTES)
                .is_none_or(|bytes| bytes > self.limits.maximum_bytes)
            {
                break;
            }
            payload = next;
            count += 1;
        }
        let page = rest
            .get(..count)
            .filter(|page| !page.is_empty())
            .ok_or(PackingError::ItemTooLarge)?;
        self.start += count;
        self.remaining_bytes = self.remaining_bytes.saturating_sub(payload);
        Ok(Some((page, count as u64)))
    }
}

/// Checks an exact inherited lower bound and an exclusive inherited upper bound.
pub fn within_bounds<K: Ord>(
    first: Option<&K>,
    last: Option<&K>,
    lower: Option<&K>,
    upper: Option<&K>,
) -> bool {
    (lower.is_none() || first == lower)
        && !matches!((last, upper), (Some(last), Some(upper)) if last >= upper)
}

/// Checks child frontiers without decoding, hashing or copying their references.
pub fn children_within<K: Ord, P>(
    children: &[Child<K, P>],
    lower: Option<&K>,
    upper: Option<&K>,
) -> bool {
    within_bounds(
        children.first().map(|child| &child.first),
        children.last().map(|child| &child.first),
        lower,
        upper,
    )
}
