//! Immutable ordered page shapes and bounded packing shared by storage engines.
//!
//! Page references, codecs, admission and publication belong to the caller.
//! Packing borrows the original admitted entries, allocates nothing and returns
//! only contiguous nonempty slices. It never reads another page or a backend.

pub mod merge;
pub mod publish;
pub mod rewrite;
pub mod search;
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
    /// Target complete encoded page size, including its header.
    pub maximum_bytes: usize,
    /// Hard complete encoded object bound, including its header.
    pub maximum_object_bytes: usize,
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

/// A stable 64-bit hash of a key's bytes. Page boundaries derive from it, so
/// it is part of the persisted tree shape and never changes within v1.
pub fn stable_hash(bytes: &[u8]) -> u64 {
    // FNV-1a, finished by the splitmix64 mixer.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3);
    }
    mix(hash)
}

const fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Splits one level's items into pages at content-defined boundaries.
///
/// Content and item boundaries close pages without looking beyond the item
/// at hand. A hard object bound can additionally cut before the next item.
/// Rewriting a page's first item therefore revisits the preceding page's tail.
/// The target byte size splits work; the object bound refuses a single item
/// that cannot fit. Hash boundaries do not imply a worst-case carry distance.
pub struct PageChunks<'a, T, L, H, const HEADER_BYTES: usize, const TARGET_BYTES: usize> {
    items: &'a [T],
    encoded_length: L,
    key_hash: H,
    salt: u64,
    start: usize,
    maximum_items: usize,
    capacity: usize,
    object_capacity: usize,
    minimum_weight: usize,
    leaf: bool,
}

impl<'a, T, L, H, E, const HEADER_BYTES: usize, const TARGET_BYTES: usize>
    PageChunks<'a, T, L, H, HEADER_BYTES, TARGET_BYTES>
where
    L: Fn(&T) -> Result<usize, E>,
    H: Fn(&T) -> u64,
{
    /// Retains original slices; construction allocates and scans nothing.
    pub fn new(
        items: &'a [T],
        limits: PackingLimits,
        level: u16,
        encoded_length: L,
        key_hash: H,
    ) -> Result<Self, PackingError<E>> {
        if TARGET_BYTES == 0 || limits.maximum_items == 0 {
            return Err(PackingError::InvalidLimits);
        }
        let maximum_items = limits.maximum_items;
        let maximum_bytes = limits.maximum_bytes;
        Ok(Self {
            items,
            encoded_length,
            key_hash,
            salt: mix(u64::from(level).wrapping_add(0x9e37_79b9_7f4a_7c15)),
            start: 0,
            maximum_items,
            capacity: maximum_bytes.saturating_sub(HEADER_BYTES),
            object_capacity: limits
                .maximum_object_bytes
                .checked_sub(HEADER_BYTES)
                .ok_or(PackingError::ItemTooLarge)?,
            // Boundaries come often enough that the item limit rarely forces
            // a cut: at least one in a quarter of the limit. No item is a
            // certain boundary, or pages would be positional and an insert
            // would shift every page after it.
            minimum_weight: TARGET_BYTES
                .saturating_mul(4)
                .div_ceil(maximum_items.max(1))
                .min(TARGET_BYTES / 2),
            leaf: level == 0,
        })
    }

    fn boundary(&self, item: &T, bytes: usize) -> bool {
        let weight = bytes.max(self.minimum_weight);
        let hash = mix((self.key_hash)(item) ^ self.salt);
        u128::from(hash) * (TARGET_BYTES as u128) < (weight as u128) << 64
    }

    /// The next page, the items it examined, and whether a boundary closed
    /// it; an unclosed page ended only because the items ran out.
    pub fn next_page(&mut self) -> Result<Option<(&'a [T], u64, bool)>, PackingError<E>> {
        let items = self.items;
        let Some(rest) = items.get(self.start..).filter(|rest| !rest.is_empty()) else {
            return Ok(None);
        };
        if self.maximum_items == 0 {
            return Err(PackingError::InvalidLimits);
        }
        let half = self.capacity / 2;
        let mut count = 0_usize;
        let mut payload = 0_usize;
        let mut closed = false;
        for item in rest {
            let bytes = (self.encoded_length)(item).map_err(PackingError::Encode)?;
            if bytes > self.object_capacity {
                return Err(PackingError::ItemTooLarge);
            }
            if count > 0 && bytes > self.object_capacity.saturating_sub(payload) {
                closed = true;
                break;
            }
            if self.leaf && bytes > half {
                // A large item closes the page it joins. Cutting before it
                // instead would make this page depend on the next item, so
                // removing the large item would leave a stale cut.
                count += 1;
                closed = true;
                break;
            }
            payload = payload.saturating_add(bytes);
            count += 1;
            if count == self.maximum_items
                || (count >= 2 && (payload > half || self.boundary(item, bytes)))
            {
                closed = true;
                break;
            }
        }
        let page = &rest[..count];
        self.start += count;
        Ok(Some((page, count as u64, closed)))
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
