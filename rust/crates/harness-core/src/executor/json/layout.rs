//! Heap layout of typed records, described by JSON position.
//!
//! Every type a bounded decode produces implements [`HeapLayout`], usually
//! through `#[derive(HeapLayout)]` beside its serde derive. The layout names,
//! for each JSON position, the allocation one container causes there: a
//! collection buffer, a B-tree or a box, each with the `size_of` of the slot
//! it holds, plus the serde `Content` buffers that tagged and untagged enums
//! and flattened records build before choosing a variant. Admission walks the
//! document against the layout and reserves the walk's peak before decoding.
//!
//! A JSON container at a position the type ignores (an unknown field) costs
//! only the `Content` an enclosing buffer holds. A container of a kind no
//! candidate type accepts at a typed position is refused: the decode would
//! reject it, and no bound is claimed for shapes the type does not describe.
use serde::__private229::de::Content;
use std::cell::RefCell;

/// Allocations one JSON container causes when decoded at a described position.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Alloc {
    vec: u64,
    tree: u64,
}
impl Alloc {
    /// A struct, tuple or fixed array: stored in its parent's slot.
    pub const INLINE: Self = Self { vec: 0, tree: 0 };
    /// `Vec<T>` or `VecDeque<T>`, grown one element at a time.
    pub const fn vec<T>() -> Self {
        Self {
            vec: size_of::<T>() as u64,
            tree: 0,
        }
    }
    /// `BTreeMap<K, V>`, or `BTreeSet<K>` with `V = ()`.
    pub const fn tree<K, V>() -> Self {
        Self {
            vec: 0,
            tree: (size_of::<K>() + size_of::<V>()) as u64,
        }
    }
    /// `BTreeMap<K, V>` decoded through an intermediate `Vec<(K, V)>`, which
    /// a stable sort orders with at most one scratch slot per entry.
    pub const fn entries<K, V>() -> Self {
        Self {
            vec: size_of::<(K, V)>() as u64,
            ..Self::tree::<K, V>()
        }
    }
    /// Bytes the decoded container keeps, and the largest buffer it holds
    /// only while it is built: the vector's previous buffer while it moves,
    /// or the sort scratch of an entry map.
    fn bytes(self, entries: u64) -> (u64, u64) {
        let (kept, moving) = vec_growth(self.vec, entries);
        let sort = if self.tree == 0 { 0 } else { self.vec };
        let kept = kept.saturating_add(tree_bytes(self.tree, entries));
        (kept, moving.max(sort.saturating_mul(entries)))
    }
}

/// Typed heap layout at one JSON position.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum JsonLayout {
    /// A scalar: a string, number, boolean, null or unit variant. Strings are
    /// charged at every position as though they decode to a `String`.
    Scalar,
    /// A record, enum, collection or map.
    Node {
        /// Allocation of a JSON array here, or `None` when the type rejects one.
        seq: Option<Alloc>,
        /// Allocation of a JSON object here, or `None` when the type rejects one.
        map: Option<Alloc>,
        /// The decode buffers this container's subtree as `Content` before
        /// choosing a variant: an internally or adjacently tagged enum, or a
        /// record with flattened fields.
        tagged: bool,
        /// Object members by serde name, `"*"` for any key. A name may repeat
        /// when several enum variants share it.
        fields: &'static [(&'static str, &'static JsonLayout)],
        /// Array elements by position; a single element applies to all.
        elements: &'static [&'static JsonLayout],
        /// Layouts whose members also appear in this object.
        flatten: &'static [&'static JsonLayout],
    },
    /// `Box<T>`, `Arc<T>` or `Rc<T>`: one allocation of `bytes`, then `inner`.
    Boxed {
        /// Bytes of the box.
        bytes: u64,
        /// Layout of the boxed value.
        inner: &'static JsonLayout,
    },
    /// Any of several layouts: the variants of an untagged enum or of an
    /// adjacently tagged enum's content.
    Any {
        /// The decode buffers the value as `Content` before trying variants.
        tagged: bool,
        /// Candidate layouts.
        of: &'static [&'static JsonLayout],
    },
    /// `inner` decoded `times` over, each copy alive at once: a value a
    /// custom function decodes, converts and re-encodes.
    Scaled {
        /// Copies alive together.
        times: u64,
        /// Layout of each copy.
        inner: &'static JsonLayout,
    },
}
impl JsonLayout {
    /// A struct object; listed fields name their layouts.
    pub const fn record(fields: &'static [(&'static str, &'static JsonLayout)]) -> Self {
        Self::Node {
            seq: None,
            map: Some(Alloc::INLINE),
            tagged: false,
            fields,
            elements: &[],
            flatten: &[],
        }
    }
    /// A JSON array decoded as a collection; one element layout applies to all.
    pub const fn list(alloc: Alloc, element: &'static [&'static JsonLayout]) -> Self {
        Self::Node {
            seq: Some(alloc),
            map: None,
            tagged: false,
            fields: &[],
            elements: element,
            flatten: &[],
        }
    }
    /// A tuple or fixed array: positional elements stored inline.
    pub const fn tuple(elements: &'static [&'static JsonLayout]) -> Self {
        Self::list(Alloc::INLINE, elements)
    }
    /// A JSON object decoded as a map; `values` names its `"*"` entry layout.
    pub const fn map(alloc: Alloc, values: &'static [(&'static str, &'static JsonLayout)]) -> Self {
        Self::Node {
            seq: None,
            map: Some(alloc),
            tagged: false,
            fields: values,
            elements: &[],
            flatten: &[],
        }
    }
}

/// Heap layout of a type's serde decode from JSON.
///
/// Implement it with `#[derive(HeapLayout)]`, which reads the type's serde
/// attributes. A field decoded by a custom function names its layout with
/// `#[heap_layout(as = Type)]`.
pub trait HeapLayout {
    /// The layout at the position this type decodes.
    const LAYOUT: &'static JsonLayout;
}

/// Most distinct layouts one position may stand for.
const CANDIDATES: usize = 8;
/// Deepest chain of boxes, alternatives and flattened records resolved.
const RESOLUTION_DEPTH: u32 = 32;

/// The concrete layouts one JSON value may decode as.
#[derive(Clone, Copy)]
struct Candidates {
    nodes: [Option<&'static JsonLayout>; CANDIDATES],
    /// Largest chain of boxes allocated before the value.
    once: u64,
    /// Some alternative buffers the value as `Content`.
    tagged: bool,
    /// More candidates or deeper chains than admission resolves.
    overflow: bool,
    /// Copies of the value alive together, relative to its container.
    scale: u64,
}
impl Candidates {
    const NONE: Self = Self {
        nodes: [None; CANDIDATES],
        once: 0,
        tagged: false,
        overflow: false,
        scale: 1,
    };
    fn add(&mut self, layout: &'static JsonLayout, once: u64, depth: u32) {
        if depth > RESOLUTION_DEPTH {
            self.overflow = true;
            return;
        }
        match layout {
            JsonLayout::Scalar => self.once = self.once.max(once),
            JsonLayout::Node { .. } => {
                self.once = self.once.max(once);
                if self.nodes().any(|node| std::ptr::eq(node, layout)) {
                    return;
                }
                match self.nodes.iter_mut().find(|slot| slot.is_none()) {
                    Some(slot) => *slot = Some(layout),
                    None => self.overflow = true,
                }
            }
            JsonLayout::Boxed { bytes, inner } => {
                self.add(inner, once.saturating_add(*bytes), depth + 1);
            }
            JsonLayout::Any { tagged, of } => {
                self.tagged |= *tagged;
                for layout in *of {
                    self.add(layout, once, depth + 1);
                }
            }
            JsonLayout::Scaled { times, inner } => {
                self.scale = self.scale.max(*times);
                self.add(inner, once, depth + 1);
            }
        }
    }
    fn nodes(&self) -> impl Iterator<Item = &'static JsonLayout> + '_ {
        self.nodes.iter().map_while(|node| *node)
    }
    fn is_empty(&self) -> bool {
        self.nodes.first().is_none_or(Option::is_none)
    }
    /// The value is buffered as `Content` before its type decodes it.
    fn buffers(&self) -> bool {
        self.tagged
            || self
                .nodes()
                .any(|node| matches!(node, JsonLayout::Node { tagged: true, .. }))
    }
    /// Every member layout named `key`, or the `"*"` layouts of candidates
    /// without such a member.
    fn members(&self, key: &[u8], into: &mut Self) {
        for node in self.nodes() {
            if !members(node, key, false, into, 0) {
                members(node, key, true, into, 0);
            }
        }
    }
    fn elements(&self, index: usize, into: &mut Self) {
        for node in self.nodes() {
            if let JsonLayout::Node { elements, .. } = node {
                let element = match elements {
                    [only] => Some(*only),
                    elements => elements.get(index).copied(),
                };
                if let Some(element) = element {
                    into.add(element, 0, 0);
                }
            }
        }
    }
}
/// Adds the members of `node` and its flattened layouts named `key` (or
/// `"*"` when `any`), and reports whether there were any.
fn members(
    node: &'static JsonLayout,
    key: &[u8],
    any: bool,
    into: &mut Candidates,
    depth: u32,
) -> bool {
    let JsonLayout::Node {
        fields, flatten, ..
    } = node
    else {
        return false;
    };
    let mut found = false;
    for (name, layout) in *fields {
        if (any && *name == "*") || (!any && name.as_bytes() == key) {
            into.add(layout, 0, 0);
            found = true;
        }
    }
    for layout in *flatten {
        let mut flattened = Candidates::NONE;
        flattened.add(layout, 0, depth + 1);
        into.overflow |= flattened.overflow;
        for node in flattened.nodes() {
            found |= members(node, key, any, into, depth + 1);
        }
    }
    found
}

/// Bytes admitted for a layout walk.
///
/// The decode keeps every collection, box and string it builds. Everything
/// else is held only while one container is built, and the decoder builds one
/// container at a time: a vector's previous buffer while it moves, an entry
/// map's sort scratch, and the `Content` that a tagged enum buffers until its
/// variant is chosen. A tagged subtree's buffers are released before its next
/// sibling's are made, so the peak holds the kept bytes, one tagged subtree's
/// buffers, and one moving buffer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LayoutCost {
    /// Collections, boxes and trees the decoded value keeps.
    kept: u64,
    /// Decoded string and owned map key bytes.
    strings: u64,
    /// Longest decoded string, the bound on the decoder's escape scratch.
    longest: u64,
    /// `Content` buffers of the tagged subtree being walked.
    buffering: u64,
    /// Largest `Content` buffers of one tagged subtree.
    buffered: u64,
    /// Largest buffer held while one container is built.
    moving: u64,
    /// Containers at positions the type ignores.
    pub(crate) undescribed: u64,
    /// The document has a shape the layout does not bound.
    pub(crate) refused: bool,
}
impl LayoutCost {
    /// Maximum heap bytes of the typed decode.
    pub(crate) fn bytes(self) -> u64 {
        // Escaped strings decode through one scratch vector that grows by
        // doubling and is reused for every later string.
        self.kept
            .saturating_add(self.strings)
            .saturating_add(vec_bytes(1, self.longest))
            .saturating_add(self.buffered)
            .saturating_add(self.moving)
    }
    /// Longest decoded string or key.
    pub(crate) const fn longest(self) -> u64 {
        self.longest
    }
    fn string(&mut self, bytes: u64, owned: bool, scale: u64) {
        if owned {
            self.strings = self.strings.saturating_add(bytes.saturating_mul(scale));
        }
        self.longest = self.longest.max(bytes);
    }
    fn add(&mut self, placement: &Placement<'_>, map: bool, entries: u64) {
        let candidates = &placement.candidates;
        let tagged = candidates.buffers();
        let mut kept = 0;
        if candidates.is_empty() {
            self.undescribed += 1;
        } else {
            let mut accepted = false;
            for node in candidates.nodes() {
                let JsonLayout::Node {
                    seq, map: object, ..
                } = node
                else {
                    continue;
                };
                let Some(alloc) = (if map { *object } else { *seq }) else {
                    continue;
                };
                accepted = true;
                let (bytes, moving) = alloc.bytes(entries);
                kept = kept.max(bytes.saturating_mul(placement.scale));
                self.moving = self.moving.max(moving.saturating_mul(placement.scale));
            }
            self.refused |= !accepted;
        }
        self.kept = self.kept.saturating_add(kept);
        self.buffering = self.buffering.saturating_add(
            content_bytes(map, entries, placement.buffered, tagged).saturating_mul(placement.scale),
        );
        // This container's visit ends after its subtree's: a buffered
        // subtree's root closes the subtree.
        if !placement.buffered && tagged {
            self.buffered = self.buffered.max(self.buffering);
            self.buffering = 0;
        }
    }
}
/// Final and previous capacity, in bytes, of a vector pushed one element at
/// a time from no capacity: it starts at the allocator's minimum capacity and
/// doubles.
fn vec_growth(slot: u64, entries: u64) -> (u64, u64) {
    if slot == 0 || entries == 0 {
        return (0, 0);
    }
    let mut capacity = match slot {
        1 => 8,
        2..=1024 => 4,
        _ => 1,
    };
    let mut previous = 0;
    while capacity < entries {
        previous = capacity;
        capacity = capacity.saturating_mul(2);
    }
    (slot.saturating_mul(capacity), slot.saturating_mul(previous))
}
/// Peak bytes of a growing vector: its last two buffers together.
fn vec_bytes(slot: u64, entries: u64) -> u64 {
    let (last, previous) = vec_growth(slot, entries);
    last.saturating_add(previous)
}
/// Bytes of a B-tree of `entries`. A leaf holds eleven slots and a header; an
/// internal node adds twelve edges. Up to eleven entries fit the root leaf.
/// Beyond that every leaf holds at least five entries and every internal node
/// at least two children, and building the tree frees no node.
fn tree_bytes(slot: u64, entries: u64) -> u64 {
    if slot == 0 || entries == 0 {
        return 0;
    }
    let leaf = slot.saturating_mul(11).saturating_add(32);
    if entries <= 11 {
        return leaf;
    }
    let leaves = entries / 5;
    leaf.saturating_mul(leaves)
        .saturating_add(leaf.saturating_add(96).saturating_mul(leaves - 1))
}
/// A tagged enum buffers its whole subtree as `Content`, one vector per
/// container, and collects its own entries again when an enclosing buffer
/// replays it. Values move out of buffers without copying.
fn content_bytes(map: bool, entries: u64, buffered: bool, tagged: bool) -> u64 {
    let copies = u64::from(buffered) + u64::from(tagged);
    if copies == 0 {
        return 0;
    }
    let slot = if map {
        size_of::<(Content<'static>, Content<'static>)>()
    } else {
        size_of::<Content<'static>>()
    } as u64;
    vec_bytes(slot, entries).saturating_mul(copies)
}

/// One position in an admission walk.
#[derive(Clone, Copy)]
pub(super) struct Placement<'a> {
    candidates: Candidates,
    buffered: bool,
    /// Copies of this value alive together.
    scale: u64,
    cost: &'a RefCell<LayoutCost>,
}
impl<'a> Placement<'a> {
    fn new(
        candidates: Candidates,
        buffered: bool,
        scale: u64,
        cost: &'a RefCell<LayoutCost>,
    ) -> Self {
        let scale = scale.saturating_mul(candidates.scale);
        let mut charged = cost.borrow_mut();
        charged.kept = charged
            .kept
            .saturating_add(candidates.once.saturating_mul(scale));
        charged.refused |= candidates.overflow;
        Self {
            candidates,
            buffered,
            scale,
            cost,
        }
    }
    pub(super) fn root(layout: &'static JsonLayout, cost: &'a RefCell<LayoutCost>) -> Self {
        let mut candidates = Candidates::NONE;
        candidates.add(layout, 0, 0);
        Self::new(candidates, false, 1, cost)
    }
    /// A string value. Enum labels and identifiers own no bytes, but a string
    /// is charged at every position as though it decodes to a `String`.
    pub(super) fn string(self, bytes: usize) {
        self.cost
            .borrow_mut()
            .string(bytes as u64, true, self.scale);
    }
    /// A `serde_json::Value` decodes here, which expands a raw JSON member.
    pub(super) fn decodes_value(self) -> bool {
        let value = <serde_json::Value as HeapLayout>::LAYOUT;
        self.candidates
            .nodes()
            .any(|node| std::ptr::eq(node, value))
    }
    /// The same position, for a raw JSON document a `Value` expands in place.
    pub(super) fn expanded(self) -> Self {
        let mut candidates = Candidates::NONE;
        candidates.add(<serde_json::Value as HeapLayout>::LAYOUT, 0, 0);
        Self::new(candidates, self.buffered, self.scale, self.cost)
    }
}
/// Counts one container's entries and charges it however its visit ends.
pub(super) struct Entries<'a> {
    placement: Option<Placement<'a>>,
    map: bool,
    pub(super) count: u64,
}
impl<'a> Entries<'a> {
    pub(super) fn new(placement: Option<Placement<'a>>, map: bool) -> Self {
        Self {
            placement,
            map,
            count: 0,
        }
    }
    /// An object key. Map keys and keys buffered as content own their bytes;
    /// struct field names only select a field.
    pub(super) fn key(&self, bytes: usize) {
        if let Some(placement) = self.placement {
            let candidates = &placement.candidates;
            let owned = placement.buffered
                || candidates.buffers()
                || candidates.nodes().any(|node| {
                    matches!(node, JsonLayout::Node { map: Some(alloc), .. } if *alloc != Alloc::INLINE)
                });
            placement
                .cost
                .borrow_mut()
                .string(bytes as u64, owned, placement.scale);
        }
    }
    pub(super) fn child(&self, key: Option<&[u8]>, index: usize) -> Option<Placement<'a>> {
        self.placement.map(|placement| {
            let mut children = Candidates::NONE;
            match key {
                Some(key) => placement.candidates.members(key, &mut children),
                None => placement.candidates.elements(index, &mut children),
            }
            Placement::new(
                children,
                placement.buffered || placement.candidates.buffers(),
                placement.scale,
                placement.cost,
            )
        })
    }
}
impl Drop for Entries<'_> {
    fn drop(&mut self) {
        if let Some(placement) = &self.placement {
            placement
                .cost
                .borrow_mut()
                .add(placement, self.map, self.count);
        }
    }
}

mod std_layouts;
pub use std_layouts::markers;
