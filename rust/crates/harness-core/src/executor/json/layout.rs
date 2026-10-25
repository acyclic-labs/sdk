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
    /// Most `Content` buffers one chain of alternatives nests at this value:
    /// an untagged enum inside another buffers it again.
    buffers: u64,
    /// More candidates or deeper chains than admission resolves.
    overflow: bool,
    /// Copies of the value alive together, relative to its container.
    scale: u64,
}
impl Candidates {
    const NONE: Self = Self {
        nodes: [None; CANDIDATES],
        once: 0,
        buffers: 0,
        overflow: false,
        scale: 1,
    };
    fn add(&mut self, layout: &'static JsonLayout, once: u64, depth: u32) {
        self.add_within(layout, once, depth, 0);
    }
    /// Adds `layout`, reached inside `buffers` buffering alternatives.
    fn add_within(&mut self, layout: &'static JsonLayout, once: u64, depth: u32, buffers: u64) {
        if depth > RESOLUTION_DEPTH {
            self.overflow = true;
            return;
        }
        match layout {
            JsonLayout::Scalar => {
                self.once = self.once.max(once);
                self.buffers = self.buffers.max(buffers);
            }
            JsonLayout::Node { tagged, .. } => {
                self.once = self.once.max(once);
                self.buffers = self.buffers.max(buffers + u64::from(*tagged));
                if self.nodes().any(|node| std::ptr::eq(node, layout)) {
                    return;
                }
                match self.nodes.iter_mut().find(|slot| slot.is_none()) {
                    Some(slot) => *slot = Some(layout),
                    None => self.overflow = true,
                }
            }
            JsonLayout::Boxed { bytes, inner } => {
                self.add_within(inner, once.saturating_add(*bytes), depth + 1, buffers);
            }
            JsonLayout::Any { tagged, of } => {
                for layout in *of {
                    self.add_within(layout, once, depth + 1, buffers + u64::from(*tagged));
                }
            }
            JsonLayout::Scaled { times, inner } => {
                self.scale = self.scale.max(*times);
                self.add_within(inner, once, depth + 1, buffers);
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
        self.buffers > 0
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

/// What a record's flattened fields keep and build.
#[derive(Clone, Copy, Default)]
struct Flattened {
    kept: u64,
    moving: u64,
    /// A flattened map owns its keys.
    owns_keys: bool,
}
/// What the flattened fields of `node` keep when its object has `entries`
/// members: every flattened collection may receive each member, and every
/// flattened box is allocated once. A chain deeper than admission resolves
/// keeps an unbounded amount, so its decode is refused.
fn flattened_cost(node: &'static JsonLayout, entries: u64, depth: u32) -> Flattened {
    let JsonLayout::Node { flatten, .. } = node else {
        return Flattened::default();
    };
    let mut cost = Flattened::default();
    for layout in *flatten {
        let mut flattened = Candidates::NONE;
        flattened.add(layout, 0, depth + 1);
        if flattened.overflow {
            cost.kept = u64::MAX;
            continue;
        }
        let mut field = 0_u64;
        for inner in flattened.nodes() {
            let mut bytes = 0;
            if let JsonLayout::Node {
                map: Some(alloc), ..
            } = inner
            {
                let (kept, moving) = alloc.bytes(entries);
                bytes = kept;
                cost.moving = cost.moving.max(moving);
                cost.owns_keys |= *alloc != Alloc::INLINE;
            }
            let nested = flattened_cost(inner, entries, depth + 1);
            cost.moving = cost.moving.max(nested.moving);
            cost.owns_keys |= nested.owns_keys;
            field = field.max(bytes.saturating_add(nested.kept));
        }
        cost.kept = cost.kept.saturating_add(
            field
                .saturating_add(flattened.once)
                .saturating_mul(flattened.scale),
        );
    }
    cost
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
    /// Longest debug rendering of a string or key: a variant that rejects a
    /// string formats it into its error, which lives until the next try.
    message: u64,
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
            .saturating_add(if self.message == 0 || !messages_counted() {
                0
            } else {
                vec_bytes(1, self.message.saturating_add(MESSAGE_FRAME))
            })
            .saturating_add(self.buffered)
            .saturating_add(self.moving)
    }
    /// Longest decoded string or key.
    pub(crate) const fn longest(self) -> u64 {
        self.longest
    }
    fn string(&mut self, text: &[u8], owned: bool, scale: u64) {
        let bytes = text.len() as u64;
        if owned {
            self.strings = self.strings.saturating_add(bytes.saturating_mul(scale));
        }
        self.longest = self.longest.max(bytes);
        self.message = self.message.max(debug_bytes(text));
    }
    /// The copies of an escaped string the `Content` buffers around it own.
    /// Each buffer holds one, and every enclosing buffer that replays the
    /// value into another clones it again. An unescaped string borrows the
    /// document and owns nothing there. A string no open subtree encloses is
    /// its own buffered subtree and closes at once.
    fn buffered_string(&mut self, bytes: u64, copies: u64, closes: bool, scale: u64) {
        let owned = bytes.saturating_mul(copies).saturating_mul(scale);
        if closes {
            self.buffered = self.buffered.max(self.buffering.saturating_add(owned));
        } else {
            self.buffering = self.buffering.saturating_add(owned);
        }
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
                let (mut bytes, mut moving) = alloc.bytes(entries);
                if map && flattened_counted() {
                    let flattened = flattened_cost(node, entries, 0);
                    bytes = bytes.saturating_add(flattened.kept);
                    moving = moving.max(flattened.moving);
                }
                kept = kept.max(bytes.saturating_mul(placement.scale));
                self.moving = self.moving.max(moving.saturating_mul(placement.scale));
            }
            self.refused |= !accepted;
        }
        self.kept = self.kept.saturating_add(kept);
        self.buffering = self.buffering.saturating_add(
            content_bytes(map, entries, placement.copies()).saturating_mul(placement.scale),
        );
        // This container's visit ends after its subtree's: a buffered
        // subtree's root closes the subtree.
        if placement.buffers == 0 && tagged {
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
/// Bytes of an error message around a rendered string: its type and the
/// expectation of the type that rejected it.
const MESSAGE_FRAME: u64 = 256;
/// An upper bound on the bytes `{:?}` renders `text` as, quotes included: an
/// escaped ASCII character takes two bytes, a control character six
/// (`\u{1f}`), and a non-ASCII character at most `\u{10ffff}`, under four per
/// byte it spans.
fn debug_bytes(text: &[u8]) -> u64 {
    text.iter()
        .map(|byte| match byte {
            b'\t' | b'\r' | b'\n' | b'\\' | b'"' | b'\'' => 2,
            0..0x20 | 0x7f => 6,
            0x80.. => 4,
            _ => 1,
        })
        .sum::<u64>()
        .saturating_add(2)
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
/// container, and every enclosing buffer that replays it collects its entries
/// again: `copies` buffers hold the container at once.
fn content_bytes(map: bool, entries: u64, copies: u64) -> u64 {
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
    /// Enclosing `Content` buffers that hold this value.
    buffers: u64,
    /// Copies of this value alive together.
    scale: u64,
    cost: &'a RefCell<LayoutCost>,
}
impl<'a> Placement<'a> {
    fn new(
        candidates: Candidates,
        buffers: u64,
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
            buffers,
            scale,
            cost,
        }
    }
    pub(super) fn root(layout: &'static JsonLayout, cost: &'a RefCell<LayoutCost>) -> Self {
        let mut candidates = Candidates::NONE;
        candidates.add(layout, 0, 0);
        Self::new(candidates, 0, 1, cost)
    }
    /// `Content` buffers holding this value: the enclosing ones, and each one
    /// its type nests around it before choosing a variant.
    fn copies(&self) -> u64 {
        let copies = self.buffers.saturating_add(self.candidates.buffers);
        #[cfg(test)]
        let copies = copies.min(tests::COPY_CAP.get());
        copies
    }
    /// A string value. Enum labels and identifiers own no bytes, but a string
    /// is charged at every position as though it decodes to a `String`, and
    /// an `escaped` one is also owned by every buffer holding it.
    pub(super) fn string(self, text: &[u8], escaped: bool) {
        let mut cost = self.cost.borrow_mut();
        cost.string(text, true, self.scale);
        if escaped {
            cost.buffered_string(
                text.len() as u64,
                self.copies(),
                self.buffers == 0,
                self.scale,
            );
        }
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
        Self::new(candidates, self.buffers, self.scale, self.cost)
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
    /// An object key. Map keys own their bytes, and an `escaped` key is also
    /// owned by every buffer holding its object; struct field names only
    /// select a field. The object's own visit closes its buffered subtree.
    pub(super) fn key(&self, key: &[u8], escaped: bool) {
        if let Some(placement) = self.placement {
            let candidates = &placement.candidates;
            let owned = candidates.nodes().any(|node| {
                matches!(node, JsonLayout::Node { map: Some(alloc), .. } if *alloc != Alloc::INLINE)
                    || (flattened_counted() && flattened_cost(node, 0, 0).owns_keys)
            });
            let mut cost = placement.cost.borrow_mut();
            cost.string(key, owned, placement.scale);
            if escaped {
                cost.buffered_string(key.len() as u64, placement.copies(), false, placement.scale);
            }
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
                placement.copies(),
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

#[cfg(test)]
fn flattened_counted() -> bool {
    tests::FLATTENED.get()
}
#[cfg(not(test))]
const fn flattened_counted() -> bool {
    true
}
#[cfg(test)]
fn messages_counted() -> bool {
    tests::MESSAGES.get()
}
#[cfg(not(test))]
const fn messages_counted() -> bool {
    true
}

#[cfg(test)]
pub(super) mod tests {
    use std::cell::Cell;
    thread_local! {
        /// Most `Content` buffers counted at one position; a negative control
        /// lowers it to reproduce a model that counts too few copies.
        pub(in crate::executor::json) static COPY_CAP: Cell<u64> = const { Cell::new(u64::MAX) };
        /// Whether rejected strings' error messages are reserved; a negative
        /// control clears it.
        pub(in crate::executor::json) static MESSAGES: Cell<bool> = const { Cell::new(true) };
        /// Whether flattened collections are reserved; a negative control
        /// clears it.
        pub(in crate::executor::json) static FLATTENED: Cell<bool> = const { Cell::new(true) };
    }
}
