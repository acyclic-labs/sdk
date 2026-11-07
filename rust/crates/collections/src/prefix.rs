//! Retained prefix extraction over the original mutable radix.
use super::*;

/// A prefix wider than the complete original key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrefixTooLong {
    /// The supplied prefix length in bytes.
    pub observed: usize,
    /// The index's complete key width in bytes.
    pub maximum: usize,
}

impl std::fmt::Display for PrefixTooLong {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "index prefix has {} bytes, maximum {}",
            self.observed, self.maximum
        )
    }
}

impl std::error::Error for PrefixTooLong {}

/// An extracted ordered prefix and its original physical backing and funding.
///
/// The redundant parent branch remains here, funded and allocated, until this
/// value is destroyed or consumed. Extraction can therefore run under an
/// owner's lock without running payload destructors or funding retirement there.
pub struct ExtractedPrefix<V, A, const N: usize> {
    entries: FixedIndex<V, A, N>,
    parent: Link<V, A, N>,
}

impl<V, A, const N: usize> ExtractedPrefix<V, A, N> {
    /// Borrows the detached entries for the original admitted index operations.
    pub fn index(&self) -> &FixedIndex<V, A, N> {
        &self.entries
    }

    /// Retires the empty parent branch and retains the extracted index.
    /// Call this after leaving any lock that original funding may re-enter.
    pub fn into_index(self) -> FixedIndex<V, A, N> {
        let Self { entries, parent } = self;
        drop(parent);
        entries
    }

    /// Consumes the detached entries in unsigned big-endian key order.
    ///
    /// Each yielded entry retains its original leaf funding through value
    /// destruction. Branch backing retires only after its links are detached.
    /// This needs no new admission and cannot strand entries on cancellation;
    /// dropping a partially consumed iterator retires the remaining subtree.
    /// Call this after leaving any lock that original funding may re-enter.
    ///
    /// Traversal uses the same constant-space retirement cursor as whole-index
    /// destruction. It allocates nothing, examines only extracted nodes, and
    /// performs O(extracted nodes) total work. Each call moves just the yielded
    /// complete key/value; it does not accumulate output or restart from a root.
    /// The infallible retained-owner `visit` sink observes node examinations,
    /// including retirement on iterator drop. It never admits fresh work.
    pub fn into_entries<F: FnMut(u64)>(self, visit: F) -> ExtractedEntries<V, A, N, F> {
        ExtractedEntries {
            entries: self.into_index(),
            visit,
        }
    }
}

/// An owned, allocation-free retirement cursor over one extracted prefix.
/// Its original subtree remains private while it is consumed or retired.
pub struct ExtractedEntries<V, A, const N: usize, F: FnMut(u64)> {
    entries: FixedIndex<V, A, N>,
    visit: F,
}

impl<V, A, const N: usize, F: FnMut(u64)> ExtractedEntries<V, A, N, F> {
    /// Admits each node examination on the original caller before consuming it.
    /// Checks cancellation even when empty, allocates nothing and copies no key
    /// or value. Refusal yields no entry and preserves all remaining entries in
    /// order, although admitted rotations may already have changed their shape.
    ///
    /// An uninterrupted traversal costs O(consumed nodes) in total, with constant
    /// space; each refused attempt pays for its admitted examinations. Dropping
    /// after refusal or caller withdrawal still retires every
    /// remaining owner through the infallible retained observation sink, without
    /// fresh admission. Yielded values keep their original leaf funding.
    pub fn try_next<W: IndexReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<Option<Entry<V, A, N>>, W::Error> {
        work.validate()?;
        try_take_first_leaf(&mut self.entries.root, &mut |items| {
            work.examine(items)?;
            (self.visit)(items);
            Ok(())
        })
        .map(|leaf| leaf.map(Entry::from_leaf))
    }
}

impl<V, A, const N: usize, F: FnMut(u64)> Iterator for ExtractedEntries<V, A, N, F> {
    type Item = Entry<V, A, N>;

    fn next(&mut self) -> Option<Self::Item> {
        take_first_leaf(&mut self.entries.root, &mut self.visit).map(Entry::from_leaf)
    }
}

impl<V, A, const N: usize, F: FnMut(u64)> std::iter::FusedIterator
    for ExtractedEntries<V, A, N, F>
{
}

impl<V, A, const N: usize, F: FnMut(u64)> Drop for ExtractedEntries<V, A, N, F> {
    fn drop(&mut self) {
        while let Some(leaf) = take_first_leaf(&mut self.entries.root, &mut self.visit) {
            drop(leaf);
        }
    }
}

impl<V, A, const N: usize> FixedIndex<V, A, N> {
    /// Extracts all original keys less than or equal to `through`.
    ///
    /// Like byte-prefix extraction, this is infallible retained-owner cleanup:
    /// cancellation cannot strand owners, and no new work or funding is admitted.
    /// It allocates nothing, copies no key or value, and retires no backing. The
    /// returned owner retains the one redundant branch until outside the lock.
    /// Both resulting indexes preserve their original search invariants.
    ///
    /// One descent locates the cut's original comparison leaf. Only branches
    /// before its first differing bit can straddle the cut; a second descent
    /// relinks those branches. At most `17N + 3` nodes/key bytes are observed,
    /// independent of the population on either side. The bounded descent uses
    /// O(N) stack space, not O(population) space. Delivery uses the existing
    /// allocation-free, constant-space owned iterator over extracted nodes.
    pub fn extract_through(
        &mut self,
        through: [u8; N],
        mut visit: impl FnMut(u64),
    ) -> ExtractedPrefix<V, A, N> {
        let mut cursor = self.root.as_ref();
        let (boundary, selected) = loop {
            let Some(node) = cursor else {
                return ExtractedPrefix {
                    entries: FixedIndex::default(),
                    parent: None,
                };
            };
            visit(1);
            match node.kind.as_ref() {
                Kind::Leaf { key, .. } => {
                    visit(N as u64 + 1);
                    let boundary = difference(key, &through);
                    break (boundary, boundary.is_none_or(|bit| !high(key, bit)));
                }
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } => {
                    cursor = if high(&through, *bit) { upper } else { low }.as_ref();
                }
            }
        };
        let (selected, retained, parent) =
            split_through(self.root.take(), &through, boundary, selected, &mut visit);
        self.root = retained;
        ExtractedPrefix {
            entries: FixedIndex { root: selected },
            parent,
        }
    }

    /// Extracts every key starting with the exact original byte prefix.
    ///
    /// This is retained-owner cleanup: the caller serializes the original
    /// index's lifetime and supplies its infallible observation sink. It needs
    /// no new admission, so cancellation cannot strand live entries. It performs
    /// no allocation, copy of a key or value, destruction, or new funding claim.
    /// All extracted backing keeps its original tokens in the returned owner.
    /// An empty prefix extracts the whole index; an oversized one changes nothing.
    ///
    /// At most 16N + 3 nodes and `prefix.len()` key bytes are examined,
    /// independent of the number of matching or unrelated entries. `visit`
    /// observes each node and comparison byte before use. Actual subtree
    /// destruction is deferred and costs only the nodes being retired.
    pub fn extract_prefix(
        &mut self,
        prefix: &[u8],
        mut visit: impl FnMut(u64),
    ) -> Result<ExtractedPrefix<V, A, N>, PrefixTooLong> {
        if prefix.len() > N {
            return Err(PrefixTooLong {
                observed: prefix.len(),
                maximum: N,
            });
        }
        let empty = || ExtractedPrefix {
            entries: FixedIndex::default(),
            parent: None,
        };
        if prefix.is_empty() {
            return Ok(ExtractedPrefix {
                entries: FixedIndex {
                    root: self.root.take(),
                },
                parent: None,
            });
        }

        let mut cursor = self.root.as_ref();
        let mut parent = None;
        while let Some(node) = cursor {
            visit(1);
            match node.kind.as_ref() {
                Kind::Branch {
                    bit,
                    low,
                    high: upper,
                } if bit.byte < prefix.len() => {
                    parent = Some(*bit);
                    cursor = if high(prefix, *bit) { upper } else { low }.as_ref();
                }
                _ => break,
            }
        }
        // Compressed branches omit common coordinates. Check one actual leaf
        // before treating the selected subtree as an exact prefix match.
        while let Some(node) = cursor {
            visit(1);
            match node.kind.as_ref() {
                Kind::Leaf { key, .. } => {
                    visit(prefix.len() as u64);
                    if !key.starts_with(prefix) {
                        return Ok(empty());
                    }
                    break;
                }
                Kind::Branch { low, .. } => cursor = low.as_ref(),
            }
        }
        if cursor.is_none() {
            return Ok(empty());
        }

        Ok(self.extract_at(parent, |bit| high(prefix, bit), visit))
    }

    // Exact-leaf removal and prefix removal unlink through the same original
    // parent. The returned owner keeps even the empty branch's backing live.
    pub(super) fn extract_at(
        &mut self,
        parent: Option<Bit>,
        mut select_high: impl FnMut(Bit) -> bool,
        mut visit: impl FnMut(u64),
    ) -> ExtractedPrefix<V, A, N> {
        let mut link = &mut self.root;
        if let Some(parent) = parent {
            loop {
                visit(1);
                match link.as_ref().map(|node| node.kind.as_ref()) {
                    Some(Kind::Branch { bit, .. }) if *bit == parent => break,
                    _ => {}
                }
                match link.as_mut().map(|node| node.kind.as_mut()) {
                    Some(Kind::Branch {
                        bit,
                        low,
                        high: upper,
                    }) => link = if select_high(*bit) { upper } else { low },
                    _ => unreachable!("extraction follows the original verified parent"),
                }
            }
            let mut branch = link
                .take()
                .unwrap_or_else(|| unreachable!("verified prefix parent"));
            let Kind::Branch {
                bit,
                low,
                high: upper,
            } = branch.kind.as_mut()
            else {
                unreachable!("prefix parent is an original branch")
            };
            let (selected, sibling) = if select_high(*bit) {
                (upper, low)
            } else {
                (low, upper)
            };
            let root = selected.take();
            *link = sibling.take();
            ExtractedPrefix {
                entries: FixedIndex { root },
                parent: Some(branch),
            }
        } else {
            ExtractedPrefix {
                entries: FixedIndex { root: link.take() },
                parent: None,
            }
        }
    }
}

// Coordinates strictly increase on the original cut path. All other links move
// intact. Exactly one branch becomes redundant when a nonempty tree is split
// into two nonempty trees; the original owner defers its backing retirement.
fn split_through<V, A, const N: usize>(
    mut root: Link<V, A, N>,
    through: &[u8; N],
    boundary: Option<Bit>,
    selected: bool,
    visit: &mut impl FnMut(u64),
) -> (Link<V, A, N>, Link<V, A, N>, Link<V, A, N>) {
    let Some(node) = root.as_mut() else {
        return (None, None, None);
    };
    visit(1);
    if let Kind::Branch {
        bit,
        low,
        high: upper,
    } = node.kind.as_mut()
        && boundary.is_none_or(|boundary| *bit < boundary)
    {
        if high(through, *bit) {
            let (below, above, parent) =
                split_through(upper.take(), through, boundary, selected, visit);
            *upper = below;
            if upper.is_none() {
                debug_assert!(parent.is_none());
                let below = low.take();
                return (below, above, root);
            }
            return (root, above, parent);
        }
        let (below, above, parent) = split_through(low.take(), through, boundary, selected, visit);
        *low = above;
        if low.is_none() {
            debug_assert!(parent.is_none());
            let above = upper.take();
            return (below, above, root);
        }
        return (below, root, parent);
    }
    if selected {
        (root, None, None)
    } else {
        (None, root, None)
    }
}
