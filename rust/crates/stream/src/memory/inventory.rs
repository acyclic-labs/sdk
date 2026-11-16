// The retained inventory never relocates an entry. Segment `k` holds ordinals
// `2^k - 1 .. 2^(k+1) - 1` in one funded allocation that is installed once and
// never moved. Growth therefore copies nothing and never holds an old backing
// beside a new one; an ordinal lookup is one logarithm and two indexings.
const RETAINED_SEGMENTS: usize = usize::BITS as usize;

struct RetainedSegment {
    entries: Vec<Retained>,
    // Entries retire before their funding.
    _allocation: Allocation,
}

struct RetainedInventory {
    segments: [Option<RetainedSegment>; RETAINED_SEGMENTS],
    /// Segments `0..installed` are present; every later one is absent.
    installed: usize,
    len: usize,
}

impl Default for RetainedInventory {
    fn default() -> Self {
        Self {
            segments: std::array::from_fn(|_| None),
            installed: 0,
            len: 0,
        }
    }
}

/// The segment holding `ordinal` and its offset there.
fn retained_slot(ordinal: usize) -> (usize, usize) {
    let position = ordinal
        .checked_add(1)
        .unwrap_or_else(|| unreachable!("ordinals are below usize::MAX"));
    let segment = position.ilog2() as usize;
    (segment, position - (1 << segment))
}

/// Entries held by segments `0..segments`.
fn retained_capacity(segments: usize) -> usize {
    u32::try_from(segments)
        .ok()
        .and_then(|segments| 1_usize.checked_shl(segments))
        .map_or(usize::MAX, |bound| bound - 1)
}

impl RetainedInventory {
    fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn capacity(&self) -> usize {
        retained_capacity(self.installed)
    }

    /// The next segment to install.
    fn next_segment(&self) -> usize {
        self.installed
    }

    fn get(&self, ordinal: usize) -> Option<&Retained> {
        if ordinal >= self.len {
            return None;
        }
        let (segment, offset) = retained_slot(ordinal);
        self.segments.get(segment)?.as_ref()?.entries.get(offset)
    }

    fn get_mut(&mut self, ordinal: usize) -> Option<&mut Retained> {
        if ordinal >= self.len {
            return None;
        }
        let (segment, offset) = retained_slot(ordinal);
        self.segments
            .get_mut(segment)?
            .as_mut()?
            .entries
            .get_mut(offset)
    }

    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn first(&self) -> Option<&Retained> {
        self.get(0)
    }

    fn last(&self) -> Option<&Retained> {
        self.get(self.len.checked_sub(1)?)
    }

    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn iter(&self) -> impl DoubleEndedIterator<Item = &Retained> {
        self.segments
            .iter()
            .flatten()
            .flat_map(|segment| &segment.entries)
    }

    /// Installs the next segment; it must have room for exactly its ordinals.
    fn install(&mut self, segment: RetainedSegment) {
        let slot = self
            .segments
            .get_mut(self.installed)
            .unwrap_or_else(|| unreachable!("every ordinal has a segment"));
        debug_assert!(slot.is_none() && segment.entries.is_empty());
        debug_assert!(segment.entries.capacity() >= 1 << self.installed);
        *slot = Some(segment);
        self.installed += 1;
    }

    #[cfg(any(test, not(target_arch = "wasm32")))]
    /// Installs an unfunded next segment when full. Only restore and its
    /// controls append outside a funded reservation.
    fn reserve_unfunded(&mut self) -> Option<()> {
        if self.len < self.capacity() {
            return Some(());
        }
        let mut entries = Vec::new();
        entries.try_reserve_exact(1 << self.installed).ok()?;
        self.install(RetainedSegment {
            entries,
            _allocation: Default::default(),
        });
        Some(())
    }

    /// Appends at the next ordinal, which an installed segment already funds.
    /// A segment is never resized, so no earlier entry moves.
    fn push(&mut self, entry: Retained) -> usize {
        let ordinal = self.len;
        let (segment, offset) = retained_slot(ordinal);
        let entries = &mut self
            .segments
            .get_mut(segment)
            .and_then(Option::as_mut)
            .unwrap_or_else(|| unreachable!("an installed reservation funds the next ordinal"))
            .entries;
        debug_assert_eq!(entries.len(), offset);
        debug_assert!(entries.len() < entries.capacity());
        entries.push(entry);
        self.len += 1;
        ordinal
    }
}
