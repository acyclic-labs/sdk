struct RecordBatch {
    records: Vec<Record>,
    _allocation: Allocation,
}

type HistoryRanges = acyclic_collections::PersistentIndex<Arc<RecordBatch>, Allocation>;

// Only sealed ancestor batches enter this index. The newest batch remains
// uniquely owned while prepared ingress binds its original body and headers.
// Roots live on paths and admitted pins, never on every historical node.
struct PreparedHistory {
    history: Arc<History>,
    ranges: HistoryRanges,
}

struct HistoryIndexWork<'a>(&'a MemoryStream);

impl acyclic_collections::IndexWork for HistoryIndexWork<'_> {
    type Error = StreamError;
    type Allocation = Allocation;

    fn check(&mut self) -> Result<(), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.0.work {
            work.record(work.meter.check())?;
        }
        Ok(())
    }

    fn charge_items(&mut self, items: u64) -> Result<(), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.0.work {
            work.record(work.meter.examine(items, 0))?;
        }
        let _ = items;
        Ok(())
    }

    fn claim(&mut self, bytes: u64) -> Result<Allocation, StreamError> {
        self.0
            .allocate(usize::try_from(bytes).map_err(|_| StreamError::Capacity)?)
    }
}

struct HistoryReadWork<F>(F);

impl<E, F: FnMut(u64) -> Result<(), E>> acyclic_collections::IndexReadWork for HistoryReadWork<F> {
    type Error = E;

    fn validate(&mut self) -> Result<(), E> {
        (self.0)(0)
    }

    fn examine(&mut self, items: u64) -> Result<(), E> {
        (self.0)(items)?;
        #[cfg(test)]
        HISTORY_METADATA_READS.with(|count| count.set(count.get() + items as usize));
        Ok(())
    }
}
enum History {
    Batch {
        parent: Option<Arc<Self>>,
        keys: KeyTree,
        records: Arc<RecordBatch>,
        _allocation: Allocation,
    },
    Prefix {
        source: Option<Arc<Self>>,
        keys: KeyTree,
        tail: u64,
        _allocation: Allocation,
    },
}

impl History {
    fn keys(&self) -> &KeyTree {
        match self {
            Self::Batch { keys, .. } | Self::Prefix { keys, .. } => keys,
        }
    }

    #[cfg(test)]
    fn parent(&self) -> Option<&Arc<Self>> {
        match self {
            Self::Batch { parent, .. } => parent.as_ref(),
            Self::Prefix { source, .. } => source.as_ref(),
        }
    }

    #[cfg(test)]
    fn boundary(&self) -> u64 {
        match self {
            Self::Batch { records, .. } => {
                records.records.first().map_or(0, |record| record.sequence)
            }
            Self::Prefix { tail, .. } => *tail,
        }
    }

    fn take_parent(&mut self) -> Option<Arc<Self>> {
        match self {
            Self::Batch { parent, .. } => parent.take(),
            Self::Prefix { source, .. } => source.take(),
        }
    }
}

impl Drop for History {
    fn drop(&mut self) {
        let mut cursor = self.take_parent();
        while let Some(node) = cursor {
            let Ok(mut history) = Arc::try_unwrap(node) else {
                break;
            };
            cursor = history.take_parent();
        }
    }
}

// Original ancestry traversal retained only as the independent test oracle.
#[cfg(test)]
struct HistoryWalk<'a> {
    next: Option<&'a Arc<History>>,
    ceiling: u64,
    from: u64,
}

#[cfg(test)]
impl<'a> HistoryWalk<'a> {
    fn new(next: Option<&'a Arc<History>>, ceiling: u64, from: u64) -> Self {
        Self {
            next,
            ceiling,
            from,
        }
    }
}

#[cfg(test)]
impl<'a> Iterator for HistoryWalk<'a> {
    type Item = (&'a History, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.ceiling <= self.from {
            return None;
        }
        let node = self.next?.as_ref();
        #[cfg(test)]
        HISTORY_VISITS.with(|count| count.set(count.get() + 1));
        let ceiling = self.ceiling;
        match node {
            History::Batch {
                parent, records, ..
            } => {
                self.next = parent.as_ref();
                if let Some(first) = records.records.first() {
                    self.ceiling = self.ceiling.min(first.sequence);
                }
            }
            History::Prefix { source, tail, .. } => {
                self.next = source.as_ref();
                self.ceiling = self.ceiling.min(*tail);
            }
        }
        Some((node, ceiling))
    }
}

fn history_window(records: Arc<RecordBatch>, ceiling: u64, from: u64) -> Option<HistoryWindow> {
    let first = records.records.first()?.sequence;
    let batch_end = records
        .records
        .len()
        .try_into()
        .ok()
        .and_then(|length: u64| first.checked_add(length))
        .unwrap_or(u64::MAX);
    let end_sequence = ceiling.min(batch_end);
    if end_sequence <= first.max(from) {
        return None;
    }
    let end = usize::try_from(end_sequence - first).ok()?;
    let start = usize::try_from(from.saturating_sub(first)).ok()?;
    Some(HistoryWindow {
        records,
        start,
        end,
    })
}

#[cfg(test)]
std::thread_local! { static HISTORY_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(test)]
std::thread_local! { static HISTORY_METADATA_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

// Pages that use the sealed fixed-width index.
#[cfg(test)]
std::thread_local! { static HISTORY_SEEKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// Immutable page source shared by reads and follows. The current batch is
/// read directly; sealed ancestors use the same fixed-width position index.
/// A cold seek has at most 64 branch decisions, independent of fork depth.
/// Each result retains the original batch and exact inherited exclusive cut.
struct PinnedHistory {
    root: Option<Arc<History>>,
    ranges: HistoryRanges,
    tail: u64,
}
impl PinnedHistory {
    fn new(path: &PathState, tail: u64) -> Self {
        Self {
            root: path.history.clone(),
            ranges: path.ranges.clone(),
            tail,
        }
    }

    fn empty(tail: u64) -> Self {
        Self {
            root: None,
            ranges: HistoryRanges::default(),
            tail,
        }
    }

    fn page<E>(
        &mut self,
        after: u64,
        budget: u32,
        mut examine: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<HistoryWindow>, E> {
        if budget == 0 || after >= self.tail {
            return Ok(None);
        }
        examine(1)?;
        #[cfg(test)]
        HISTORY_VISITS.with(|count| count.set(count.get() + 1));
        let ceiling = self.tail.min(after.saturating_add(u64::from(budget)));
        if let Some(History::Batch { records, .. }) = self.root.as_deref()
            && records
                .records
                .first()
                .is_some_and(|first| first.sequence <= after)
        {
            return Ok(history_window(Arc::clone(records), ceiling, after));
        }
        #[cfg(test)]
        HISTORY_SEEKS.with(|count| count.set(count.get() + 1));
        let Some(position) = self
            .ranges
            .predecessor(after.to_be_bytes(), &mut HistoryReadWork(examine))?
        else {
            return Ok(None);
        };
        let ceiling = position
            .before
            .map_or(ceiling, |before| ceiling.min(u64::from_be_bytes(before)));
        Ok(history_window(Arc::clone(position.value), ceiling, after))
    }
}

impl crate::pages::PageSource for PinnedHistory {
    type Page = HistoryWindow;

    async fn next_page(
        &mut self,
        after: u64,
        budget: u32,
    ) -> Result<Option<Self::Page>, StreamError> {
        self.page(after, budget, |_| Ok(()))
    }
}
