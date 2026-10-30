struct RecordBatch {
    records: Vec<Record>,
    _allocation: Allocation,
}
enum History {
    Batch {
        parent: Option<Arc<Self>>,
        jump: HistoryJump,
        keys: KeyTree,
        records: Arc<RecordBatch>,
        _allocation: Allocation,
    },
    Prefix {
        source: Option<Arc<Self>>,
        jump: HistoryJump,
        keys: KeyTree,
        tail: u64,
        _allocation: Allocation,
    },
}

/// One funded link per node, to an ancestor chosen as in a skew-binary
/// random-access list: the parent, or the parent's jump's jump when the
/// parent's two jumps span equal depths. Built in O(1), these links reach
/// any ancestor in at most about 2 log2 n steps, so a seek from the newest
/// node is O(log n). Its minimum boundary proves that skipping cannot cross
/// an append or fork cut required by this read. The link is derived state
/// and is rebuilt with the history.
struct HistoryJump {
    target: Option<Arc<History>>,
    /// Ancestors below this node.
    depth: u64,
    minimum: u64,
}

impl HistoryJump {
    fn new(
        parent: Option<&Arc<History>>,
        boundary: u64,
        mut examine: impl FnMut() -> Result<(), StreamError>,
    ) -> Result<Self, StreamError> {
        let Some(parent) = parent else {
            return Ok(Self {
                target: None,
                depth: 0,
                minimum: boundary,
            });
        };
        examine()?;
        let up = parent.jump();
        let depth = up.depth.checked_add(1).ok_or(StreamError::Capacity)?;
        if let Some(middle) = up.target.as_ref() {
            let across = middle.jump();
            if let Some(target) = across.target.as_ref()
                && up.depth - across.depth == across.depth - target.jump().depth
            {
                return Ok(Self {
                    target: Some(Arc::clone(target)),
                    depth,
                    minimum: boundary.min(up.minimum).min(across.minimum),
                });
            }
        }
        Ok(Self {
            target: Some(Arc::clone(parent)),
            depth,
            minimum: boundary.min(parent.boundary()),
        })
    }
}

impl History {
    fn keys(&self) -> &KeyTree {
        match self {
            Self::Batch { keys, .. } | Self::Prefix { keys, .. } => keys,
        }
    }

    fn parent(&self) -> Option<&Arc<Self>> {
        match self {
            Self::Batch { parent, .. } => parent.as_ref(),
            Self::Prefix { source, .. } => source.as_ref(),
        }
    }

    fn jump(&self) -> &HistoryJump {
        #[cfg(test)]
        HISTORY_METADATA_READS.with(|count| count.set(count.get() + 1));
        match self {
            Self::Batch { jump, .. } | Self::Prefix { jump, .. } => jump,
        }
    }

    fn boundary(&self) -> u64 {
        match self {
            Self::Batch { records, .. } => {
                records.records.first().map_or(0, |record| record.sequence)
            }
            Self::Prefix { tail, .. } => *tail,
        }
    }

    fn take_parent(&mut self) -> Option<Arc<Self>> {
        // A jump reaches an ancestor still held by the parent chain. Retire it
        // first so releasing that chain remains iterative, including deep forks.
        match self {
            Self::Batch { parent, jump, .. } => {
                jump.target = None;
                parent.take()
            }
            Self::Prefix { source, jump, .. } => {
                jump.target = None;
                source.take()
            }
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

struct HistoryWalk<'a> {
    next: Option<&'a Arc<History>>,
    ceiling: u64,
    from: u64,
}

impl<'a> HistoryWalk<'a> {
    fn new(next: Option<&'a Arc<History>>, ceiling: u64, from: u64) -> Self {
        Self {
            next,
            ceiling,
            from,
        }
    }
}

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
        if let Some(target) = node
            .jump()
            .target
            .as_ref()
            .filter(|_| node.jump().minimum >= ceiling)
        {
            self.next = Some(target);
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

/// Pages that started from the root instead of a retained path.
#[cfg(test)]
std::thread_local! { static HISTORY_SEEKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// The longest retained path, in nodes.
#[cfg(test)]
std::thread_local! { static HISTORY_PATH_PEAK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// Immutable page source, shared by lazy reads and follows. A page may end at
/// a batch or inherited cut before exhausting the caller's record budget.
///
/// The source keeps the nodes its last page walked through, each with the
/// ceiling below which it and everything older is visible. Each one is a valid
/// start for any later position under its ceiling, so a page at or after the
/// last resumes from the deepest one still above it: only the first page, or
/// one before the last, seeks from the root in O(log n). Moving forward never
/// visits a node twice, so later pages cost O(1) amortised.
struct PinnedHistory {
    root: Option<Arc<History>>,
    tail: u64,
    /// The nodes from the root to the last page's node, ceilings non-increasing.
    path: Vec<(Arc<History>, u64)>,
    /// The position the path was walked for.
    at: u64,
}
impl PinnedHistory {
    fn new(root: Option<Arc<History>>, tail: u64) -> Self {
        Self {
            root,
            tail,
            path: Vec::new(),
            at: 0,
        }
    }

    /// Pins `appended`, whose history extends this one's: every retained node
    /// stays a valid start under its ceiling, so the path carries over.
    fn appended(&mut self, root: Option<Arc<History>>, tail: u64) -> Self {
        Self {
            root,
            tail,
            path: std::mem::take(&mut self.path),
            at: self.at,
        }
    }

    fn retain(&mut self, node: Arc<History>, ceiling: u64) {
        self.path.push((node, ceiling));
        #[cfg(test)]
        HISTORY_PATH_PEAK.with(|peak| peak.set(peak.get().max(self.path.len())));
    }

    fn page<E>(
        &mut self,
        after: u64,
        budget: u32,
        mut examine: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<HistoryWindow>, E> {
        if budget == 0 || after >= self.tail {
            return Ok(None);
        }
        if after < self.at {
            self.path.clear();
        }
        self.at = after;
        // A node whose ceiling is at or below `after` holds nothing it reads.
        while self
            .path
            .last()
            .is_some_and(|(_, ceiling)| *ceiling <= after)
        {
            self.path.pop();
        }
        let (mut node, mut ceiling) = match self.path.pop() {
            Some((node, ceiling)) => (Some(node), ceiling),
            None => {
                #[cfg(test)]
                HISTORY_SEEKS.with(|count| count.set(count.get() + 1));
                (self.root.clone(), self.tail)
            }
        };
        while let Some(current) = node {
            examine()?;
            #[cfg(test)]
            HISTORY_VISITS.with(|count| count.set(count.get() + 1));
            if after >= ceiling {
                return Ok(None);
            }
            if let History::Batch { records, .. } = current.as_ref()
                && records
                    .records
                    .first()
                    .is_some_and(|first| first.sequence <= after)
            {
                let window = history_window(
                    Arc::clone(records),
                    ceiling.min(after.saturating_add(u64::from(budget))),
                    after,
                );
                self.retain(current, ceiling);
                return Ok(window);
            }
            let jump = current.jump();
            let next = if let Some(target) = jump.target.as_ref().filter(|_| jump.minimum > after) {
                Some((Arc::clone(target), ceiling.min(jump.minimum)))
            } else {
                current
                    .parent()
                    .map(|parent| (Arc::clone(parent), ceiling.min(current.boundary())))
            };
            self.retain(current, ceiling);
            (node, ceiling) = next.map_or((None, ceiling), |(node, ceiling)| (Some(node), ceiling));
        }
        Ok(None)
    }
}

impl crate::pages::PageSource for PinnedHistory {
    type Page = HistoryWindow;

    async fn next_page(
        &mut self,
        after: u64,
        budget: u32,
    ) -> Result<Option<Self::Page>, StreamError> {
        self.page(after, budget, || Ok(()))
    }
}
