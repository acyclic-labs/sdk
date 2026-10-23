/// Incremental cursor over immutable history.
pub(crate) struct HistoryCursor {
    root: Option<Arc<History>>,
    first: Option<HistoryWindow>,
    windows: VecDeque<HistoryWindow>,
    next: u64,
}
impl HistoryCursor {
    fn prepare(path: &PathState, from: u64, anchor: Option<CommitId>) -> Result<Self, StreamError> {
        if from > path.tail {
            return Err(StreamError::OutOfRange);
        }
        let start = if anchor.is_some() {
            from.checked_sub(1).ok_or(StreamError::InvalidArgument)?
        } else {
            from
        };
        let mut cursor = Self::new(path, path.tail, start);
        if let Some(anchor) = anchor {
            let record = cursor.next_record().ok_or(StreamError::PrefixNotRetained)?;
            if record.sequence != from - 1 || record.commit_id != anchor {
                return Err(StreamError::PrefixNotRetained);
            }
        }
        Ok(cursor)
    }
    fn peek_record(&self) -> Option<&Record> {
        let window = self.first.as_ref()?;
        window.records.records.get(window.start)
    }
    fn check_anchor(&self, path: &PathState, anchor: CommitId) -> Result<(), StreamError> {
        if let Some(root) = &self.root
            && HistoryWalk::new(path.history.as_ref(), path.tail, 0)
                .any(|(node, _)| std::ptr::eq(node, root.as_ref()))
        {
            return Ok(());
        }
        let from = self
            .next
            .checked_sub(1)
            .ok_or(StreamError::InvalidArgument)?;
        let mut previous = Self::new(path, self.next, from);
        if previous
            .next_record()
            .is_none_or(|record| record.sequence != from || record.commit_id != anchor)
        {
            return Err(StreamError::PrefixNotRetained);
        }
        Ok(())
    }
    pub(crate) fn at(next: u64) -> Self {
        Self {
            root: None,
            first: None,
            windows: VecDeque::new(),
            next,
        }
    }
    fn new(path: &PathState, tail: u64, next: u64) -> Self {
        if let Some(window) = path.prefix_window(next, tail) {
            return Self {
                root: path.history.clone(),
                first: Some(window),
                windows: VecDeque::new(),
                next,
            };
        }
        let mut cursor = Self::at(next);
        cursor.sync(path.history.as_ref(), tail);
        cursor
    }
    fn sync(&mut self, root: Option<&Arc<History>>, tail: u64) {
        if match (&self.root, root) {
            (Some(old), Some(root)) => Arc::ptr_eq(old, root),
            (None, None) => true,
            _ => false,
        } {
            return;
        }
        // The common follow update is one append above the retained root.
        // Reuse the cursor queue directly instead of allocating a scratch Vec.
        if let (Some(old), Some(current)) = (&self.root, root) {
            #[cfg(test)]
            HISTORY_VISITS.with(|count| count.set(count.get() + 1));
            if let History::Batch {
                parent: Some(parent),
                records,
                ..
            } = current.as_ref()
                && Arc::ptr_eq(old, parent)
            {
                if let Some(window) = history_window(Arc::clone(records), tail, self.next) {
                    self.push_window(window);
                }
                self.root = root.cloned();
                return;
            }
        }
        let mut added = Self::at(self.next);
        let mut crossed_prefix = false;
        for (node, ceiling) in HistoryWalk::new(root, tail, self.next) {
            if !crossed_prefix
                && self
                    .root
                    .as_ref()
                    .is_some_and(|old| std::ptr::eq(old.as_ref(), node))
            {
                if let Some(first) = added.first.take() {
                    self.push_window(first);
                }
                self.windows.extend(added.windows);
                self.root = root.cloned();
                return;
            }
            match node {
                History::Batch { records, .. } => {
                    if let Some(window) = history_window(Arc::clone(records), ceiling, self.next)
                        && let Some(first) = added.first.replace(window)
                    {
                        added.windows.push_front(first);
                    }
                }
                History::Prefix { .. } => crossed_prefix = true,
            }
        }
        self.first = added.first;
        self.windows = added.windows;
        self.root = root.cloned();
    }
    fn push_window(&mut self, window: HistoryWindow) {
        if self.first.is_none() {
            self.first = Some(window);
        } else {
            self.windows.push_back(window);
        }
    }
    fn next_record(&mut self) -> Option<Record> {
        let window = self.first.as_mut()?;
        let record = window.records.records.get(window.start)?.clone();
        window.start += 1;
        self.next = record.sequence.checked_add(1)?;
        if window.start == window.end {
            self.first = self.windows.pop_front();
        }
        Some(record)
    }
}
