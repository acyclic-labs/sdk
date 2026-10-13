struct RecordBatch {
    records: Vec<Record>,
    _allocation: Allocation,
}
impl std::ops::Deref for RecordBatch {
    type Target = [Record];
    fn deref(&self) -> &[Record] {
        &self.records
    }
}

enum History {
    Batch {
        parent: Option<Arc<Self>>,
        records: Arc<RecordBatch>,
        _allocation: Allocation,
    },
    Prefix {
        source: Option<Arc<Self>>,
        tail: u64,
        _allocation: Allocation,
    },
}

impl Drop for History {
    fn drop(&mut self) {
        let mut cursor = match self {
            Self::Batch { parent, .. } => parent.take(),
            Self::Prefix { source, .. } => source.take(),
        };
        while let Some(node) = cursor {
            let Ok(mut history) = Arc::try_unwrap(node) else {
                break;
            };
            cursor = match &mut history {
                Self::Batch { parent, .. } => parent.take(),
                Self::Prefix { source, .. } => source.take(),
            };
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
        let ceiling = self.ceiling;
        match node {
            History::Batch {
                parent, records, ..
            } => {
                self.next = parent.as_ref();
                if let Some(first) = records.first() {
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

fn collect_history_windows(
    history: Option<&Arc<History>>,
    tail: u64,
    from: u64,
    windows: &mut Vec<HistoryWindow>,
) {
    for (node, ceiling) in HistoryWalk::new(history, tail, from) {
        if let History::Batch { records, .. } = node
            && let Some(window) = history_window(Arc::clone(records), ceiling, from)
        {
            windows.push(window);
        }
    }
    windows.reverse();
}

fn history_window(records: Arc<RecordBatch>, ceiling: u64, from: u64) -> Option<HistoryWindow> {
    let first = records.first()?.sequence;
    let batch_end = records
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
