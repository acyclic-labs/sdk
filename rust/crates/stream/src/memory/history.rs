use super::*;

pub(super) struct HistoryWindow {
    pub(super) records: Arc<NativeStreamOutput<Vec<Record>>>,
    pub(super) end: usize,
}

/// Incremental cursor over the immutable history graph used by `follow`.
pub(crate) struct HistoryCursor {
    pub(super) root: Option<Arc<History>>,
    pub(super) windows: Vec<HistoryWindow>,
    pub(super) window: usize,
    pub(super) next: u64,
}

impl HistoryCursor {
    pub(crate) fn at(next: u64) -> Self {
        Self {
            root: None,
            windows: Vec::new(),
            window: 0,
            next,
        }
    }

    pub(super) fn new(history: Option<&Arc<History>>, tail: u64, next: u64) -> Self {
        let mut cursor = Self::at(next);
        cursor.rebuild(history, tail);
        cursor
    }

    pub(super) fn rebuild(&mut self, root: Option<&Arc<History>>, tail: u64) {
        self.root = root.cloned();
        self.windows = collect_history_windows(root, tail);
        self.window = 0;
    }

    pub(super) fn sync(&mut self, history: Option<&Arc<History>>, tail: u64) {
        let Some(root) = history else {
            self.rebuild(None, tail);
            return;
        };
        if self.root.as_ref().is_some_and(|old| Arc::ptr_eq(old, root)) {
            return;
        }

        // Append suffixes extend the index; changed prefixes require rebuilding.
        let mut cursor = Some(Arc::clone(root));
        let mut suffix = Vec::new();
        let mut reaches_old = false;
        while let Some(node) = cursor {
            if self
                .root
                .as_ref()
                .is_some_and(|old| Arc::ptr_eq(old, &node))
            {
                reaches_old = true;
                break;
            }
            match node.as_ref() {
                History::Batch {
                    parent, records, ..
                } => {
                    suffix.push(Arc::clone(records));
                    cursor = parent.clone();
                }
                History::Prefix { .. } => break,
            }
        }
        if !reaches_old {
            self.rebuild(Some(root), tail);
            return;
        }

        suffix.reverse();
        let mut added = suffix
            .into_iter()
            .filter_map(|records| history_window(records, tail))
            .collect::<Vec<_>>();
        self.windows.append(&mut added);
        self.root = Some(Arc::clone(root));
    }

    pub(super) fn next_record(&mut self) -> Option<Record> {
        loop {
            let window = self.windows.get(self.window)?;
            let first = window.records.value.first()?.sequence;
            // Canonical batches are contiguous; the sequence determines the index.
            let index = usize::try_from(self.next.saturating_sub(first)).ok()?;
            if index < window.end {
                let record = window.records.value.get(index)?.clone();
                self.next = record.sequence.checked_add(1)?;
                return Some(record);
            }
            self.window += 1;
        }
    }
}

pub(super) fn collect_history_windows(
    history: Option<&Arc<History>>,
    tail: u64,
) -> Vec<HistoryWindow> {
    let mut cursor = history.cloned();
    let mut ceiling = tail;
    let mut batches = Vec::new();
    while let Some(history) = cursor {
        match history.as_ref() {
            History::Batch {
                parent, records, ..
            } => {
                batches.push((Arc::clone(records), ceiling));
                cursor = parent.clone();
            }
            History::Prefix { source, tail, .. } => {
                ceiling = ceiling.min(*tail);
                cursor = source.clone();
            }
        }
    }
    batches.reverse();
    batches
        .into_iter()
        .filter_map(|(records, ceiling)| history_window(records, ceiling))
        .collect()
}

pub(super) fn history_window(
    records: Arc<NativeStreamOutput<Vec<Record>>>,
    ceiling: u64,
) -> Option<HistoryWindow> {
    let first = records.value.first()?.sequence;
    let batch_end = records
        .value
        .len()
        .try_into()
        .ok()
        .and_then(|length: u64| first.checked_add(length))
        .unwrap_or(u64::MAX);
    let end_sequence = ceiling.min(batch_end);
    if end_sequence <= first {
        return None;
    }
    let end = usize::try_from(end_sequence - first).ok()?;
    Some(HistoryWindow { records, end })
}

pub(super) fn read_history(
    history: Option<&Arc<History>>,
    tail: u64,
    from: u64,
    limit: usize,
    meter: WireMeter<'_>,
) -> MemoryResult<(NativeStreamOutput<Vec<Record>>, Option<std::io::Error>)> {
    // The request limit bounds this stack index.
    let end = tail.min(from.saturating_add(limit as u64));
    let capacity =
        usize::try_from(end.saturating_sub(from)).map_err(|_| StreamError::LimitExceeded)?;
    let mut output = vec(meter, capacity)?;
    let mut cursor = history;
    let mut ceiling = tail;
    let mut windows: [Option<&[Record]>; MAX_ITEMS] = [None; MAX_ITEMS];
    let mut count = 0;
    while ceiling > from
        && let Some(node) = cursor
    {
        admit_item(meter)?;
        match node.as_ref() {
            History::Batch {
                parent, records, ..
            } => {
                if let Some(first) = records.value.first().map(|record| record.sequence) {
                    let offset = |sequence: u64| {
                        usize::try_from(sequence.saturating_sub(first))
                            .map_or(records.value.len(), |offset| {
                                offset.min(records.value.len())
                            })
                    };
                    let start = offset(from);
                    if let Some(slice) = records
                        .value
                        .get(start..offset(ceiling.min(end)).max(start))
                        && !slice.is_empty()
                    {
                        let slot = windows.get_mut(count).ok_or(StreamError::Unavailable)?;
                        *slot = Some(slice);
                        count += 1;
                    }
                    ceiling = ceiling.min(first);
                }
                cursor = parent.as_ref();
            }
            History::Prefix { source, tail, .. } => {
                ceiling = ceiling.min(*tail);
                cursor = source.as_ref();
            }
        }
        record_item(meter)?;
    }
    let mut terminal = None;
    for window in windows.iter().take(count).rev().flatten() {
        for record in *window {
            if let Err(error) = admit_item(meter) {
                terminal = Some(error);
                break;
            }
            let mut read = match crate::memory::bytes(meter, false, record.value.len()) {
                Ok(read) => read,
                Err(error) => {
                    terminal = Some(error);
                    break;
                }
            };
            // Clone only immutable aliases, never a guessed body capacity/copy.
            output.value.push(record.clone());
            let read = record_bytes(&mut read, record.value.len());
            let examined = record_item(meter);
            if let Err(error) = read.and(examined).and_then(|()| check(meter)) {
                terminal = Some(error);
                break;
            }
        }
        if terminal.is_some() {
            break;
        }
    }
    Ok((output, terminal))
}

pub(super) fn is_direct_child(parent: &StreamPath, candidate: &StreamPath) -> bool {
    candidate.as_str().rsplit_once('/').map(|(path, _)| path) == Some(parent.as_str())
}
