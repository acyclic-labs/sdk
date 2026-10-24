struct RecordBatch {
    records: Vec<Record>,
    _allocation: Allocation,
}
enum History {
    Batch {
        parent: Option<Arc<Self>>,
        jump: HistoryJump,
        records: Arc<RecordBatch>,
        _allocation: Allocation,
    },
    Prefix {
        source: Option<Arc<Self>>,
        jump: HistoryJump,
        tail: u64,
        _allocation: Allocation,
    },
}

/// One funded link per node skips a power-of-two ancestry range. Its minimum
/// boundary proves that skipping cannot cross an append or fork cut required
/// by this read. The link is derived state and is rebuilt with the history.
struct HistoryJump {
    target: Option<Arc<History>>,
    ordinal: u64,
    minimum: u64,
}

impl HistoryJump {
    fn new(
        parent: Option<&Arc<History>>,
        boundary: u64,
        mut examine: impl FnMut() -> Result<(), StreamError>,
    ) -> Result<Self, StreamError> {
        let Some(mut cursor) = parent else {
            return Ok(Self {
                target: None,
                ordinal: 0,
                minimum: boundary,
            });
        };
        examine()?;
        let ordinal = cursor
            .jump()
            .ordinal
            .checked_add(1)
            .ok_or(StreamError::Capacity)?;
        let target = ordinal & (ordinal - 1);
        let mut minimum = boundary;
        loop {
            minimum = minimum.min(cursor.boundary());
            let jump = cursor.jump();
            if jump.ordinal == target {
                return Ok(Self {
                    target: Some(Arc::clone(cursor)),
                    ordinal,
                    minimum,
                });
            }
            if let Some(next) = jump
                .target
                .as_ref()
                .filter(|_| (jump.ordinal & (jump.ordinal - 1)) >= target)
            {
                minimum = minimum.min(jump.minimum);
                cursor = next;
            } else {
                cursor = cursor.parent().ok_or(StreamError::Unavailable)?;
            }
            examine()?;
        }
    }
}

impl History {
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
