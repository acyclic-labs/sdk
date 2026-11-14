// The original exact-key/global-cut radix is shared with every primitive.
// Values are sequence positions only; original records remain in their history.
const RECORD_KEY_WIDTH: usize = crate::MAX_RECORD_KEY_BYTES + 8;
type RecordKeys = acyclic_collections::KeyedIndex<(), Allocation, RECORD_KEY_WIDTH>;

/// A path version's view of its key lineage. Every version and fork of one
/// lineage shares the lineage's newest index and cuts it at its own tail, so
/// a retained namespace cut never keeps a superseded index alive: only a
/// reader's [`KeyPin`] does, until that reader is released.
#[derive(Clone, Default)]
struct KeyTree {
    lineage: Option<Arc<KeyLineage>>,
    before: Option<u64>,
}

struct KeyLineage {
    newest: crate::query_sync::SyncMutex<KeyNewest>,
    _allocation: Allocation,
}

// Every holder's tail is at most `through`, and a holder reads only below its
// tail, so an extension adds entries no existing holder can observe.
struct KeyNewest {
    index: RecordKeys,
    through: u64,
}

/// One reader's immutable key index at its view's cut.
#[derive(Clone, Default)]
struct KeyPin {
    index: RecordKeys,
    before: Option<u64>,
}

impl KeyLineage {
    fn with<T>(&self, use_newest: impl FnOnce(&mut KeyNewest) -> T) -> T {
        use_newest(
            &mut self
                .newest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl KeyTree {
    fn cap(&self, before: u64) -> Self {
        Self {
            lineage: self.lineage.clone(),
            before: Some(self.before.map_or(before, |old| old.min(before))),
        }
    }

    /// The newest index, cut at this view and at the holder's `tail`.
    fn pin(&self, tail: u64) -> KeyPin {
        KeyPin {
            index: self
                .lineage
                .as_ref()
                .map_or_else(RecordKeys::default, |lineage| {
                    lineage.with(|newest| newest.index.clone())
                }),
            before: Some(self.before.map_or(tail, |before| before.min(tail))),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn insertion_bound(&self, count: u64) -> Option<acyclic_collections::IndexInsertionBound> {
        let mut bound = self.pin(u64::MAX).index.insertion_bound(count)?;
        if count != 0 {
            bound.allocation_bytes = bound
                .allocation_bytes
                .checked_add(acyclic_collections::arc_allocation_bytes::<KeyLineage>() as u64)?;
        }
        Some(bound)
    }

    fn append(
        &self,
        records: &Arc<RecordBatch>,
        provider: &MemoryStream,
    ) -> Result<Self, StreamError> {
        self.append_reserved(records, &mut HistoryIndexWork(provider))
    }

    /// Extends this view with `records`, which start at the view's tail. The
    /// lineage is extended in place when this view is its newest; otherwise
    /// the view's cut starts a new lineage.
    fn append_reserved(
        &self,
        records: &Arc<RecordBatch>,
        work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<Self, StreamError> {
        let cut = self
            .before
            .map_or(records.start, |before| before.min(records.start));
        let through = records.start + records.values.len() as u64;
        let (mut tree, newest) = match &self.lineage {
            Some(lineage) => lineage.with(|newest| {
                if newest.through == cut {
                    (newest.index.clone(), true)
                } else {
                    (newest.index.cap(cut), false)
                }
            }),
            None => (RecordKeys::default(), false),
        };
        let mut changed = false;
        for (index, record) in records.values.iter().enumerate() {
            for key in record.headers.keys() {
                let key = acyclic_collections::binary_key::<RECORD_KEY_WIDTH, _>(key, work)
                    .map_err(|error| match error {
                        acyclic_collections::BinaryKeyError::Work(error) => error,
                        _ => StreamError::InvalidArgument,
                    })?;
                tree = tree.insert(key, records.start + index as u64, (), work)?;
                changed = true;
            }
        }
        if let Some(lineage) = self.lineage.as_ref().filter(|_| newest) {
            // The superseded index retires after the lock, once its last
            // reader pin is released.
            let superseded = lineage.with(|current| {
                (current.through == cut).then(|| {
                    current.through = through;
                    std::mem::replace(&mut current.index, tree.clone())
                })
            });
            if superseded.is_some() {
                return Ok(Self {
                    lineage: Some(Arc::clone(lineage)),
                    before: None,
                });
            }
        }
        if !changed {
            // No key was added; the lineage may hold entries at or above
            // `cut` that this path never published.
            return Ok(self.cap(cut));
        }
        let allocation =
            work.claim(acyclic_collections::arc_allocation_bytes::<KeyLineage>() as u64)?;
        Ok(Self {
            lineage: Some(Arc::new(KeyLineage {
                newest: crate::query_sync::SyncMutex::new(KeyNewest {
                    index: tree,
                    through,
                }),
                _allocation: allocation,
            })),
            before: None,
        })
    }

    #[cfg(test)]
    fn seek<E>(
        &self,
        read: &crate::ReadKeySeek,
        from: u64,
        before: u64,
        examine: &mut impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<u64>, E> {
        self.pin(before).seek(read, from, before, examine)
    }
}

impl KeyPin {
    /// The nearest selected position at cursor `from`, in the read's order.
    fn seek<E>(
        &self,
        read: &crate::ReadKeySeek,
        from: u64,
        before: u64,
        examine: &mut impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<u64>, E> {
        let mut work = HistoryReadWork(examine);
        let mut encode = |key: &[u8]| {
            match acyclic_collections::binary_key::<RECORD_KEY_WIDTH, _>(key, &mut work) {
                Ok(key) => Ok(Some(key)),
                Err(acyclic_collections::BinaryKeyError::Work(error)) => Err(error),
                // Public requests and stored headers already validate this width.
                Err(_) => Ok(None),
            }
        };
        let Some(first) = encode(&read.key)? else {
            return Ok(None);
        };
        let last = match &read.end {
            None => first,
            Some(end) => match encode(end)?.and_then(acyclic_collections::KeyedRange::below) {
                Some(last) => last,
                None => return Ok(None),
            },
        };
        let range = acyclic_collections::KeyedRange {
            first,
            last,
            from,
            before: self.before.map_or(before, |cut| cut.min(before)),
        };
        self.index
            .entry(
                &range,
                read.order == crate::ReadOrder::LatestFirst,
                &mut work,
            )
            .map(|entry| entry.map(|entry| entry.sequence))
    }
}
