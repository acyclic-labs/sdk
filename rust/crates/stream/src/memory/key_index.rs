// The original exact-key/global-cut radix is shared with every primitive.
// Values are sequence positions only; original records remain in their history.
const RECORD_KEY_WIDTH: usize = crate::MAX_RECORD_KEY_BYTES + 8;
type RecordKeys = acyclic_collections::KeyedIndex<(), Allocation, RECORD_KEY_WIDTH>;

#[derive(Clone, Default)]
struct KeyTree {
    // Current paths and admitted key reads own roots. Historical record nodes
    // never keep superseded radix paths alive. A fork's cut is allocation-free.
    root: Option<Arc<KeyRoot>>,
    before: Option<u64>,
}

struct KeyRoot {
    index: RecordKeys,
    _allocation: Allocation,
}

impl KeyTree {
    fn cap(&self, before: u64) -> Self {
        Self {
            root: self.root.clone(),
            before: Some(self.before.map_or(before, |old| old.min(before))),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn insertion_bound(&self, count: u64) -> Option<acyclic_collections::IndexInsertionBound> {
        let mut bound = match &self.root {
            Some(root) => root.index.insertion_bound(count),
            None => RecordKeys::default().insertion_bound(count),
        }?;
        if count != 0 {
            bound.allocation_bytes = bound
                .allocation_bytes
                .checked_add(acyclic_collections::arc_allocation_bytes::<KeyRoot>() as u64)?;
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

    fn append_reserved(
        &self,
        records: &Arc<RecordBatch>,
        work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<Self, StreamError> {
        let mut tree = self.root.as_ref().map_or_else(RecordKeys::default, |root| {
            self.before
                .map_or_else(|| root.index.clone(), |before| root.index.cap(before))
        });
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
        if !changed {
            return Ok(self.clone());
        }
        let allocation =
            work.claim(acyclic_collections::arc_allocation_bytes::<KeyRoot>() as u64)?;
        Ok(Self {
            root: Some(Arc::new(KeyRoot {
                index: tree,
                _allocation: allocation,
            })),
            before: None,
        })
    }

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
        let empty;
        let index = if let Some(root) = &self.root {
            &root.index
        } else {
            empty = RecordKeys::default();
            &empty
        };
        index
            .entry(
                &range,
                read.order == crate::ReadOrder::LatestFirst,
                &mut work,
            )
            .map(|entry| entry.map(|entry| entry.sequence))
    }
}
