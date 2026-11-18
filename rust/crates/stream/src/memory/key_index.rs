// The original exact-key/global-cut radix is shared with every primitive.
// Values are sequence positions only; original records remain in their history.
const RECORD_KEY_WIDTH: usize = acyclic_stream_wire::headers::MAX_RECORD_KEY_BYTES + 8;
type RecordKeys = acyclic_collections::KeyedIndex<(), Allocation, RECORD_KEY_WIDTH>;

impl LineageIndex for RecordKeys {
    fn cut(
        &self,
        before: u64,
        _work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<Self, StreamError> {
        Ok(self.cap(before))
    }
}

/// A path version's view of its key lineage.
type KeyTree = IndexLineage<RecordKeys>;

/// One reader's immutable key index at its view's cut.
#[derive(Clone, Default)]
struct KeyPin {
    index: RecordKeys,
    before: u64,
}

impl KeyTree {
    fn key_pin(&self, tail: u64) -> KeyPin {
        let before = self.before.map_or(tail, |before| before.min(tail));
        KeyPin {
            index: self.newest().cap(before),
            before,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn insertion_bound(&self, count: u64) -> Option<acyclic_collections::IndexInsertionBound> {
        let mut bound = self.newest().insertion_bound(count)?;
        if count != 0 {
            bound.allocation_bytes =
                bound
                    .allocation_bytes
                    .checked_add(
                        acyclic_collections::arc_allocation_bytes::<Lineage<RecordKeys>>() as u64,
                    )?;
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

    /// Extends this view with `records`, which start at the view's tail.
    fn append_reserved(
        &self,
        records: &Arc<RecordBatch>,
        work: &mut impl acyclic_collections::IndexWork<Error = StreamError, Allocation = Allocation>,
    ) -> Result<Self, StreamError> {
        let (mut tree, extends) = self.base(&records.origin, records.start, work)?;
        let mut changed = false;
        for (index, record) in records.values.iter().enumerate() {
            for key in record.headers.keys() {
                let key = acyclic_collections::binary_key::<RECORD_KEY_WIDTH, _>(key, work)
                    .map_err(|error| match error {
                        acyclic_collections::BinaryKeyError::Work(error) => error,
                        _ => StreamError::InvalidArgument,
                    })?;
                tree = tree.insert(&key, records.start + index as u64, (), work)?;
                changed = true;
            }
        }
        self.publish(&records.origin, records.start, extends, changed, tree, work)
    }

    #[cfg(test)]
    fn seek(
        &self,
        read: &crate::ReadKeySeek,
        from: u64,
        before: u64,
        examine: &mut impl FnMut(u64) -> Result<(), StreamError>,
    ) -> Result<Option<u64>, StreamError> {
        self.key_pin(before).seek(read, from, before, examine)
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
            before: self.before.min(before),
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
