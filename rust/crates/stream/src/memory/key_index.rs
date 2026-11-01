// The original exact-key/global-cut radix is shared with every primitive.
// Values are sequence positions only; original records remain in their history.
const RECORD_KEY_WIDTH: usize = crate::MAX_RECORD_KEY_BYTES + 8;
type RecordKeys = acyclic_collections::KeyedIndex<(), Allocation, RECORD_KEY_WIDTH>;

#[derive(Clone, Default)]
struct KeyTree(RecordKeys);

impl KeyTree {
    fn cap(&self, before: u64) -> Self {
        Self(self.0.cap(before))
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
        let mut tree = self.clone();
        for record in &records.records {
            for key in record.headers.keys() {
                let key = acyclic_collections::binary_key::<RECORD_KEY_WIDTH, _>(key, work)
                    .map_err(|error| match error {
                        acyclic_collections::BinaryKeyError::Work(error) => error,
                        _ => StreamError::InvalidArgument,
                    })?;
                tree.0 = tree.0.insert(key, record.sequence, (), work)?;
            }
        }
        Ok(tree)
    }

    fn seek<E>(
        &self,
        key: &[u8],
        from: u64,
        before: u64,
        examine: &mut impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<u64>, E> {
        let mut work = HistoryReadWork(examine);
        let key = match acyclic_collections::binary_key::<RECORD_KEY_WIDTH, _>(key, &mut work) {
            Ok(key) => key,
            Err(acyclic_collections::BinaryKeyError::Work(error)) => return Err(error),
            // Public requests and stored headers already validate this width.
            Err(_) => return Ok(None),
        };
        self.0
            .seek(key, from, before, &mut work)
            .map(|position| position.map(|(sequence, ())| sequence))
    }
}
