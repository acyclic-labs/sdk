use super::*;

struct Store(crate::MemoryObjectStore);
type Event = (u8, ObjectId, u64);
impl AsyncObjectStore for Store {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        differential::record::<Event>((0, id, bytes.len() as u64));
        self.0.put(id, bytes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        differential::record::<Event>((1, id, maximum));
        self.0.read(id, maximum, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        differential::record::<Event>((2, id, 0));
        self.0.contains(id, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        for request in requests {
            differential::record::<Event>((3, request.object_id, request.maximum_bytes));
        }
        crate::async_storage::read_many_sequential(self, requests, work).await
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn public_provider_calls_preserve_full_bytes_errors_work_and_backend_trace(
        entries in 1_u32..8, maximum_bytes in 1_u64..48,
        ops in prop::collection::vec((0_u8..7, 0_u8..8), 0..48),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let [baseline, candidate] = differential::pair::<_, _, Event>(
            || CachedObjectStore::new(Store(crate::MemoryObjectStore::default()), ObjectCacheOptions {
                maximum_entries: entries, maximum_bytes,
                maximum_in_flight: 4, maximum_waiters_per_object: 4,
            }).unwrap(),
            |store, historical| {
                if historical {
                    store.lock_state().unwrap().historical_recency = Some(BTreeSet::new());
                }
                runtime.block_on(async {
                    let token = CancellationToken::new();
                    let mut output = Vec::new();
                    for &(op, value) in [
                        (0, 0), (0, 1), (3, 0), (1, 0), (2, 1), (4, 0),
                        (0, 2), (6, 2), (1, 0), (5, 0), (1, 0),
                    ].iter().chain(&ops) {
                        let bytes: OwnedBytes = super::bytes(value).into();
                        let id = crate::HashedObject::new(crate::ObjectKind::BlobChunk, bytes.clone()).object_id();
                        let key = DecodedCacheKey::new::<Vec<u8>>(id, Default::default());
                        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
                        let result = match op {
                            0 => store.put(id, bytes, &mut work).await.map(|()| Output::Clear),
                            1 => store.read(id, 80, &mut work).await.map(|read| Output::Bytes(Some(read.bytes.to_vec()))),
                            2 => store.contains(id, &mut work).await.map(Output::Bool),
                            3 => store.decoded_cache_admit(key, decoded(value, 8)).map(|admission| {
                                let (shared, value) = match admission {
                                    DecodedCacheAdmission::Shared(value) => (true, value),
                                    DecodedCacheAdmission::Uncached(value) => (false, value),
                                };
                                Output::Admission(shared, value.value.downcast_ref::<Vec<u8>>().unwrap().clone())
                            }),
                            4 => store.decoded_cache_get(key).map(|value| Output::Bytes(value.map(|value|
                                value.value.downcast_ref::<Vec<u8>>().unwrap().clone()))),
                            5 => store.clear().map(|()| Output::Clear),
                            _ => {
                                let requests = [ObjectReadRequest { object_id: id, maximum_bytes: 0 },
                                    ObjectReadRequest { object_id: id, maximum_bytes: 80 }];
                                store.read_many(&requests, &mut work).await.map(|(reads, _)| {
                                    Output::Bytes(Some(reads.into_iter().flat_map(|read| read.bytes.to_vec()).collect()))
                                })
                            }
                        }.map_err(|error| format!("{error:?}"));
                        output.push((result, work.work, store.stats().unwrap()));
                        store.lock_state().unwrap().recency_order();
                    }
                    output
                })
            },
        );
        prop_assert_eq!(candidate, baseline);
    }
}
