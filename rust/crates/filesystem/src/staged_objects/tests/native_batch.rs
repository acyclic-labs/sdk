use super::*;
use acyclic_test_support::differential::{observe, record};

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Write(ObjectId, Vec<u8>),
    Read(ObjectId),
}

struct Observed<S>(S);

impl<S: AsyncObjectStore> AsyncObjectStore for Observed<S> {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        record(Event::Write(id, bytes.to_vec()));
        self.0.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        for write in writes {
            record(Event::Write(write.object_id, write.bytes.to_vec()));
        }
        self.0.put_many(writes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        record(Event::Read(id));
        self.0.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.0.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.0.contains(id, work).await
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Individual,
    Batch,
    Staged,
}

// Each run creates its own provider, journal, staging, and ledger. The old
// implementation writes each object separately; Batch is an independent batch
// oracle for the exact additional staging work and allocations.
fn run(
    runtime: &tokio::runtime::Runtime,
    mode: Mode,
    cancelled: bool,
    count: usize,
) -> ((Result<Vec<Vec<u8>>, String>, WorkCounters), Vec<Event>) {
    observe(
        || {
            runtime.block_on(async {
                let directory = tempfile::tempdir().unwrap();
                let (old, provider) = open_staged(directory.path()).await.unwrap();
                let store =
                    StagedObjects::open(Observed(old.inner), directory.path().to_path_buf())
                        .await
                        .unwrap();
                let token = CancellationToken::new();
                let writes = (0..count)
                    .map(|index| {
                        let (object_id, bytes) = blob(OwnedBytes::from(vec![index as u8; 32]));
                        ObjectWrite { object_id, bytes }
                    })
                    .collect::<Vec<_>>();
                let ids = writes
                    .iter()
                    .map(|write| write.object_id)
                    .collect::<Vec<_>>();
                store
                    .put_many(
                        &writes,
                        &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
                    )
                    .await
                    .unwrap();
                let native = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token).unwrap();
                if cancelled {
                    token.cancel();
                }
                (directory, provider, store, writes, ids, token, native)
            })
        },
        |(directory, provider, store, writes, ids, token, native)| {
            runtime.block_on(async {
        let began = std::time::Instant::now();
        let mut work = AuthorityWork::native(&native, &token);
        let result = match mode {
            Mode::Staged => store.flush_before_publish(PublicationScope::Closure {
                objects: &ids, proven_at: 0,
            }, &mut work).await.map(drop),
            Mode::Batch => {
                match work.accounting().check().map_err(crate::async_storage::object_error) {
                    Ok(()) => store.inner.put_many(&writes, &mut work).await,
                    Err(error) => Err(error),
                }
            }
            Mode::Individual => {
                let mut result = work.accounting().check().map_err(crate::async_storage::object_error);
                for write in &writes {
                    if result.is_err() { break; }
                    result = store.inner.put(write.object_id, write.bytes.clone(), &mut work).await;
                }
                result
            }
        };
        let elapsed = began.elapsed();
        let cost = native.receipt();
        let result = match result {
            Err(error) => Err(error.to_string()),
            Ok(()) => {
                let read_token = CancellationToken::new();
                let mut read_work = AuthorityWork::new(WorkBudget::UNBOUNDED, &read_token);
                let mut output = Vec::new();
                for &id in &ids {
                    output.push(store.inner.read(id, 32, &mut read_work).await.unwrap().bytes.to_vec());
                }
                Ok(output)
            }
        };
        eprintln!("staged publication {mode:?} count={count} cancelled={cancelled}: {elapsed:?}, {cost:?}");
        drop(store);
        drop(provider);
        if result.is_ok() {
            assert!(reopen_contains(directory.path(), &ids).await.unwrap());
        }
        (result, cost)
    })
        },
    )
}

fn exact_batch_work(actual: WorkCounters, batch: WorkCounters, count: usize) -> bool {
    actual.durability_operations == batch.durability_operations
        && actual.backend_write_operations == batch.backend_write_operations
        && actual.object_bytes_written == batch.object_bytes_written
        && actual.bytes_hashed == batch.bytes_hashed
        && actual.allocation_operations == batch.allocation_operations + 4
        && actual.items_examined == batch.items_examined + 3 * count as u64
}

#[test]
fn native_publication_batches_exact_work_with_individual_fsync_negative_control() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for count in [4, 8] {
        let ((old_result, old), old_trace) = run(&runtime, Mode::Individual, false, count);
        let ((batch_result, batch), batch_trace) = run(&runtime, Mode::Batch, false, count);
        let ((result, cost), trace) = run(&runtime, Mode::Staged, false, count);
        assert_eq!(result, old_result);
        assert_eq!(result, batch_result);
        assert_eq!(trace, old_trace);
        assert_eq!(trace, batch_trace);
        assert_eq!(cost.object_bytes_written, 32 * count as u64);
        assert!(
            exact_batch_work(cost, batch, count),
            "{cost:?} vs {batch:?}"
        );
        assert!(
            !exact_batch_work(old, batch, count),
            "per-object negative control passed"
        );
        assert!(old.durability_operations > cost.durability_operations);
    }
    let ((old_error, _), old_trace) = run(&runtime, Mode::Individual, true, 4);
    let ((error, cost), trace) = run(&runtime, Mode::Staged, true, 4);
    assert!(error.is_err());
    assert_eq!(error, old_error);
    assert_eq!(trace, old_trace);
    assert_eq!(cost.backend_write_operations, 0);
    assert_eq!(cost.durability_operations, 0);
}
