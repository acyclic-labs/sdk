use super::*;
use acyclic_test_support::differential::{observe, record};

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Write(ObjectId, Vec<u8>),
    Read(ObjectId),
}

struct Observed<S>(S, Option<std::fs::File>, std::sync::atomic::AtomicU64);

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
        let original = work.original.ok_or(ObjectStoreError::NativeUnavailable)?;
        let before = original.receipt().allocation_operations;
        self.0.put_many(writes, work).await?;
        self.2.fetch_add(
            original.receipt().allocation_operations - before,
            Ordering::Relaxed,
        );
        if let Some(file) = &self.1 {
            let original = work.original.ok_or(ObjectStoreError::NativeUnavailable)?;
            acyclic_native_runtime::sync_file(
                file,
                acyclic_native_runtime::Durability::Full,
                Some(original.runtime_meter().as_ref()),
            )
            .map_err(|error| ObjectStoreError::Rejected(error.to_string()))?;
        }
        Ok(())
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
    ExtraFlush,
}

type Contents = Result<Vec<Vec<u8>>, String>;
type PublicationRun = (Contents, WorkCounters, Vec<Event>, Option<u64>);

// Each run creates its own provider, journal, staging, and ledger. The old
// implementation writes each object separately; Batch is an independent batch
// oracle for the additional staging work. Backend allocations are measured
// within each publication: worker-queue contention may allocate a capacity
// waiter in only one of two independent backend runs.
fn run(
    runtime: &tokio::runtime::Runtime,
    mode: Mode,
    cancelled: bool,
    count: usize,
) -> Result<PublicationRun, Box<dyn std::error::Error>> {
    let (result, trace) = observe(
        || {
            runtime.block_on(async {
                let directory = tempfile::tempdir()?;
                let (old, provider) = open_staged(directory.path()).await?;
                let extra = if matches!(mode, Mode::ExtraFlush) {
                    Some(tempfile::tempfile()?)
                } else {
                    None
                };
                let store = StagedObjects::open(
                    Observed(old.inner, extra, std::sync::atomic::AtomicU64::new(0)),
                    directory.path().to_path_buf(),
                )
                .await?;
                let token = CancellationToken::new();
                let mut writes = Vec::new();
                for index in 0..count {
                    let marker = u8::try_from(index)?;
                    let (object_id, bytes) = blob(OwnedBytes::from(vec![marker; 32]));
                    writes.push(ObjectWrite { object_id, bytes });
                }
                let ids = writes
                    .iter()
                    .map(|write| write.object_id)
                    .collect::<Vec<_>>();
                store
                    .put_many(
                        &writes,
                        &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
                    )
                    .await?;
                let native = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token)
                    .map_err(|(error, _)| error)?;
                if cancelled {
                    token.cancel();
                }
                Ok::<_, Box<dyn std::error::Error>>((
                    directory, provider, store, writes, ids, token, native,
                ))
            })
        },
        |state| {
            runtime.block_on(async {
                let (directory, provider, store, writes, ids, token, native) = state?;
                let initial_allocations = native.receipt().allocation_operations;
                let began = std::time::Instant::now();
                let mut work = AuthorityWork::native(&native, &token);
                let result = match mode {
                    Mode::Staged | Mode::ExtraFlush => store
                        .flush_before_publish(
                            PublicationScope::Closure { objects: &ids, proven_at: 0 },
                            &mut work,
                        )
                        .await
                        .map(drop),
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
                let backend = store.inner.2.load(Ordering::Relaxed);
                let backend = (backend != 0).then_some(initial_allocations + backend);
                let result = if let Err(error) = result {
                    Err(error.to_string())
                } else {
                        let read_token = CancellationToken::new();
                        let mut read_work = AuthorityWork::new(WorkBudget::UNBOUNDED, &read_token);
                        let mut output = Vec::new();
                        for &id in &ids {
                            output.push(store.inner.read(id, 32, &mut read_work).await?.bytes.to_vec());
                        }
                    Ok(output)
                };
                eprintln!(
                    "staged publication {mode:?} count={count} cancelled={cancelled}: {elapsed:?}, {cost:?}, backend_allocations={backend:?}"
                );
                drop(store);
                drop(provider);
                if result.is_ok() {
                    assert!(reopen_contains(directory.path(), &ids).await?);
                }
                Ok::<_, Box<dyn std::error::Error>>((result, cost, backend))
            })
        },
    );
    let (result, cost, backend) = result?;
    Ok((result, cost, trace, backend))
}

fn exact_batch_work(
    actual: WorkCounters,
    batch: WorkCounters,
    backend: Option<u64>,
    count: usize,
) -> bool {
    // Independent journals have real timestamps. Their protobuf varint widths
    // change frame length: each extra byte is hashed once, encoded twice
    // (inline placement and final encoding), and copied once into the frame.
    // Pin the work beyond those physical output bytes, without clock coupling.
    // The observed backend baseline includes the ledger's initial allocation.
    // Staging adds exactly the flush frame and the three drain vectors.
    actual.durability_operations == 1
        && batch.durability_operations == 1
        && actual.backend_write_operations == batch.backend_write_operations
        && actual.object_bytes_written == batch.object_bytes_written
        && actual.bytes_hashed + batch.output_bytes == batch.bytes_hashed + actual.output_bytes
        && actual.bytes_encoded + 2 * batch.output_bytes
            == batch.bytes_encoded + 2 * actual.output_bytes
        && actual.bytes_copied + batch.output_bytes == batch.bytes_copied + actual.output_bytes
        && backend.is_some_and(|operations| actual.allocation_operations == operations + 4)
        && actual.items_examined == batch.items_examined + 3 * count as u64
}

#[test]
fn native_publication_batches_exact_work_with_individual_fsync_negative_control()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    for count in [4, 8] {
        let (old_result, old, old_trace, old_backend) =
            run(&runtime, Mode::Individual, false, count)?;
        let (batch_result, batch, batch_trace, batch_backend) =
            run(&runtime, Mode::Batch, false, count)?;
        let (result, cost, trace, backend) = run(&runtime, Mode::Staged, false, count)?;
        let (extra_result, extra, extra_trace, extra_backend) =
            run(&runtime, Mode::ExtraFlush, false, count)?;
        assert_eq!(Some(batch.allocation_operations), batch_backend);
        assert_eq!(result, old_result);
        assert_eq!(result, batch_result);
        assert_eq!(trace, old_trace);
        assert_eq!(trace, batch_trace);
        assert_eq!(extra_result, result);
        assert_eq!(extra_trace, trace);
        assert_eq!(cost.durability_operations, 1);
        assert_eq!(batch.durability_operations, 1);
        assert_eq!(old.durability_operations, count as u64);
        assert_eq!(extra.durability_operations, 2);
        assert!(
            !exact_batch_work(extra, batch, extra_backend, count),
            "extra flush passed the same work pins"
        );
        assert_eq!(cost.object_bytes_written, 32 * count as u64);
        assert!(
            exact_batch_work(cost, batch, backend, count),
            "{cost:?} vs {batch:?}"
        );
        assert!(
            !exact_batch_work(old, batch, old_backend, count),
            "per-object negative control passed"
        );
    }
    let (old_error, _, old_trace, _) = run(&runtime, Mode::Individual, true, 4)?;
    let (error, cost, trace, _) = run(&runtime, Mode::Staged, true, 4)?;
    assert!(error.is_err());
    assert_eq!(error, old_error);
    assert_eq!(trace, old_trace);
    assert_eq!(cost.backend_write_operations, 0);
    assert_eq!(cost.durability_operations, 0);
    Ok(())
}
