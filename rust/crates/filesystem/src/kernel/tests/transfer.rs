use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::memory::MemoryObjectStore;

#[path = "../../../tests/support/transfer_manifest.rs"]
mod transfer_manifest;
use transfer_manifest::fixture;

#[tokio::test]
async fn import_keeps_cumulative_work_live_backing_and_cancelled_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, bodies) = fixture();
    let cancellation = CancellationToken::new();
    let reference_store = MemoryObjectStore::default();
    let mut reference =
        crate::AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    let mut first = WorkCounters::UNCHARGED;
    for (index, (id, body)) in manifest.objects.iter().zip(&bodies).enumerate() {
        reference_store
            .put_stored(*id, body.clone(), &mut reference)
            .await?;
        if index == 0 {
            first = reference.spent().unwrap();
        }
    }
    let expected = reference.spent().unwrap();
    let live = reference.allocations.live_bytes();
    assert!(
        live > 0,
        "destination backing must retain its original issuer"
    );
    let budget = WorkBudget {
        backend_write_operations: bodies.len() as u64,
        peak_allocation_bytes: expected.peak_allocation_bytes,
        ..crate::performance::BOUNDARY_BUDGET
    };
    let imported = MemoryObjectStore::default();
    let receipt = import_generation_batch_async(
        &imported,
        &manifest,
        TransferCursor::START,
        &bodies,
        2,
        budget,
        &cancellation,
    )
    .await?;
    assert_eq!(receipt.value, TransferCursor::new(2));
    assert_eq!(
        receipt.work, expected,
        "one original frame funds the whole page"
    );
    for (id, body) in manifest.objects.iter().zip(&bodies) {
        let read = imported
            .read(*id, body.len() as u64, &mut reference)
            .await?;
        assert_eq!(read.bytes, *body);
    }
    drop(reference_store);
    assert!(reference.allocations.live_bytes() < live);

    // The deciding write is denied against the same cumulative allowance.
    for (bound, counter) in [
        (
            WorkBudget {
                backend_write_operations: 1,
                ..budget
            },
            "backend_write_operations",
        ),
        (
            WorkBudget {
                peak_allocation_bytes: expected.peak_allocation_bytes - 1,
                ..budget
            },
            "peak_allocation_bytes",
        ),
    ] {
        let limited = MemoryObjectStore::default();
        let failure = import_generation_batch_async(
            &limited,
            &manifest,
            TransferCursor::START,
            &bodies,
            2,
            bound,
            &cancellation,
        )
        .await
        .err()
        .ok_or("a second write escaped its original credit")?;
        assert!(matches!(
            failure.error,
            GenerationTransferError::Object(ObjectStoreError::Work(WorkError::BudgetExceeded {
                counter: observed, ..
            })) if observed == counter
        ));
        failure.work.as_deref().unwrap().verify(bound)?;
        assert!(matches!(
            limited
                .read(manifest.objects[1], 1024, &mut reference)
                .await,
            Err(ObjectStoreError::Missing)
        ));
    }

    // The actual first write settles before caller or completion-producer loss.
    for cancelled in [false, true] {
        let token = CancellationToken::new();
        let (producer, receiver) = tokio::sync::oneshot::channel();
        let paused = crate::test_support::PausedStore::new(
            std::sync::Arc::new(MemoryObjectStore::default()),
            Some(receiver),
        );
        let mut importing = Box::pin(import_generation_batch_async(
            &paused,
            &manifest,
            TransferCursor::START,
            &bodies,
            2,
            budget,
            &token,
        ));
        assert!(futures::poll!(importing.as_mut()).is_pending());
        if cancelled {
            token.cancel();
            producer
                .send(())
                .map_err(|_| "import lost its original completion")?;
        } else {
            drop(producer);
        }
        let failure = importing.await.err().ok_or("withdrawn import continued")?;
        if cancelled {
            assert!(matches!(
                failure.error,
                GenerationTransferError::Cancelled(_)
            ));
        } else {
            assert!(matches!(failure.error, GenerationTransferError::Object(
                ObjectStoreError::Rejected(ref cause)
            ) if cause == "cache fill producer lost"));
        }
        assert_eq!(*failure.work().unwrap(), first);
        let read = paused
            .inner
            .read(manifest.objects[0], 1024, &mut reference)
            .await?;
        assert_eq!(read.bytes, bodies[0]);
        assert!(matches!(
            paused
                .inner
                .read(manifest.objects[1], 1024, &mut reference)
                .await,
            Err(ObjectStoreError::Missing)
        ));
    }
    Ok(())
}
