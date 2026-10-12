use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_batch_keeps_all_acknowledged_prefix_and_original_error()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, work, cancellation) =
        observed_native_store(WorkBudget::UNBOUNDED, false, false, true);
    let writes = [
        b"first".as_slice(),
        b"second".as_slice(),
        b"third".as_slice(),
    ]
    .map(|body| ObjectWrite {
        object_id: ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, body),
        },
        bytes: OwnedBytes::copy_from_slice(body),
    });
    let error = ({
        store
            .put_many(
                &writes,
                &mut crate::AuthorityWork::native(&work, &cancellation),
            )
            .await
    })
    .err()
    .ok_or("partial batch unexpectedly succeeded")?;
    assert!(matches!(
        error,
        ObjectStoreError::LogicalObjects(Error {
            code: wire::ErrorCode::Unavailable
        })
    ));
    assert_eq!(work.receipt().object_bytes_written, 10);
    assert!(work.receipt().bytes_hashed >= 26 + 3 * crate::OBJECT_DIGEST_ENVELOPE_BYTES);
    for index in [0, 2] {
        assert_eq!(
            store
                .provider
                .inner
                .get(
                    wire::GetObjectRequest {
                        bucket: Some(store.bucket().clone()),
                        object_key: object_key(writes[index].object_id),
                        ..Default::default()
                    },
                    10
                )
                .await?
                .body()
                .as_ref(),
            writes[index].bytes.as_ref()
        );
    }
    assert!(
        store
            .provider
            .inner
            .get(
                wire::GetObjectRequest {
                    bucket: Some(store.bucket().clone()),
                    object_key: object_key(writes[1].object_id),
                    ..Default::default()
                },
                10
            )
            .await
            .is_err()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_batch_is_one_original_provider_call_with_retained_vector_and_aliases()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.object_bytes_read = 8;
    budget.peak_allocation_bytes = 100_000;
    let (store, work, cancellation) = observed_native_store(budget, false, false, false);
    let initial_operations = work.receipt().allocation_operations;
    let baseline = work.remaining()?.peak_allocation_bytes;
    let requests = [ObjectReadRequest {
        object_id: native_test_id(),
        maximum_bytes: 4,
    }; 2];
    let batch = ({
        store
            .read_many(
                &requests,
                &mut crate::AuthorityWork::native(&work, &cancellation),
            )
            .await
            .map(|(values, allocation)| (values, allocation.expect("native batch allocation")))
    })?;
    assert_eq!(store.provider.calls.load(SeqCst), 1);
    assert_eq!(store.provider.maximum.load(SeqCst), 8);
    assert_eq!(work.receipt().object_bytes_read, 8);
    assert_eq!(work.receipt().bytes_hashed, 8);
    // Both vector frames, original request controls and both prepared owners are funded.
    assert!(work.receipt().allocation_operations - initial_operations >= 16);
    assert_eq!(
        work.receipt().bytes_copied,
        2 * (store.bucket.name.len() as u64 + object_key(native_test_id()).len() as u64) + 8
    );
    let alias = batch.0[0].bytes.clone();
    let (reads, vector_custody) = batch;
    assert!(vector_custody.work().shares_ledger(&work));
    assert_eq!(
        vector_custody.bytes(),
        reads.capacity() as u64 * size_of::<ObjectRead>() as u64
    );
    drop(reads);
    let with_vector = work.remaining()?.peak_allocation_bytes;
    drop(vector_custody);
    assert!(work.remaining()?.peak_allocation_bytes > with_vector);
    assert!(work.remaining()?.peak_allocation_bytes < baseline);
    drop(alias);
    assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_batch_finite_data_and_budget_denial_precede_provider_and_hash()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    let requests = [ObjectReadRequest {
        object_id: native_test_id(),
        maximum_bytes: 4,
    }; 2];
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = 0;
    let (store, work, cancellation) = observed_native_store(budget, false, false, false);
    assert!(
        ({
            store
                .read_many(
                    &requests,
                    &mut crate::AuthorityWork::native(&work, &cancellation),
                )
                .await
                .map(|(values, allocation)| (values, allocation.expect("native batch allocation")))
        })
        .is_err()
    );
    assert_eq!(store.provider.calls.load(SeqCst), 0);
    assert_eq!(work.receipt().bytes_copied, 0);
    assert_eq!(work.receipt().backend_read_operations, 0);
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_hashed = 4;
    let (store, work, cancellation) = observed_native_store(budget, false, false, false);
    assert!(
        ({
            store
                .read_many(
                    &requests,
                    &mut crate::AuthorityWork::native(&work, &cancellation),
                )
                .await
                .map(|(values, allocation)| (values, allocation.expect("native batch allocation")))
        })
        .is_err()
    );
    assert_eq!(store.provider.calls.load(SeqCst), 1);
    assert_eq!(work.receipt().object_bytes_read, 8);
    assert_eq!(work.receipt().bytes_hashed, 4);
    let mut budget = WorkBudget::UNBOUNDED;
    budget.object_bytes_read = 7;
    let (store, work, cancellation) = observed_native_store(budget, false, false, false);
    assert!(
        ({
            store
                .read_many(
                    &requests,
                    &mut crate::AuthorityWork::native(&work, &cancellation),
                )
                .await
                .map(|(values, allocation)| (values, allocation.expect("native batch allocation")))
        })
        .is_err()
    );
    assert_eq!(store.provider.maximum.load(SeqCst), 7);
    assert_eq!(work.receipt().bytes_hashed, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_batch_error_cancellation_and_panic_keep_one_call_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    use futures::FutureExt as _;
    let requests = [ObjectReadRequest {
        object_id: native_test_id(),
        maximum_bytes: 4,
    }; 2];
    for (cancel, panic, error) in [
        (true, false, false),
        (false, true, false),
        (false, false, true),
        (true, false, true),
    ] {
        let (store, work, cancellation) =
            observed_native_store(WorkBudget::UNBOUNDED, cancel, panic, error);
        let future = async {
            store
                .read_many(
                    &requests,
                    &mut crate::AuthorityWork::native(&work, &cancellation),
                )
                .await
                .map(|(values, allocation)| (values, allocation.expect("native batch allocation")))
        };
        let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
        if panic {
            assert!(result.is_err());
        } else if error {
            assert!(matches!(
                result,
                Ok(Err(ObjectStoreError::LogicalObjects(Error {
                    code: wire::ErrorCode::Unavailable
                })))
            ));
        } else {
            assert!(matches!(
                result,
                Ok(Err(
                    ObjectStoreError::Cancelled | ObjectStoreError::Native(_)
                ))
            ));
            assert_eq!(work.receipt().object_bytes_read, 8);
        }
        assert_eq!(work.receipt().object_probes, 2);
        assert_eq!(work.receipt().bytes_hashed, 0);
    }
    Ok(())
}
