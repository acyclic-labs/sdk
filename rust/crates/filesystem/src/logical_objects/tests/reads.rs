use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_response_controls_deny_before_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    for partial_failure in [false, true] {
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes =
            crate::NativeMaterializationWork::accounting_state_bytes() + 4096;
        let (store, work, cancellation) =
            observed_native_store(budget, false, false, partial_failure);
        store.provider.maximum.store(8192, SeqCst);
        let writes = [b"body".as_slice(), b"other".as_slice()].map(|body| ObjectWrite {
            object_id: ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: object_digest(ObjectKind::BlobChunk, body),
            },
            bytes: OwnedBytes::copy_from_slice(body),
        });
        let error = ({
            let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
            let __object_result = store.put_many(&writes, &mut __object_work).await;
            __object_result
        })
        .err()
        .ok_or("unbounded response was accepted")?;
        assert!(matches!(
            error,
            ObjectStoreError::NativeObjects(acyclic_objects::v1::NativeObjectsError {
                cause: acyclic_objects::v1::NativeObjectsCause::Admission(_),
                ..
            }) | ObjectStoreError::Work(_)
                | ObjectStoreError::Native(_)
        ));
        assert_eq!(work.receipt().object_bytes_written, 0);
        assert_eq!(store.provider.calls.load(SeqCst), 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert!(work.receipt().peak_allocation_bytes <= budget.peak_allocation_bytes);
        for write in &writes {
            assert!(
                store
                    .provider
                    .inner
                    .get(
                        wire::GetObjectRequest {
                            bucket: Some(store.bucket().clone()),
                            object_key: object_key(write.object_id),
                            ..Default::default()
                        },
                        10
                    )
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_provider_alias_retains_original_vec_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = work.remaining()?.peak_allocation_bytes;
    let mut custody = work.claim(4, 1, crate::WorkCounters::UNCHARGED)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(4)
        .map_err(crate::NativeTreeMaterializationError::AllocationFailed)?;
    custody.reconcile_capacity(bytes.capacity() as u64)?;
    let copy = WorkCounters {
        bytes_copied: 4,
        ..WorkCounters::UNCHARGED
    };
    work.admit(copy)?;
    bytes.extend_from_slice(b"body");
    work.record_admitted(copy)?;
    work.admit(WorkCounters::UNCHARGED)?;
    let object = crate::storage::HashedObject::from_native_vec(
        ObjectKind::BlobChunk,
        &mut bytes,
        &mut custody,
        &work,
        &cancellation,
    )?;
    let id = object.object_id();
    let caller_alias = object.bytes().slice(1..);
    let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let store = LogicalObjectStore::new(Arc::new(provider), bucket);
    ({
        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result = store.put_hashed(object, &mut __object_work).await;
        __object_result
    })?;
    drop(custody);
    let provider_alias = store
        .provider
        .get(
            wire::GetObjectRequest {
                bucket: Some(store.bucket().clone()),
                object_key: object_key(id),
                ..Default::default()
            },
            4,
        )
        .await?
        .into_body();
    drop(caller_alias);
    let retained = work.remaining()?.peak_allocation_bytes;
    assert!(retained < baseline);
    drop(store);
    assert!(work.remaining()?.peak_allocation_bytes >= retained);
    assert!(work.remaining()?.peak_allocation_bytes < baseline);
    drop(provider_alias);
    assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_original_quota_and_all_byte_aliases()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.object_bytes_read = 4;
    budget.peak_allocation_bytes = 100_000;
    let (store, work, cancellation) = observed_native_store(budget, false, false, false);
    let baseline = work.remaining()?.peak_allocation_bytes;
    let initial_operations = work.receipt().allocation_operations;
    let read = ({
        let authority_input_0 = native_test_id();

        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result = store
            .read(authority_input_0, u64::MAX, &mut __object_work)
            .await;
        __object_result
    })?;
    assert_eq!(store.provider.maximum.load(SeqCst), 4);
    assert_eq!(store.provider.calls.load(SeqCst), 1);
    // Original request, future and prepared-byte controls are real pre-I/O allocations.
    assert!(work.receipt().allocation_operations - initial_operations >= 7);
    assert_eq!(work.receipt().object_bytes_read, 4);
    assert_eq!(work.receipt().bytes_hashed, 4);
    assert_eq!(
        work.receipt().bytes_copied,
        store.bucket.name.len() as u64 + object_key(native_test_id()).len() as u64 + 4
    );
    let read = read;
    let alias = read.bytes.slice(1..);
    let retained = work.remaining()?.peak_allocation_bytes;
    assert!(retained < baseline - 4);
    drop(read);
    assert_eq!(work.remaining()?.peak_allocation_bytes, retained);
    drop(alias);
    assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_denies_named_data_and_hash_before_their_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = 0;
    let (store, work, cancellation) = observed_native_store(budget, false, false, false);
    let before = work.receipt();
    assert!(
        ({
            let authority_input_0 = native_test_id();

            let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
            let __object_result = store.read(authority_input_0, 4, &mut __object_work).await;
            __object_result
        })
        .is_err()
    );
    assert_eq!(store.provider.calls.load(SeqCst), 0);
    assert_eq!(work.receipt(), before);
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_hashed = 0;
    let (store, work, cancellation) = observed_native_store(budget, false, false, false);
    assert!(
        ({
            let authority_input_0 = native_test_id();

            let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
            let __object_result = store.read(authority_input_0, 4, &mut __object_work).await;
            __object_result
        })
        .is_err()
    );
    assert_eq!(store.provider.calls.load(SeqCst), 1);
    assert_eq!(work.receipt().object_bytes_read, 4);
    assert_eq!(work.receipt().bytes_hashed, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_error_cancellation_and_panic_keep_attempted_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    use futures::FutureExt as _;
    for (cancel, panic, error) in [
        (true, false, false),
        (false, true, false),
        (false, false, true),
        (true, false, true),
    ] {
        let (store, work, cancellation) =
            observed_native_store(WorkBudget::UNBOUNDED, cancel, panic, error);
        let future = async {
            let authority_input_0 = native_test_id();

            let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
            let __object_result = store.read(authority_input_0, 4, &mut __object_work).await;
            __object_result
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
            assert_eq!(work.receipt().object_bytes_read, 4);
        }
        assert_eq!(work.receipt().bytes_hashed, 0);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_opaque_dto_is_not_known_memory_shared_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let memory = crate::MemoryObjectStore::default();
    ({
        let authority_input_0 = native_test_id();
        let authority_input_1 = OwnedBytes::from_static(b"body");
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = memory
            .put(authority_input_0, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    })?;
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .unwrap_or_else(|_| panic!("test accounting construction failed"));
    let before = work.receipt().allocation_operations;
    let shared = ({
        let authority_input_0 = native_test_id();

        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result = memory.read(authority_input_0, 4, &mut __object_work).await;
        __object_result
    })?;
    assert_eq!(shared.retention, ObjectReadRetention::Shared);
    assert_eq!(work.receipt().allocation_operations, before);
    let (logical, work, cancellation) =
        observed_native_store(WorkBudget::UNBOUNDED, false, false, false);
    let opaque = ({
        let authority_input_0 = native_test_id();

        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result = logical.read(authority_input_0, 4, &mut __object_work).await;
        __object_result
    })?;
    assert_eq!(
        opaque.retention,
        ObjectReadRetention::Owned { logical_bytes: 4 }
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_live_control_ceiling_denies_before_provider()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    let (inner, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes()
        + bucket.name.len() as u64
        + object_key(native_test_id()).len() as u64
        + 2;
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(budget, &cancellation)
        .unwrap_or_else(|_| panic!("test accounting construction failed"));
    let provider = ObservedProvider {
        inner,
        original: work.clone(),
        calls: 0.into(),
        maximum: 0.into(),
        cancel: false,
        panic: false,
        error: false,
    };
    let store = LogicalObjectStore::new(Arc::new(provider), bucket);
    let error = ({
        let authority_input_0 = native_test_id();

        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result = store.read(authority_input_0, 100, &mut __object_work).await;
        __object_result
    })
    .err()
    .ok_or("unfunded native controls reached the provider")?;
    assert!(matches!(
        error,
        ObjectStoreError::NativeObjects(acyclic_objects::v1::NativeObjectsError {
            cause: acyclic_objects::v1::NativeObjectsCause::Admission(_),
            ..
        }) | ObjectStoreError::Native(_)
            | ObjectStoreError::Work(_)
    ));
    assert_eq!(store.provider.maximum.load(SeqCst), 0);
    assert_eq!(store.provider.calls.load(SeqCst), 0);
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().bytes_hashed, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_and_native_logical_reads_share_request_and_payload_accounting()
-> Result<(), Box<dyn std::error::Error>> {
    let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let store = LogicalObjectStore::new(Arc::new(provider), bucket);
    let cancellation = CancellationToken::new();
    ({
        let authority_input_0 = native_test_id();
        let authority_input_1 = OwnedBytes::from_static(b"body");
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = store
            .put(authority_input_0, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    })?;
    let ordinary = ({
        let authority_input_0 = native_test_id();
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = store.read(authority_input_0, 4, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })?;
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .unwrap_or_else(|_| panic!("test accounting construction failed"));
    let before = work.receipt().allocation_operations;
    let native = ({
        let authority_input_0 = native_test_id();

        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result = store.read(authority_input_0, 4, &mut __object_work).await;
        __object_result
    })?;
    assert_eq!(native.bytes, ordinary.value.bytes);
    assert_eq!(native.retention, ObjectReadRetention::Shared);
    assert_eq!(
        work.receipt().object_bytes_read,
        ordinary.work.object_bytes_read
    );
    assert_eq!(work.receipt().bytes_hashed, ordinary.work.bytes_hashed);
    assert!(work.receipt().bytes_copied >= ordinary.work.bytes_copied);
    assert!(work.receipt().allocation_operations - before > ordinary.work.allocation_operations);
    Ok(())
}
