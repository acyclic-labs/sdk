use super::*;
use acyclic_fs::ObjectKind;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn object(bytes: &'static [u8]) -> (ObjectId, OwnedBytes) {
    let bytes = OwnedBytes::from(bytes.to_vec());
    (
        ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(&bytes),
        },
        bytes,
    )
}

fn unique_namespace(prefix: &str) -> String {
    format!("{prefix}-{}", js_sys::Date::now().to_bits())
}

#[wasm_bindgen_test]
async fn opfs_is_authoritative_authenticated_and_persistent() {
    let name = unique_namespace("acyclic-fs-opfs-authoritative-v1");
    let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
    let (object_id, bytes) = object(b"persistent-opfs-body");
    let cancellation = CancellationToken::new();
    {
        let __object_args = (
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(&store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS put failed: {}", failure.error));
    drop(store);

    let reopened = OpfsAcceleratedObjectStore::open(&name, 1_024)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS reopen failed: {error}"));
    let read = {
        let __object_args = (object_id, 1_024, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::read(
            &reopened,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS read failed: {}", failure.error));
    assert_eq!(read.value.bytes, bytes);

    reopened
        .write(
            object_id,
            &OwnedBytes::from(b"corrupt".to_vec()),
            &cancellation,
        )
        .await
        .unwrap_or_else(|error| unreachable!("corruption setup failed: {error}"));
    let failure = {
        let __object_args = (object_id, 1_024, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = AsyncObjectStore::read(
            &reopened,
            __object_args.0,
            __object_args.1,
            &mut __object_work,
        )
        .await;
        __object_work.receipt(__object_result)
    }
    .err()
    .unwrap_or_else(|| unreachable!("corrupt OPFS body was accepted"));
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
}

#[wasm_bindgen_test]
async fn opfs_batches_preserve_order() {
    let name = unique_namespace("acyclic-fs-opfs-batch-v1");
    let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
    let objects = [object(b"first"), object(b"second"), object(b"third")];
    let writes: Vec<_> = objects
        .iter()
        .map(|(object_id, bytes)| ObjectWrite {
            object_id: *object_id,
            bytes: bytes.clone(),
        })
        .collect();
    let cancellation = CancellationToken::new();
    {
        let __object_args = (&writes, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result =
            AsyncObjectStore::put_many(&store, __object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS batch put failed: {}", failure.error));
    let requests: Vec<_> = objects
        .iter()
        .rev()
        .map(|(object_id, _)| ObjectReadRequest {
            object_id: *object_id,
            maximum_bytes: 1_024,
        })
        .collect();
    let reads = {
        let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result =
            AsyncObjectStore::read_many(&store, __object_args.0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS batch read failed: {}", failure.error));
    let actual: Vec<_> = reads.value.into_iter().map(|read| read.bytes).collect();
    let expected: Vec<_> = objects
        .iter()
        .rev()
        .map(|(_, bytes)| bytes.clone())
        .collect();
    assert_eq!(actual, expected);
}

#[wasm_bindgen_test]
async fn opfs_batch_admits_retained_carriers_before_materializing_next_window() {
    let name = unique_namespace("acyclic-fs-opfs-retained-carriers-v1");
    let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
        .await
        .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
    let writes: Vec<_> = (0_u8..32)
        .map(|index| {
            let bytes = OwnedBytes::from(vec![index; 1_024]);
            ObjectWrite {
                object_id: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: object_digest(&bytes),
                },
                bytes,
            }
        })
        .collect();
    let requests: Vec<_> = writes
        .iter()
        .map(|write| ObjectReadRequest {
            object_id: write.object_id,
            maximum_bytes: 1_024,
        })
        .collect();
    let cancellation = CancellationToken::new();
    {
        let __object_args = (&writes, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store.put_many(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS setup failed: {}", failure.error));
    let read = {
        let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
    .unwrap_or_else(|failure| unreachable!("OPFS read failed: {}", failure.error));
    let required = allocation_bytes::<ObjectRead>(requests.len())
        .unwrap_or_else(|error| unreachable!("vector size failed: {error}"))
        + allocation_bytes::<(ObjectId, Blob, u64)>(16)
            .unwrap_or_else(|error| unreachable!("handle size failed: {error}"))
        + allocation_bytes::<ObjectResult<ObjectRead>>(16)
            .unwrap_or_else(|error| unreachable!("result size failed: {error}"))
        + 16 * 1_024
        + 16 * 2 * 1_024;
    assert!(read.work.peak_allocation_bytes >= required);
    let required = read.work.peak_allocation_bytes;
    let cloned = read.value[0].bytes.clone();
    drop(read);
    assert_eq!(cloned.as_ref(), writes[0].bytes.as_ref());
    let denied = {
        let __object_args = (
            &requests,
            WorkBudget {
                peak_allocation_bytes: required - 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
    .err()
    .unwrap_or_else(|| unreachable!("retained carriers escaped admission"));
    assert_eq!(denied.work.object_bytes_read, 16 * 1_024);
    assert!(matches!(denied.error, ObjectStoreError::Work(_)));

    // Complete sibling reads after failure to retain their work and allocation peak.
    store
        .write(
            writes[16].object_id,
            &OwnedBytes::from(vec![255; 1_024]),
            &cancellation,
        )
        .await
        .unwrap_or_else(|error| unreachable!("corruption setup failed: {error}"));
    let corrupt = {
        let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
    .err()
    .unwrap_or_else(|| unreachable!("corrupt OPFS body was accepted"));
    assert!(matches!(corrupt.error, ObjectStoreError::Corrupt));
    assert_eq!(corrupt.work.object_bytes_read, 32 * 1_024);
    assert!(corrupt.work.peak_allocation_bytes >= required);
}
