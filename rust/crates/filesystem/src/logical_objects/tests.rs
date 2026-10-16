use super::*;
use crate::storage::{HashedObject, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectKind};
use crate::{CancellationToken, WorkBudget};
use acyclic_objects::v1::MemoryObjects;
use proptest::test_runner::TestCaseError;

const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;
type Store = LogicalObjectStore<MemoryObjects>;

fn store(maximum: u64) -> Result<Store, ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    LogicalObjectStore::new(Arc::new(provider), bucket).with_object_limit(maximum)
}

#[tokio::test]
async fn authenticated_batch_preserves_raw_validation_and_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"body"));
    let store = store(4)?;
    let objects = [object.clone(), object.clone()];
    let budget = WorkBudget {
        bytes_hashed: 0,
        ..BUDGET
    };
    let mut work = AuthorityWork::new(budget, &cancel);
    store.put_many_hashed(&objects, &mut work).await?;
    assert_eq!(work.spent().bytes_hashed, 0);
    assert_eq!(work.spent().object_bytes_written, 4);
    assert_eq!(work.spent().backend_write_operations, 1);
    work.spent().verify(budget)?;
    let writes = [ObjectWrite {
        object_id: object.object_id(),
        bytes: object.bytes().clone(),
    }];
    assert!(
        store
            .put_many(&writes, &mut AuthorityWork::new(budget, &cancel))
            .await
            .is_err()
    );
    let read = store
        .read(
            object.object_id(),
            4,
            &mut AuthorityWork::new(BUDGET, &cancel),
        )
        .await?;
    assert_eq!(read.bytes, *object.bytes());
    let mut wrong = writes;
    wrong[0].bytes = OwnedBytes::from_static(b"bad");
    assert!(matches!(
        store
            .put_many(&wrong, &mut AuthorityWork::new(BUDGET, &cancel))
            .await,
        Err(ObjectStoreError::DigestMismatch)
    ));
    Ok(())
}

#[tokio::test]
async fn authenticated_batch_denials_precede_publication() -> Result<(), Box<dyn std::error::Error>>
{
    let cancel = CancellationToken::new();
    let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"body"));
    for budget in [
        WorkBudget {
            allocation_operations: 0,
            ..BUDGET
        },
        WorkBudget {
            peak_allocation_bytes: 0,
            ..BUDGET
        },
        WorkBudget {
            backend_write_operations: 0,
            ..BUDGET
        },
        WorkBudget {
            object_bytes_written: 3,
            ..BUDGET
        },
    ] {
        let store = store(4)?;
        let mut work = AuthorityWork::new(budget, &cancel);
        assert!(
            store
                .put_many_hashed(std::slice::from_ref(&object), &mut work)
                .await
                .is_err()
        );
        assert_eq!(work.spent().bytes_hashed, 0);
        work.spent().verify(budget)?;
        assert!(matches!(
            store
                .read(
                    object.object_id(),
                    4,
                    &mut AuthorityWork::new(BUDGET, &cancel)
                )
                .await,
            Err(ObjectStoreError::Missing)
        ));
    }
    let store = store(4)?;
    cancel.cancel();
    let mut work = AuthorityWork::new(BUDGET, &cancel);
    assert!(store.put_many_hashed(&[object], &mut work).await.is_err());
    assert_eq!(work.spent(), WorkCounters::UNCHARGED);
    Ok(())
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn authenticated_native_batch_keeps_the_raw_batch_boundary_and_footer()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(e, _)| e)?;
    let baseline = original.live_allocation_bytes();
    let store = store(4)?;
    let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"body"));
    let before = original.receipt();
    assert!(matches!(
        store
            .put_many_hashed(
                std::slice::from_ref(&object),
                &mut AuthorityWork::native(&original, &cancel)
            )
            .await,
        Err(ObjectStoreError::NativeUnavailable)
    ));
    assert_eq!(original.receipt(), before);
    assert_eq!(original.live_allocation_bytes(), baseline);
    let writes = [ObjectWrite {
        object_id: object.object_id(),
        bytes: object.bytes().clone(),
    }];
    store
        .put_many(&writes, &mut AuthorityWork::native(&original, &cancel))
        .await?;
    assert_eq!(original.receipt().object_bytes_written, 4);
    original.receipt().verify(BUDGET)?;
    drop(store);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

proptest::proptest! {
    // Reuse of constructor authentication preserves complete values/errors and
    // every receipt field except exactly the discarded validation hashes.
    #[test]
    fn authenticated_batch_matches_raw_for_arbitrary_bodies_limits_and_duplicates(
        bodies in proptest::collection::vec(proptest::collection::vec(proptest::prelude::any::<u8>(), 0..129), 0..9),
        maximum in 1u16..132,
        duplicate in proptest::prelude::any::<bool>(),
    ) {
        let mut objects: Vec<_> = bodies.into_iter().map(|body| HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from(body))).collect();
        if duplicate && let Some(first) = objects.first().cloned() { objects.push(first); }
        let writes: Vec<_> = objects.iter().map(|o| ObjectWrite { object_id: o.object_id(), bytes: o.bytes().clone() }).collect();
        let raw = store(u64::from(maximum)).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let hashed = store(u64::from(maximum)).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let cancelled = CancellationToken::new();
        let runtime = tokio::runtime::Builder::new_current_thread().build().map_err(|e| TestCaseError::fail(e.to_string()))?;
        runtime.block_on(async {
            let mut expected = AuthorityWork::new(BUDGET, &cancelled);
            let mut actual = AuthorityWork::new(BUDGET, &cancelled);
            let expected_result = raw.put_many(&writes, &mut expected).await;
            let actual_result = hashed.put_many_hashed(&objects, &mut actual).await;
            proptest::prop_assert_eq!(actual_result.is_ok(), expected_result.is_ok());
            let success = actual_result.is_ok();
            if let (Err(actual), Err(expected)) = (actual_result, expected_result) {
                proptest::prop_assert_eq!(actual.to_string(), expected.to_string());
            }
            let discarded: u64 = objects.iter().take_while(|o| o.length() <= u64::from(maximum))
                .map(|o| o.length() + OBJECT_DIGEST_ENVELOPE_BYTES).sum();
            let mut receipt = expected.spent();
            receipt.bytes_hashed -= discarded;
            proptest::prop_assert_eq!(actual.spent(), receipt);
            actual.spent().verify(BUDGET).map_err(|e| TestCaseError::fail(e.to_string()))?;
            if success {
                for object in &objects {
                    let a = raw.read(object.object_id(), object.length(), &mut AuthorityWork::new(BUDGET, &cancelled)).await.map_err(|e| TestCaseError::fail(e.to_string()))?;
                    let b = hashed.read(object.object_id(), object.length(), &mut AuthorityWork::new(BUDGET, &cancelled)).await.map_err(|e| TestCaseError::fail(e.to_string()))?;
                    proptest::prop_assert_eq!(a, b);
                }
            }
            Ok(())
        })?;
    }
}

#[path = "decoded.rs"]
mod decoded;
