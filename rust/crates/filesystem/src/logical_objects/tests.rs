use super::*;
use crate::storage::{HashedObject, ObjectKind};
use crate::{CancellationToken, WorkBudget};
use acyclic_objects::MemoryObjects;
use proptest::test_runner::TestCaseError;

const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;
type Store = LogicalObjectStore<MemoryObjects>;

fn store(maximum: u64) -> Result<Store, ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let provider = MemoryObjects::memory();
    LogicalObjectStore::new(Arc::new(provider)).with_object_limit(maximum)
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
                .map(|o| o.length()).sum();
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

#[tokio::test]
async fn a_publication_root_keeps_its_closure_through_every_collection()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    for (rooted, enumerated) in [(true, true), (true, false), (false, true), (false, false)] {
        let mut store = store(64)?;
        if rooted {
            store = store.with_publication_root(acyclic_objects::RootId::new(b"volume".to_vec())?);
        }
        let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"live"));
        let id = object.object_id();
        let ids = [id];
        store
            .put_hashed(object, &mut AuthorityWork::new(BUDGET, &cancel))
            .await?;
        // A composition without a collection handle cannot enumerate.
        let scope = if enumerated {
            crate::PublicationScope::Closure {
                objects: &ids,
                proven_at: 0,
            }
        } else {
            crate::PublicationScope::Everything
        };
        let _hold = store
            .flush_before_publish(scope, &mut AuthorityWork::new(BUDGET, &cancel))
            .await?;
        // Admission covers one cut; only a root survives the next ones.
        for _ in 0..3 {
            store.provider().collect().await?;
        }
        let read = store
            .read(id, 64, &mut AuthorityWork::new(BUDGET, &cancel))
            .await;
        if rooted {
            assert_eq!(read?.bytes, OwnedBytes::from_static(b"live"));
        } else {
            // Negative control: the same publication without a root is swept.
            assert!(read.is_err());
        }
    }
    Ok(())
}

async fn survives_collections(store: &Store, id: ObjectId) -> Result<bool, ObjectStoreError> {
    for _ in 0..3 {
        store
            .provider()
            .collect()
            .await
            .map_err(ObjectStoreError::LogicalObjects)?;
    }
    let cancel = CancellationToken::new();
    Ok(store
        .read(id, 64, &mut AuthorityWork::new(BUDGET, &cancel))
        .await
        .is_ok())
}

#[tokio::test]
async fn each_root_retains_what_its_clones_staged() -> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    for flush_b in [true, false] {
        let a = acyclic_objects::RootId::new(b"a".to_vec())?;
        let b = acyclic_objects::RootId::new(b"b".to_vec())?;
        let first = store(64)?.with_publication_root(a.clone());
        let second = first.clone().with_publication_root(b.clone());
        // Staged through the second root's clone, published first by the
        // first root.
        let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"shared"));
        let id = object.object_id();
        second
            .put_hashed(object, &mut AuthorityWork::new(BUDGET, &cancel))
            .await?;
        let _hold = first
            .flush_before_publish(
                crate::PublicationScope::Everything,
                &mut AuthorityWork::new(BUDGET, &cancel),
            )
            .await?;
        if flush_b {
            let _hold = second
                .flush_before_publish(
                    crate::PublicationScope::Everything,
                    &mut AuthorityWork::new(BUDGET, &cancel),
                )
                .await?;
        }
        first.provider().release(&a).await?;
        // Negative control: with the second root's retention also released,
        // nothing keeps the blob.
        if !flush_b {
            first.provider().release(&b).await?;
        }
        assert_eq!(survives_collections(&first, id).await?, flush_b);
    }
    Ok(())
}

#[tokio::test]
async fn a_root_retains_inside_the_callers_work() -> Result<(), Box<dyn std::error::Error>> {
    let root = acyclic_objects::RootId::new(b"volume".to_vec())?;
    let store = store(64)?.with_publication_root(root);
    let exhausted = WorkBudget {
        backend_write_operations: 0,
        ..BUDGET
    };
    let cancel = CancellationToken::new();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    for (body, budget, token) in [
        (&b"no budget"[..], exhausted, &cancel),
        (&b"cancelled"[..], BUDGET, &cancelled),
    ] {
        let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::copy_from_slice(body));
        let id = object.object_id();
        store
            .put_hashed(object, &mut AuthorityWork::new(BUDGET, &cancel))
            .await?;
        let refused = store
            .flush_before_publish(
                crate::PublicationScope::Everything,
                &mut AuthorityWork::new(budget, token),
            )
            .await;
        assert!(refused.is_err());
        // A refused flush retained nothing.
        assert!(!survives_collections(&store, id).await?);
    }
    // A refused flush keeps the blob staged for the root's next publication,
    // here on a fresh store whose staging holds no swept blob.
    let store = LogicalObjectStore::new(Arc::clone(store.provider()))
        .with_publication_root(acyclic_objects::RootId::new(b"next".to_vec())?);
    let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"kept"));
    let id = object.object_id();
    store
        .put_hashed(object, &mut AuthorityWork::new(BUDGET, &cancel))
        .await?;
    let refused = store
        .flush_before_publish(
            crate::PublicationScope::Everything,
            &mut AuthorityWork::new(exhausted, &cancel),
        )
        .await;
    assert!(refused.is_err());
    let _hold = store
        .flush_before_publish(
            crate::PublicationScope::Everything,
            &mut AuthorityWork::new(BUDGET, &cancel),
        )
        .await?;
    assert!(survives_collections(&store, id).await?);
    Ok(())
}
