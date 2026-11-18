use acyclic_fs::{
    AsyncObjectStore, MemoryObjectStore, ObjectFailure, ObjectReadRequest, ObjectStoreError,
    object_digest,
};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::WorkBudget;
use acyclic_stream::OwnedBytes;

#[tokio::test]
async fn object_admission_rejects_size_identity_absence_and_batch_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        MemoryObjectStore::new(0),
        Err(ObjectStoreError::TooLarge {
            observed: 0,
            maximum: 0,
        })
    ));
    let store = MemoryObjectStore::new(Some(4))?;
    let unlimited = MemoryObjectStore::new(None)?;
    let oversized = OwnedBytes::from_static(b"oversized");
    let oversized_id = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: object_digest(&oversized),
    };
    for store in [&unlimited, &MemoryObjectStore::default()] {
        provider_receipt!(
            store,
            put(oversized_id, oversized.clone()),
            WorkBudget::UNBOUNDED
        )?;
        assert_eq!(
            provider_receipt!(store, read(oversized_id, 9), WorkBudget::UNBOUNDED)?
                .value
                .bytes,
            oversized
        );
    }
    let object_id = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: object_digest(b"data"),
    };
    assert!(matches!(
        provider_receipt!(
            store,
            put(object_id, OwnedBytes::from_static(b"oversized")),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    assert!(matches!(
        provider_receipt!(
            store,
            put(object_id, OwnedBytes::from_static(b"nope")),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::DigestMismatch,
            ..
        })
    ));
    assert!(!provider_receipt!(store, contains(object_id), WorkBudget::UNBOUNDED)?.value);
    assert!(matches!(
        provider_receipt!(store, read(object_id, 4), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    provider_receipt!(
        store,
        put(object_id, OwnedBytes::from_static(b"data")),
        WorkBudget::UNBOUNDED
    )?;
    provider_receipt!(
        store,
        put(object_id, OwnedBytes::from_static(b"data")),
        WorkBudget::UNBOUNDED
    )?;
    assert!(matches!(
        provider_receipt!(
            store,
            read_many(&[ObjectReadRequest {
                object_id,
                maximum_bytes: 3,
            }]),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge {
                observed: 4,
                maximum: 3,
            },
            ..
        })
    ));
    let missing = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([31; 32]),
    };
    assert!(matches!(
        provider_receipt!(
            store,
            read_many(&[ObjectReadRequest {
                object_id: missing,
                maximum_bytes: 4,
            }]),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    let mut denied = WorkBudget::UNBOUNDED;
    denied.object_probes = 0;
    assert!(matches!(
        provider_receipt!(store, contains(object_id), denied),
        Err(ObjectFailure {
            error: ObjectStoreError::Work(_),
            ..
        })
    ));
    Ok(())
}
