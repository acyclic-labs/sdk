use super::*;
use crate::NativeMaterializationWork;
use crate::storage::{HashedObject, ObjectKind};

const BUDGET: WorkBudget = WorkBudget {
    object_probes: 128,
    object_bytes_read: 131_072,
    object_bytes_written: 131_072,
    bytes_hashed: 131_072,
    bytes_copied: 131_072,
    backend_read_operations: 32,
    backend_write_operations: 32,
    allocation_operations: 32,
    peak_allocation_bytes: 131_072,
    items_examined: 128,
    items_returned: 128,
    ..WorkCounters::UNCHARGED
};

#[tokio::test]
async fn object_allocation_denial_cannot_publish_an_object() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 1,
        ..BUDGET
    };
    let original = NativeMaterializationWork::new(budget, &cancel).map_err(|(error, _)| error)?;
    let store = MemoryObjectStore::default();
    let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"body"));
    assert!(matches!(
        store
            .put_hashed(
                object.clone(),
                &mut AuthorityWork::native(&original, &cancel)
            )
            .await,
        Err(ObjectStoreError::Native(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert!(!provider_receipt!(store, contains(object.object_id()), WorkBudget::UNBOUNDED)?.value);
    original.receipt().verify(budget)?;
    Ok(())
}

#[tokio::test]
async fn native_object_bytes_retain_the_original_allocation_until_last_alias_drops()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let original = NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut allocation = original.claim(4, 1, WorkCounters::UNCHARGED)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(4)?;
    allocation.resize(bytes.capacity() as u64)?;
    bytes.extend_from_slice(b"body");
    let object = HashedObject::from_native_vec(
        ObjectKind::BlobChunk,
        &mut bytes,
        &mut allocation,
        &original,
        &cancel,
    )?;
    let id = object.object_id();
    let store = MemoryObjectStore::default();
    store
        .put_hashed(object, &mut AuthorityWork::native(&original, &cancel))
        .await?;
    let view = provider_receipt!(store, read(id, 4), WorkBudget::UNBOUNDED)?.value;
    let reader = NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
    let native_view = store
        .read(id, 4, &mut AuthorityWork::native(&reader, &cancel))
        .await?;
    let alias = native_view.bytes.clone();
    provider_receipt!(
        store,
        put(id, OwnedBytes::from_static(b"body")),
        WorkBudget::UNBOUNDED
    )?;
    let second = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"second"));
    store
        .put_hashed(second, &mut AuthorityWork::native(&original, &cancel))
        .await?;
    assert_eq!(
        provider_receipt!(store, read(id, 4), WorkBudget::UNBOUNDED)?
            .value
            .bytes
            .as_ref(),
        b"body"
    );
    drop(native_view);
    drop(store);
    assert!(original.live_allocation_bytes() > baseline);
    assert_eq!(view.bytes.as_ref(), b"body");
    assert_eq!(alias.as_ref(), b"body");
    drop(view);
    assert!(original.live_allocation_bytes() > baseline);
    drop(alias);
    assert_eq!(original.live_allocation_bytes(), baseline);
    original.receipt().verify(BUDGET)?;
    Ok(())
}
