use super::*;
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_original_quota_and_all_byte_aliases()
-> Result<(), Box<dyn std::error::Error>> {
    let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let store = LogicalObjectStore::new(Arc::new(provider), bucket);
    let bytes = OwnedBytes::from_static(b"body");
    let id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, &bytes),
    };
    let cancellation = CancellationToken::new();
    store
        .put(
            id,
            bytes,
            &mut AuthorityWork::new(crate::WorkBudget::UNBOUNDED, &cancellation),
        )
        .await?;
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            object_bytes_read: 4,
            peak_allocation_bytes: 100_000,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let baseline = work.remaining()?.peak_allocation_bytes;
    let read = store
        .read(id, 100, &mut AuthorityWork::native(&work, &cancellation))
        .await?;
    let alias = read.bytes.clone();
    drop(read);
    assert_eq!(alias.as_ref(), b"body");
    assert!(work.remaining()?.peak_allocation_bytes < baseline);
    drop(alias);
    assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
    Ok(())
}
