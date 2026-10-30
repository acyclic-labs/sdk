use super::*;

#[tokio::test]
async fn malformed_backend_batch_cardinality_releases_all_flights()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = Arc::new(MemoryObjectStore::default());
    let cancellation = CancellationToken::new();
    let objects = [object(b"first-short"), object(b"second-short")];
    for (object_id, bytes) in &objects {
        (object_work!(inner, put(*object_id, bytes.clone()), B, &cancellation)).await?;
    }
    let cached = CachedObjectStore::new(ShortBatchStore { inner }, options(4, 64))?;
    let requests = objects.map(|(object_id, bytes)| ObjectReadRequest {
        object_id,
        maximum_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    });
    let failure = (object_work!(cached, read_many(&requests), B, &cancellation))
        .await
        .err()
        .ok_or("short backend batch was accepted")?;
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    assert_eq!(cached.stats()?.in_flight, 0);
    Ok(())
}

#[tokio::test]
async fn mixed_batches_preserve_backend_multiget_order_duplicates_and_exact_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let objects = [object(b"warm"), object(b"second"), object(b"third")];
    for (object_id, bytes) in &objects {
        (object_work!(inner, put(*object_id, bytes.clone()), B, &cancellation)).await?;
    }
    let (counting, scalar_reads, batch_reads) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(8, 128))?;
    object_work!(cached, read(objects[0].0, 4), B, &cancellation).await?;
    let requests = [
        ObjectReadRequest {
            object_id: objects[0].0,
            maximum_bytes: 4,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: objects[2].0,
            maximum_bytes: 5,
        },
    ];
    let receipt = object_work!(cached, read_many(&requests), B, &cancellation).await?;
    assert_eq!(
        receipt
            .value
            .iter()
            .map(|value| value.bytes.as_ref())
            .collect::<Vec<_>>(),
        vec![b"warm".as_slice(), b"second", b"second", b"third"]
    );

    assert_eq!(scalar_reads.load(Ordering::Acquire), 1);
    assert_eq!(batch_reads.load(Ordering::Acquire), 1);

    let unequal = [
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 1,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
    ];
    let failure = (object_work!(cached, read_many(&unequal), B, &cancellation))
        .await
        .err()
        .ok_or("per-request byte limit was ignored")?;
    assert!(matches!(
        failure.error,
        ObjectStoreError::TooLarge {
            observed: 6,
            maximum: 1
        }
    ));
    assert_eq!(batch_reads.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn fs_composition_consumes_the_cache_without_a_second_runtime_or_adapter()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"embedded-fs");
    (object_work!(inner, put(object_id, bytes.clone()), B, &cancellation)).await?;
    let (counting, scalar_reads, _) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(8, 128))?;
    let fs = crate::Fs::new(
        crate::MemoryAuthorityStore::default(),
        cached,
        crate::EmbeddedCapabilities::MEMORY,
    );
    let first = fs.export_object(object_id, 11, B, &cancellation).await?;
    let warm = fs.export_object(object_id, 11, B, &cancellation).await?;
    assert_eq!(first.value.bytes, bytes);
    assert_eq!(warm.value.bytes, bytes);

    assert_eq!(scalar_reads.load(Ordering::Acquire), 1);

    let (imported_id, imported_bytes) = object(b"facade-import");
    let imported = fs
        .import_object(imported_id, imported_bytes.clone(), B, &cancellation)
        .await?;
    assert_eq!(imported.value, ());

    let exported = fs.export_object(imported_id, 13, B, &cancellation).await?;
    assert_eq!(exported.value.bytes, imported_bytes);

    Ok(())
}
