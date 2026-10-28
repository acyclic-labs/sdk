use super::*;

fn transport() -> Arc<Transport> {
    Arc::new(Transport {
        store: Arc::new(object_store::memory::InMemory::new()),
        prefix: Path::from("snapshot-tests"),
        capabilities: Capabilities {
            conditional_put: true,
            read_after_write: true,
            multipart: false,
            range_reads: true,
        },
    })
}

#[tokio::test]
async fn manifest_pages_replay_every_metadata_page_in_original_order() -> Result<(), Error> {
    let transport = transport();
    let source: Arc<dyn ImmutableBodies> = transport.clone();
    let mut pages = Vec::new();
    for index in 0..1025 {
        let core = MemoryObjects::new(MemoryOptions::default())?;
        core.create_bucket(wire::CreateBucketRequest {
            name: format!("bucket-{index:04}"),
            mutation: None,
        })
        .await?;
        let (_, chunk) = core.snapshot(Arc::clone(&source)).await?;
        pages.extend(chunk);
    }
    let expected: Vec<_> = pages
        .iter()
        .map(|bytes| blake3::hash(bytes).as_bytes().to_vec())
        .collect();
    let root = transport.persist_snapshot(73, pages).await?;
    assert_eq!(root.page_count, 1025);
    let mut next = root.manifest.clone();
    let mut observed = Vec::new();
    let mut chunks = 0;
    while !next.is_empty() {
        let bytes = transport.metadata("manifests", &hex::encode(&next)).await?;
        let page = ManifestPage::decode(bytes.as_slice()).map_err(backend)?;
        assert!(!page.pages.is_empty() && page.pages.len() <= MANIFEST_PAGE_ITEMS);
        observed.extend(page.pages);
        next = page.next;
        chunks += 1;
    }
    assert_eq!(chunks, 3);
    assert_eq!(observed, expected);
    let restored = transport.load_snapshot(&root, &[7; 32]).await?;
    for index in 0..1025 {
        let bucket = restored
            .head_bucket(wire::HeadBucketRequest {
                bucket: Some(wire::BucketRef {
                    name: format!("bucket-{index:04}"),
                }),
            })
            .await?;
        assert_eq!(
            bucket.bucket.map(|bucket| bucket.name),
            Some(format!("bucket-{index:04}"))
        );
    }
    assert_eq!(restored.snapshot(source).await?.0, 73);
    for count in [1024, 1026] {
        let mut malformed = root.clone();
        malformed.page_count = count;
        assert!(transport.load_snapshot(&malformed, &[7; 32]).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn manifest_empty_state_and_checksum_failures_preserve_candidate_isolation()
-> Result<(), Error> {
    let transport = transport();
    let root = transport.persist_snapshot(0, Vec::new()).await?;
    assert!(root.manifest.is_empty());
    assert_eq!(root.page_count, 0);
    assert!(transport.load_snapshot(&root, &[3; 32]).await.is_ok());
    let mut invalid = root.clone();
    invalid.manifest = vec![0; 32];
    assert!(transport.load_snapshot(&invalid, &[3; 32]).await.is_err());
    invalid.manifest.clear();
    invalid.page_count = 1;
    assert!(transport.load_snapshot(&invalid, &[3; 32]).await.is_err());
    assert!(transport.load_snapshot(&root, &[3; 31]).await.is_err());

    let root = transport.persist_snapshot(1, vec![Vec::new()]).await?;
    transport
        .store
        .put(
            &transport.path(&format!("manifests/{}", hex::encode(&root.manifest))),
            bytes::Bytes::from_static(b"changed after publication").into(),
        )
        .await
        .map_err(backend)?;
    assert!(transport.load_snapshot(&root, &[3; 32]).await.is_err());
    // A failed replay never mutates or publishes the previous valid root.
    let empty = transport.persist_snapshot(0, Vec::new()).await?;
    assert!(transport.load_snapshot(&empty, &[3; 32]).await.is_ok());
    Ok(())
}

#[tokio::test]
async fn a_large_logical_metadata_record_round_trips_through_snapshot_publication()
-> Result<(), Error> {
    let transport = transport();
    let source: Arc<dyn ImmutableBodies> = transport.clone();
    let core = MemoryObjects::new(MemoryOptions::default())?;
    let bucket = wire::BucketRef {
        name: "long-bucket-".repeat(32),
    };
    core.create_bucket(wire::CreateBucketRequest {
        name: bucket.name.clone(),
        mutation: None,
    })
    .await?;
    let key = "key".repeat(1025);
    let expected = core
        .put(
            wire::PutObjectHeader {
                bucket: Some(bucket.clone()),
                object_key: key.clone(),
                metadata: Some(wire::ObjectMetadata {
                    content_type: "a".repeat(3 * 1024 * 1024),
                    ..Default::default()
                }),
                ..Default::default()
            },
            OwnedBytes::from_static(b"retained"),
        )
        .await?;
    let (sequence, pages) = core.snapshot(source).await?;
    assert!(pages.iter().any(|page| page.len() > 2 * 1024 * 1024));
    let root = transport.persist_snapshot(sequence, pages).await?;
    let restored = transport.load_snapshot(&root, &[11; 32]).await?;
    assert_eq!(
        restored
            .head(wire::HeadObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: key.clone(),
                ..Default::default()
            })
            .await?
            .object,
        Some(expected)
    );
    assert_eq!(
        restored
            .get(
                wire::GetObjectRequest {
                    bucket: Some(bucket),
                    object_key: key,
                    ..Default::default()
                },
                8
            )
            .await?
            .into_body(),
        OwnedBytes::from_static(b"retained")
    );
    Ok(())
}
