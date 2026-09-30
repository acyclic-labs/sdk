use super::*;

fn bucket() -> Option<wire::BucketRef> {
    Some(wire::BucketRef {
        name: "recovery".into(),
    })
}
fn put(key: &str) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: bucket(),
        object_key: key.into(),
        mutation: Some(wire::MutationIdentity {
            idempotency_key: key.into(),
        }),
        ..Default::default()
    }
}
fn get(key: &str) -> wire::GetObjectRequest {
    wire::GetObjectRequest {
        bucket: bucket(),
        object_key: key.into(),
        ..Default::default()
    }
}
fn reopen(root: &Path) -> Result<MemoryObjects, LocalOpenError> {
    open(root.to_path_buf(), LocalObjectsLimits::default(), None)
}
async fn seeded(root: &Path) -> Result<MemoryObjects, Box<dyn std::error::Error>> {
    let core = reopen(root)?;
    core.create_bucket(wire::CreateBucketRequest {
        name: "recovery".into(),
        mutation: None,
    })
    .await?;
    core.put(put("retained"), Bytes::from_static(b"before"))
        .await?;
    Ok(core)
}
async fn assert_poisoned(core: &MemoryObjects) {
    assert_eq!(
        core.get(get("retained"), 6).await.err().map(|e| e.code),
        Some(Unavailable)
    );
    assert_eq!(
        core.head_bucket(wire::HeadBucketRequest { bucket: bucket() })
            .await
            .err()
            .map(|e| e.code),
        Some(Unavailable)
    );
    assert_eq!(
        core.list(wire::ListObjectsRequest {
            bucket: bucket(),
            page_size: 1,
            ..Default::default()
        })
        .await
        .err()
        .map(|e| e.code),
        Some(Unavailable)
    );
    assert_eq!(
        core.put(put("retained"), Bytes::from_static(b"before"))
            .await
            .err()
            .map(|e| e.code),
        Some(Unavailable)
    );
}

#[tokio::test]
async fn incomplete_final_frames_repair_without_publishing_a_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    for cut in [0, 2, 35, 40] {
        let root = tempfile::tempdir()?;
        let core = seeded(root.path()).await?;
        let path = root.path().join("mutations.log");
        let before = fs::read(&path)?;
        core.journal
            .as_ref()
            .ok_or("missing journal")?
            .fault_bytes
            .store(cut, Ordering::Release);
        assert_eq!(
            core.put(put("interrupted"), Bytes::from_static(b"after"))
                .await
                .err()
                .map(|e| e.code),
            Some(Unavailable)
        );
        assert_poisoned(&core).await;
        drop(core);
        let core = reopen(root.path())?;
        assert_eq!(fs::read(&path)?, before);
        assert_eq!(
            core.get(get("retained"), 6).await?.body,
            Bytes::from_static(b"before")
        );
        assert_eq!(
            core.get(get("interrupted"), 5).await.err().map(|e| e.code),
            Some(NotFound)
        );
        core.put(put("interrupted"), Bytes::from_static(b"after"))
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn complete_uncertain_frame_recovers_its_exact_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    core.journal
        .as_ref()
        .ok_or("missing journal")?
        .fault_sync
        .store(true, Ordering::Release);
    let query = put("uncertain");
    assert_eq!(
        core.put(query.clone(), Bytes::from_static(b"after"))
            .await
            .err()
            .map(|e| e.code),
        Some(Unavailable)
    );
    assert_poisoned(&core).await;
    drop(core);
    let core = reopen(root.path())?;
    let recovered = core
        .head(wire::HeadObjectRequest {
            bucket: bucket(),
            object_key: "uncertain".into(),
            ..Default::default()
        })
        .await?
        .object
        .ok_or("missing recovered object")?;
    let before = fs::read(root.path().join("mutations.log"))?;
    assert_eq!(
        core.put(query, Bytes::from_static(b"after")).await?,
        recovered
    );
    assert_eq!(fs::read(root.path().join("mutations.log"))?, before);
    Ok(())
}

#[tokio::test]
async fn complete_corruption_fails_closed_without_truncating_the_journal()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    drop(seeded(root.path()).await?);
    let path = root.path().join("mutations.log");
    let mut bytes = fs::read(&path)?;
    let last = bytes.last_mut().ok_or("empty journal")?;
    *last ^= 1;
    fs::write(&path, &bytes)?;
    assert!(matches!(reopen(root.path()), Err(LocalOpenError::Corrupt)));
    assert_eq!(fs::read(&path)?, bytes);
    Ok(())
}

#[tokio::test]
async fn multipart_recovery_preserves_parts_composite_ranges_and_completion_retries()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    let upload = core
        .create_multipart(wire::CreateMultipartRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            ..Default::default()
        })
        .await?;
    let mut parts = Vec::new();
    for (number, bytes) in [
        (1, Bytes::from(vec![b'a'; 5 * 1024 * 1024])),
        (2, Bytes::from_static(b"end")),
    ] {
        parts.push(
            core.upload_part(
                wire::UploadPartHeader {
                    bucket: bucket(),
                    object_key: "multipart".into(),
                    upload_id: upload.upload_id.clone(),
                    part_number: number,
                    mutation: None,
                },
                bytes,
            )
            .await?,
        );
    }
    drop(core);
    let core = reopen(root.path())?;
    assert_eq!(
        core.list_parts(wire::ListPartsRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            upload_id: upload.upload_id.clone(),
            page_size: 10,
            ..Default::default()
        })
        .await?
        .parts,
        parts
    );
    let query = wire::CompleteMultipartRequest {
        bucket: bucket(),
        object_key: "multipart".into(),
        upload_id: upload.upload_id,
        parts,
        mutation: Some(wire::MutationIdentity {
            idempotency_key: "completion".into(),
        }),
        ..Default::default()
    };
    let receipt = core.complete_multipart(query.clone()).await?;
    drop(core);
    let core = reopen(root.path())?;
    let range = wire::GetObjectRequest {
        range: Some(wire::ByteRange {
            selection: Some(wire::byte_range::Selection::Bytes(wire::InclusiveRange {
                start: 5 * 1024 * 1024 - 2,
                end: Some(5 * 1024 * 1024 + 1),
            })),
        }),
        ..get("multipart")
    };
    assert_eq!(core.get(range, 4).await?.body, Bytes::from_static(b"aaen"));
    core.delete(wire::DeleteObjectRequest {
        bucket: bucket(),
        object_key: "multipart".into(),
        ..Default::default()
    })
    .await?;
    drop(core);
    let core = reopen(root.path())?;
    assert_eq!(core.complete_multipart(query).await?, receipt);
    assert_eq!(
        core.get(get("multipart"), 10).await.err().map(|e| e.code),
        Some(NotFound)
    );
    Ok(())
}

#[tokio::test]
async fn logical_quota_failure_does_not_persist_a_body_or_retry_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let options = LocalObjectsLimits {
        maximum_object_bytes: 5,
        maximum_bytes: 5,
        ..Default::default()
    };
    let core = open(root.path().to_path_buf(), options, None)?;
    core.create_bucket(wire::CreateBucketRequest {
        name: "recovery".into(),
        mutation: None,
    })
    .await?;
    core.put(put("retained"), Bytes::from_static(b"1234"))
        .await?;
    let before = fs::read(root.path().join("mutations.log"))?;
    let count = fs::read_dir(root.path().join("segments"))?.count();
    assert_eq!(
        core.put(put("new"), Bytes::from_static(b"ab"))
            .await
            .err()
            .map(|e| e.code),
        Some(QuotaExceeded)
    );
    assert_eq!(fs::read(root.path().join("mutations.log"))?, before);
    assert_eq!(fs::read_dir(root.path().join("segments"))?.count(), count);
    drop(core);
    let core = open(root.path().to_path_buf(), options, None)?;
    core.delete(wire::DeleteObjectRequest {
        bucket: bucket(),
        object_key: "retained".into(),
        ..Default::default()
    })
    .await?;
    core.put(put("new"), Bytes::from_static(b"ab")).await?;
    assert_eq!(
        core.get(get("new"), 2).await?.body,
        Bytes::from_static(b"ab")
    );
    Ok(())
}

#[tokio::test]
async fn uncertain_batch_item_prevents_later_exact_retries_in_the_same_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let core = seeded(root.path()).await?;
    core.journal
        .as_ref()
        .ok_or("missing journal")?
        .fault_bytes
        .store(2, Ordering::Release);
    let results = core
        .put_batch(vec![
            (put("interrupted"), Bytes::from_static(b"after")),
            (put("retained"), Bytes::from_static(b"before")),
            (put("later"), Bytes::from_static(b"later")),
        ])
        .await;
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|result| {
        result
            .as_ref()
            .err()
            .is_some_and(|error| error.code == Unavailable)
    }));
    drop(core);
    let core = reopen(root.path())?;
    assert_eq!(
        core.get(get("retained"), 6).await?.body,
        Bytes::from_static(b"before")
    );
    assert_eq!(
        core.get(get("later"), 5).await.err().map(|e| e.code),
        Some(NotFound)
    );
    Ok(())
}
