use super::*;
use std::collections::BTreeSet;

fn segments(root: &Path) -> Result<BTreeSet<std::path::PathBuf>, std::io::Error> {
    std::fs::read_dir(root.join("segments"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect()
}
async fn seeded(root: &Path) -> Result<LocalObjects, Box<dyn std::error::Error>> {
    let provider = create(root).await?;
    provider
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    Ok(provider)
}

#[tokio::test]
async fn collection_retains_replacement_and_original_retry_receipt_across_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let provider = seeded(root.path()).await?;
    let before_body = Bytes::from(vec![b'b'; 65_537]);
    let after_body = Bytes::from(vec![b'a'; 65_537]);
    let original = provider.put(put("value"), before_body.clone()).await?;
    let mut replacement = put("value");
    replacement.mutation = identity("replacement");
    provider.put(replacement, after_body.clone()).await?;
    let before = segments(root.path())?;
    assert_eq!(before.len(), 2);
    assert!(matches!(
        provider.collect_garbage(0).await,
        Err(LocalOpenError::Invalid)
    ));
    assert!(matches!(
        provider.collect_garbage(1).await,
        Err(LocalOpenError::Invalid)
    ));
    assert_eq!(segments(root.path())?, before);
    let report = provider.collect_garbage(2).await?;
    assert_eq!(report.segments_examined, 2);
    assert_eq!(report.segments_removed, 1);
    assert_eq!(segments(root.path())?.len(), 1);
    drop(provider);
    let provider = create(root.path()).await?;
    assert_eq!(provider.put(put("value"), before_body).await?, original);
    assert_eq!(provider.get(get("value"), 65_537).await?.body, after_body);
    Ok(())
}

#[tokio::test]
async fn staged_parts_remain_private_and_retained_until_abort()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let provider = seeded(root.path()).await?;
    let upload = provider
        .create_multipart(wire::CreateMultipartRequest {
            bucket: bucket(),
            object_key: "staged".into(),
            ..Default::default()
        })
        .await?;
    let part = provider
        .upload_part(
            wire::UploadPartHeader {
                bucket: bucket(),
                object_key: "staged".into(),
                upload_id: upload.upload_id.clone(),
                part_number: 1,
                mutation: None,
            },
            Bytes::from_static(b"private"),
        )
        .await?;
    assert_eq!(provider.collect_garbage(10).await?.segments_removed, 0);
    drop(provider);
    let provider = create(root.path()).await?;
    assert_eq!(
        provider
            .list_parts(wire::ListPartsRequest {
                bucket: bucket(),
                object_key: "staged".into(),
                upload_id: upload.upload_id.clone(),
                page_size: 1,
                ..Default::default()
            })
            .await?
            .parts,
        [part]
    );
    assert_eq!(
        provider.get(get("staged"), 10).await.err().map(|e| e.code),
        Some(wire::ErrorCode::NotFound)
    );
    provider
        .abort_multipart(wire::AbortMultipartRequest {
            bucket: bucket(),
            object_key: "staged".into(),
            upload_id: upload.upload_id,
            mutation: None,
        })
        .await?;
    assert_eq!(provider.collect_garbage(10).await?.segments_removed, 1);
    drop(provider);
    let provider = create(root.path()).await?;
    assert_eq!(
        provider.get(get("staged"), 10).await.err().map(|e| e.code),
        Some(wire::ErrorCode::NotFound)
    );
    Ok(())
}

#[tokio::test]
async fn corrupt_retained_body_stops_collection_before_any_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let provider = seeded(root.path()).await?;
    provider
        .put(put("value"), Bytes::from(vec![b'b'; 65_537]))
        .await?;
    let old = segments(root.path())?;
    let mut replacement = put("value");
    replacement.mutation = identity("replacement");
    provider
        .put(replacement, Bytes::from(vec![b'a'; 65_537]))
        .await?;
    let all = segments(root.path())?;
    let live = all.difference(&old).next().ok_or("missing live segment")?;
    let mut bytes = std::fs::read(live)?;
    *bytes.last_mut().ok_or("empty segment")? ^= 1;
    std::fs::write(live, bytes)?;
    assert!(matches!(
        provider.collect_garbage(10).await,
        Err(LocalOpenError::Corrupt)
    ));
    assert_eq!(segments(root.path())?, all);
    Ok(())
}

#[tokio::test]
async fn collection_waits_for_an_admitted_physical_read_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let provider = seeded(root.path()).await?;
    provider
        .put(put("deleted"), Bytes::from(vec![b'a'; 65_537]))
        .await?;
    let lease = provider.body_io.clone().read_owned().await;
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: bucket(),
            object_key: "deleted".into(),
            ..Default::default()
        })
        .await?;
    let collector = provider.clone();
    let mut task = tokio::spawn(async move { collector.collect_garbage(10).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut task)
            .await
            .is_err()
    );
    assert_eq!(segments(root.path())?.len(), 1);
    drop(lease);
    assert_eq!(task.await??.segments_removed, 1);
    Ok(())
}

#[tokio::test]
async fn dominant_inline_bytes_trigger_owned_maintenance_after_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let provider = seeded(root.path()).await?;
    let body = Bytes::from(vec![42; 1024]);
    let first = provider.put(put("first"), body.clone()).await?;
    assert!(segments(root.path())?.is_empty());
    assert!(provider.core.local_maintenance_due()?);
    drop(provider);
    let provider = create(root.path()).await?;
    assert!(provider.core.local_maintenance_due()?);
    provider.put(put("second"), body.clone()).await?;
    assert_eq!(segments(root.path())?.len(), 1);
    let before = segments(root.path())?;
    assert!(provider.put_batch(Vec::new()).await.is_empty());
    assert!(provider.get_batch(Vec::new()).await.is_empty());
    assert_eq!(segments(root.path())?, before);
    assert_eq!(provider.put(put("first"), body.clone()).await?, first);
    drop(provider);
    let provider = create(root.path()).await?;
    assert_eq!(provider.get(get("first"), 1024).await?.body, body);
    assert_eq!(provider.get(get("second"), 1024).await?.body, body);
    Ok(())
}
