use super::*;

pub(super) async fn objects_history(root: &Path, seed: u64) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(|e| e.to_string())?;
    let bucket = provider
        .create_bucket(wire::CreateBucketRequest {
            name: "consumer-history".into(),
            mutation: None,
        })
        .await
        .map_err(|e| e.to_string())?
        .bucket
        .ok_or("bucket creation returned no identity")?;
    let mut expected = std::collections::BTreeMap::<String, Bytes>::new();
    let mut rng = seed | 1;
    for step in 0..48_u64 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let key = format!("entry-{:02}", rng % 8);
        if step % 5 == 2 {
            let request = wire::DeleteObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: key.clone(),
                mutation: Some(wire::MutationIdentity {
                    idempotency_key: format!("delete-{step}"),
                }),
                ..Default::default()
            };
            let result = provider
                .delete(request.clone())
                .await
                .map_err(|e| e.to_string())?;
            let replay = provider.delete(request).await.map_err(|e| e.to_string())?;
            if result != replay {
                return Err(format!("step {step}: delete retry changed its outcome"));
            }
            expected.remove(&key);
        } else {
            let body = Bytes::copy_from_slice(&rng.to_le_bytes());
            let request = wire::PutObjectHeader {
                bucket: Some(bucket.clone()),
                object_key: key.clone(),
                mutation: Some(wire::MutationIdentity {
                    idempotency_key: format!("put-{step}"),
                }),
                ..Default::default()
            };
            let object = provider
                .put(request.clone(), body.clone().into())
                .await
                .map_err(|e| e.to_string())?;
            let replay = provider
                .put(request, body.clone().into())
                .await
                .map_err(|e| e.to_string())?;
            if object != replay || object.size != body.len() as u64 {
                return Err(format!("step {step}: put retry or size differs"));
            }
            expected.insert(key.clone(), body);
        }
        verify_objects_current(&provider, &bucket, &expected, &key, step).await?;
    }
    drop(provider);
    let reopened = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(|e| e.to_string())?;
    reopened
        .head_bucket(wire::HeadBucketRequest {
            bucket: Some(bucket.clone()),
        })
        .await
        .map_err(|e| e.to_string())?;
    for (key, body) in expected {
        let observed = reopened
            .get(
                wire::GetObjectRequest {
                    bucket: Some(bucket.clone()),
                    object_key: key.clone(),
                    ..Default::default()
                },
                8,
            )
            .await
            .map_err(|e| e.to_string())?;
        if observed.body().as_ref() != body.as_ref() {
            return Err(format!("reopened object {key} differs from model"));
        }
    }
    Ok(())
}

pub(super) async fn objects_many_chunks_setup(root: &Path) -> Result<(), String> {
    const CHUNK_BYTES: usize = 1_024 * 1_024;
    const CHUNKS: usize = 48;
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(|e| e.to_string())?;
    let bucket = provider
        .create_bucket(wire::CreateBucketRequest {
            name: "many-chunks".into(),
            mutation: None,
        })
        .await
        .map_err(|e| e.to_string())?
        .bucket
        .ok_or("bucket creation returned no identity")?;
    let mut body = vec![0_u8; CHUNK_BYTES * CHUNKS];
    for (index, chunk) in body.chunks_mut(CHUNK_BYTES).enumerate() {
        chunk.fill(u8::try_from(index).map_err(|_| "chunk index exceeds byte range")?);
    }
    provider
        .put(
            wire::PutObjectHeader {
                bucket: Some(bucket),
                object_key: "many-chunks".into(),
                mutation: Some(wire::MutationIdentity {
                    idempotency_key: "put-many-chunks".into(),
                }),
                ..Default::default()
            },
            acyclic_stream::OwnedBytes::from(body),
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn objects_sparse_first_put(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(|e| e.to_string())?;
    let bucket = provider
        .create_bucket(wire::CreateBucketRequest {
            name: "sparse".into(),
            mutation: None,
        })
        .await
        .map_err(|e| e.to_string())?
        .bucket
        .ok_or("sparse bucket creation returned no identity")?;
    provider
        .put(
            wire::PutObjectHeader {
                bucket: Some(bucket),
                object_key: "only-object".into(),
                ..Default::default()
            },
            acyclic_stream::OwnedBytes::from_static(b"sparse body"),
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn objects_sparse_reopen_gc(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(|e| e.to_string())?;
    let bucket = wire::BucketRef {
        name: "sparse".into(),
    };
    provider
        .head_bucket(wire::HeadBucketRequest {
            bucket: Some(bucket.clone()),
        })
        .await
        .map_err(|e| e.to_string())?;
    provider
        .collect_garbage(100)
        .await
        .map_err(|e| e.to_string())?;
    let object = provider
        .get(
            wire::GetObjectRequest {
                bucket: Some(bucket),
                object_key: "only-object".into(),
                ..Default::default()
            },
            11,
        )
        .await
        .map_err(|e| e.to_string())?;
    if object.body().as_ref() != b"sparse body" {
        return Err("sparse body changed after reopen and collection".into());
    }
    Ok(())
}

pub(super) async fn objects_many_chunks_startup(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(|e| e.to_string())?;
    provider
        .head_bucket(wire::HeadBucketRequest {
            bucket: Some(wire::BucketRef {
                name: "many-chunks".into(),
            }),
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) async fn verify_objects_current(
    provider: &impl ObjectsProvider,
    bucket: &wire::BucketRef,
    expected: &std::collections::BTreeMap<String, Bytes>,
    selected_key: &str,
    step: u64,
) -> Result<(), String> {
    let mut observed = std::collections::BTreeSet::new();
    let mut continuation = String::new();
    for page_number in 0..4 {
        let page = provider
            .list(wire::ListObjectsRequest {
                bucket: Some(bucket.clone()),
                page_size: 3,
                continuation_token: continuation,
                ..Default::default()
            })
            .await
            .map_err(|e| e.to_string())?;
        for entry in page.entries {
            if entry.object.is_none() || !observed.insert(entry.object_key) {
                return Err(format!(
                    "step {step}: listing returned a duplicate or missing object"
                ));
            }
        }
        continuation = page.continuation_token;
        if continuation.is_empty() {
            break;
        }
        if page_number == 3 {
            return Err(format!("step {step}: listing continuation did not finish"));
        }
    }
    if observed != expected.keys().cloned().collect() {
        return Err(format!("step {step}: listing differs from model"));
    }
    let request = wire::GetObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: selected_key.to_owned(),
        range: Some(wire::ByteRange {
            selection: Some(wire::byte_range::Selection::Bytes(wire::InclusiveRange {
                start: 1,
                end: Some(3),
            })),
        }),
        ..Default::default()
    };
    match (expected.get(selected_key), provider.get(request, 3).await) {
        (Some(body), Ok(result)) if body.get(1..4) == Some(result.body().as_ref()) => Ok(()),
        (None, Err(error)) if error.code == wire::ErrorCode::NotFound => Ok(()),
        _ => Err(format!("step {step}: range read differs from model")),
    }
}
