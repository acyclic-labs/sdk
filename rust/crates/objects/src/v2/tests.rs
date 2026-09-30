use super::{Error, MemoryObjects, MemoryOptions, ObjectsProvider, wire};
use bytes::Bytes;

#[cfg(any(feature = "grpc", all(feature = "http", not(target_arch = "wasm32"))))]
#[allow(clippy::too_many_lines)]
pub(super) async fn exercise_uploads<'a>(
    provider: &dyn ObjectsProvider,
    put_stream: impl Fn(
        wire::PutObjectHeader,
        super::UploadBody,
    ) -> futures::future::BoxFuture<'a, Result<wire::ObjectInfo, Error>>,
    part_stream: impl Fn(
        wire::UploadPartHeader,
        super::UploadBody,
    ) -> futures::future::BoxFuture<'a, Result<wire::UploadedPart, Error>>,
) -> Result<(), Error> {
    use futures::{StreamExt, future::Either, stream};
    provider
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    let bytes = Bytes::from(vec![23; 135_000]);
    let result = put_stream(
        put("stream"),
        stream::iter([Ok(Bytes::new()), Ok(bytes.clone())]).boxed(),
    )
    .await?;
    assert_eq!(result.size, 135_000);
    assert_eq!(provider.get(get("stream"), 135_000).await?.body, bytes);
    let failure = || {
        stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(Error::from(wire::ErrorCode::AccessDenied)),
        ])
        .boxed()
    };
    let mut failed = put("failed");
    failed.mutation = identity("source-failure");
    assert_eq!(
        put_stream(failed.clone(), failure())
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    assert_eq!(
        provider
            .head(wire::HeadObjectRequest {
                bucket: bucket(),
                object_key: "failed".into(),
                ..Default::default()
            })
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::NotFound)
    );
    let accepted = put_stream(
        failed,
        stream::iter([Ok(Bytes::from_static(b"complete"))]).boxed(),
    )
    .await?;
    assert_eq!(accepted.size, 8);
    assert_eq!(
        put_stream(put("stream"), failure())
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    assert_eq!(provider.get(get("stream"), 135_000).await?.body, bytes);
    let (started, started_rx) = futures::channel::oneshot::channel();
    let pending = stream::once(async move {
        let _ = started.send(());
        Ok(Bytes::from_static(b"partial"))
    })
    .chain(stream::pending())
    .boxed();
    let cancelled = put_stream(put("cancelled"), pending);
    match futures::future::select(cancelled, started_rx).await {
        Either::Right((Ok(()), request)) => drop(request),
        _ => return Err(wire::ErrorCode::Unavailable.into()),
    }
    assert_eq!(
        provider
            .head(wire::HeadObjectRequest {
                bucket: bucket(),
                object_key: "cancelled".into(),
                ..Default::default()
            })
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::NotFound)
    );
    let upload = provider
        .create_multipart(wire::CreateMultipartRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            ..Default::default()
        })
        .await?;
    let header = wire::UploadPartHeader {
        bucket: bucket(),
        object_key: "multipart".into(),
        upload_id: upload.upload_id.clone(),
        part_number: 1,
        mutation: None,
    };
    let part = part_stream(header.clone(), stream::iter([Ok(bytes.clone())]).boxed()).await?;
    assert_eq!(
        part_stream(header, failure())
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    let parts = provider
        .list_parts(wire::ListPartsRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            upload_id: upload.upload_id.clone(),
            ..Default::default()
        })
        .await?;
    assert_eq!(parts.parts, vec![part]);
    provider
        .abort_multipart(wire::AbortMultipartRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            upload_id: upload.upload_id,
            mutation: None,
        })
        .await?;
    for key in ["stream", "failed"] {
        provider
            .delete(wire::DeleteObjectRequest {
                bucket: bucket(),
                object_key: key.into(),
                ..Default::default()
            })
            .await?;
    }
    provider
        .delete_bucket(wire::DeleteBucketRequest {
            bucket: bucket(),
            mutation: None,
        })
        .await?;
    Ok(())
}

#[cfg(any(feature = "grpc", all(feature = "http", not(target_arch = "wasm32"))))]
pub(super) async fn exercise_streaming<'a>(
    provider: &dyn ObjectsProvider,
    get: impl Fn(
        wire::GetObjectRequest,
        u64,
    ) -> futures::future::BoxFuture<'a, Result<super::Download, Error>>,
) -> Result<(), Error> {
    use futures::StreamExt;
    provider
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    let bytes = Bytes::from(vec![42; 135_000]);
    provider.put(put("stream"), bytes.clone()).await?;
    let mut selected = get(self::get("stream"), 135_000).await?;
    assert_eq!(
        selected.header.object.as_ref().map(|info| info.size),
        Some(135_000)
    );
    let mut result = Vec::new();
    let mut chunks = 0;
    while let Some(chunk) = selected.body.next().await {
        let chunk = chunk?;
        assert!(chunk.len() <= 65_536);
        result.extend_from_slice(&chunk);
        chunks += 1;
    }
    assert_eq!(chunks, 3);
    assert_eq!(result, bytes);
    let mut cancelled = get(self::get("stream"), 135_000).await?;
    assert!(cancelled.body.next().await.transpose()?.is_some());
    drop(cancelled);
    assert_eq!(
        get(self::get("stream"), 135_000)
            .await?
            .collect(135_000)
            .await?
            .body,
        bytes
    );
    assert_eq!(
        get(self::get("stream"), 16)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    provider
        .put(put("terminal-error"), Bytes::from_static(b"body"))
        .await?;
    let failed = get(self::get("terminal-error"), 16).await?;
    assert_eq!(
        failed.collect(16).await.err().map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    for key in ["stream", "terminal-error"] {
        provider
            .delete(wire::DeleteObjectRequest {
                bucket: bucket(),
                object_key: key.into(),
                ..Default::default()
            })
            .await?;
    }
    provider
        .delete_bucket(wire::DeleteBucketRequest {
            bucket: bucket(),
            mutation: None,
        })
        .await?;
    Ok(())
}

fn bucket() -> Option<wire::BucketRef> {
    Some(wire::BucketRef {
        name: "customer.inputs".into(),
    })
}
fn identity(key: &str) -> Option<wire::MutationIdentity> {
    Some(wire::MutationIdentity {
        idempotency_key: key.into(),
    })
}
fn put(key: &str) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: bucket(),
        object_key: key.into(),
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
async fn provider(options: MemoryOptions) -> Result<MemoryObjects, Error> {
    let provider = MemoryObjects::new(options)?;
    provider
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    Ok(provider)
}

#[tokio::test]
async fn conditional_publication_and_retry_do_not_overwrite_later_values() -> Result<(), Error> {
    let provider = provider(MemoryOptions::default()).await?;
    let mut original = put("value");
    original.mutation = identity("publish-once");
    let first = provider
        .put(original.clone(), Bytes::from_static(b"first"))
        .await?;
    let mut replace = put("value");
    replace.preconditions = Some(wire::Preconditions {
        condition: Some(wire::preconditions::Condition::IfMatch(first.etag.clone())),
    });
    let (left, right) = tokio::join!(
        provider.put(replace.clone(), Bytes::from_static(b"left")),
        provider.put(replace, Bytes::from_static(b"right"))
    );
    assert_ne!(left.is_ok(), right.is_ok());
    assert_eq!(
        left.as_ref()
            .err()
            .or(right.as_ref().err())
            .map(|error| error.code),
        Some(wire::ErrorCode::PreconditionFailed)
    );
    let current = provider.get(get("value"), 16).await?;
    assert_eq!(
        provider
            .put(original.clone(), Bytes::from_static(b"first"))
            .await?,
        first
    );
    assert_eq!(provider.get(get("value"), 16).await?, current);
    assert_eq!(
        provider
            .put(original, Bytes::from_static(b"changed"))
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::IdempotencyMismatch)
    );
    let mut absent = put("value");
    absent.preconditions = Some(wire::Preconditions {
        condition: Some(wire::preconditions::Condition::IfAbsent(true)),
    });
    assert_eq!(
        provider
            .put(absent, Bytes::new())
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::PreconditionFailed)
    );
    Ok(())
}

#[tokio::test]
async fn quota_failure_rolls_back_publication_and_retry_receipt() -> Result<(), Error> {
    let provider = provider(MemoryOptions {
        maximum_bytes: 5,
        maximum_entries: 16,
    })
    .await?;
    provider
        .put(put("old"), Bytes::from_static(b"1234"))
        .await?;
    let mut attempted = put("new");
    attempted.mutation = identity("retry");
    assert_eq!(
        provider
            .put(attempted.clone(), Bytes::from_static(b"ab"))
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    assert_eq!(
        provider
            .get(get("new"), 16)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::NotFound)
    );
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: bucket(),
            object_key: "old".into(),
            ..Default::default()
        })
        .await?;
    provider.put(attempted, Bytes::from_static(b"ab")).await?;
    assert_eq!(
        provider
            .get(get("new"), 1)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    let mut suffix = get("new");
    suffix.range = Some(wire::ByteRange {
        selection: Some(wire::byte_range::Selection::SuffixLength(1)),
    });
    assert_eq!(
        provider.get(suffix, 1).await?.body,
        Bytes::from_static(b"b")
    );
    Ok(())
}

#[tokio::test]
async fn listing_is_live_query_bound_and_counts_common_prefixes() -> Result<(), Error> {
    let provider = provider(MemoryOptions::default()).await?;
    for key in ["a/file", "a/other", "c", "é/file"] {
        provider.put(put(key), Bytes::new()).await?;
    }
    let mut query = wire::ListObjectsRequest {
        bucket: bucket(),
        delimiter: "/".into(),
        page_size: 1,
        ..Default::default()
    };
    let page = provider.list(query.clone()).await?;
    assert_eq!(page.common_prefixes, ["a/"]);
    assert!(page.is_truncated);
    assert!(page.entries.is_empty());
    query.continuation_token = page.continuation_token;
    let mut changed = query.clone();
    changed.prefix = "a".into();
    assert_eq!(
        provider.list(changed).await.err().map(|error| error.code),
        Some(wire::ErrorCode::InvalidArgument)
    );
    provider.put(put("b"), Bytes::new()).await?; // Inserted after the cursor, visible on the next live page.
    provider.put(put("0"), Bytes::new()).await?; // Inserted before the cursor, intentionally missed.
    let page = provider.list(query.clone()).await?;
    assert_eq!(
        page.entries.first().map(|entry| entry.object_key.as_str()),
        Some("b")
    );
    query.continuation_token = page.continuation_token;
    let page = provider.list(query.clone()).await?;
    assert_eq!(
        page.entries.first().map(|entry| entry.object_key.as_str()),
        Some("c")
    );
    query.continuation_token = page.continuation_token;
    let page = provider.list(query).await?;
    assert_eq!(page.common_prefixes, ["é/"]);
    assert!(!page.is_truncated);
    Ok(())
}

#[tokio::test]
async fn multipart_failure_keeps_parts_and_completion_is_atomic_and_replayable() -> Result<(), Error>
{
    let provider = provider(MemoryOptions::default()).await?;
    let old = provider
        .put(put("value"), Bytes::from_static(b"old"))
        .await?;
    let upload = provider
        .create_multipart(wire::CreateMultipartRequest {
            bucket: bucket(),
            object_key: "value".into(),
            ..Default::default()
        })
        .await?;
    let first = provider
        .upload_part(
            wire::UploadPartHeader {
                bucket: bucket(),
                object_key: "value".into(),
                upload_id: upload.upload_id.clone(),
                part_number: 1,
                mutation: None,
            },
            Bytes::from(vec![b'a'; 5 * 1024 * 1024]),
        )
        .await?;
    let last = provider
        .upload_part(
            wire::UploadPartHeader {
                bucket: bucket(),
                object_key: "value".into(),
                upload_id: upload.upload_id.clone(),
                part_number: 2,
                mutation: None,
            },
            Bytes::from_static(b"end"),
        )
        .await?;
    let mut complete = wire::CompleteMultipartRequest {
        bucket: bucket(),
        object_key: "value".into(),
        upload_id: upload.upload_id.clone(),
        parts: vec![first.clone(), last.clone()],
        preconditions: Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfMatch("stale".into())),
        }),
        mutation: identity("complete-once"),
    };
    assert_eq!(
        provider
            .complete_multipart(complete.clone())
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::PreconditionFailed)
    );
    assert_eq!(
        provider.get(get("value"), 16).await?.body,
        Bytes::from_static(b"old")
    );
    let parts = provider
        .list_parts(wire::ListPartsRequest {
            bucket: bucket(),
            object_key: "value".into(),
            upload_id: upload.upload_id.clone(),
            page_size: 1,
            after_part_number: 0,
        })
        .await?;
    assert_eq!(parts.parts, [first]);
    assert_eq!(parts.next_part_number, 1);
    assert!(parts.is_truncated);
    complete.preconditions = Some(wire::Preconditions {
        condition: Some(wire::preconditions::Condition::IfMatch(old.etag)),
    });
    let info = provider.complete_multipart(complete.clone()).await?;
    assert_eq!(info.size, 5 * 1024 * 1024 + 3);
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: bucket(),
            object_key: "value".into(),
            ..Default::default()
        })
        .await?;
    assert_eq!(provider.complete_multipart(complete).await?, info); // Retried completion does not resurrect the deleted object.
    assert_eq!(
        provider
            .get(get("value"), 16)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::NotFound)
    );
    let aborted = provider
        .abort_multipart(wire::AbortMultipartRequest {
            bucket: bucket(),
            object_key: "value".into(),
            upload_id: upload.upload_id,
            mutation: None,
        })
        .await?;
    assert!(!aborted.existed);
    Ok(())
}

#[cfg(any(feature = "grpc", all(feature = "http", not(target_arch = "wasm32"))))]
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)] // One ordered provider lifecycle qualifies every public RPC through each transport.
pub(super) async fn exercise_provider(
    client: &dyn ObjectsProvider,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    client
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    assert_eq!(
        client
            .head_bucket(wire::HeadBucketRequest { bucket: bucket() })
            .await?
            .bucket,
        bucket()
    );
    let body = Bytes::from(vec![b'x'; 2 * 65536 + 17]);
    let info = client
        .put(
            wire::PutObjectHeader {
                bucket: bucket(),
                object_key: "value".into(),
                ..Default::default()
            },
            body.clone(),
        )
        .await?;
    let get = wire::GetObjectRequest {
        bucket: bucket(),
        object_key: "value".into(),
        ..Default::default()
    };
    assert_eq!(client.get(get.clone(), body.len() as u64).await?.body, body);
    assert_eq!(
        client
            .get(get.clone(), 1)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    let mut conditional = get.clone();
    conditional.if_none_match = info.etag.clone();
    assert_eq!(
        client
            .get(conditional, u64::MAX)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::NotModified)
    );
    let mut ranged = get;
    ranged.range = Some(wire::ByteRange {
        selection: Some(wire::byte_range::Selection::SuffixLength(17)),
    });
    assert_eq!(client.get(ranged, 17).await?.body.len(), 17);
    assert_eq!(
        client
            .head(wire::HeadObjectRequest {
                bucket: bucket(),
                object_key: "value".into(),
                ..Default::default()
            })
            .await?
            .object,
        Some(info)
    );
    assert_eq!(
        client
            .list(wire::ListObjectsRequest {
                bucket: bucket(),
                ..Default::default()
            })
            .await?
            .entries
            .len(),
        1
    );
    let upload = client
        .create_multipart(wire::CreateMultipartRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            ..Default::default()
        })
        .await?;
    let part = client
        .upload_part(
            wire::UploadPartHeader {
                bucket: bucket(),
                object_key: "multipart".into(),
                upload_id: upload.upload_id.clone(),
                part_number: 1,
                mutation: None,
            },
            body.clone(),
        )
        .await?;
    assert_eq!(
        client
            .list_parts(wire::ListPartsRequest {
                bucket: bucket(),
                object_key: "multipart".into(),
                upload_id: upload.upload_id.clone(),
                ..Default::default()
            })
            .await?
            .parts,
        std::slice::from_ref(&part)
    );
    client
        .complete_multipart(wire::CompleteMultipartRequest {
            bucket: bucket(),
            object_key: "multipart".into(),
            upload_id: upload.upload_id.clone(),
            parts: vec![part],
            ..Default::default()
        })
        .await?;
    assert!(
        !client
            .abort_multipart(wire::AbortMultipartRequest {
                bucket: bucket(),
                object_key: "multipart".into(),
                upload_id: upload.upload_id,
                mutation: None
            })
            .await?
            .existed
    );
    client
        .put(
            wire::PutObjectHeader {
                bucket: bucket(),
                object_key: "malformed".into(),
                ..Default::default()
            },
            Bytes::from_static(b"no"),
        )
        .await?;
    assert_eq!(
        client
            .get(
                wire::GetObjectRequest {
                    bucket: bucket(),
                    object_key: "malformed".into(),
                    ..Default::default()
                },
                2
            )
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::Unavailable)
    );
    assert_eq!(
        client
            .delete_bucket(wire::DeleteBucketRequest {
                bucket: bucket(),
                mutation: None
            })
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::PreconditionFailed)
    );
    client
        .put(
            wire::PutObjectHeader {
                bucket: bucket(),
                object_key: "terminal-error".into(),
                ..Default::default()
            },
            Bytes::from_static(b"partial"),
        )
        .await?;
    assert_eq!(
        client
            .get(
                wire::GetObjectRequest {
                    bucket: bucket(),
                    object_key: "terminal-error".into(),
                    ..Default::default()
                },
                7
            )
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    for key in ["value", "multipart", "malformed", "terminal-error"] {
        assert!(
            client
                .delete(wire::DeleteObjectRequest {
                    bucket: bucket(),
                    object_key: key.into(),
                    ..Default::default()
                })
                .await?
                .existed
        );
    }
    assert!(
        client
            .delete_bucket(wire::DeleteBucketRequest {
                bucket: bucket(),
                mutation: None
            })
            .await?
            .existed
    );
    Ok(())
}
