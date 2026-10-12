use super::{Error, MemoryObjects, MemoryOptions, ObjectsProvider, wire};
use acyclic_stream::OwnedBytes;

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]
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
    let bytes = OwnedBytes::from(vec![23; 135_000]);
    let result = put_stream(
        put("stream"),
        stream::iter([Ok(OwnedBytes::new()), Ok(bytes.clone())]).boxed().into(),
    )
    .await?;
    assert_eq!(result.size, 135_000);
    assert_eq!(provider.get(get("stream"), 135_000).await?.into_body(), bytes);
    let failure = || {
        stream::iter([
            Ok(OwnedBytes::from_static(b"partial")),
            Err(Error::from(wire::ErrorCode::AccessDenied)),
        ])
        .boxed().into()
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
        stream::iter([Ok(OwnedBytes::from_static(b"complete"))]).boxed().into(),
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
    assert_eq!(provider.get(get("stream"), 135_000).await?.into_body(), bytes);
    let (started, started_rx) = futures::channel::oneshot::channel();
    let pending = stream::once(async move {
        let _ = started.send(());
        Ok(OwnedBytes::from_static(b"partial"))
    })
    .chain(stream::pending())
    .boxed().into();
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
    let part = part_stream(header.clone(), stream::iter([Ok(bytes.clone())]).boxed().into()).await?;
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

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
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
    let bytes = OwnedBytes::from((0..=250u8).cycle().take(135_000).collect::<Vec<_>>());
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
    assert_eq!(result.as_slice(), bytes.as_slice());
    let mut cancelled = get(self::get("stream"), 135_000).await?;
    assert!(cancelled.body.next().await.transpose()?.is_some());
    drop(cancelled);
    assert_eq!(
        get(self::get("stream"), 135_000)
            .await?
            .collect(135_000)
            .await?
            .into_body(),
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
        .put(put("terminal-error"), OwnedBytes::from_static(b"body"))
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

pub(super) fn bucket() -> Option<wire::BucketRef> {
    Some(wire::BucketRef {
        name: "customer.inputs".into(),
    })
}
pub(super) fn identity(key: &str) -> Option<wire::MutationIdentity> {
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
pub(super) fn get(key: &str) -> wire::GetObjectRequest {
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
#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]
async fn native_batch_preserves_order_bounds_and_failed_item_rollback() -> Result<(), Error> {
    let provider = provider(MemoryOptions {
        maximum_bytes: 6,
        maximum_entries: 20,
    })
    .await?;
    let mut first = put("first");
    first.mutation = identity("first-receipt");
    let mut second = put("second");
    second.mutation = identity("second-receipt");
    let mut invalid = put("invalid");
    invalid.object_key = "\0".into();
    let results = provider
        .put_batch(vec![
            (first.clone(), OwnedBytes::from_static(b"abc")),
            (second.clone(), OwnedBytes::from_static(b"1234")),
            (invalid, OwnedBytes::new()),
            (put("third"), OwnedBytes::from_static(b"xyz")),
        ])
        .await;
    assert_eq!(results.len(), 4);
    let original = results
        .first()
        .cloned()
        .ok_or(Error::from(wire::ErrorCode::Unavailable))??;
    assert_eq!(
        results
            .get(1)
            .and_then(|value| value.as_ref().err())
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    assert_eq!(
        results
            .get(2)
            .and_then(|value| value.as_ref().err())
            .map(|error| error.code),
        Some(wire::ErrorCode::InvalidArgument)
    );
    assert_eq!(
        results
            .get(3)
            .and_then(|value| value.as_ref().ok())
            .map(|info| info.size),
        Some(3)
    );
    let reads = provider
        .get_batch(vec![
            (get("third"), 3),
            (get("first"), 2),
            (get("second"), 8),
            (get("first"), 3),
        ])
        .await;
    assert_eq!(reads.len(), 4);
    assert_eq!(
        reads
            .first()
            .and_then(|value| value.as_ref().ok())
            .map(|object| object.body().as_ref()),
        Some(b"xyz".as_slice())
    );
    assert_eq!(
        reads
            .get(1)
            .and_then(|value| value.as_ref().err())
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    assert_eq!(
        reads
            .get(2)
            .and_then(|value| value.as_ref().err())
            .map(|error| error.code),
        Some(wire::ErrorCode::NotFound)
    );
    assert_eq!(
        reads
            .get(3)
            .and_then(|value| value.as_ref().ok())
            .and_then(|object| object.header().object.as_ref()),
        Some(&original)
    );
    // Earlier successful items keep exact receipts; the failed item created none.
    let replay = provider
        .put_batch(vec![(first, OwnedBytes::from_static(b"abc"))])
        .await;
    assert_eq!(
        replay.first().and_then(|value| value.as_ref().ok()),
        Some(&original)
    );
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: bucket(),
            object_key: "third".into(),
            ..Default::default()
        })
        .await?;
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: bucket(),
            object_key: "first".into(),
            ..Default::default()
        })
        .await?;
    assert_eq!(
        provider
            .put(second, OwnedBytes::from_static(b"1234"))
            .await?
            .size,
        4
    );
    assert!(provider.put_batch(Vec::new()).await.is_empty());
    assert!(provider.get_batch(Vec::new()).await.is_empty());
    Ok(())
}

#[tokio::test]
async fn conditional_publication_and_retry_do_not_overwrite_later_values() -> Result<(), Error> {
    let provider = provider(MemoryOptions::default()).await?;
    let mut original = put("value");
    original.mutation = identity("publish-once");
    let first = provider
        .put(original.clone(), OwnedBytes::from_static(b"first"))
        .await?;
    let mut replace = put("value");
    replace.preconditions = Some(wire::Preconditions {
        condition: Some(wire::preconditions::Condition::IfMatch(first.etag.clone())),
    });
    let (left, right) = tokio::join!(
        provider.put(replace.clone(), OwnedBytes::from_static(b"left")),
        provider.put(replace, OwnedBytes::from_static(b"right"))
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
            .put(original.clone(), OwnedBytes::from_static(b"first"))
            .await?,
        first
    );
    assert_eq!(provider.get(get("value"), 16).await?, current);
    assert_eq!(
        provider
            .put(original, OwnedBytes::from_static(b"changed"))
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
            .put(absent, OwnedBytes::new())
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
        .put(put("old"), OwnedBytes::from_static(b"1234"))
        .await?;
    let mut attempted = put("new");
    attempted.mutation = identity("retry");
    assert_eq!(
        provider
            .put(attempted.clone(), OwnedBytes::from_static(b"ab"))
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
    provider.put(attempted, OwnedBytes::from_static(b"ab")).await?;
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
        provider.get(suffix, 1).await?.into_body(),
        OwnedBytes::from_static(b"b")
    );
    Ok(())
}

#[tokio::test]
async fn listing_is_live_query_bound_and_counts_common_prefixes() -> Result<(), Error> {
    let provider = provider(MemoryOptions::default()).await?;
    for key in ["a/file", "a/other", "c", "é/file"] {
        provider.put(put(key), OwnedBytes::new()).await?;
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
    provider.put(put("b"), OwnedBytes::new()).await?; // Inserted after the cursor, visible on the next live page.
    provider.put(put("0"), OwnedBytes::new()).await?; // Inserted before the cursor, intentionally missed.
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

/// Pages a listing to exhaustion as `(name, is_common_prefix)` in name order.
async fn paged_listing(
    keys: &[String],
    prefix: &str,
    delimiter: &str,
    page_size: u32,
) -> Result<Vec<(String, bool)>, Error> {
    let provider = provider(MemoryOptions::default()).await?;
    for key in keys {
        provider.put(put(key), OwnedBytes::new()).await?;
    }
    let mut query = wire::ListObjectsRequest {
        bucket: bucket(),
        prefix: prefix.into(),
        delimiter: delimiter.into(),
        page_size,
        ..Default::default()
    };
    let mut listed = Vec::new();
    loop {
        let page = provider.list(query.clone()).await?;
        let mut entries: Vec<_> = page
            .entries
            .into_iter()
            .map(|entry| (entry.object_key, false))
            .chain(
                page.common_prefixes
                    .into_iter()
                    .map(|prefix| (prefix, true)),
            )
            .collect();
        entries.sort();
        assert!(entries.len() <= page_size as usize);
        assert!(!page.is_truncated || entries.len() == page_size as usize);
        listed.extend(entries);
        assert!(listed.len() <= keys.len(), "pagination repeated an entry");
        if !page.is_truncated {
            return Ok(listed);
        }
        query.continuation_token = page.continuation_token;
    }
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        rng_seed: proptest::test_runner::RngSeed::Fixed(7),
        failure_persistence: None,
        ..proptest::prelude::ProptestConfig::with_cases(64)
    })]

    /// Cursor-seeking pagination yields exactly the full prefix/delimiter listing, including multi-byte and `char::MAX` key boundaries.
    #[test]
    fn paginated_listing_matches_full_listing(
        keys in proptest::collection::vec(listing_name(1..6), 0..24),
        prefix in listing_name(0..3),
        delimiter in proptest::sample::select(vec!["", "/", "b", "ab", "\u{10FFFF}"]),
        page_size in 1..5_u32,
    ) {
        let mut expected = std::collections::BTreeSet::new();
        for key in keys.iter().filter_map(|key| key.strip_prefix(&prefix)) {
            expected.insert(match key.split_once(delimiter).filter(|_| !delimiter.is_empty()) {
                Some((head, _)) => (format!("{prefix}{head}{delimiter}"), true),
                None => (format!("{prefix}{key}"), false),
            });
        }
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let listed = runtime
            .block_on(paged_listing(&keys, &prefix, delimiter, page_size))
            .map_err(|error| proptest::test_runner::TestCaseError::fail(format!("{error:?}")))?;
        proptest::prop_assert_eq!(listed, expected.into_iter().collect::<Vec<_>>());
    }
}

fn listing_name(
    length: std::ops::Range<usize>,
) -> impl proptest::strategy::Strategy<Value = String> {
    use proptest::strategy::Strategy;
    proptest::collection::vec(
        proptest::sample::select(vec!['a', 'b', '/', 'é', char::MAX]),
        length,
    )
    .prop_map(String::from_iter)
}

#[tokio::test]
async fn multipart_failure_keeps_parts_and_completion_is_atomic_and_replayable() -> Result<(), Error>
{
    let provider = provider(MemoryOptions::default()).await?;
    let old = provider
        .put(put("value"), OwnedBytes::from_static(b"old"))
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
            OwnedBytes::from(vec![b'a'; 5 * 1024 * 1024]),
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
            OwnedBytes::from_static(b"end"),
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
        provider.get(get("value"), 16).await?.into_body(),
        OwnedBytes::from_static(b"old")
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
    assert_completed_multipart_range(&provider, &info).await?;
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

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    reason = "one ordered provider lifecycle qualifies every public RPC through each transport"
)]
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
    let body = OwnedBytes::from(vec![b'x'; 2 * 65536 + 17]);
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
    assert_eq!(client.get(get.clone(), body.len() as u64).await?.into_body(), body);
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
    assert_eq!(client.get(ranged, 17).await?.body().len(), 17);
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
            OwnedBytes::from_static(b"no"),
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
            OwnedBytes::from_static(b"partial"),
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

async fn assert_completed_multipart_range(
    provider: &impl ObjectsProvider,
    info: &wire::ObjectInfo,
) -> Result<(), Error> {
    assert_eq!(info.size, 5 * 1024 * 1024 + 3);
    let mut crossing = get("value");
    crossing.range = Some(wire::ByteRange {
        selection: Some(wire::byte_range::Selection::Bytes(wire::InclusiveRange {
            start: 5 * 1024 * 1024 - 2,
            end: Some(5 * 1024 * 1024 + 1),
        })),
    });
    assert_eq!(
        provider.get(crossing.clone(), 4).await?.into_body(),
        OwnedBytes::from_static(b"aaen")
    );
    assert_eq!(
        provider
            .get(crossing, 3)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) struct NativeTransportMeter {
    pub memory: std::sync::atomic::AtomicU64,
    pub limit: std::sync::atomic::AtomicU64,
    pub cancelled: std::sync::atomic::AtomicBool,
    pub cancel_on_copy: std::sync::atomic::AtomicU64,
    pub quota_on_copy: std::sync::atomic::AtomicU64,
    pub allocations: std::sync::atomic::AtomicU64,
    pub allocation_limit: std::sync::atomic::AtomicU64,
    pending_allocations: parking_lot::Mutex<(u64, u64)>,
    pub reads: std::sync::atomic::AtomicU64,
    pub writes: std::sync::atomic::AtomicU64,
    reserved: std::sync::atomic::AtomicU64,
    table: bool,
    pub read_limit: std::sync::atomic::AtomicU64,
    pub write_limit: std::sync::atomic::AtomicU64,
}
#[cfg(not(target_arch = "wasm32"))]
impl NativeTransportMeter {
    pub(super) fn new() -> Self {
        Self {
            memory: 0.into(), limit: u64::MAX.into(), cancelled: false.into(),
            cancel_on_copy: u64::MAX.into(), quota_on_copy: u64::MAX.into(),
            allocations: 0.into(),
            allocation_limit: u64::MAX.into(), pending_allocations: parking_lot::Mutex::new((0, 0)),
            reads: 0.into(), writes: 0.into(), reserved: 0.into(),
            table: false, read_limit: u64::MAX.into(), write_limit: u64::MAX.into(),
        }
    }
    pub(super) fn new_table() -> Self { Self { table: true, ..Self::new() } }
    fn table_event(&self, maximum: usize, counter: &std::sync::atomic::AtomicU64, limit: &std::sync::atomic::AtomicU64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        if !self.table { return Err(std::io::ErrorKind::Unsupported.into()); }
        assert_eq!(maximum, 0, "table events transfer no physical bytes");
        acyclic_native_runtime::NativeReadMeter::check(self)?;
        counter.fetch_update(SeqCst, SeqCst,
            |count| count.checked_add(1).filter(|next| *next <= limit.load(SeqCst)))
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::PermissionDenied))?;
        Ok(())
    }
    fn claim(&self, bytes: u64, allocations: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        acyclic_native_runtime::NativeReadMeter::check(self)?;
        let pending = self.pending_allocations.lock();
        let retained = self.memory.load(SeqCst).checked_add(bytes)
            .ok_or(std::io::ErrorKind::OutOfMemory)?;
        let attempts = self.allocations.load(SeqCst).checked_add(allocations)
            .ok_or(std::io::ErrorKind::OutOfMemory)?;
        if retained.checked_add(pending.0).is_none_or(|value| value > self.limit.load(SeqCst))
            || attempts.checked_add(pending.1).is_none_or(|value| value > self.allocation_limit.load(SeqCst))
        {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        self.memory.fetch_add(bytes, SeqCst);
        self.allocations.fetch_add(allocations, SeqCst);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeTransportInputGuard {
    _allocation: acyclic_native_runtime::ReadAllocation,
    dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeTransportInputGuard {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl acyclic_native_runtime::NativeReadMeter for NativeTransportMeter {
    fn check(&self) -> std::io::Result<()> {
        if self.cancelled.load(std::sync::atomic::Ordering::SeqCst) {
            Err(std::io::ErrorKind::Interrupted.into())
        } else { Ok(()) }
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> { self.claim(bytes, 1) }
    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        self.check()?;
        let mut pending = self.pending_allocations.lock();
        let retained = pending.0.checked_add(bytes).ok_or(std::io::ErrorKind::OutOfMemory)?;
        let attempts = pending.1.checked_add(1).ok_or(std::io::ErrorKind::OutOfMemory)?;
        if self.memory.load(SeqCst).checked_add(retained).is_none_or(|value| value > self.limit.load(SeqCst))
            || self.allocations.load(SeqCst).checked_add(attempts).is_none_or(|value| value > self.allocation_limit.load(SeqCst))
        {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        *pending = (retained, attempts);
        Ok(())
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        let mut pending = self.pending_allocations.lock();
        assert!(pending.0 >= bytes && pending.1 > 0, "allocation credit cannot retire twice");
        pending.0 -= bytes;
        pending.1 -= 1;
    }
    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut pending = self.pending_allocations.lock();
        assert!(pending.0 >= bytes && pending.1 > 0, "owner attempt requires its original allocation credit");
        pending.0 -= bytes;
        pending.1 -= 1;
        self.memory.fetch_add(bytes, SeqCst);
        self.allocations.fetch_add(1, SeqCst);
        Ok(())
    }
    fn allocation_failed(&self, _: std::collections::TryReserveError) -> std::io::Error {
        std::io::ErrorKind::OutOfMemory.into()
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> { self.claim(0, 1) }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> { self.claim(bytes, 0) }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        self.memory.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        (bytes, self.check())
    }
    fn release(&self, bytes: u64) {
        let previous = self.memory.fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
        assert!(previous >= bytes, "original allocation claim cannot retire twice");
    }
    fn admit_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> { self.check() }
    fn record_codec(&self, encoded: u64, copied: u64, _: u64) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        if encoded == 0 && copied >= self.cancel_on_copy.load(SeqCst) {
            self.cancelled.store(true, SeqCst);
        }
        if encoded == 0 && copied >= self.quota_on_copy.load(SeqCst) {
            self.limit.store(0, SeqCst);
        }
        Ok(())
    }
    fn examine(&self, _: u64, copied: u64) -> std::io::Result<()> { self.record_codec(0, copied, 0) }
    fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> { self.check() }
    fn record_examination(&self, _: u64, copied: u64) -> std::io::Result<()> { self.record_codec(0, copied, 0) }
    fn reserve_operations(&self, _: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.check()?;
        self.reserved.fetch_add(count, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn cancel_reserved_operations(&self, _: acyclic_native_runtime::NativeOperationKind, count: u64) {
        let previous = self.reserved.fetch_sub(count, std::sync::atomic::Ordering::SeqCst);
        assert!(previous >= count, "operation credit released twice");
    }
    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        assert!(self.reserved.fetch_sub(1, SeqCst) > 0, "RPC attempt lacks its original reservation");
        match kind {
            acyclic_native_runtime::NativeOperationKind::Read => { self.reads.fetch_add(1, SeqCst); }
            acyclic_native_runtime::NativeOperationKind::Write => { self.writes.fetch_add(1, SeqCst); }
            _ => panic!("transport cannot invent a non-RPC backend operation"),
        }
        self.check()
    }
    fn reserve_bytes(&self, _: acyclic_native_runtime::NativeOperationKind, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn cancel_reserved_bytes(&self, _: acyclic_native_runtime::NativeOperationKind, _: u64) { panic!("transport cannot reserve invented physical bytes") }
    fn record_reserved_bytes(&self, _: acyclic_native_runtime::NativeOperationKind, _: u64, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn admit_bytes(&self, _: usize) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn admit_read(&self, maximum: usize) -> std::io::Result<()> { self.table_event(maximum, &self.reads, &self.read_limit) }
    fn admit_operations(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn record_operations(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn record_read(&self, _: usize) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn admit_write(&self, maximum: usize) -> std::io::Result<()> { self.table_event(maximum, &self.writes, &self.write_limit) }
    fn record_write(&self, _: usize) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn admit_write_operations(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn record_write_operations(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
    fn admit_durability(&self) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_wire_aliases_keep_original_claim_after_context_retirement()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    use acyclic_native_runtime::NativeReadMeter;
    use std::sync::{Arc, atomic::Ordering::SeqCst};
    let observed = Arc::new(NativeTransportMeter::new());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let errors = super::NativeTransportErrors::new(&meter)?;
    let controls = errors;
    let (data, allocation) = controls.copy(b"immutable native wire")?;
    let wire = controls.wire_bytes(data, allocation)?;
    let alias = wire.slice(1..4);
    let attempts = observed.allocations.load(SeqCst);
    drop(controls);
    let retained = observed.memory.load(SeqCst);
    assert!(retained > 0);
    observed.cancelled.store(true, SeqCst);
    observed.limit.store(0, SeqCst);
    let last = alias.clone();
    assert_eq!(last.as_ref(), b"mmu");
    assert_eq!(observed.allocations.load(SeqCst), attempts);
    drop(wire);
    drop(alias);
    assert_eq!(observed.memory.load(SeqCst), retained);
    drop(last);
    assert_eq!(observed.memory.load(SeqCst), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn exercise_native_transport<P: ObjectsProvider>(
    provider: &P,
    physical_get_calls: impl Fn() -> u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use acyclic_native_runtime::NativeReadMeter;
    use std::sync::{Arc, atomic::Ordering::SeqCst};
    let bucket = wire::BucketRef { name: "native.controls".into() };
    provider.create_bucket(wire::CreateBucketRequest { name: bucket.name.clone(), ..Default::default() }).await?;
    let bytes = acyclic_stream::OwnedBytes::from(vec![b'n'; 131_089]);
    for key in ["native-body", "terminal-error", "truncated", "huge-header", "corrupt-codec", "malformed"] {
        provider.put(wire::PutObjectHeader {
            bucket: Some(bucket.clone()), object_key: key.into(), ..Default::default()
        }, bytes.clone()).await?;
    }
    let query = |key: &str| wire::GetObjectRequest {
        bucket: Some(bucket.clone()), object_key: key.into(), ..Default::default()
    };
    let observed = Arc::new(NativeTransportMeter::new());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let object = native_transport_get(provider, query("native-body"), bytes.len() as u64, meter.clone()).await?;
    assert_eq!(object.body(), &bytes);
    assert!(object.body().matches_meter(&meter));
    assert_eq!(observed.writes.load(SeqCst), 0);
    assert_eq!(observed.reserved.load(SeqCst), 0);
    let clone = object.body().clone();
    let slice = object.body().slice(1..17);
    let empty = object.body().slice(0..0);
    let allocations = observed.allocations.load(SeqCst);
    drop(object);
    assert!(observed.memory.load(SeqCst) >= bytes.len() as u64);
    observed.cancelled.store(true, SeqCst);
    observed.limit.store(0, SeqCst);
    assert_eq!(slice.as_ref(), &bytes[1..17]);
    drop(clone);
    drop(slice);
    assert!(observed.memory.load(SeqCst) > 0);
    assert_eq!(observed.allocations.load(SeqCst), allocations);
    drop(empty);
    assert_eq!(observed.memory.load(SeqCst), 0);

    for (cancelled, limit, expected) in [
        (true, u64::MAX, std::io::ErrorKind::Interrupted),
        (false, 1, std::io::ErrorKind::OutOfMemory),
    ] {
        let observed = Arc::new(NativeTransportMeter::new());
        observed.cancelled.store(cancelled, SeqCst);
        observed.limit.store(limit, SeqCst);
        let calls_before = physical_get_calls();
        let error = native_transport_get(provider, query("native-body"), bytes.len() as u64, observed.clone())
            .await.err().ok_or("native preparation accepted cancellation/quota")?;
        match error.into_cause() {
            super::NativeObjectsCause::Admission(error) => assert_eq!(error.kind(), expected),
            other => return Err(format!("flattened native admission cause: {other}").into()),
        }
        assert_eq!(observed.memory.load(SeqCst), 0);
        assert_eq!(physical_get_calls(), calls_before, "preparation failure dispatched a real GET");
        assert_eq!(observed.reads.load(SeqCst), 0);
        assert_eq!(observed.writes.load(SeqCst), 0);
        assert_eq!(observed.reserved.load(SeqCst), 0);
    }
    for expected in [std::io::ErrorKind::Interrupted, std::io::ErrorKind::OutOfMemory] {
        let observed = Arc::new(NativeTransportMeter::new());
        if expected == std::io::ErrorKind::Interrupted {
            observed.cancel_on_copy.store(65_536, SeqCst);
        } else {
            observed.quota_on_copy.store(65_536, SeqCst);
        }
        let error = native_transport_get(provider, query("native-body"), bytes.len() as u64, observed.clone())
            .await.err().ok_or("native transport ignored in-flight cancellation/quota")?;
        let failure = error;
        let object = failure.object.as_ref().ok_or("native failure lost decoded metadata")?;
        assert_eq!(object.body().as_ref(), &bytes[..65_536]);
        assert!(object.header().object.is_some());
        assert!(matches!(&failure.cause, super::NativeObjectsCause::Admission(error) if error.kind() == expected));
        let prefix_alias = object.body().slice(0..1);
        drop(failure);
        assert!(observed.memory.load(SeqCst) >= bytes.len() as u64);
        drop(prefix_alias);
        assert_eq!(observed.memory.load(SeqCst), 0);
    }
    let observed = Arc::new(NativeTransportMeter::new());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let (input, custody) = native_get_input(query("native-body"), &meter)?;
    let calls_before = physical_get_calls();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = NativeTransportInputGuard { _allocation: custody, dropped: dropped.clone() };
    let input = super::ObjectsNativeRequest::new(input, guard, &meter)?;
    let future = provider.get_native(input, bytes.len() as u64, meter)?;
    assert!(observed.memory.load(SeqCst) > 0);
    assert!(!dropped.load(SeqCst), "factory detached the original opaque input guard");
    observed.cancelled.store(true, SeqCst);
    drop(future);
    assert!(dropped.load(SeqCst), "cancellation reconstructed rather than releasing the original guard");
    assert_eq!(physical_get_calls(), calls_before, "dropping an unpolled read dispatched GET");
    assert_eq!(observed.memory.load(SeqCst), 0);

    let foreign = Arc::new(NativeTransportMeter::new());
    let foreign_meter: Arc<dyn NativeReadMeter> = foreign.clone();
    let (input, custody) = native_get_input(query("native-body"), &foreign_meter)?;
    let current = Arc::new(NativeTransportMeter::new());
    let calls_before = physical_get_calls();
    let input = super::ObjectsNativeRequest::new(input, custody, &foreign_meter)?;
    let error = provider.get_native(input, bytes.len() as u64, current.clone())
        .err().ok_or("foreign request custody became native authority")?;
    assert!(matches!(error.into_cause(), super::NativeObjectsCause::Admission(error) if error.kind() == std::io::ErrorKind::InvalidInput));
    assert_eq!(current.allocations.load(SeqCst), 0);
    assert_eq!(current.memory.load(SeqCst), 0);
    assert_eq!(foreign.memory.load(SeqCst), 0);
    assert_eq!(physical_get_calls(), calls_before);

    let observed = Arc::new(NativeTransportMeter::new());
    let receipt = native_transport_put(provider, &bucket, "native-put", bytes.clone(), observed.clone()).await?;
    assert_eq!(receipt.info().size, bytes.len() as u64);
    assert_eq!(observed.reads.load(SeqCst), 0);
    assert_eq!(observed.reserved.load(SeqCst), 0);
    assert!(observed.memory.load(SeqCst) > 0);
    drop(receipt);
    assert_eq!(observed.memory.load(SeqCst), 0);
    assert_eq!(provider.get(query("native-put"), bytes.len() as u64).await?.body(), &bytes);
    let observed = Arc::new(NativeTransportMeter::new());
    let error = native_transport_put(provider, &bucket, "native-bad-receipt", bytes.clone(), observed.clone())
        .await.err().ok_or("native publication accepted a malformed acknowledgement")?;
    let failure = error;
    assert!(matches!(&failure.cause, super::NativeObjectsCause::Service(error) if error.code == wire::ErrorCode::Unavailable));
    let allocations = observed.allocations.load(SeqCst);
    observed.cancelled.store(true, SeqCst);
    observed.limit.store(0, SeqCst);
    assert_eq!(failure.receipt.as_ref().ok_or("native error detached decoded metadata")?.info().size, bytes.len() as u64 + 1);
    assert!(observed.memory.load(SeqCst) > 0);
    assert_eq!(observed.allocations.load(SeqCst), allocations);
    drop(failure);
    assert_eq!(observed.memory.load(SeqCst), 0);
    for (key, expected) in [("native-cancelled-put", std::io::ErrorKind::Interrupted), ("native-quota-put", std::io::ErrorKind::OutOfMemory)] {
        let observed = Arc::new(NativeTransportMeter::new());
        if expected == std::io::ErrorKind::Interrupted {
            observed.cancel_on_copy.store(65_536, SeqCst);
        } else {
            observed.quota_on_copy.store(65_536, SeqCst);
        }
        let error = native_transport_put(provider, &bucket, key, bytes.clone(), observed.clone())
            .await.err().ok_or("native PUT published after cancellation/quota")?;
        assert!(matches!(error.into_cause(), super::NativeObjectsCause::Admission(error) if error.kind() == expected));
        assert_eq!(provider.head(wire::HeadObjectRequest {
            bucket: Some(bucket.clone()), object_key: key.into(), ..Default::default()
        }).await.err().ok_or("cancelled native PUT became visible")?.code, wire::ErrorCode::NotFound);
    }

    for (key, code) in [
        ("terminal-error", wire::ErrorCode::AccessDenied),
        ("truncated", wire::ErrorCode::Unavailable),
        ("huge-header", wire::ErrorCode::QuotaExceeded),
        ("corrupt-codec", wire::ErrorCode::Unavailable),
        ("malformed", wire::ErrorCode::Unavailable),
    ] {
        let observed = Arc::new(NativeTransportMeter::new());
        let error = native_transport_get(provider, query(key), bytes.len() as u64, observed.clone())
            .await.err().ok_or("native transport accepted invalid response")?;
        match error.into_cause() {
            super::NativeObjectsCause::Service(error) => assert_eq!(error.code, code),
            other => return Err(format!("changed native service cause: {other}").into()),
        }
        assert_eq!(observed.memory.load(SeqCst), 0);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_transport_get<P: ObjectsProvider>(
    provider: &P,
    query: wire::GetObjectRequest,
    maximum: u64,
    meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<super::Object, super::NativeObjectsError> {
    let (query, custody) = native_get_input(query, &meter)?;
    let request = super::ObjectsNativeRequest::new(query, custody, &meter)?;
    provider.get_native(request, maximum, meter)?.await
}

#[cfg(not(target_arch = "wasm32"))]
fn native_get_input(
    query: wire::GetObjectRequest,
    meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<(wire::GetObjectRequest, acyclic_native_runtime::ReadAllocation), super::NativeObjectsError> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::empty(meter);
    let mut text = |value: &str| super::memory::clone_native_text(value, meter, &mut allocation);
    let query = wire::GetObjectRequest {
        bucket: query.bucket.as_ref().map(|bucket| text(&bucket.name)
            .map(|name| wire::BucketRef { name })).transpose()?,
        object_key: text(&query.object_key)?, if_match: text(&query.if_match)?,
        if_none_match: text(&query.if_none_match)?, range: query.range,
    };
    Ok((query, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_transport_put<P: ObjectsProvider>(
    provider: &P,
    bucket: &wire::BucketRef,
    key: &str,
    body: acyclic_stream::OwnedBytes,
    meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<super::ObjectsPutReceipt, super::NativeObjectsError> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&meter);
    let header = wire::PutObjectHeader {
        bucket: Some(wire::BucketRef {
            name: super::memory::clone_native_text(&bucket.name, &meter, &mut allocation)?,
        }),
        object_key: super::memory::clone_native_text(key, &meter, &mut allocation)?,
        ..Default::default()
    };
    let request = super::ObjectsNativeRequest::new((header, body), allocation, &meter)?;
    provider.put_native(request, meter)?.await
}
