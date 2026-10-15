use super::*;

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
            (get(bucket(), "third"), 3),
            (get(bucket(), "first"), 2),
            (get(bucket(), "second"), 8),
            (get(bucket(), "first"), 3),
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
    let current = provider.get(get(bucket(), "value"), 16).await?;
    assert_eq!(
        provider
            .put(original.clone(), OwnedBytes::from_static(b"first"))
            .await?,
        first
    );
    assert_eq!(provider.get(get(bucket(), "value"), 16).await?, current);
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
            .get(get(bucket(), "new"), 16)
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
    provider
        .put(attempted, OwnedBytes::from_static(b"ab"))
        .await?;
    assert_eq!(
        provider
            .get(get(bucket(), "new"), 1)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    let mut suffix = get(bucket(), "new");
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

    /// Checks cursor pagination across prefix, delimiter and Unicode boundaries.
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
        provider.get(get(bucket(), "value"), 16).await?.into_body(),
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
            .get(get(bucket(), "value"), 16)
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

pub(super) async fn assert_completed_multipart_range(
    provider: &impl ObjectsProvider,
    info: &wire::ObjectInfo,
) -> Result<(), Error> {
    assert_eq!(info.size, 5 * 1024 * 1024 + 3);
    let mut crossing = get(bucket(), "value");
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
