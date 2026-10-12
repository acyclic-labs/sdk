use super::*;

#[cfg(not(target_arch = "wasm32"))]
fn put_input_bytes(header: &wire::PutObjectHeader) -> usize {
    let mut bytes = header.object_key.capacity()
        + header
            .bucket
            .as_ref()
            .map_or(0, |bucket| bucket.name.capacity())
        + header
            .mutation
            .as_ref()
            .map_or(0, |identity| identity.idempotency_key.capacity());
    if let Some(metadata) = &header.metadata {
        bytes += metadata.content_type.capacity()
            + metadata.content_encoding.capacity()
            + metadata.cache_control.capacity()
            + metadata.content_disposition.capacity()
            + metadata.content_language.capacity();
        bytes += metadata.user.len() * std::mem::size_of::<(String, String)>();
        bytes += metadata
            .user
            .iter()
            .map(|(key, value)| key.capacity() + value.capacity())
            .sum::<usize>();
    }
    if let Some(wire::Preconditions {
        condition: Some(wire::preconditions::Condition::IfMatch(etag)),
    }) = &header.preconditions
    {
        bytes += etag.capacity();
    }
    bytes
}

#[cfg(not(target_arch = "wasm32"))]
fn get_input_bytes(query: &wire::GetObjectRequest) -> usize {
    query.object_key.capacity()
        + query.if_match.capacity()
        + query.if_none_match.capacity()
        + query
            .bucket
            .as_ref()
            .map_or(0, |bucket| bucket.name.capacity())
}

#[cfg(not(target_arch = "wasm32"))]
use crate::v1::tests::NativeTransportMeter;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_reuses_retry_engine_and_retires_shared_state_claims()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::atomic::Ordering::SeqCst;
    let observed = Arc::new(NativeTransportMeter::new_table());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    let header = wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: "native-publication".into(),
        metadata: Some(wire::ObjectMetadata {
            content_type: "application/native".into(),
            user: BTreeMap::from([("custody".into(), "original".into())]),
            ..Default::default()
        }),
        mutation: Some(wire::MutationIdentity {
            idempotency_key: "native-retry".into(),
        }),
        ..Default::default()
    };
    let first_input = header.clone();
    let custody = ReadAllocation::adopt(&meter, put_input_bytes(&first_input))?;
    let input = ObjectsNativeRequest::new(
        (first_input, OwnedBytes::from_static(b"actual")),
        custody,
        &meter,
    )?;
    let first = provider.put_native(input, Arc::clone(&meter))?.await?;
    let retry_input = header.clone();
    let custody = ReadAllocation::adopt(&meter, put_input_bytes(&retry_input))?;
    let input = ObjectsNativeRequest::new(
        (retry_input, OwnedBytes::from_static(b"actual")),
        custody,
        &meter,
    )?;
    let retried = provider.put_native(input, Arc::clone(&meter))?.await?;
    assert_eq!(first.info(), retried.info());
    let mut changed = header;
    changed.metadata = None;
    let custody = ReadAllocation::adopt(&meter, put_input_bytes(&changed))?;
    let input = ObjectsNativeRequest::new(
        (changed, OwnedBytes::from_static(b"actual")),
        custody,
        &meter,
    )?;
    let mismatch = provider
        .put_native(input, Arc::clone(&meter))?
        .await
        .err()
        .ok_or("retry accepted changed metadata")?;
    assert!(matches!(
        mismatch,
        NativeObjectsError {
            cause: NativeObjectsCause::Service(Error {
                code: IdempotencyMismatch
            }),
            ..
        }
    ));
    let query = wire::GetObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: "native-publication".into(),
        ..Default::default()
    };
    let custody = ReadAllocation::adopt(&meter, get_input_bytes(&query))?;
    let input = ObjectsNativeRequest::new(query, custody, &meter)?;
    let native_read = provider.get_native(input, 6, Arc::clone(&meter))?.await?;
    assert_eq!(native_read.body().as_slice(), b"actual");
    assert_eq!(native_read.header().object.as_ref(), Some(first.info()));
    drop(native_read);
    let read = provider
        .get(
            wire::GetObjectRequest {
                bucket: Some(bucket),
                object_key: "native-publication".into(),
                ..Default::default()
            },
            6,
        )
        .await?;
    assert_eq!(read.body().as_slice(), b"actual");
    assert_eq!(read.header().object.as_ref(), Some(first.info()));
    drop(read);
    drop(first);
    drop(retried);
    assert!(
        observed.memory.load(SeqCst) > 0,
        "state retains its physical metadata and retry bytes"
    );
    drop(provider);
    assert_eq!(observed.memory.load(SeqCst), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_uses_current_authority_not_body_payer()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::atomic::Ordering::SeqCst;
    let original = Arc::new(NativeTransportMeter::new_table());
    let original_meter: Arc<dyn NativeReadMeter> = original.clone();
    let allocation = ReadAllocation::new(&original_meter, 4)?;
    let body = OwnedBytes::from_vec_with_guard(vec![1, 2, 3, 4], allocation, original_meter)
        .map_err(|failure| super::super::native_preparation_error(failure.into_error()))?;
    let alias = body.clone();
    let current = Arc::new(NativeTransportMeter::new_table());
    let meter: Arc<dyn NativeReadMeter> = current.clone();
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    let header = wire::PutObjectHeader {
        bucket: Some(bucket),
        object_key: "current".into(),
        ..Default::default()
    };
    let custody = ReadAllocation::adopt(&meter, put_input_bytes(&header))?;
    let input = ObjectsNativeRequest::new((header, body), custody, &meter)?;
    current.cancelled.store(true, SeqCst);
    let failure = provider
        .put_native(input, Arc::clone(&meter))
        .err()
        .ok_or("body payer bypassed current cancellation")?;
    assert!(
        matches!(&failure.cause, NativeObjectsCause::Admission(error)
        if error.kind() == std::io::ErrorKind::Interrupted)
    );
    assert_eq!(current.memory.load(SeqCst), 0);
    assert!(original.memory.load(SeqCst) >= 4);
    drop(alias);
    assert_eq!(original.memory.load(SeqCst), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_batch_iterator_retains_its_actual_result_vector_until_drop()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::atomic::Ordering::SeqCst;
    let observed = Arc::new(NativeTransportMeter::new_table());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    for key in ["a", "b"] {
        provider
            .put(
                wire::PutObjectHeader {
                    bucket: Some(bucket.clone()),
                    object_key: key.into(),
                    ..Default::default()
                },
                OwnedBytes::from_static(b"body"),
            )
            .await?;
    }
    let queries = ["a", "b"]
        .into_iter()
        .map(|key| {
            (
                wire::GetObjectRequest {
                    bucket: Some(bucket.clone()),
                    object_key: key.into(),
                    ..Default::default()
                },
                4,
            )
        })
        .collect::<Vec<_>>();
    let input_bytes = queries.capacity() * std::mem::size_of::<(wire::GetObjectRequest, u64)>()
        + queries
            .iter()
            .map(|(query, _)| get_input_bytes(query))
            .sum::<usize>();
    let custody = ReadAllocation::adopt(&meter, input_bytes)?;
    let input = ObjectsNativeRequest::new(queries, custody, &meter)?;
    let batch = provider
        .get_batch_native(input, Arc::clone(&meter))?
        .await?;
    let mut results = batch;
    drop(results.next().ok_or("missing first native result")??);
    drop(results.next().ok_or("missing second native result")??);
    assert!(results.next().is_none());
    assert!(
        observed.memory.load(SeqCst) > 0,
        "exhaustion does not free vector storage"
    );
    drop(results);
    assert_eq!(observed.memory.load(SeqCst), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_write_batch_keeps_individual_rollback_and_ordered_receipts()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::atomic::Ordering::SeqCst;
    let observed = Arc::new(NativeTransportMeter::new_table());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let (mut provider, bucket) = MemoryObjects::with_default_bucket();
    provider.options.maximum_bytes = 8;
    let requests = [
        ("a", b"four".as_slice()),
        ("b", b"too-large".as_slice()),
        ("c", b"four".as_slice()),
    ]
    .into_iter()
    .map(|(key, body)| {
        (
            wire::PutObjectHeader {
                bucket: Some(bucket.clone()),
                object_key: key.into(),
                ..Default::default()
            },
            OwnedBytes::from_static(body),
        )
    })
    .collect::<Vec<_>>();
    let input_bytes = requests.capacity()
        * std::mem::size_of::<(wire::PutObjectHeader, OwnedBytes)>()
        + requests
            .iter()
            .map(|(header, _)| put_input_bytes(header))
            .sum::<usize>();
    let custody = ReadAllocation::adopt(&meter, input_bytes)?;
    let input = ObjectsNativeRequest::new(requests, custody, &meter)?;
    let results = provider
        .put_batch_native(input, Arc::clone(&meter))?
        .await?;
    assert_eq!(results.results().len(), 3);
    assert!(results.results()[0].is_ok());
    assert!(matches!(
        &results.results()[1],
        Err(NativeObjectsError {
            cause: NativeObjectsCause::Service(Error {
                code: QuotaExceeded
            }),
            ..
        })
    ));
    assert!(results.results()[2].is_ok());
    for key in ["a", "c"] {
        let object = provider
            .get(
                wire::GetObjectRequest {
                    bucket: Some(bucket.clone()),
                    object_key: key.into(),
                    ..Default::default()
                },
                4,
            )
            .await?;
        assert_eq!(object.body().as_slice(), b"four");
    }
    assert!(matches!(
        provider
            .get(
                wire::GetObjectRequest {
                    bucket: Some(bucket),
                    object_key: "b".into(),
                    ..Default::default()
                },
                9
            )
            .await,
        Err(Error { code: NotFound })
    ));
    drop(results);
    drop(provider);
    assert_eq!(observed.memory.load(SeqCst), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_table_events_precede_lookup_and_publication_without_late_fabrication()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::atomic::Ordering::SeqCst;
    let observed = Arc::new(NativeTransportMeter::new_table());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    provider
        .put(
            wire::PutObjectHeader {
                bucket: Some(bucket.clone()),
                object_key: "existing".into(),
                ..Default::default()
            },
            OwnedBytes::from_static(b"data"),
        )
        .await?;
    let query = || wire::GetObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: "existing".into(),
        ..Default::default()
    };
    observed.read_limit.store(0, SeqCst);
    let request = query();
    let custody = ReadAllocation::adopt(&meter, get_input_bytes(&request))?;
    let input = ObjectsNativeRequest::new(request, custody, &meter)?;
    let denied = provider
        .get_native(input, 4, Arc::clone(&meter))?
        .await
        .err()
        .ok_or("lookup bypassed original backend admission")?;
    assert!(matches!(&denied.cause, NativeObjectsCause::Admission(error)
        if error.kind() == std::io::ErrorKind::PermissionDenied));
    assert_eq!(observed.memory.load(SeqCst), 0);

    observed.read_limit.store(u64::MAX, SeqCst);
    let request = query();
    let custody = ReadAllocation::adopt(&meter, get_input_bytes(&request))?;
    let input = ObjectsNativeRequest::new(request, custody, &meter)?;
    let future = provider.get_native(input, 4, Arc::clone(&meter))?;
    // Deny response allocation after the funded lookup.
    observed.limit.store(observed.memory.load(SeqCst), SeqCst);
    let copy_denied = future
        .await
        .err()
        .ok_or("header copy bypassed allocation admission")?;
    assert!(
        matches!(&copy_denied.cause, NativeObjectsCause::Admission(error)
        if error.kind() == std::io::ErrorKind::OutOfMemory)
    );
    assert_eq!(observed.memory.load(SeqCst), 0);

    observed.limit.store(u64::MAX, SeqCst);
    observed.write_limit.store(0, SeqCst);
    let header = wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: "unpublished".into(),
        ..Default::default()
    };
    let custody = ReadAllocation::adopt(&meter, put_input_bytes(&header))?;
    let input =
        ObjectsNativeRequest::new((header, OwnedBytes::from_static(b"data")), custody, &meter)?;
    let denied = provider
        .put_native(input, Arc::clone(&meter))?
        .await
        .err()
        .ok_or("publication bypassed original backend admission")?;
    assert!(matches!(&denied.cause, NativeObjectsCause::Admission(error)
        if error.kind() == std::io::ErrorKind::PermissionDenied));
    assert!(matches!(
        provider
            .get(
                wire::GetObjectRequest {
                    bucket: Some(bucket),
                    object_key: "unpublished".into(),
                    ..Default::default()
                },
                4
            )
            .await,
        Err(Error { code: NotFound })
    ));
    assert_eq!(observed.memory.load(SeqCst), 0);

    let input = ObjectsNativeRequest::new(Vec::new(), ReadAllocation::empty(&meter), &meter)?;
    drop(
        provider
            .get_batch_native(input, Arc::clone(&meter))?
            .await?,
    );
    let input = ObjectsNativeRequest::new(Vec::new(), ReadAllocation::empty(&meter), &meter)?;
    drop(
        provider
            .put_batch_native(input, Arc::clone(&meter))?
            .await?,
    );
    assert_eq!(observed.memory.load(SeqCst), 0);
    Ok(())
}
