use super::*;
use crate::v1;

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]
pub(in crate::v1) async fn exercise_uploads<'a>(
    provider: &dyn ObjectsProvider,
    put_stream: impl Fn(
        wire::PutObjectHeader,
        v1::UploadBody,
    ) -> futures::future::BoxFuture<'a, Result<wire::ObjectInfo, Error>>,
    part_stream: impl Fn(
        wire::UploadPartHeader,
        v1::UploadBody,
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
        stream::iter([Ok(OwnedBytes::new()), Ok(bytes.clone())])
            .boxed()
            .into(),
    )
    .await?;
    assert_eq!(result.size, 135_000);
    assert_eq!(
        provider
            .get(get(bucket(), "stream"), 135_000)
            .await?
            .into_body(),
        bytes
    );
    let failure = || {
        stream::iter([
            Ok(OwnedBytes::from_static(b"partial")),
            Err(Error::from(wire::ErrorCode::AccessDenied)),
        ])
        .boxed()
        .into()
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
        stream::iter([Ok(OwnedBytes::from_static(b"complete"))])
            .boxed()
            .into(),
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
    assert_eq!(
        provider
            .get(get(bucket(), "stream"), 135_000)
            .await?
            .into_body(),
        bytes
    );
    let (started, started_rx) = futures::channel::oneshot::channel();
    let pending = stream::once(async move {
        let _ = started.send(());
        Ok(OwnedBytes::from_static(b"partial"))
    })
    .chain(stream::pending())
    .boxed()
    .into();
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
    let part = part_stream(
        header.clone(),
        stream::iter([Ok(bytes.clone())]).boxed().into(),
    )
    .await?;
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
pub(in crate::v1) async fn exercise_streaming<'a>(
    provider: &dyn ObjectsProvider,
    get: impl Fn(
        wire::GetObjectRequest,
        u64,
    ) -> futures::future::BoxFuture<'a, Result<v1::Download, Error>>,
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
    let mut selected = get(self::get(bucket(), "stream"), 135_000).await?;
    assert_eq!(
        selected.header.object.as_ref().map(|info| info.size),
        Some(135_000)
    );
    let mut result = Vec::new();
    while let Some(chunk) = selected.body.next().await {
        let chunk = chunk?;
        assert!(chunk.len() <= 65_536);
        result.extend_from_slice(&chunk);
    }
    assert_eq!(result.as_slice(), bytes.as_slice());
    let mut cancelled = get(self::get(bucket(), "stream"), 135_000).await?;
    assert!(cancelled.body.next().await.transpose()?.is_some());
    drop(cancelled);
    assert_eq!(
        get(self::get(bucket(), "stream"), 135_000)
            .await?
            .collect(135_000)
            .await?
            .into_body(),
        bytes
    );
    assert_eq!(
        get(self::get(bucket(), "stream"), 16)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    provider
        .put(put("malformed"), OwnedBytes::from_static(b"no"))
        .await?;
    assert_eq!(
        provider
            .get(self::get(bucket(), "malformed"), 2)
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::Unavailable)
    );
    provider
        .put(put("terminal-error"), OwnedBytes::from_static(b"body"))
        .await?;
    let failed = get(self::get(bucket(), "terminal-error"), 16).await?;
    assert_eq!(
        failed.collect(16).await.err().map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    for key in ["stream", "malformed", "terminal-error"] {
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
