use super::*;
use futures::StreamExt as _;
use std::time::Duration;

fn delayed_body(delay: Duration) -> crate::v1::UploadBody {
    stream::iter([Ok(acyclic_stream::OwnedBytes::from_static(b"first-"))])
        .chain(stream::once(async move {
            tokio::time::sleep(delay).await;
            Ok(acyclic_stream::OwnedBytes::from_static(b"last"))
        }))
        .boxed()
        .into()
}

#[tokio::test]
async fn http_transfer_timeouts_bound_uploads_without_publishing_partial_values()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = start().await?;
    let client = HttpObjects::new(&server.endpoint, "exact-token", 1024 * 1024, None)?;
    for timeout in [Duration::ZERO, Duration::from_secs(1801)] {
        assert_eq!(
            client
                .clone()
                .with_transfer_timeout(timeout)
                .err()
                .unwrap()
                .code,
            wire::ErrorCode::InvalidArgument
        );
    }
    let short = client
        .clone()
        .with_transfer_timeout(Duration::from_millis(80))?;
    let long = client
        .clone()
        .with_transfer_timeout(Duration::from_secs(2))?;
    let bucket = wire::BucketRef {
        name: "http.deadlines".into(),
    };
    client
        .create_bucket(wire::CreateBucketRequest {
            name: bucket.name.clone(),
            mutation: None,
        })
        .await?;
    let header = |key: &str| wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    };
    let ack = long
        .put_stream(header("complete"), delayed_body(Duration::from_millis(160)))
        .await?;
    assert_eq!(ack.size, 10);
    let error = short
        .put_stream(
            header("cancelled"),
            delayed_body(Duration::from_millis(160)),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, wire::ErrorCode::Unavailable);
    assert_eq!(
        client
            .head(wire::HeadObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: "cancelled".into(),
                ..Default::default()
            })
            .await
            .err()
            .unwrap()
            .code,
        wire::ErrorCode::NotFound
    );
    let upload = client
        .create_multipart(wire::CreateMultipartRequest {
            bucket: Some(bucket.clone()),
            object_key: "parts".into(),
            ..Default::default()
        })
        .await?;
    let part = |part_number| wire::UploadPartHeader {
        bucket: Some(bucket.clone()),
        object_key: "parts".into(),
        upload_id: upload.upload_id.clone(),
        part_number,
        mutation: None,
    };
    assert_eq!(
        long.upload_part_stream(part(1), delayed_body(Duration::from_millis(160)))
            .await?
            .size,
        10
    );
    assert_eq!(
        short
            .upload_part_stream(part(2), delayed_body(Duration::from_millis(160)))
            .await
            .err()
            .unwrap()
            .code,
        wire::ErrorCode::Unavailable
    );
    let parts = client
        .list_parts(wire::ListPartsRequest {
            bucket: Some(bucket),
            object_key: "parts".into(),
            upload_id: upload.upload_id,
            page_size: 10,
            ..Default::default()
        })
        .await?;
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].part_number, 1);
    let _ = server.shutdown.send(());
    server.task.await??;
    Ok(())
}

#[tokio::test]
async fn http_extended_transfers_can_outlast_the_default_request_deadline()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = start().await?;
    let client = HttpObjects::new(&server.endpoint, "exact-token", 1024 * 1024, None)?;
    let extended = client
        .clone()
        .with_transfer_timeout(Duration::from_secs(45))?;
    let bucket = wire::BucketRef {
        name: "http.extended".into(),
    };
    client
        .create_bucket(wire::CreateBucketRequest {
            name: bucket.name.clone(),
            mutation: None,
        })
        .await?;
    let put = |key: &str| wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    };
    let upload = client
        .create_multipart(wire::CreateMultipartRequest {
            bucket: Some(bucket.clone()),
            object_key: "parts".into(),
            ..Default::default()
        })
        .await?;
    let part = wire::UploadPartHeader {
        bucket: Some(bucket.clone()),
        object_key: "parts".into(),
        upload_id: upload.upload_id,
        part_number: 1,
        mutation: None,
    };
    let (default_result, put_result, part_result) = tokio::join!(
        client.put_stream(put("default"), delayed_body(Duration::from_secs(31))),
        extended.put_stream(put("extended"), delayed_body(Duration::from_secs(31))),
        extended.upload_part_stream(part, delayed_body(Duration::from_secs(31))),
    );
    assert_eq!(
        default_result.err().unwrap().code,
        wire::ErrorCode::Unavailable
    );
    assert_eq!(put_result?.size, 10);
    assert_eq!(part_result?.size, 10);
    let downloaded = client
        .get(
            wire::GetObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: "extended".into(),
                ..Default::default()
            },
            10,
        )
        .await?;
    assert_eq!(downloaded.body().as_ref(), b"first-last");
    assert_eq!(
        client
            .head(wire::HeadObjectRequest {
                bucket: Some(bucket),
                object_key: "default".into(),
                ..Default::default()
            })
            .await
            .err()
            .unwrap()
            .code,
        wire::ErrorCode::NotFound
    );
    let _ = server.shutdown.send(());
    server.task.await??;
    Ok(())
}

#[tokio::test]
async fn http_download_deadline_is_absolute_and_metadata_retains_its_budget()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bucket = wire::Bucket {
        bucket: Some(wire::BucketRef {
            name: "http.download".into(),
        }),
        created_at: Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        }),
    };
    let metadata = json::encode("Bucket", &bucket)?;
    let header = wire::GetObjectResponse {
        frame: Some(wire::get_object_response::Frame::Header(
            wire::GetObjectHeader {
                object: Some(wire::ObjectInfo {
                    size: 4,
                    etag: "opaque".into(),
                    last_modified: Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    };
    let mut download = vec![framed("GetObjectResponse", &header)?];
    for byte in b"body" {
        download.push(framed(
            "GetObjectResponse",
            &wire::GetObjectResponse {
                frame: Some(wire::get_object_response::Frame::Body(
                    crate::v1::response::plain_body(vec![*byte]),
                )),
            },
        )?);
    }
    let app = Router::new()
        .route(
            "/v1/objects/buckets/head",
            post(move || {
                let metadata = metadata.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(160)).await;
                    ([("content-type", "application/json")], metadata)
                }
            }),
        )
        .route(
            "/v1/objects/objects/get",
            post(move || {
                let download = download.clone();
                async move {
                    let body = stream::iter(download.into_iter().enumerate()).then(
                        |(index, frame)| async move {
                            if index > 1 {
                                tokio::time::sleep(Duration::from_millis(75)).await;
                            }
                            Ok::<_, std::io::Error>(Bytes::from(frame))
                        },
                    );
                    (
                        [("content-type", "application/x-ndjson")],
                        Body::from_stream(body),
                    )
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let client = HttpObjects::new(
        &format!("http://{}", listener.local_addr()?),
        "fixture",
        1024 * 1024,
        None,
    )?;
    let (shutdown, receive) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = receive.await;
            })
            .await
    });
    let short = client
        .clone()
        .with_transfer_timeout(Duration::from_millis(80))?;
    assert_eq!(
        short
            .head_bucket(wire::HeadBucketRequest {
                bucket: bucket.bucket.clone()
            })
            .await?,
        bucket
    );
    let query = wire::GetObjectRequest {
        bucket: bucket.bucket,
        object_key: "stream".into(),
        ..Default::default()
    };
    let absolute = client
        .clone()
        .with_transfer_timeout(Duration::from_millis(200))?;
    let mut body = absolute.get_stream(query.clone(), 4).await?.body;
    let mut received = Vec::new();
    let error = loop {
        match body.next().await {
            Some(Ok(bytes)) => received.extend_from_slice(&bytes),
            Some(Err(error)) => break error,
            None => panic!("the download must exhaust its absolute deadline"),
        }
    };
    assert_eq!(error.code, wire::ErrorCode::Unavailable);
    assert!(!received.is_empty() && received.len() < 4);
    assert_eq!(received, b"body"[..received.len()]);
    let complete = client
        .with_transfer_timeout(Duration::from_secs(2))?
        .get(query, 4)
        .await?;
    assert_eq!(complete.body().as_ref(), b"body");
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}
