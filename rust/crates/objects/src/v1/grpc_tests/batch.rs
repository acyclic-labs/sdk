#![allow(
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "Fixture helpers build small known batches and fail loudly on fixture errors."
)]
use super::*;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Observed {
    inner: wire::objects_service_server::ObjectsServiceServer<Fixture>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl tonic::server::NamedService for Observed {
    const NAME: &'static str = "acyclic.objects.v1.ObjectsService";
}

impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for Observed {
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future =
        <wire::objects_service_server::ObjectsServiceServer<Fixture> as tonic::codegen::Service<
            tonic::codegen::http::Request<tonic::body::Body>,
        >>::Future;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        self.calls.lock().unwrap().push(request.uri().path().into());
        tonic::codegen::Service::call(&mut self.inner, request)
    }
}

pub(super) async fn put(
    memory: &MemoryObjects,
    mut frames: tonic::Streaming<wire::PutObjectsRequest>,
) -> Result<wire::PutObjectsResponse, Status> {
    use wire::{put_objects_chunk::Frame as C, put_objects_request::Frame as F};
    let Some(wire::PutObjectsRequest {
        frame: Some(F::Header(header)),
    }) = frames.message().await?
    else {
        return Err(Status::invalid_argument("batch header"));
    };
    let mut framing = crate::request::BatchUploadFraming::new(&header).map_err(status)?;
    let mut bodies: Vec<BytesMut> = (0..header.objects.len()).map(|_| BytesMut::new()).collect();
    while let Some(frame) = frames.message().await? {
        framing.accept(&frame).map_err(status)?;
        if let Some(F::Chunk(wire::PutObjectsChunk {
            index,
            frame: Some(C::Body(body)),
        })) = frame.frame
        {
            let index =
                usize::try_from(index).map_err(|_| Status::invalid_argument("item index"))?;
            bodies
                .get_mut(index)
                .ok_or_else(|| Status::invalid_argument("item index"))?
                .extend_from_slice(&body);
        }
    }
    framing.finish().map_err(status)?;
    let invalid_index = header
        .objects
        .iter()
        .position(|object| object.object_key == "invalid-metadata");
    let results = memory
        .put_batch(
            header
                .objects
                .into_iter()
                .zip(bodies.into_iter().map(|body| body.freeze().into()))
                .collect(),
        )
        .await;
    Ok(wire::PutObjectsResponse {
        results: results
            .into_iter()
            .enumerate()
            .map(|(index, result)| wire::PutObjectResult {
                result: Some(match result {
                    Ok(mut object) => {
                        if invalid_index == Some(index) {
                            object.metadata = Some(wire::ObjectMetadata {
                                content_type: "invalid\r\nmetadata".into(),
                                ..Default::default()
                            });
                        }
                        wire::put_object_result::Result::Object(object)
                    }
                    Err(error) => wire::put_object_result::Result::Error(wire::ErrorDetail {
                        code: error.code as i32,
                        request_id: "batch".into(),
                    }),
                }),
            })
            .collect(),
    })
}

pub(super) async fn get(
    memory: &MemoryObjects,
    query: wire::GetObjectsRequest,
) -> Result<stream::BoxStream<'static, Result<wire::GetObjectsResponse, Status>>, Status> {
    crate::request::get_batch(&query).map_err(status)?;
    let mode = query
        .objects
        .first()
        .and_then(|selection| selection.request.as_ref())
        .map(|query| query.object_key.clone())
        .unwrap_or_default();
    let results = memory
        .get_batch(
            query
                .objects
                .into_iter()
                .map(|selection| {
                    selection
                        .request
                        .map(|request| (request, selection.maximum_bytes))
                        .ok_or_else(|| Status::invalid_argument("selection"))
                })
                .collect::<Result<_, _>>()?,
        )
        .await;
    let mut frames = Vec::new();
    for (result, index) in results.into_iter().zip(0_u32..) {
        use wire::get_objects_response::Frame;
        let mut emit = |frame| {
            frames.push(Ok(wire::GetObjectsResponse {
                index,
                frame: Some(frame),
            }));
        };
        match result {
            Ok(object) => {
                emit(Frame::Header(object.header().clone()));
                for chunk in object.body().chunks(65_536) {
                    emit(Frame::Body(wire::Body {
                        codec: wire::Codec::None as i32,
                        data: chunk.to_vec(),
                        decoded_length: chunk.len() as u64,
                    }));
                }
                emit(Frame::Complete(true));
            }
            Err(error) => emit(Frame::Error(wire::ErrorDetail {
                code: error.code as i32,
                request_id: "batch".into(),
            })),
        }
    }
    match mode.as_str() {
        "wrong-index" => {
            frames
                .first_mut()
                .and_then(|frame| frame.as_mut().ok())
                .ok_or_else(|| Status::internal("fixture frame"))?
                .index = 1;
        }
        "missing-item" => {
            frames.pop();
        }
        "extra-item" => frames.push(
            frames
                .first()
                .cloned()
                .ok_or_else(|| Status::internal("fixture frame"))?,
        ),
        "unknown-error" => {
            frames
                .first_mut()
                .and_then(|frame| frame.as_mut().ok())
                .ok_or_else(|| Status::internal("fixture frame"))?
                .frame = Some(wire::get_objects_response::Frame::Error(
                wire::ErrorDetail::default(),
            ));
        }
        _ => {}
    }
    Ok(stream::iter(frames).boxed())
}

async fn assert_malformed_batches(client: &GrpcObjects, bucket: &wire::BucketRef) {
    for key in ["wrong-index", "missing-item", "extra-item", "unknown-error"] {
        let requests = [key, "empty"]
            .map(|key| {
                (
                    wire::GetObjectRequest {
                        bucket: Some(bucket.clone()),
                        object_key: key.into(),
                        ..Default::default()
                    },
                    0,
                )
            })
            .into();
        let results = client.get_batch(requests).await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| {
            result
                .as_ref()
                .is_err_and(|error| error.code == wire::ErrorCode::Unavailable)
        }));
    }
}

async fn assert_individual_put_outcomes(
    client: &GrpcObjects,
    bucket: &wire::BucketRef,
) -> Result<(), Error> {
    let existing = wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: "large".into(),
        preconditions: Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfAbsent(true)),
        }),
        ..Default::default()
    };
    let next = wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: "after-error".into(),
        ..Default::default()
    };
    let results = client
        .put_batch(vec![(existing, vec![1].into()), (next, vec![2].into())])
        .await;
    let [rejected, published]: [_; 2] = results
        .try_into()
        .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
    assert_eq!(
        rejected.err().map(|error| error.code),
        Some(wire::ErrorCode::PreconditionFailed)
    );
    assert_eq!(published?.size, 1);
    Ok(())
}

#[tokio::test]
async fn grpc_batch_round_trip_is_ordered_bounded_and_uses_one_credential_per_rpc()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let memory = MemoryObjects::new(MemoryOptions::default())?;
    let bucket = wire::BucketRef {
        name: "batch".into(),
    };
    memory
        .create_bucket(wire::CreateBucketRequest {
            name: bucket.name.clone(),
            mutation: None,
        })
        .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let pem = certified.cert.pem();
    let identity = Identity::from_pem(pem.clone(), certified.signing_key.serialize_pem());
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let rpc_calls = Arc::new(Mutex::new(Vec::new()));
    let observed = rpc_calls.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .add_service(Observed {
                inner: wire::objects_service_server::ObjectsServiceServer::new(Fixture(memory)),
                calls: rpc_calls,
            })
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let callbacks = calls.clone();
    let client = GrpcObjects::connect_with_credential(
        &endpoint,
        super::super::grpc::CredentialProvider::new(move || {
            callbacks.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok("exact-token".into()))
        }),
        Some(pem.as_bytes()),
        None,
    )
    .await?;
    let put = |key: &str| wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    };
    let body = vec![42; 131_073];
    let results = client
        .put_batch(vec![
            (put("large"), body.clone().into()),
            (put("empty"), Vec::new().into()),
        ])
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let [large, empty]: [_; 2] = results.try_into().map_err(|_| "receipt count")?;
    assert_eq!(large?.size, body.len() as u64);
    assert_eq!(empty?.size, 0);
    let get = |key: &str, maximum| {
        (
            wire::GetObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: key.into(),
                ..Default::default()
            },
            maximum,
        )
    };
    let selected = client
        .get_batch(vec![
            get("empty", 0),
            get("absent", 0),
            get("large", 1),
            get("large", body.len() as u64),
        ])
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_malformed_batches(&client, &bucket).await;
    let [empty, missing, bounded, large]: [_; 4] =
        selected.try_into().map_err(|_| "selection count")?;
    assert!(empty?.body().is_empty());
    assert_eq!(
        missing.err().map(|error| error.code),
        Some(wire::ErrorCode::NotFound)
    );
    assert_eq!(
        bounded.err().map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded)
    );
    assert_eq!(large?.body().as_ref(), body.as_slice());
    assert_individual_put_outcomes(&client, &bucket).await?;
    assert!(client.put_batch(Vec::new()).await.is_empty());
    assert!(client.get_batch(Vec::new()).await.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 7);
    let oversized = vec![(put("bounded"), Vec::new().into()); 1001];
    assert!(client.put_batch(oversized).await.iter().all(|result| {
        result
            .as_ref()
            .is_err_and(|error| error.code == wire::ErrorCode::QuotaExceeded)
    }));
    let denied = GrpcObjects::connect_with_credential(
        &endpoint,
        super::super::grpc::CredentialProvider::new(|| async {
            Err(acyclic_auth::credential::CredentialError::Invalid)
        }),
        Some(pem.as_bytes()),
        None,
    )
    .await?;
    assert!(
        denied
            .put_batch(vec![(put("denied"), Vec::new().into())])
            .await
            .iter()
            .all(|result| result
                .as_ref()
                .is_err_and(|error| error.code == wire::ErrorCode::AccessDenied))
    );
    let methods: Vec<_> = observed
        .lock()
        .unwrap()
        .iter()
        .map(|path| path.rsplit('/').next().unwrap().to_owned())
        .collect();
    assert_eq!(
        methods,
        [
            "PutObjects",
            "GetObjects",
            "GetObjects",
            "GetObjects",
            "GetObjects",
            "GetObjects",
            "PutObjects"
        ]
    );
    for invalid_index in 0..3 {
        let requests = (0..3)
            .map(|index| {
                (
                    put(if index == invalid_index {
                        "invalid-metadata"
                    } else {
                        "valid-receipt"
                    }),
                    vec![index as u8; 7].into(),
                )
            })
            .collect();
        let results = client.put_batch(requests).await;
        assert_eq!(results.len(), 3);
        for (index, result) in results.into_iter().enumerate() {
            assert_eq!(result.is_ok(), index != invalid_index);
            if let Ok(receipt) = result {
                assert_eq!(receipt.size, 7);
            }
        }
    }
    #[cfg(feature = "local")]
    for invalid_index in 0..3 {
        use acyclic_native_runtime::NativeReadMeter;
        let counts = Arc::new(crate::physical::read_tests::CountingMeter::default());
        let meter: Arc<dyn NativeReadMeter> = counts.clone();
        let requests = (0..3)
            .map(|index| {
                (
                    put(if index == invalid_index {
                        "invalid-metadata"
                    } else {
                        "valid-receipt"
                    }),
                    vec![index as u8; 7].into(),
                )
            })
            .collect();
        let input = super::super::ObjectsNativeRequest::new(requests, (), &meter)?;
        let batch = client.put_batch_native(input, meter)?.await?;
        assert_eq!(batch.len(), 3);
        for (index, result) in batch.enumerate() {
            let error = result.expect_err("batch validation must retain its failure");
            assert_eq!(error.receipt.is_some(), index != invalid_index);
            if let Some(receipt) = error.receipt {
                assert_eq!(receipt.info().size, 7);
            }
        }
        assert_eq!(counts.written.load(Ordering::SeqCst), 14);
        assert_eq!(counts.retained(), 0);
    }
    let _ = stop.send(());
    server.await??;
    Ok(())
}
