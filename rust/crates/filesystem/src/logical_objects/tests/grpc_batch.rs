use super::*;
use acyclic_objects::{GrpcObjects, ObjectsProvider, wire};
use bytes::BytesMut;
use std::{
    convert::Infallible,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tonic::{
    Request, Response, Status,
    body::Body,
    server::{ClientStreamingService, NamedService},
};

#[derive(Clone, Debug, Default, PartialEq)]
struct Calls {
    batch: usize,
    unary: usize,
    admissions: usize,
    charges: usize,
}

fn genuine_batch(calls: &Calls) -> bool {
    *calls
        == Calls {
            batch: 1,
            unary: 0,
            admissions: 1,
            charges: 1,
        }
}

#[derive(Clone)]
struct Service {
    memory: MemoryObjects,
    calls: Arc<Mutex<Calls>>,
    fan_out: bool,
    backend: Arc<Mutex<Option<GrpcObjects>>>,
}

impl ClientStreamingService<wire::PutObjectsRequest> for Service {
    type Response = wire::PutObjectsResponse;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Self::Response>, Status>> + Send>>;

    fn call(
        &mut self,
        request: Request<tonic::Streaming<wire::PutObjectsRequest>>,
    ) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            if request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some("Bearer batch-token")
            {
                return Err(Status::unauthenticated("credential"));
            }
            let mut stream = request.into_inner();
            let Some(wire::PutObjectsRequest {
                frame: Some(wire::put_objects_request::Frame::Header(header)),
            }) = stream.message().await?
            else {
                return Err(Status::invalid_argument("header"));
            };
            let mut framing = acyclic_objects::request::BatchUploadFraming::new(&header)
                .map_err(|_| Status::invalid_argument("header"))?;
            service.calls.lock().unwrap().admissions += 1;
            let mut bodies = vec![BytesMut::new(); header.objects.len()];
            while let Some(frame) = stream.message().await? {
                framing
                    .accept(&frame)
                    .map_err(|_| Status::invalid_argument("frame"))?;
                if let Some(wire::put_objects_request::Frame::Chunk(wire::PutObjectsChunk {
                    index,
                    frame: Some(wire::put_objects_chunk::Frame::Body(body)),
                })) = frame.frame
                {
                    bodies[index as usize].extend_from_slice(&body);
                }
            }
            framing
                .finish()
                .map_err(|_| Status::invalid_argument("EOF"))?;
            service.calls.lock().unwrap().charges += 1;
            let requests = header
                .objects
                .into_iter()
                .zip(bodies.into_iter().map(|body| body.freeze().into()))
                .collect::<Vec<_>>();
            let values = if service.fan_out {
                let backend = service
                    .backend
                    .lock()
                    .unwrap()
                    .clone()
                    .ok_or_else(|| Status::internal("fan-out client"))?;
                let mut values = Vec::new();
                for (header, body) in requests {
                    values.push(backend.put(header, body).await);
                }
                values
            } else {
                service.memory.put_batch(requests).await
            };
            Ok(Response::new(wire::PutObjectsResponse {
                results: values
                    .into_iter()
                    .map(|result| wire::PutObjectResult {
                        result: Some(match result {
                            Ok(info) => wire::put_object_result::Result::Object(info),
                            Err(error) => {
                                wire::put_object_result::Result::Error(wire::ErrorDetail {
                                    code: error.code as i32,
                                    request_id: "batch".into(),
                                })
                            }
                        }),
                    })
                    .collect(),
            }))
        })
    }
}

impl ClientStreamingService<wire::PutObjectRequest> for Service {
    type Response = wire::ObjectInfo;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Self::Response>, Status>> + Send>>;

    fn call(&mut self, request: Request<tonic::Streaming<wire::PutObjectRequest>>) -> Self::Future {
        let memory = self.memory.clone();
        Box::pin(async move {
            let mut stream = request.into_inner();
            let Some(wire::PutObjectRequest {
                frame: Some(wire::put_object_request::Frame::Header(header)),
            }) = stream.message().await?
            else {
                return Err(Status::invalid_argument("unary header"));
            };
            let mut framing = acyclic_objects::request::UploadFraming::default();
            let mut body = BytesMut::new();
            while let Some(frame) = stream.message().await? {
                let result = match frame.frame {
                    Some(wire::put_object_request::Frame::Body(bytes)) => {
                        body.extend_from_slice(&bytes);
                        framing.body(&bytes)
                    }
                    Some(wire::put_object_request::Frame::Complete(value)) => {
                        framing.complete(value)
                    }
                    _ => return Err(Status::invalid_argument("unary frame")),
                };
                result.map_err(|_| Status::invalid_argument("unary framing"))?;
            }
            framing
                .finish()
                .map_err(|_| Status::invalid_argument("unary EOF"))?;
            memory
                .put(header, body.freeze().into())
                .await
                .map(Response::new)
                .map_err(|_| Status::internal("unary put"))
        })
    }
}

impl NamedService for Service {
    const NAME: &'static str = "acyclic.objects.v1.ObjectsService";
}

impl tower::Service<http::Request<Body>> for Service {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let service = self.clone();
        if request.uri().path() == "/acyclic.objects.v1.ObjectsService/PutObjects" {
            service.calls.lock().unwrap().batch += 1;
            Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    wire::PutObjectsResponse,
                    wire::PutObjectsRequest,
                >::default())
                .client_streaming(service, request)
                .await)
            })
        } else if request.uri().path() == "/acyclic.objects.v1.ObjectsService/PutObject" {
            service.calls.lock().unwrap().unary += 1;
            Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    wire::ObjectInfo,
                    wire::PutObjectRequest,
                >::default())
                .client_streaming(service, request)
                .await)
            })
        } else {
            Box::pin(async {
                Ok(Status::unimplemented("unary RPC forbidden by batch test").into_http())
            })
        }
    }
}

#[tokio::test]
async fn native_filesystem_batch_has_one_rpc_admission_and_charge_with_fan_out_control()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    for fan_out in [false, true] {
        let (memory, bucket) = MemoryObjects::with_default_bucket();
        let calls = Arc::new(Mutex::new(Calls::default()));
        let observed = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let cert = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        let pem = cert.cert.pem();
        let identity =
            tonic::transport::Identity::from_pem(pem.clone(), cert.signing_key.serialize_pem());
        let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let backend = Arc::new(Mutex::new(None));
        let fan_out_client = backend.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?
                .add_service(Service {
                    memory,
                    calls,
                    fan_out,
                    backend,
                })
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
        });
        let provider =
            GrpcObjects::connect(&endpoint, "batch-token", Some(pem.as_bytes()), None).await?;
        *fan_out_client.lock().unwrap() = Some(provider.clone());
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let cancel = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let objects = [
            HashedObject::new(ObjectKind::BlobChunk, vec![1; 65_537].into()),
            HashedObject::new(ObjectKind::BlobChunk, vec![2; 17].into()),
        ];
        let writes = objects
            .iter()
            .map(|object| ObjectWrite {
                object_id: object.object_id(),
                bytes: object.bytes().clone(),
            })
            .collect::<Vec<_>>();
        store
            .put_many(&writes, &mut AuthorityWork::native(&original, &cancel))
            .await?;
        assert_eq!(original.receipt().backend_write_operations, 1);
        assert_eq!(original.receipt().object_bytes_written, 65_554);
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(genuine_batch(&observed.lock().unwrap()), !fan_out);
        assert_eq!(observed.lock().unwrap().unary, if fan_out { 2 } else { 0 });
        let before = observed.lock().unwrap().clone();
        let denied = crate::NativeMaterializationWork::new(
            WorkBudget {
                backend_write_operations: 0,
                ..BUDGET
            },
            &cancel,
        )
        .map_err(|(error, _)| error)?;
        let denied_baseline = denied.live_allocation_bytes();
        assert!(
            store
                .put_many(&writes, &mut AuthorityWork::native(&denied, &cancel))
                .await
                .is_err()
        );
        assert_eq!(denied.receipt().backend_write_operations, 0);
        assert_eq!(denied.live_allocation_bytes(), denied_baseline);
        assert_eq!(*observed.lock().unwrap(), before);
        cancel.cancel();
        assert!(
            store
                .put_many(&writes, &mut AuthorityWork::native(&original, &cancel))
                .await
                .is_err()
        );
        assert_eq!(*observed.lock().unwrap(), before);
        assert_eq!(original.live_allocation_bytes(), baseline);
        let _ = stop.send(());
        server.await??;
    }
    Ok(())
}
