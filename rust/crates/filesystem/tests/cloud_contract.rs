//! Hosted and generated clients replay Cloud's filesystem handler-test JSON.

#[path = "support/cloud_contract.rs"]
mod contract;

use acyclic_fs::model::FilesystemProfile;
use acyclic_fs::wire::filesystem::v2::{
    self as wire, filesystem_service_client::FilesystemServiceClient,
};
use acyclic_fs::{Fs, HostedFsOptions, IdempotencyKey};
use futures::future::{Ready, ready};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;
use std::{
    convert::Infallible,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Request, Response, Status,
    body::Body,
    server::{NamedService, UnaryService},
};

fn message<M: Message + Default>(name: &str, value: &Value) -> Result<M, Status> {
    let decode = || -> Result<M, Box<dyn std::error::Error>> {
        let descriptor = DescriptorPool::decode(acyclic_fs::FILE_DESCRIPTOR_SET)?
            .get_message_by_name(&format!("acyclic.filesystem.v2.{name}"))
            .ok_or("missing descriptor")?;
        let text = serde_json::to_string(value)?;
        let message = DynamicMessage::deserialize(
            descriptor,
            &mut serde_json::Deserializer::from_str(&text),
        )?;
        Ok(message.transcode_to()?)
    };
    decode().map_err(|error| Status::internal(error.to_string()))
}

#[derive(Clone)]
struct GoldenService {
    cases: Arc<Vec<Value>>,
    seen: Arc<Mutex<Vec<String>>>,
}

struct Unary<Q, R> {
    service: GoldenService,
    method: &'static str,
    marker: PhantomData<fn(Q) -> R>,
}

impl<Q: Message + Default + Send + 'static, R: Message + Default + Send + 'static> UnaryService<Q>
    for Unary<Q, R>
{
    type Response = R;
    type Future = Ready<Result<Response<R>, Status>>;

    fn call(&mut self, request: Request<Q>) -> Self::Future {
        let call = || -> Result<Response<R>, Status> {
            if request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some("Bearer fixture")
            {
                return Err(Status::unauthenticated("missing bearer"));
            }
            let candidates: Vec<_> = self
                .service
                .cases
                .iter()
                .filter(|case| case["method"] == self.method)
                .collect();
            let first = candidates
                .first()
                .ok_or_else(|| Status::unimplemented("missing golden"))?;
            let input = first["input"]
                .as_str()
                .ok_or_else(|| Status::internal("missing input"))?;
            let json = contract::json(input, request.get_ref())
                .map_err(|error| Status::internal(error.to_string()))?;
            // Cloud's probe sends an empty handshake; the hosted facade sends its required identity.
            let case = if self.method == "Handshake" {
                first
            } else {
                candidates
                    .iter()
                    .find(|case| case["request"] == json)
                    .ok_or_else(|| {
                        Status::invalid_argument(format!(
                            "{} differs from Cloud JSON: {json}",
                            self.method
                        ))
                    })?
            };
            self.service
                .seen
                .lock()
                .map_err(|_| Status::internal("poisoned lock"))?
                .push(self.method.into());
            let output = case["output"]
                .as_str()
                .ok_or_else(|| Status::internal("missing output"))?;
            Ok(Response::new(message(output, &case["response"])?))
        };
        ready(call())
    }
}

impl NamedService for GoldenService {
    const NAME: &'static str = "acyclic.filesystem.v2.FilesystemService";
}

impl tower::Service<http::Request<Body>> for GoldenService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            macro_rules! route {
                ($method:literal, $input:ident, $output:ident) => {{
                    let unary = Unary::<wire::$input, wire::$output> {
                        service,
                        method: $method,
                        marker: PhantomData,
                    };
                    Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(unary, request)
                        .await)
                }};
            }
            match request.uri().path().rsplit('/').next() {
                Some("Handshake") => route!("Handshake", HandshakeRequest, HandshakeResponse),
                Some("CreateWorkspace") => {
                    route!("CreateWorkspace", CreateWorkspaceRequest, WorkspaceResponse)
                }
                Some("ApplyTransaction") => route!(
                    "ApplyTransaction",
                    ApplyTransactionRequest,
                    MutationResponse
                ),
                Some("GetHead") => route!("GetHead", GetHeadRequest, GenerationResponse),
                Some("Read") => route!("Read", ReadRequest, ReadResponse),
                Some("ListDirectory") => {
                    route!("ListDirectory", ListDirectoryRequest, ListDirectoryResponse)
                }
                Some("DeleteWorkspace") => {
                    route!("DeleteWorkspace", DeleteWorkspaceRequest, MutationResponse)
                }
                _ => {
                    let mut response = http::Response::new(Body::empty());
                    response
                        .headers_mut()
                        .insert("grpc-status", http::HeaderValue::from_static("12"));
                    response.headers_mut().insert(
                        "content-type",
                        http::HeaderValue::from_static("application/grpc"),
                    );
                    Ok(response)
                }
            }
        })
    }
}

#[tokio::test]
async fn hosted_and_generated_clients_match_cloud_filesystem_roundtrip()
-> Result<(), Box<dyn std::error::Error>> {
    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../test-fixtures/cloud-filesystem/golden.json"
    ))?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let service = GoldenService {
        cases: Arc::new(cases.clone()),
        seen: seen.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (stop, stopping) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopping.await;
            }),
    );
    let hosted = Fs::hosted(HostedFsOptions::new(&endpoint, "fixture")).await?;
    let workspace = hosted
        .create_workspace(
            "same-name",
            FilesystemProfile::Portable,
            IdempotencyKey::from_bytes([1; 16]),
        )
        .await?;
    assert_eq!(workspace.name(), "same-name");
    let mut transaction = workspace.begin_transaction(IdempotencyKey::from_bytes([2; 16]));
    transaction.create_file("/value", b"probe".to_vec(), None);
    let written = transaction.commit(16).await?;
    assert_eq!(
        &contract::json("MutationResponse", &written)?,
        cases
            .get(2)
            .and_then(|case| case.get("response"))
            .ok_or("missing write golden response")?
    );
    let head = workspace.head().await?;
    assert_eq!(head.read("/value", 64).await?.as_ref(), b"probe");
    let page = head.list_directory("/", None, 16).await?;
    assert_eq!(
        &contract::json(
            "ListDirectoryResponse",
            &wire::ListDirectoryResponse { page: Some(page) }
        )?,
        cases
            .get(5)
            .and_then(|case| case.get("response"))
            .ok_or("missing list golden response")?
    );
    let deleted = workspace
        .delete(IdempotencyKey::from_bytes([4; 16]))
        .await?;
    assert_eq!(
        &contract::json("MutationResponse", &deleted)?,
        cases
            .get(9)
            .and_then(|case| case.get("response"))
            .ok_or("missing delete golden response")?
    );
    assert_eq!(
        *seen.lock().map_err(|_| "poisoned lock")?,
        [
            "Handshake",
            "CreateWorkspace",
            "ApplyTransaction",
            "GetHead",
            "Read",
            "ListDirectory",
            "DeleteWorkspace"
        ]
    );

    replay_generated(&endpoint, &cases).await?;
    drop(head);
    drop(workspace);
    drop(hosted);
    let _ = stop.send(());
    server.await??;
    Ok(())
}

async fn replay_generated(
    endpoint: &str,
    cases: &[Value],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FilesystemServiceClient::connect(endpoint.to_owned()).await?;
    for case in cases {
        macro_rules! replay {
            ($input:ident, $method:ident) => {{
                let mut request = Request::new(message::<wire::$input>(
                    case["input"].as_str().ok_or("input missing")?,
                    &case["request"],
                )?);
                request
                    .metadata_mut()
                    .insert("authorization", "Bearer fixture".parse()?);
                let response = client.$method(request).await?.into_inner();
                assert_eq!(
                    contract::json(case["output"].as_str().ok_or("output missing")?, &response)?,
                    case["response"]
                );
            }};
        }
        match case["method"].as_str() {
            Some("Handshake") => replay!(HandshakeRequest, handshake),
            Some("CreateWorkspace") => replay!(CreateWorkspaceRequest, create_workspace),
            Some("ApplyTransaction") => replay!(ApplyTransactionRequest, apply_transaction),
            Some("GetHead") => replay!(GetHeadRequest, get_head),
            Some("Read") => replay!(ReadRequest, read),
            Some("ListDirectory") => replay!(ListDirectoryRequest, list_directory),
            Some("DeleteWorkspace") => replay!(DeleteWorkspaceRequest, delete_workspace),
            _ => return Err("unexpected golden method".into()),
        }
    }
    drop(client);
    Ok(())
}
