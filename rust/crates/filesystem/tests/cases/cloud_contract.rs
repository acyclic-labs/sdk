//! Hosted and generated clients replay Cloud's filesystem handler-test JSON.

#[path = "../support/cloud_contract.rs"]
mod contract;

use acyclic_fs::model::FilesystemProfile;
use acyclic_fs_wire::wire::filesystem::v2::{
    self as wire, filesystem_service_client::FilesystemServiceClient,
};
use acyclic_fs::{HostedFs, HostedFsOptions, IdempotencyKey};
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
        let descriptor = DescriptorPool::decode(acyclic_fs_wire::FILE_DESCRIPTOR_SET)?
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
                Some("GetGeneration") => {
                    route!("GetGeneration", GetGenerationRequest, GenerationResponse)
                }
                Some("OpenWorkspace") => {
                    route!("OpenWorkspace", OpenWorkspaceRequest, WorkspaceResponse)
                }
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

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// Golden cases whose responses the hosted roundtrip reproduces, in its call order.
const ROUNDTRIP_CASES: [usize; 7] = [2, 4, 5, 6, 7, 8, 9];

/// Fields whose values a server assigns, so two servers agree on them only up to renaming.
const ASSIGNED: [&str; 3] = ["generationId", "workspaceId", "fileId"];

/// Renames every server-assigned identity to its order of first appearance, so
/// two traces are equal when they agree on everything else and on which
/// responses name the same identity.
fn renamed(mut responses: Vec<Value>) -> Vec<Value> {
    fn walk(value: &mut Value, seen: &mut Vec<(String, String)>) {
        match value {
            Value::Object(fields) => {
                for (key, child) in fields.iter_mut() {
                    match child {
                        Value::String(id) if ASSIGNED.contains(&key.as_str()) => {
                            let identity = (key.clone(), id.clone());
                            let index = seen
                                .iter()
                                .position(|known| *known == identity)
                                .unwrap_or_else(|| {
                                    seen.push(identity);
                                    seen.len() - 1
                                });
                            *child = Value::String(format!("{key}#{index}"));
                        }
                        _ => walk(child, seen),
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| walk(item, seen)),
            _ => {}
        }
    }
    let mut seen = Vec::new();
    responses
        .iter_mut()
        .for_each(|response| walk(response, &mut seen));
    responses
}

/// Cloud's handler-test JSON. Its handshake names the descriptor it was captured
/// against, which the SDK's descriptor has since moved past, so the replay names
/// the SDK's own. The live test checks that the real Filesystem service names it too.
fn golden() -> TestResult<Vec<Value>> {
    let mut cases: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/cloud-filesystem/golden.json"))?;
    *cases
        .first_mut()
        .and_then(|handshake| handshake.pointer_mut("/response/protocol/protocol/descriptorDigest"))
        .ok_or("missing handshake golden")? = Value::String(
        blake3::hash(acyclic_fs_wire::FILE_DESCRIPTOR_SET)
            .to_hex()
            .to_string(),
    );
    Ok(cases)
}

/// Cloud's handler-test responses for the hosted roundtrip.
fn golden_roundtrip(cases: &[Value]) -> TestResult<Vec<Value>> {
    ROUNDTRIP_CASES
        .iter()
        .map(|&index| {
            Ok(cases
                .get(index)
                .and_then(|case| case.get("response"))
                .ok_or("missing roundtrip golden response")?
                .clone())
        })
        .collect()
}

/// Creates, writes, reads, lists, removes, reads a retained generation and deletes
/// one workspace, returning each response as Cloud's JSON.
/// How the roundtrip reopens the generation it just wrote.
#[derive(Clone, Copy)]
enum Head {
    /// `GetHead`, as Cloud's handler tests do.
    Current,
    /// `GetGeneration` of the written generation, for a bearer without `GetHead`.
    Written,
}

async fn hosted_roundtrip(hosted: &HostedFs, reopen: Head) -> TestResult<Vec<Value>> {
    // Names the failing call, since a live server's status alone does not.
    let at =
        |step: &'static str| move |error: acyclic_fs::HostedFsError| format!("{step}: {error}");
    let read = |contents: &[u8]| {
        contract::json(
            "ReadResponse",
            &wire::ReadResponse {
                contents: contents.to_vec(),
            },
        )
    };
    let list = |page| {
        contract::json(
            "ListDirectoryResponse",
            &wire::ListDirectoryResponse { page: Some(page) },
        )
    };
    let mut responses = Vec::new();
    let workspace = hosted
        .create_workspace(
            "same-name",
            FilesystemProfile::Portable,
            IdempotencyKey::from_bytes([1; 16]),
        )
        .await
        .map_err(at("create workspace"))?;
    assert_eq!(workspace.name(), "same-name");
    let mut transaction = workspace.begin_transaction(IdempotencyKey::from_bytes([2; 16]));
    transaction.create_file("/value", b"probe".to_vec(), None);
    let written = transaction.commit(16).await.map_err(at("write"))?;
    responses.push(contract::json("MutationResponse", &written)?);
    let head = match reopen {
        Head::Current => workspace.head().await.map_err(at("head"))?,
        Head::Written => workspace
            .generation(
                written
                    .generation
                    .ok_or("missing written generation")?
                    .generation_id,
            )
            .await
            .map_err(at("written generation"))?,
    };
    responses.push(read(
        head.read("/value", 64)
            .await
            .map_err(at("read head"))?
            .as_ref(),
    )?);
    responses.push(list(
        head.list_directory("/", None, 16)
            .await
            .map_err(at("list head"))?,
    )?);
    let reopened = hosted
        .open_workspace("same-name")
        .await
        .map_err(at("open workspace"))?;
    let mut transaction = reopened.begin_transaction(IdempotencyKey::from_bytes([3; 16]));
    transaction.remove("/value");
    let removed = transaction.commit(16).await.map_err(at("remove"))?;
    responses.push(contract::json("MutationResponse", &removed)?);
    let generation = reopened
        .generation(
            removed
                .generation
                .ok_or("missing removed generation")?
                .generation_id,
        )
        .await
        .map_err(at("generation"))?;
    responses.push(list(
        generation
            .list_directory("/", None, 16)
            .await
            .map_err(at("list removed"))?,
    )?);
    responses.push(read(
        head.read("/value", 64)
            .await
            .map_err(at("read retained"))?
            .as_ref(),
    )?);
    responses.push(contract::json(
        "MutationResponse",
        &workspace
            .delete(IdempotencyKey::from_bytes([4; 16]))
            .await
            .map_err(at("delete"))?,
    )?);
    Ok(responses)
}

#[tokio::test]
async fn hosted_and_generated_clients_match_cloud_filesystem_roundtrip() -> TestResult {
    let cases = golden()?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let service = GoldenService {
        cases: Arc::new(hosted_setup_cases(&cases)?),
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
    let hosted = HostedFs::connect(HostedFsOptions::new(&endpoint, "fixture")).await?;
    let golden = golden_roundtrip(&cases)?;
    // The replaying server answers with Cloud's own bytes, so the trace is exact.
    let responses = hosted_roundtrip(&hosted, Head::Current).await?;
    assert_eq!(responses, golden);
    assert_eq!(
        *seen.lock().map_err(|_| "poisoned lock")?,
        [
            "Handshake",
            "CreateWorkspace",
            "ApplyTransaction",
            "GetHead",
            "Read",
            "ListDirectory",
            "OpenWorkspace",
            "ApplyTransaction",
            "GetGeneration",
            "ListDirectory",
            "Read",
            "DeleteWorkspace"
        ]
    );
    // The comparison a live server gets: equal up to renaming, and no looser.
    assert_eq!(renamed(responses.clone()), renamed(golden.clone()));
    let mut changed = responses.clone();
    *changed
        .get_mut(2)
        .and_then(|list| list.pointer_mut("/page/entries/0/name/bytes"))
        .ok_or("missing listed name")? = Value::String("b3RoZXI=".into());
    assert_ne!(renamed(changed), renamed(golden.clone()));
    // A server that reuses one generation where Cloud names two distinct ones differs.
    let mut merged = responses;
    let written = merged
        .first()
        .and_then(|write| write.pointer("/generation/generationId"))
        .cloned()
        .ok_or("missing written generation")?;
    *merged
        .get_mut(3)
        .and_then(|removal| removal.pointer_mut("/generation/generationId"))
        .ok_or("missing removed generation")? = written;
    assert_ne!(renamed(merged), renamed(golden));

    // The live run's reopening of the written generation yields the same responses.
    assert_eq!(
        hosted_roundtrip(&hosted, Head::Written).await?,
        golden_roundtrip(&cases)?
    );

    replay_generated(&endpoint, &cases).await?;
    drop(hosted);
    let _ = stop.send(());
    server.await??;
    Ok(())
}

/// Creates the Objects bucket the Filesystem Hosts store into, as activation does
/// in a deployed cloud, through the fabric's Objects Edge.
async fn provision_storage(endpoints: &Value, bucket: &str, ca: &[u8]) -> TestResult {
    use acyclic_objects::{GrpcObjects, ObjectsProvider, wire};
    let objects = GrpcObjects::connect(
        endpoints["objects"]["endpoint"]
            .as_str()
            .ok_or("endpoints file has no objects.endpoint")?,
        endpoints["objects"]["bearer"]
            .as_str()
            .ok_or("endpoints file has no objects.bearer")?,
        Some(ca),
        None,
    )
    .await?;
    let created = objects
        .create_bucket(wire::CreateBucketRequest {
            name: bucket.into(),
            mutation: Some(wire::MutationIdentity {
                idempotency_key: format!("local-cloud-storage-{bucket}"),
            }),
        })
        .await;
    match created {
        Err(error) if error.code != wire::ErrorCode::AlreadyExists => {
            Err(format!("create storage bucket {bucket}: {error}").into())
        }
        _ => Ok(()),
    }
}

/// The same roundtrip against the real Filesystem service through its Edge, from
/// `node scripts/local-cloud.mjs --topology full`, which names the Edge, its CA,
/// a locally minted Filesystem bearer and the Hosts' storage bucket in
/// `ACYCLIC_LOCAL_CLOUD`.
#[tokio::test]
#[ignore = "needs a local cloud: node scripts/local-cloud.mjs --topology full"]
async fn hosted_roundtrip_matches_cloud_handlers_on_a_local_cloud() -> TestResult {
    let file = std::env::var("ACYCLIC_LOCAL_CLOUD")
        .map_err(|_| "ACYCLIC_LOCAL_CLOUD names the local cloud's endpoints file")?;
    let endpoints: Value = serde_json::from_slice(&std::fs::read(file)?)?;
    let fs = &endpoints["fs"];
    let field = |name: &str| {
        fs[name]
            .as_str()
            .map(str::to_owned)
            .ok_or(format!("endpoints file has no fs.{name}"))
    };
    let ca = std::fs::read(field("ca_file")?)?;
    provision_storage(&endpoints, &field("storage_bucket")?, &ca).await?;
    let hosted = HostedFs::connect(
        HostedFsOptions::new(field("endpoint")?, field("bearer")?).with_ca_certificate(ca),
    )
    .await?;
    let cases = golden()?;
    let golden = renamed(golden_roundtrip(&cases)?);
    // The gate's locally minted Filesystem bearer omits GetHead (and every
    // operation past Cloud's own e2e calls), so this run reopens the written
    // generation instead; the responses compared are the same. Once the bearer
    // carries GetHead this refusal fails, and the run should use Head::Current.
    let probe = hosted
        .create_workspace(
            "head-probe",
            FilesystemProfile::Portable,
            IdempotencyKey::from_bytes([5; 16]),
        )
        .await?;
    match probe.head().await {
        Err(acyclic_fs::HostedFsError::Status(status))
            if status.code() == tonic::Code::PermissionDenied => {}
        other => return Err(format!("GetHead is no longer refused: {:?}", other.err()).into()),
    }
    probe.delete(IdempotencyKey::from_bytes([6; 16])).await?;
    assert_eq!(
        renamed(hosted_roundtrip(&hosted, Head::Written).await?),
        golden
    );
    println!(
        "local cloud: hosted Filesystem roundtrip matched {} Cloud handler responses",
        golden.len()
    );
    Ok(())
}

fn hosted_setup_cases(cases: &[Value]) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let mut result = cases.to_vec();
    let mut workspace = cases.get(1).ok_or("missing workspace golden")?["response"].clone();
    let written = cases
        .get(2)
        .ok_or("missing written golden")?
        .pointer("/response/generation")
        .cloned()
        .unwrap_or_default();
    workspace
        .get_mut("workspace")
        .and_then(Value::as_object_mut)
        .ok_or("missing workspace response")?
        .insert("head".into(), written);
    result.push(serde_json::json!({
        "method": "OpenWorkspace", "input": "OpenWorkspaceRequest", "output": "WorkspaceResponse",
        "request": {"name": "same-name"}, "response": workspace
    }));
    let removed = cases
        .get(6)
        .ok_or("missing removed golden")?
        .pointer("/response/generation")
        .cloned()
        .unwrap_or_default();
    // Each reopened generation answers as itself: the removal's, and the write's for Head::Written.
    let written = cases
        .get(2)
        .and_then(|write| write.pointer("/response/generation"))
        .cloned()
        .ok_or("missing written golden")?;
    for generation in [removed, written] {
        result.push(serde_json::json!({
            "method": "GetGeneration", "input": "GetGenerationRequest", "output": "GenerationResponse",
            "request": {"generation": generation}, "response": {"generation": generation}
        }));
    }
    Ok(result)
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
