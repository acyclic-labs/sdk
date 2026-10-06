//! Executable, typed Filesystem/Harness scenario export.
//!
//! The public qualification table describes the scenario graph.  This module
//! adds the wire evidence: every request is a generated protobuf value sent to
//! the production Rust adapter, and every response is encoded from the value
//! returned by that adapter.  Consumers can therefore validate bytes and
//! identities without treating a prose fixture table as a protocol input.

use std::collections::BTreeMap;

use futures::StreamExt;
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::{iter, wrappers::TcpListenerStream};
use tonic::{Request, Status};
use tonic::transport::Server;

use acyclic_fs::wire::filesystem::v2 as fs_wire;
use acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemService;
use acyclic_harness::{
    wire as harness_wire,
    wire_api::{HarnessWireApi, current_protocol},
};

use super::{
    filesystem_harness::{empty_filesystem_service, ensure_transfer_source, filesystem_service},
    harness_backend::StatefulHarnessFixtureBackend as HarnessFixtureBackend,
};

const SOURCE: &str = "rust/crates/sdk-examples/src/fixtures/filesystem_harness_scenarios.rs";

/// Export all typed scenario evidence from one fresh production fixture.
pub async fn export() -> Result<Vec<Value>, String> {
    let service = filesystem_service().map_err(|error| error.to_string())?;
    ensure_transfer_source(&service)
        .await
        .map_err(|error| error.to_string())?;
    let mut evidence = export_filesystem(service)
        .await
        .map_err(|error| error.to_string())?;
    evidence.extend(export_harness().await.map_err(|error| error.to_string())?);
    Ok(evidence)
}

fn b64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0] as usize;
        let second = chunk.get(1).copied().unwrap_or_default() as usize;
        let third = chunk.get(2).copied().unwrap_or_default() as usize;
        output.push(ALPHABET[first >> 2] as char);
        output.push(ALPHABET[((first & 3) << 4) | (second >> 4)] as char);
        output.push(if chunk.len() > 1 {
            ALPHABET[((second & 15) << 2) | (third >> 6)] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            ALPHABET[third & 63] as char
        } else {
            '='
        });
    }
    output
}

fn digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn rpc_name(family: &str, operation: &str) -> String {
    let service = match family {
        "filesystem" => "FilesystemService",
        "harness" => "HarnessService",
        _ => "Service",
    };
    format!("acyclic.{family}.v2.{service}/{operation}")
}

fn evidence<Req: Message, Resp: Message>(
    family: &str,
    operation: &str,
    request_type: &str,
    request: &Req,
    response_type: &str,
    response: &Resp,
    state: &BTreeMap<&'static str, String>,
) -> Value {
    let request_bytes = request.encode_to_vec();
    let response_bytes = response.encode_to_vec();
    json!({
        "family": family,
        "operation": operation,
        "rpc": rpc_name(family, operation),
        "source": SOURCE,
        "request": {
            "type": request_type,
            "bytes_base64": b64(&request_bytes),
            "sha256": digest(&request_bytes),
        },
        "response": {
            "type": response_type,
            "bytes_base64": b64(&response_bytes),
            "sha256": digest(&response_bytes),
        },
        "state": state,
    })
}

fn result_evidence<Req: Message, Resp: Message>(
    family: &str,
    operation: &str,
    request_type: &str,
    request: &Req,
    response_type: &str,
    result: Result<tonic::Response<Resp>, Status>,
    state: &BTreeMap<&'static str, String>,
) -> Value {
    match result {
        Ok(response) => evidence(
            family,
            operation,
            request_type,
            request,
            response_type,
            &response.into_inner(),
            state,
        ),
        Err(error) => {
            let request_bytes = request.encode_to_vec();
            json!({
                "family": family,
                "operation": operation,
                "rpc": rpc_name(family, operation),
                "source": SOURCE,
                "request": {
                    "type": request_type,
                    "bytes_base64": b64(&request_bytes),
                    "sha256": digest(&request_bytes),
                },
                "response": {
                    "status": error.code().to_string(),
                    "code": format!("{:?}", error.code()),
                    "message": error.message(),
                },
                "state": state,
            })
        }
    }
}

fn workspace_ref(workspace: &fs_wire::Workspace) -> Result<fs_wire::WorkspaceRef, Status> {
    workspace
        .workspace
        .clone()
        .ok_or_else(|| Status::internal("workspace response omitted identity"))
}

fn head_ref(workspace: &fs_wire::Workspace) -> Result<fs_wire::GenerationRef, Status> {
    workspace
        .head
        .clone()
        .ok_or_else(|| Status::internal("workspace response omitted head"))
}

async fn export_filesystem<S>(service: S) -> Result<Vec<Value>, Status>
where
    S: FilesystemService + Clone + Send + Sync + 'static,
{
    let mut output = Vec::new();
    let mut state = BTreeMap::new();

    let handshake = fs_wire::HandshakeRequest { protocol: None };
    let response = service.handshake(Request::new(handshake.clone())).await?.into_inner();
    output.push(evidence(
        "filesystem",
        "Handshake",
        "acyclic.filesystem.v2.HandshakeRequest",
        &handshake,
        "acyclic.filesystem.v2.HandshakeResponse",
        &response,
        &state,
    ));

    // The production fixture already contains the deterministic `fixture`
    // workspace.  Create a second workspace through the wire API so this
    // scenario exercises the real create response and then carries its
    // returned identity through subsequent calls.
    let create = fs_wire::CreateWorkspaceRequest {
        name: "scenario-control".into(),
        profile: fs_wire::FilesystemProfile::Portable as i32,
        operation: Some(fs_wire::OperationOptions {
            idempotency_key: vec![0x11; 16],
        }),
    };
    let created = service
        .create_workspace(Request::new(create.clone()))
        .await?
        .into_inner();
    let created_workspace = created
        .workspace
        .clone()
        .ok_or_else(|| Status::internal("create response omitted workspace"))?;
    let created_ref = workspace_ref(&created_workspace)?;
    state.insert("created_workspace_id", b64(&created_ref.workspace_id));
    output.push(evidence(
        "filesystem",
        "CreateWorkspace",
        "acyclic.filesystem.v2.CreateWorkspaceRequest",
        &create,
        "acyclic.filesystem.v2.WorkspaceResponse",
        &created,
        &state,
    ));

    // Read-only operations use the seeded workspace, whose returned head is
    // the actual generation identity used in every following protobuf input.
    let open_fixture = fs_wire::OpenWorkspaceRequest {
            selector: Some(fs_wire::open_workspace_request::Selector::Name(
                "fixture".into(),
            )),
    };
    let opened_fixture = service
        .open_workspace(Request::new(open_fixture.clone()))
        .await?
        .into_inner()
        ;
    let fixture = opened_fixture
        .workspace
        .clone()
        .ok_or_else(|| Status::internal("fixture workspace missing"))?;
    output.push(evidence(
        "filesystem",
        "OpenWorkspace",
        "acyclic.filesystem.v2.OpenWorkspaceRequest",
        &open_fixture,
        "acyclic.filesystem.v2.WorkspaceResponse",
        &opened_fixture,
        &state,
    ));
    let fixture_ref = workspace_ref(&fixture)?;
    let fixture_head = head_ref(&fixture)?;
    state.insert("fixture_workspace_id", b64(&fixture_ref.workspace_id));
    state.insert("fixture_generation_id", b64(&fixture_head.generation_id));

    let get_head = fs_wire::GetHeadRequest {
        workspace: Some(fixture_ref.clone()),
    };
    let head = service.get_head(Request::new(get_head.clone())).await?.into_inner();
    output.push(evidence(
        "filesystem",
        "GetHead",
        "acyclic.filesystem.v2.GetHeadRequest",
        &get_head,
        "acyclic.filesystem.v2.GenerationResponse",
        &head,
        &state,
    ));

    let get_generation = fs_wire::GetGenerationRequest {
        generation: Some(fixture_head.clone()),
    };
    let generation = service
        .get_generation(Request::new(get_generation.clone()))
        .await?
        .into_inner();
    output.push(evidence(
        "filesystem",
        "GetGeneration",
        "acyclic.filesystem.v2.GetGenerationRequest",
        &get_generation,
        "acyclic.filesystem.v2.GenerationResponse",
        &generation,
        &state,
    ));

    let read = fs_wire::ReadRequest {
        generation: Some(fixture_head.clone()),
        path: "/hello".into(),
        range: None,
        maximum_bytes: 1024,
    };
    let read_response = service.read(Request::new(read.clone())).await?.into_inner();
    if read_response.contents != b"rust-fixture" {
        return Err(Status::internal("seeded read response changed"));
    }
    output.push(evidence(
        "filesystem",
        "Read",
        "acyclic.filesystem.v2.ReadRequest",
        &read,
        "acyclic.filesystem.v2.ReadResponse",
        &read_response,
        &state,
    ));

    let stat = fs_wire::StatRequest {
        generation: Some(fixture_head.clone()),
        path: "/hello".into(),
    };
    let stat_response = service.stat(Request::new(stat.clone())).await?.into_inner();
    output.push(evidence(
        "filesystem",
        "Stat",
        "acyclic.filesystem.v2.StatRequest",
        &stat,
        "acyclic.filesystem.v2.StatResponse",
        &stat_response,
        &state,
    ));

    let list = fs_wire::ListDirectoryRequest {
        generation: Some(fixture_head.clone()),
        path: "/".into(),
        page: Some(fs_wire::PageOptions {
            maximum_items: 32,
            after: None,
        }),
    };
    let list_response = service
        .list_directory(Request::new(list.clone()))
        .await?
        .into_inner();
    output.push(evidence(
        "filesystem",
        "ListDirectory",
        "acyclic.filesystem.v2.ListDirectoryRequest",
        &list,
        "acyclic.filesystem.v2.ListDirectoryResponse",
        &list_response,
        &state,
    ));

    // ReadLink uses the symlink seeded by the production fixture. The response
    // is emitted from the real wire adapter so the generated evidence carries
    // the same target bytes as the fixture's semantic scenario.
    let read_link = fs_wire::ReadLinkRequest {
        generation: Some(fixture_head.clone()),
        path: "/link".into(),
        maximum_bytes: 1024,
    };
    match service.read_link(Request::new(read_link.clone())).await {
        Ok(response) => {
            let response = response.into_inner();
            if response.contents != b"hello" {
                return Err(Status::internal("seeded symlink target changed"));
            }
            output.push(evidence(
                "filesystem",
                "ReadLink",
                "acyclic.filesystem.v2.ReadLinkRequest",
                &read_link,
                "acyclic.filesystem.v2.ReadResponse",
                &response,
                &state,
            ));
        }
        Err(error) => {
            output.push(json!({
                "family": "filesystem",
                "operation": "ReadLink",
                "rpc": "acyclic.filesystem.v2.FilesystemService/ReadLink",
                "source": SOURCE,
                "request": {
                    "type": "acyclic.filesystem.v2.ReadLinkRequest",
                    "bytes_base64": b64(&read_link.encode_to_vec()),
                    "sha256": digest(&read_link.encode_to_vec()),
                },
                "response": {
                    "status": error.code().to_string(),
                    "code": format!("{:?}", error.code()),
                    "message": error.message()
                },
                "state": state,
            }));
        }
    }

    macro_rules! record_rpc {
        ($method:ident, $operation:literal, $request:expr, $request_type:literal, $response_type:literal) => {{
            let request = $request;
            let result = service.$method(Request::new(request.clone())).await;
            output.push(result_evidence(
                "filesystem",
                $operation,
                $request_type,
                &request,
                $response_type,
                result,
                &state,
            ));
        }};
    }

    record_rpc!(
        plan_extents,
        "PlanExtents",
        fs_wire::PlanExtentsRequest {
            generation: Some(fixture_head.clone()),
            path: "/hello".into(),
            range: None,
            maximum_extents: 32,
        },
        "acyclic.filesystem.v2.PlanExtentsRequest",
        "acyclic.filesystem.v2.PlanExtentsResponse"
    );
    let mutation = fs_wire::Mutation {
        mutation: Some(fs_wire::mutation::Mutation::PutFile(fs_wire::PutFile {
            path: "/scenario".into(),
            contents: b"scenario".to_vec(),
        })),
    };
    let apply = fs_wire::ApplyTransactionRequest {
        base: Some(fixture_head.clone()),
        mutations: vec![mutation.clone()],
        operation: Some(fs_wire::OperationOptions {
            idempotency_key: vec![0x31; 16],
        }),
        maximum_conflicts: 32,
    };
    let apply_result = service
        .apply_transaction(Request::new(apply.clone()))
        .await;
    if let Ok(response) = &apply_result {
        if let Some(generation) = response.get_ref().generation.clone() {
            state.insert("mutated_generation_id", b64(&generation.generation_id));
        }
    }
    output.push(result_evidence(
        "filesystem",
        "ApplyTransaction",
        "acyclic.filesystem.v2.ApplyTransactionRequest",
        &apply,
        "acyclic.filesystem.v2.MutationResponse",
        apply_result,
        &state,
    ));
    record_rpc!(
        rebase_transaction,
        "RebaseTransaction",
        fs_wire::RebaseTransactionRequest {
            base: Some(fixture_head.clone()),
            mutations: vec![mutation],
            maximum_conflicts: 32,
            operation: Some(fs_wire::OperationOptions {
                idempotency_key: vec![0x32; 16],
            }),
        },
        "acyclic.filesystem.v2.RebaseTransactionRequest",
        "acyclic.filesystem.v2.RebaseTransactionResponse"
    );
    record_rpc!(
        fork_workspace,
        "ForkWorkspace",
        fs_wire::ForkWorkspaceRequest {
            source: Some(fixture_head.clone()),
            destination_name: "scenario-fork".into(),
            operation: Some(fs_wire::OperationOptions {
                idempotency_key: vec![0x33; 16],
            }),
        },
        "acyclic.filesystem.v2.ForkWorkspaceRequest",
        "acyclic.filesystem.v2.WorkspaceResponse"
    );
    record_rpc!(
        diff,
        "Diff",
        fs_wire::DiffRequest {
            from: Some(fixture_head.clone()),
            to: Some(fixture_head.clone()),
            maximum_changes: 32,
        },
        "acyclic.filesystem.v2.DiffRequest",
        "acyclic.filesystem.v2.DiffResponse"
    );
    record_rpc!(
        rebase,
        "Rebase",
        fs_wire::RebaseRequest {
            workspace: Some(fixture_ref.clone()),
            maximum_conflicts: 32,
            operation: Some(fs_wire::OperationOptions {
                idempotency_key: vec![0x34; 16],
            }),
            maximum_generations: 32,
            maximum_changes: 32,
        },
        "acyclic.filesystem.v2.RebaseRequest",
        "acyclic.filesystem.v2.RebaseResponse"
    );
    record_rpc!(
        plan_join,
        "PlanJoin",
        fs_wire::PlanJoinRequest {
            source: Some(fixture_head.clone()),
            target: Some(fixture_head.clone()),
            maximum_changes: 32,
            maximum_conflicts: 32,
            maximum_generations: 32,
            history: fs_wire::JoinHistory::Merge as i32,
        },
        "acyclic.filesystem.v2.PlanJoinRequest",
        "acyclic.filesystem.v2.JoinPlan"
    );
    record_rpc!(
        apply_join,
        "ApplyJoin",
        fs_wire::ApplyJoinRequest {
            plan: None,
            operation: Some(fs_wire::OperationOptions {
                idempotency_key: vec![0x35; 16],
            }),
        },
        "acyclic.filesystem.v2.ApplyJoinRequest",
        "acyclic.filesystem.v2.JoinResponse"
    );
    let retain = |identity: &'static str, key: u8| fs_wire::RetainGenerationRequest {
        generation: Some(fixture_head.clone()),
        identity: identity.into(),
        operation: Some(fs_wire::OperationOptions {
            idempotency_key: vec![key; 16],
        }),
    };
    record_rpc!(
        checkpoint,
        "Checkpoint",
        retain("fixture-checkpoint", 0x36),
        "acyclic.filesystem.v2.RetainGenerationRequest",
        "acyclic.filesystem.v2.RetainGenerationResponse"
    );
    record_rpc!(
        pin,
        "Pin",
        retain("fixture-pin", 0x37),
        "acyclic.filesystem.v2.RetainGenerationRequest",
        "acyclic.filesystem.v2.RetainGenerationResponse"
    );
    // The transfer source is shared with the hosted fixture setup. Open the
    // Rust-created identity instead of creating a producer-only workspace.
    let transfer_workspace = service
        .open_workspace(Request::new(fs_wire::OpenWorkspaceRequest {
            selector: Some(fs_wire::open_workspace_request::Selector::Name(
                "scenario-export".to_owned(),
            )),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or_else(|| Status::internal("transfer source omitted workspace"))?;
    let transfer_ref = workspace_ref(&transfer_workspace)?;
    let transfer_head = head_ref(&transfer_workspace)?;
    let export_request = fs_wire::ExportRequest {
        generation: Some(transfer_head),
        after: Vec::new(),
        maximum_objects: 32,
        maximum_bytes: 1024 * 1024,
    };
    let export_request_bytes = export_request.encode_to_vec();
    let mut exported_chunks = Vec::new();
    match service.export(Request::new(export_request.clone())).await {
        Ok(response) => {
            let mut chunks = response.into_inner();
            tokio::pin!(chunks);
            let mut encoded = Vec::new();
            while let Some(chunk) = chunks.next().await {
                let chunk = chunk?;
                exported_chunks.push(chunk.clone());
                encoded.push(json!({
                    "type": "acyclic.filesystem.v2.ExportChunk",
                    "bytes_base64": b64(&chunk.encode_to_vec()),
                    "sha256": digest(&chunk.encode_to_vec()),
                    "terminal": chunk.terminal,
                }));
            }
            output.push(json!({
                "family": "filesystem",
                "operation": "Export",
                "rpc": "acyclic.filesystem.v2.FilesystemService/Export",
                "source": SOURCE,
                "request": {
                    "type": "acyclic.filesystem.v2.ExportRequest",
                    "bytes_base64": b64(&export_request_bytes),
                    "sha256": digest(&export_request_bytes),
                },
                "response_frames": encoded,
                "response": { "chunks": encoded },
                "state": state,
            }));
        }
        Err(error) => output.push(json!({
            "family": "filesystem",
            "operation": "Export",
            "rpc": "acyclic.filesystem.v2.FilesystemService/Export",
            "source": SOURCE,
            "request": {
                "type": "acyclic.filesystem.v2.ExportRequest",
                "bytes_base64": b64(&export_request_bytes),
                "sha256": digest(&export_request_bytes),
            },
            "response": {
                "status": error.code().to_string(),
                "code": format!("{:?}", error.code()),
                "message": error.message()
            },
            "state": state,
        })),
    }
    if exported_chunks.is_empty() {
        return Err(Status::internal("export produced no manifest chunk"));
    }
    let operation_id = vec![0x44; 16];
    let import_chunks: Vec<_> = exported_chunks
        .iter()
        .map(|chunk| fs_wire::ImportChunk {
            workspace: Some(transfer_ref.clone()),
            operation_id: operation_id.clone(),
            cursor: chunk.cursor.clone(),
            object_id: chunk.object_id.clone(),
            contents: chunk.contents.clone(),
            terminal: chunk.terminal,
        })
        .collect();
    let import_request_bytes = import_chunks[0].encode_to_vec();
    let import_request_chunks: Vec<_> = import_chunks
        .iter()
        .map(|chunk| {
            let bytes = chunk.encode_to_vec();
            json!({
                "type": "acyclic.filesystem.v2.ImportChunk",
                "bytes_base64": b64(&bytes),
                "sha256": digest(&bytes),
            })
        })
        .collect();
    // The generated server trait accepts tonic::Streaming, so exercise this
    // client-stream operation through the generated client over an in-process
    // loopback listener. This keeps the request typed while avoiding a second
    // hand-written stream adapter in the fixture.
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|error| Status::internal(format!("bind import fixture: {error}")))?;
    let address = listener
        .local_addr()
        .map_err(|error| Status::internal(format!("read import fixture address: {error}")))?;
    let incoming = TcpListenerStream::new(listener);
    // Import into a fresh production service. Reusing the exporting service
    // would correctly reject the already-created volume authority and would
    // turn this positive transfer conformance case into a duplicate-create
    // probe. The destination derives the named workspace identity from the
    // manifest and creates its authority only after authenticating the full
    // imported closure.
    let import_service = empty_filesystem_service()
        .map_err(|error| Status::internal(format!("empty import fixture: {error}")))?;
    let server = acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer::new(import_service);
    let server_task: JoinHandle<Result<(), tonic::transport::Error>> = tokio::spawn(async move {
        Server::builder()
            .add_service(server)
            .serve_with_incoming(incoming)
            .await
    });
    // Let the spawned tonic server register its accept loop before the
    // generated client performs its first connection attempt. Without this
    // handoff, a fast local runtime can race the listener and turn a valid
    // import into a transport error.
    tokio::task::yield_now().await;
    let mut client = None;
    let mut last_connect_error = None;
    for _ in 0..200 {
        match acyclic_fs::wire::filesystem::v2::filesystem_service_client::FilesystemServiceClient::connect(
            format!("http://{address}"),
        )
        .await
        {
            Ok(value) => {
                client = Some(value);
                break;
            }
            Err(error) => {
                last_connect_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
    let mut client = client.ok_or_else(|| {
        Status::internal(format!(
            "connect import fixture: {}",
            last_connect_error
                .map(|error| format!("{error:?}"))
                .map(|error| format!("{error}; server_finished={}", server_task.is_finished()))
                .unwrap_or_else(|| format!("server did not start; server_finished={}", server_task.is_finished()))
        ))
    })?;
    let import_result = client.import(iter(import_chunks.clone())).await;
    let invalid_chunk = fs_wire::ImportChunk {
        workspace: Some(fixture_ref.clone()),
        operation_id: vec![0x45; 16],
        cursor: Vec::new(),
        object_id: b"invalid-object".to_vec(),
        contents: b"invalid".to_vec(),
        terminal: true,
    };
    let invalid_request_bytes = invalid_chunk.encode_to_vec();
    let negative_result = client.import(iter(vec![invalid_chunk])).await;
    server_task.abort();
    let negative_probe = match negative_result {
        Ok(response) => {
            let bytes = response.into_inner().encode_to_vec();
            json!({
                "response": {
                    "type": "acyclic.filesystem.v2.ImportResponse",
                    "bytes_base64": b64(&bytes),
                    "sha256": digest(&bytes),
                }
            })
        }
        Err(error) => json!({
            "request": {
                "type": "acyclic.filesystem.v2.ImportChunk",
                "bytes_base64": b64(&invalid_request_bytes),
                "sha256": digest(&invalid_request_bytes),
            },
            "response": {
                "status": error.code().to_string(),
                "code": format!("{:?}", error.code()),
                "message": error.message()
            }
        }),
    };
    match import_result {
        Ok(response) => {
            let response = response.into_inner();
            output.push(json!({
                "family": "filesystem",
                "operation": "Import",
                "rpc": "acyclic.filesystem.v2.FilesystemService/Import",
                "source": SOURCE,
                "request": {
                    "type": "acyclic.filesystem.v2.ImportChunk",
                    "bytes_base64": b64(&import_request_bytes),
                    "sha256": digest(&import_request_bytes),
                    "chunks": import_request_chunks,
                },
                "request_frames": import_request_chunks,
                "response": {
                    "type": "acyclic.filesystem.v2.ImportResponse",
                    "bytes_base64": b64(&response.encode_to_vec()),
                    "sha256": digest(&response.encode_to_vec()),
                },
                "negative_probe": negative_probe,
                "state": state,
            }));
        }
        Err(error) => output.push(json!({
            "family": "filesystem",
            "operation": "Import",
            "rpc": "acyclic.filesystem.v2.FilesystemService/Import",
            "source": SOURCE,
            "request": {
                "type": "acyclic.filesystem.v2.ImportChunk",
                "bytes_base64": b64(&import_request_bytes),
                "sha256": digest(&import_request_bytes),
                "chunks": import_request_chunks,
            },
            "request_frames": import_request_chunks,
            "response": {
                "status": error.code().to_string(),
                "code": format!("{:?}", error.code()),
                "message": error.message()
            },
            "negative_probe": negative_probe,
            "state": state,
        })),
    }
    record_rpc!(
        issue_mount_credential,
        "IssueMountCredential",
        fs_wire::CredentialRequest {
            workspace: Some(fixture_ref.clone()),
            generation: Some(fixture_head.clone()),
            writable: false,
            expires_after_seconds: 60,
            operation: Some(fs_wire::OperationOptions {
                idempotency_key: vec![0x38; 16],
            }),
        },
        "acyclic.filesystem.v2.CredentialRequest",
        "acyclic.filesystem.v2.CredentialResponse"
    );
    record_rpc!(
        issue_s3_credential,
        "IssueS3Credential",
        fs_wire::CredentialRequest {
            workspace: Some(fixture_ref.clone()),
            generation: Some(fixture_head.clone()),
            writable: false,
            expires_after_seconds: 60,
            operation: Some(fs_wire::OperationOptions {
                idempotency_key: vec![0x39; 16],
            }),
        },
        "acyclic.filesystem.v2.CredentialRequest",
        "acyclic.filesystem.v2.CredentialResponse"
    );
    record_rpc!(
        get_source_state,
        "GetSourceState",
        fs_wire::SourceStateRequest {
            workspace: Some(fixture_ref.clone()),
        },
        "acyclic.filesystem.v2.SourceStateRequest",
        "acyclic.filesystem.v2.SourceResponse"
    );
    let source_operation = |key: u8| fs_wire::SourceOperationRequest {
        workspace: Some(fixture_ref.clone()),
        operation: Some(fs_wire::OperationOptions {
            idempotency_key: vec![key; 16],
        }),
    };
    record_rpc!(
        reconcile_source,
        "ReconcileSource",
        source_operation(0x3a),
        "acyclic.filesystem.v2.SourceOperationRequest",
        "acyclic.filesystem.v2.SourceResponse"
    );
    record_rpc!(
        rescan_source,
        "RescanSource",
        source_operation(0x3b),
        "acyclic.filesystem.v2.SourceOperationRequest",
        "acyclic.filesystem.v2.SourceResponse"
    );
    record_rpc!(
        seal_source,
        "SealSource",
        source_operation(0x3c),
        "acyclic.filesystem.v2.SourceOperationRequest",
        "acyclic.filesystem.v2.SourceResponse"
    );
    let unknown_operation = vec![0x55; 16];
    record_rpc!(
        observe,
        "Observe",
        fs_wire::ObserveRequest {
            workspace: Some(fixture_ref.clone()),
            operation_id: unknown_operation.clone(),
        },
        "acyclic.filesystem.v2.ObserveRequest",
        "acyclic.filesystem.v2.ObserveResponse"
    );
    record_rpc!(
        cancel,
        "Cancel",
        fs_wire::CancelRequest {
            workspace: Some(fixture_ref),
            operation_id: unknown_operation,
        },
        "acyclic.filesystem.v2.CancelRequest",
        "acyclic.filesystem.v2.CancelResponse"
    );
    record_rpc!(
        delete_workspace,
        "DeleteWorkspace",
        fs_wire::DeleteWorkspaceRequest {
            workspace: Some(created_ref),
            operation: Some(fs_wire::OperationOptions {
                idempotency_key: vec![0x3d; 16],
            }),
        },
        "acyclic.filesystem.v2.DeleteWorkspaceRequest",
        "acyclic.filesystem.v2.MutationResponse"
    );

    Ok(output)
}

async fn export_harness() -> Result<Vec<Value>, acyclic_harness::Error> {
    let backend = HarnessFixtureBackend::new();
    let mut output = Vec::new();
    let mut state = BTreeMap::new();
    let owner = harness_wire::Authority {
        kind: harness_wire::AggregateKind::Task as i32,
        id: "fixture".into(),
    };
    let protocol = current_protocol();
    let operation = harness_wire::OperationIdentity {
        operation_id: "00000000-0000-0000-0000-000000000001".into(),
        idempotency_key: "fixture-key".into(),
    };

    let handshake = harness_wire::HandshakeRequest {
        protocol: Some(protocol.clone()),
        ..Default::default()
    };
    let response = backend.handshake(handshake.clone()).await?;
    output.push(evidence(
        "harness",
        "Handshake",
        "acyclic.protocol.v1.HandshakeRequest",
        &handshake,
        "acyclic.protocol.v1.HandshakeResponse",
        &response,
        &state,
    ));

    let command = harness_wire::CommandEnvelope {
        protocol: Some(protocol.clone()),
        authority: Some(owner.clone()),
        operation: Some(operation.clone()),
        action_type: "fixture.submit".into(),
        canonical_action_json: br#"{"fixture":true}"#.to_vec(),
        intent_digest: vec![0x23; 32],
        ..Default::default()
    };
    let admission = backend.submit(command.clone()).await?;
    state.insert("operation_id", operation.operation_id.clone());
    state.insert("authority", format!("task:{}", owner.id));
    output.push(evidence(
        "harness",
        "Submit",
        "acyclic.harness.v2.CommandEnvelope",
        &command,
        "acyclic.harness.v2.Admission",
        &admission,
        &state,
    ));

    let replay = harness_wire::ResumeRequest {
        protocol: Some(protocol.clone()),
        cursors: vec![harness_wire::ReplayCursor {
            authority: Some(owner.clone()),
            generation: "fixture-generation".into(),
            revision: 0,
        }],
    };
    let mut deliveries = backend.replay(replay.clone()).await?;
    let delivery = deliveries
        .next()
        .await
        .transpose()?
        .ok_or_else(|| acyclic_harness::Error::Storage("fixture replay was empty".into()))?;
    output.push(evidence(
        "harness",
        "Replay",
        "acyclic.harness.v2.ResumeRequest",
        &replay,
        "acyclic.harness.v2.Delivery",
        &delivery,
        &state,
    ));

    let observe = harness_wire::ObserveRequest {
        operation_id: operation.operation_id.clone(),
        protocol: Some(protocol.clone()),
        owner: Some(owner.clone()),
        scope: Some(harness_wire::Scope {
            id: "fixture-control".into(),
            capabilities: vec!["operation:observe".into(), "operation:cancel".into()],
            issuer: "fixture".into(),
            proof: vec![0; 32],
            ..Default::default()
        }),
        ..Default::default()
    };
    let observed = backend.observe(observe.clone()).await?;
    state.insert("observed_state_code", observed.state.to_string());
    state.insert("observed_revision", observed.revision.to_string());
    output.push(evidence(
        "harness",
        "Observe",
        "acyclic.harness.v2.ObserveRequest",
        &observe,
        "acyclic.harness.v2.OperationStatus",
        &observed,
        &state,
    ));

    let cancel = harness_wire::CancelRequest {
        operation_id: operation.operation_id,
        protocol: Some(protocol),
        owner: Some(owner),
        scope: Some(harness_wire::Scope {
            id: "fixture-control".into(),
            capabilities: vec!["operation:observe".into(), "operation:cancel".into()],
            issuer: "fixture".into(),
            proof: vec![0; 32],
            ..Default::default()
        }),
        idempotency_key: "fixture-cancel".into(),
        ..Default::default()
    };
    let cancelled = backend.cancel(cancel.clone()).await?;
    if let Some(status) = cancelled.status.as_ref() {
        state.insert("cancelled_state_code", status.state.to_string());
        state.insert("cancelled_revision", status.revision.to_string());
    }
    output.push(evidence(
        "harness",
        "Cancel",
        "acyclic.harness.v2.CancelRequest",
        &cancel,
        "acyclic.harness.v2.CancelResponse",
        &cancelled,
        &state,
    ));
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::export;

    #[tokio::test(flavor = "current_thread")]
    async fn export_executes_all_typed_filesystem_and_harness_steps() {
        let records = export().await.expect("typed fixture export");
        let repeated = export().await.expect("repeat typed fixture export");
        assert_eq!(records, repeated, "fresh exports must be byte-identical");
        assert_eq!(records.len(), 35);

        let operations: Vec<&str> = records
            .iter()
            .map(|record| record["operation"].as_str().expect("operation"))
            .collect();
        assert_eq!(
            operations,
            [
                "Handshake",
                "CreateWorkspace",
                "OpenWorkspace",
                "GetHead",
                "GetGeneration",
                "Read",
                "Stat",
                "ListDirectory",
                "ReadLink",
                "PlanExtents",
                "ApplyTransaction",
                "RebaseTransaction",
                "ForkWorkspace",
                "Diff",
                "Rebase",
                "PlanJoin",
                "ApplyJoin",
                "Checkpoint",
                "Pin",
                "Export",
                "Import",
                "IssueMountCredential",
                "IssueS3Credential",
                "GetSourceState",
                "ReconcileSource",
                "RescanSource",
                "SealSource",
                "Observe",
                "Cancel",
                "DeleteWorkspace",
                "Handshake",
                "Submit",
                "Replay",
                "Observe",
                "Cancel",
            ]
        );

        for record in &records {
            assert_eq!(record["source"], super::SOURCE);
            let request = record["request"].as_object().expect("typed request");
            assert!(!request["type"].as_str().unwrap_or_default().is_empty());
            assert!(request["bytes_base64"].as_str().is_some());
            assert!(request["sha256"].as_str().unwrap_or_default().starts_with("sha256:"));

            let response = record["response"].as_object().expect("response evidence");
            if let Some(bytes) = response.get("bytes_base64") {
                assert!(bytes.as_str().is_some());
                assert!(response["sha256"].as_str().unwrap_or_default().starts_with("sha256:"));
            } else if response.get("chunks").is_none() {
                assert!(!response["status"].as_str().unwrap_or_default().is_empty());
                assert!(!response["message"].as_str().unwrap_or_default().is_empty());
            }
            assert!(record["state"].is_object());
        }

        let export = records
            .iter()
            .find(|record| record["family"] == "filesystem" && record["operation"] == "Export")
            .expect("filesystem export evidence");
        let chunks = export["response"]["chunks"]
            .as_array()
            .expect("export chunks");
        assert!(!chunks.is_empty());
        assert!(chunks.iter().any(|chunk| chunk["terminal"] == true));
        for chunk in chunks {
            assert!(!chunk["bytes_base64"].as_str().unwrap_or_default().is_empty());
            assert!(chunk["sha256"].as_str().unwrap_or_default().starts_with("sha256:"));
        }

        let import = records
            .iter()
            .find(|record| record["family"] == "filesystem" && record["operation"] == "Import")
            .expect("filesystem import evidence");
        // A successful empty protobuf response encodes to an empty byte string.
        // Require the typed field and its digest instead of rejecting that valid wire value.
        assert!(import["response"]["bytes_base64"].as_str().is_some());
        assert!(import["response"]["sha256"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:")));
        assert_eq!(import["negative_probe"]["response"]["code"], "InvalidArgument");

        let delete = records
            .iter()
            .find(|record| record["operation"] == "DeleteWorkspace")
            .expect("filesystem delete evidence");
        for key in [
            "created_workspace_id",
            "fixture_workspace_id",
            "fixture_generation_id",
            "mutated_generation_id",
        ] {
            assert!(delete["state"][key]
                .as_str()
                .is_some_and(|value| !value.is_empty()));
        }

        let read_link = records
            .iter()
            .find(|record| record["operation"] == "ReadLink")
            .expect("filesystem read-link evidence");
        assert_eq!(
            read_link["response"]["type"],
            "acyclic.filesystem.v2.ReadResponse"
        );
        assert!(read_link["response"]["bytes_base64"]
            .as_str()
            .is_some_and(|bytes| !bytes.is_empty()));
        assert!(read_link["response"]["sha256"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:")));
        for operation in ["Observe", "Cancel"] {
            let response = records
                .iter()
                .find(|record| record["operation"] == operation)
                .expect("expected filesystem operation response")["response"]
                .clone();
            assert!(response["bytes_base64"]
                .as_str()
                .is_some_and(|bytes| !bytes.is_empty()));
            assert!(!response["type"]
                .as_str()
                .unwrap_or_default()
                .is_empty());
        }

        let harness_cancel = records
            .iter()
            .rev()
            .find(|record| record["family"] == "harness" && record["operation"] == "Cancel")
            .expect("harness cancel evidence");
        assert_eq!(
            harness_cancel["state"]["operation_id"],
            "00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(harness_cancel["state"]["authority"], "task:fixture");
        assert_eq!(
            harness_cancel["state"]["observed_state_code"],
            (acyclic_harness::wire::CompletionState::Succeeded as i32).to_string()
        );
        assert_eq!(harness_cancel["state"]["observed_revision"], "1");
        assert_eq!(
            harness_cancel["state"]["cancelled_state_code"],
            (acyclic_harness::wire::CompletionState::Cancelled as i32).to_string()
        );
        assert_eq!(harness_cancel["state"]["cancelled_revision"], "2");
    }
}
