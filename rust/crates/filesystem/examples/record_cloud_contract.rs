//! Records the canonical wire engine's responses to Cloud's fs handler-test inputs.

#[path = "../tests/support/cloud_contract.rs"]
mod contract;

use acyclic_fs::wire::filesystem::v2::{
    self as wire, filesystem_service_server::FilesystemService,
};
use acyclic_fs::{FilesystemWireLimits, FilesystemWireService, MemoryFs};
use serde_json::{Value, json};
use tonic::Request;

fn operation(value: u8) -> Option<wire::OperationOptions> {
    Some(wire::OperationOptions {
        idempotency_key: vec![value; 16],
    })
}

fn record<Q: prost::Message, R: prost::Message>(
    cases: &mut Vec<Value>,
    method: &str,
    input: &str,
    output: &str,
    request: &Q,
    response: &R,
) -> Result<(), Box<dyn std::error::Error>> {
    cases.push(json!({
        "method": method, "input": input, "output": output,
        "request": contract::json(input, request)?, "response": contract::json(output, response)?,
    }));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(MemoryFs::memory(), FilesystemWireLimits::default())?;
    let mut cases = Vec::new();
    let request = wire::HandshakeRequest::default();
    let response = service
        .handshake(Request::new(request.clone()))
        .await?
        .into_inner();
    record(
        &mut cases,
        "Handshake",
        "HandshakeRequest",
        "HandshakeResponse",
        &request,
        &response,
    )?;
    let request = wire::CreateWorkspaceRequest {
        name: "same-name".into(),
        profile: wire::FilesystemProfile::Portable as i32,
        operation: operation(1),
    };
    let response = service
        .create_workspace(Request::new(request.clone()))
        .await?
        .into_inner();
    record(
        &mut cases,
        "CreateWorkspace",
        "CreateWorkspaceRequest",
        "WorkspaceResponse",
        &request,
        &response,
    )?;
    let workspace = response.workspace.ok_or("workspace missing")?;
    let reference = workspace.workspace.ok_or("workspace reference missing")?;
    let request = wire::ApplyTransactionRequest {
        base: workspace.head,
        mutations: vec![wire::Mutation {
            mutation: Some(wire::mutation::Mutation::CreateFile(wire::CreateFile {
                path: "/value".into(),
                contents: b"probe".to_vec(),
                metadata: None,
            })),
        }],
        operation: operation(2),
        maximum_conflicts: 16,
    };
    let response = service
        .apply_transaction(Request::new(request.clone()))
        .await?
        .into_inner();
    assert_eq!(response.status, wire::MutationStatus::Committed as i32);
    record(
        &mut cases,
        "ApplyTransaction",
        "ApplyTransactionRequest",
        "MutationResponse",
        &request,
        &response,
    )?;
    let generation = response.generation.ok_or("generation missing")?;
    let read = read_and_list(&service, &mut cases, &reference, &generation).await?;
    remove_and_delete(&service, &mut cases, reference, generation, read).await?;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../test-fixtures/cloud-filesystem/golden.json");
    std::fs::write(path, format!("{}\n", serde_json::to_string_pretty(&cases)?))?;
    Ok(())
}

async fn read_and_list(
    service: &impl FilesystemService,
    cases: &mut Vec<Value>,
    reference: &wire::WorkspaceRef,
    generation: &wire::GenerationRef,
) -> Result<wire::ReadRequest, Box<dyn std::error::Error>> {
    let request = wire::GetHeadRequest {
        workspace: Some(reference.clone()),
    };
    let response = service
        .get_head(Request::new(request.clone()))
        .await?
        .into_inner();
    record(
        cases,
        "GetHead",
        "GetHeadRequest",
        "GenerationResponse",
        &request,
        &response,
    )?;
    let read = wire::ReadRequest {
        generation: Some(generation.clone()),
        path: "/value".into(),
        range: None,
        maximum_bytes: 64,
    };
    let response = service.read(Request::new(read.clone())).await?.into_inner();
    assert_eq!(response.contents, b"probe");
    record(
        cases,
        "Read",
        "ReadRequest",
        "ReadResponse",
        &read,
        &response,
    )?;
    let request = wire::ListDirectoryRequest {
        generation: Some(generation.clone()),
        path: "/".into(),
        page: Some(wire::PageOptions {
            maximum_items: 16,
            after: None,
        }),
    };
    let response = service
        .list_directory(Request::new(request.clone()))
        .await?
        .into_inner();
    assert_eq!(
        response.page.as_ref().ok_or("page missing")?.entries.len(),
        1
    );
    record(
        cases,
        "ListDirectory",
        "ListDirectoryRequest",
        "ListDirectoryResponse",
        &request,
        &response,
    )?;
    Ok(read)
}

async fn remove_and_delete(
    service: &impl FilesystemService,
    cases: &mut Vec<Value>,
    reference: wire::WorkspaceRef,
    generation: wire::GenerationRef,
    read: wire::ReadRequest,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = wire::ApplyTransactionRequest {
        base: Some(generation),
        mutations: vec![wire::Mutation {
            mutation: Some(wire::mutation::Mutation::Remove(wire::Remove {
                path: "/value".into(),
            })),
        }],
        operation: operation(3),
        maximum_conflicts: 16,
    };
    let response = service
        .apply_transaction(Request::new(request.clone()))
        .await?
        .into_inner();
    assert_eq!(response.status, wire::MutationStatus::Committed as i32);
    record(
        cases,
        "ApplyTransaction",
        "ApplyTransactionRequest",
        "MutationResponse",
        &request,
        &response,
    )?;
    let request = wire::ListDirectoryRequest {
        generation: response.generation,
        path: "/".into(),
        page: Some(wire::PageOptions {
            maximum_items: 16,
            after: None,
        }),
    };
    let response = service
        .list_directory(Request::new(request.clone()))
        .await?
        .into_inner();
    assert!(
        response
            .page
            .as_ref()
            .ok_or("page missing")?
            .entries
            .is_empty()
    );
    record(
        cases,
        "ListDirectory",
        "ListDirectoryRequest",
        "ListDirectoryResponse",
        &request,
        &response,
    )?;
    let response = service.read(Request::new(read.clone())).await?.into_inner();
    assert_eq!(response.contents, b"probe");
    record(
        cases,
        "Read",
        "ReadRequest",
        "ReadResponse",
        &read,
        &response,
    )?;
    let request = wire::DeleteWorkspaceRequest {
        workspace: Some(reference),
        operation: operation(4),
    };
    let response = service
        .delete_workspace(Request::new(request.clone()))
        .await?
        .into_inner();
    assert_eq!(response.status, wire::MutationStatus::Committed as i32);
    record(
        cases,
        "DeleteWorkspace",
        "DeleteWorkspaceRequest",
        "MutationResponse",
        &request,
        &response,
    )?;
    Ok(())
}
