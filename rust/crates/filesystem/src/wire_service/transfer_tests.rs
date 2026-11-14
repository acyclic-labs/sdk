use super::*;
use acyclic_fs_wire::wire::filesystem::v2::filesystem_service_server::FilesystemService as _;

#[tokio::test]
async fn admitted_import_prefix_preserves_closure_and_rejects_changed_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let source = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let created = source
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "transfer".into(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: operation(40),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("workspace missing")?;
    let workspace = created.workspace.ok_or("workspace reference missing")?;
    let committed = source
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: created.head,
            mutations: vec![mutation(wire::mutation::Mutation::CreateFile(
                wire::CreateFile {
                    path: "/value".into(),
                    contents: b"retained bytes".to_vec(),
                    metadata: None,
                },
            ))],
            operation: operation(41),
            maximum_conflicts: None,
        }))
        .await?
        .into_inner();
    let generation = committed.generation.ok_or("generation missing")?;
    let exported = source
        .export(Request::new(wire::ExportRequest {
            generation: Some(generation.clone()),
            after: Vec::new(),
            maximum_objects: 64,
            maximum_bytes: 1024 * 1024,
        }))
        .await?
        .into_inner();
    let chunks: Vec<wire::ImportChunk> = exported
        .map(|chunk| {
            chunk.map(|chunk| wire::ImportChunk {
                workspace: Some(workspace.clone()),
                operation_id: operation_bytes(42),
                cursor: chunk.cursor,
                object_id: chunk.object_id,
                contents: chunk.contents,
                terminal: chunk.terminal,
            })
        })
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    let target = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let mut incoming = stream::iter(chunks.clone().into_iter().map(Ok));
    // Deployment admission consumes the original first message, then hands the
    // exact same message and unconsumed stream to the shared import owner.
    let first = incoming.next().await.ok_or("manifest missing")??;
    assert_eq!(first.workspace.as_ref(), Some(&workspace));
    let imported = target
        .import_stream(stream::once(std::future::ready(Ok(first))).chain(incoming))
        .await?
        .into_inner()
        .outcome
        .ok_or("import outcome missing")?;
    assert_eq!(imported.generation.as_ref(), Some(&generation));
    let read = target
        .read(Request::new(wire::ReadRequest {
            generation: Some(generation),
            path: "/value".into(),
            range: None,
            maximum_bytes: 14,
        }))
        .await?
        .into_inner();
    assert_eq!(read.contents, b"retained bytes");
    let mut changed = chunks;
    changed
        .get_mut(1)
        .ok_or("object chunk missing")?
        .operation_id = operation_bytes(43);
    let refused = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?
        .import_stream(stream::iter(changed.into_iter().map(Ok)))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    Ok(())
}
