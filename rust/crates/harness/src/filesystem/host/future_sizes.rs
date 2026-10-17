use super::*;
use crate::Capabilities;
use crate::core::{AggregateKind, AuthorityIssuer};

#[tokio::test]
#[cfg(feature = "filesystem-local")]
async fn local_content_publication_futures_stay_bounded() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs = Fs::local(acyclic_fs::LocalOptions::new(directory.path()))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    Box::pin(content_publication_futures_stay_bounded(fs)).await
}

#[tokio::test]
async fn memory_content_publication_futures_stay_bounded() -> Result<()> {
    Box::pin(content_publication_futures_stay_bounded(Fs::memory())).await
}

async fn content_publication_futures_stay_bounded<
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
>(
    fs: Fs<A, O>,
) -> Result<()> {
    let provider = ProviderRef::new("sizes".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(fs, &provider, None)?;
    let owner = AgentId::from_bytes([32; 16]);
    let volume = VolumeRef::new(
        provider,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "sizes",
        [31; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "owner".into(),
        },
    );
    let scope = issuer.issue(
        "write",
        &Capabilities::new([
            volume.capability(VolumeOperation::Write, &mut None)?,
            volume.capability(VolumeOperation::Read, &mut None)?,
        ]),
        Some(owner),
        None,
        &mut None,
    )?;
    let grant = ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        &volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let key = IdempotencyKey::new("sizes")?;
    let publication = host.put_content(
        &volume,
        &grant,
        "file.txt",
        b"content",
        "text/plain",
        "file.txt",
        4096,
        &key,
        None,
        None,
    );
    let bytes = std::mem::size_of_val(&publication);
    eprintln!("local content publication future: {bytes} bytes");
    assert!(bytes < 4096, "publication future is {bytes} bytes");
    let file = publication.await?;
    let workspace = workspace_ref(volume.provider().clone(), &volume.storage_name(&mut None)?)?;
    let mutations = [];
    let apply = host.apply(&workspace, None, &mutations, &key);
    let size = std::mem::size_of_val(&apply);
    eprintln!("filesystem apply future: {size} bytes");
    assert!(
        size < 16 * 1024,
        "filesystem apply future grew to {size} bytes"
    );
    drop(apply);
    let read = host.read(&workspace, None, "/file.txt", 4096, None);
    let pinned = host.read_pinned_io(&file, 4096, None);
    let sizes = [std::mem::size_of_val(&read), std::mem::size_of_val(&pinned)];
    eprintln!("local content read/pinned future sizes: {sizes:?}");
    assert!(
        sizes.iter().all(|size| *size < 16 * 1024),
        "read/pinned future sizes: {sizes:?}"
    );
    drop((read, pinned));
    assert_eq!(
        host.read_pinned_io(&file, 4096, None).await?.as_ref(),
        b"content"
    );
    let journal = crate::filesystem::FilesystemWorkflowJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        "sizes",
        crate::filesystem::FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            maximum_bytes: 4096,
            io: None,
        },
    )?;
    let admission = crate::workflow::WorkflowAdmission {
        operation_id: crate::OperationId::from_bytes([1; 16]),
        request_digest: [2; 32],
        initial: crate::workflow::MachineCheckpoint {
            machine: crate::workflow::MachineIdentity {
                name: "sizes".into(),
                version: "1".into(),
                digest: [3; 32],
            },
            revision: 0,
            state: serde_json::Value::Null,
        },
    };
    let admit = journal.admit_with_io(admission, None);
    let read = journal.read_admission(None);
    let sizes = [std::mem::size_of_val(&admit), std::mem::size_of_val(&read)];
    eprintln!("workflow admission/read futures: {sizes:?} bytes");
    assert!(sizes.iter().all(|size| *size < 4096));
    Ok(())
}
