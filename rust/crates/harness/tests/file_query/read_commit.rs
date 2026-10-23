use super::*;
use acyclic_fs::WorkBudget;
use acyclic_harness::conversation::{
    ContentGrant, ContentResidencyVerifier, FileDescriptor, FileRef,
};

#[tokio::test]
async fn every_reader_refuses_disclosure_when_policy_changes_during_storage()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    for kind in 0..5 {
        for revoke in [false, true] {
            let (objects, fixture) = counted_fixture().await?;
            let content = objects::blob_text(false);
            fixture.write(&[("plans/a.md", Some(&content))]).await?;
            let scope = fixture.issuer.issue(
                "read-race",
                &Capabilities::new([fixture
                    .volume
                    .capability(VolumeOperation::Read, &mut None)?]),
                Some(fixture.agent),
                None,
                &mut None,
            )?;
            let reader = FilesystemContentVerifier::new(
                fixture.host.clone(),
                &fixture.issuer.verifier(&mut None)?,
                &scope,
                65536,
                None,
            )?;
            let workspace = fixture
                .filesystem
                .open_workspace(
                    fixture.volume.storage_name(&mut None)?,
                    WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await?
                .value;
            let head = workspace
                .head(WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                .await?
                .value;
            let version = hex::encode(head.id().digest().into_bytes());
            let file = FileRef::new(
                fixture.volume.clone(),
                "plans/a.md".into(),
                version,
                FileDescriptor::from_bytes(content.as_bytes(), "text/plain", &mut None)?,
                "a.md".into(),
                &mut None,
            )?;
            let generation = acyclic_harness::resources::GenerationRef::new(
                fixture.volume.provider(),
                &head.id().digest().into_bytes(),
                None,
                None,
            )?
            .0;
            let grant = ContentGrant::verify_directory_read(
                &fixture.issuer.verifier(&mut None)?,
                &scope,
                &fixture.volume,
                "",
                &mut None,
            )?;
            let (entered, release) = objects.pause(if kind == 4 {
                acyclic_fs::ObjectKind::TreePage
            } else {
                acyclic_fs::ObjectKind::BlobChunk
            })?;
            let read = read_kind(kind, &reader, &fixture, &file, &grant, &generation);
            futures::pin_mut!(read);
            let result = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                let pending = match futures::future::select(read, entered).await {
                    futures::future::Either::Left((result, _)) => {
                        return Err(format!(
                            "reader {kind}, revoke={revoke} completed before storage: {result:?}"
                        )
                        .into());
                    }
                    futures::future::Either::Right((entered, pending)) => {
                        entered?;
                        pending
                    }
                };
                if revoke {
                    access::publish_policy(&fixture, kind != 4, kind == 4).await?;
                }
                release
                    .send(())
                    .map_err(|_| "reader abandoned the pending payload")?;
                Ok::<_, Box<dyn std::error::Error>>(pending.await)
            })
            .await??;
            if revoke {
                assert_eq!(
                    result.unwrap_err().code(),
                    acyclic_harness::wire::ErrorCode::Unauthorized,
                    "reader {kind}"
                );
            } else {
                result?;
            }
        }
    }
    Ok(())
}

async fn read_kind(
    kind: u8,
    reader: &FilesystemContentVerifier<MemoryAuthorityBackend, CountingObjects>,
    fixture: &Fixture<CountingObjects>,
    file: &FileRef,
    grant: &ContentGrant,
    generation: &acyclic_harness::resources::GenerationRef,
) -> Result<()> {
    match kind {
        0 => reader.read(file, 65536, None, None)?.await.map(|_| ()),
        1 => reader
            .read_range(
                file,
                acyclic_fs::ByteRange {
                    offset: 0,
                    length: 4,
                },
                None,
                None,
            )?
            .await
            .map(|_| ()),
        2 => fixture
            .host
            .read_private_path(
                &fixture.volume,
                grant,
                "plans/a.md",
                Some(generation),
                65536,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await
            .map(|_| ()),
        3 => fixture
            .host
            .read_private_prefix(
                &fixture.volume,
                grant,
                "plans/a.md",
                generation,
                4,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await
            .map(|_| ()),
        _ => fixture
            .host
            .list_private_directory(
                &fixture.volume,
                grant,
                "plans",
                acyclic_harness::filesystem::DirectoryOptions {
                    generation: Some(generation),
                    after: None,
                    maximum_entries: 8,
                    ceiling: None,
                },
            )
            .await
            .map(|_| ()),
    }
}

async fn counted_fixture()
-> std::result::Result<(CountingObjects, Fixture<CountingObjects>), Box<dyn std::error::Error>> {
    let objects = CountingObjects::default();
    let filesystem = Fs::new(
        acyclic_fs::StreamAuthorityStore::new(Arc::new(MemoryStream::default())),
        objects.clone(),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let fixture = Fixture::with_filesystem(filesystem, objects.reads.clone()).await?;
    Ok((objects, fixture))
}
