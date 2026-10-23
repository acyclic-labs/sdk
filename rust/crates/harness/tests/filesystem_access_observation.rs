use super::*;

async fn publish_during_observation(
    fixture: &Fixture<objects::CountingObjects>,
    revoke: bool,
) -> Result<(), Box<dyn Error>> {
    if revoke {
        fixture
            .publish_policy(revoked_policy("observation-revocation", Action::Read)?)
            .await?;
    } else {
        let workspace = acyclic_harness::filesystem::workspace_ref(
            fixture.volume.provider().clone(),
            &fixture.volume.storage_name(&mut None)?,
        )?;
        fixture
            .host
            .apply(
                &workspace,
                None,
                &[acyclic_harness::filesystem::WorkspaceMutation::PutFile {
                    path: "/unrelated".into(),
                    bytes: bytes::Bytes::from_static(b"other").into(),
                }],
                &IdempotencyKey::new("unrelated-publication")?,
            )
            .await?;
    }
    Ok(())
}

async fn racing_observation(revoke: bool, range: bool) -> Result<(), Box<dyn Error>> {
    let objects = objects::CountingObjects::default();
    let fs = Fs::new(
        acyclic_fs::StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default())),
        objects.clone(),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let fixture = Fixture::with_filesystem(fs, true).await?;
    let contents = objects::blob_text(false);
    let file = fixture
        .host
        .put_content(
            &fixture.volume,
            &copies::legacy(&fixture, &fixture.volume, VolumeOperation::Write)?,
            "secrets/blob",
            contents.as_bytes(),
            "text/plain",
            "blob",
            65536,
            &IdempotencyKey::new("observation-blob")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let reader = fixture.reader(&fixture.scope)?;
    let grant = copies::legacy(&fixture, &fixture.volume, VolumeOperation::Read)?;
    let reads_before = objects
        .content_reads
        .load(std::sync::atomic::Ordering::SeqCst);
    let (entered, release) = objects.pause(acyclic_fs::ObjectKind::BlobChunk)?;
    let read = async {
        if range {
            reader
                .read_range(
                    &file,
                    acyclic_fs::ByteRange {
                        offset: 0,
                        length: contents.len() as u64,
                    },
                    None,
                    None,
                )?
                .await
        } else {
            fixture.host.read_content(&file, &grant, 65536, None).await
        }
    };
    let publication = async {
        entered.await?;
        // Stop the return fence after it captured Head, then overtake that
        // observation. The data has already been read and must not be reread.
        let (at_policy, continue_policy) = objects.pause(acyclic_fs::ObjectKind::Metadata)?;
        release
            .send(())
            .map_err(|_| "data read abandoned its gate")?;
        at_policy.await?;
        publish_during_observation(&fixture, revoke).await?;
        continue_policy
            .send(())
            .map_err(|_| "policy observation abandoned its gate")?;
        Ok::<_, Box<dyn Error>>(())
    };
    let (result, published) = futures::join!(read, publication);
    published?;
    if revoke {
        assert!(
            matches!(result, Err(error) if error.code() == acyclic_harness::wire::ErrorCode::Unauthorized)
        );
    } else {
        assert_eq!(result?.as_ref(), contents.as_bytes());
    }
    assert_eq!(
        objects
            .content_reads
            .load(std::sync::atomic::Ordering::SeqCst)
            - reads_before,
        1,
        "policy observation retried the file's data read"
    );
    Ok(())
}

#[tokio::test]
async fn return_observation_retries_unrelated_data_but_denies_revocation()
-> Result<(), Box<dyn Error>> {
    for range in [false, true] {
        for revoke in [false, true] {
            racing_observation(revoke, range).await?;
        }
    }
    Ok(())
}
