#![cfg(feature = "filesystem-local")]

use super::*;

const CONTENT_WORK_BUDGET: acyclic_fs::WorkBudget =
    acyclic_fs::performance::work_budget(100_000, 16_000_000);

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "production-backed lost delivery, reopen, immutable retry and adversarial bounds"
)]
async fn measured_upload_recovers_original_operation_not_mutable_head_after_lost_ack() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let options = acyclic_fs::LocalOptions::new(directory.path().join("bounded-content"));
    let provider = ProviderRef::new(
        "bounded-content".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let owner = AgentId::from_bytes([91; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "content".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "bounded-content",
        [92; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "bounded-content".into(),
        },
    );
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(owner),
            None,
            &mut None,
        )
        .expect("scope proof");
    let read = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Read,
        u64::MAX,
        &mut None,
    )?;
    let write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let key = IdempotencyKey::new("original-content")?;
    let cancellation = acyclic_fs::CancellationToken::new();
    {
        let fs = Fs::local(options.clone())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let host = FilesystemHost::new(fs, &provider, None)?;
        host.create_volume(&volume).await?;
        let io = acyclic_harness::filesystem::PreparationIo::from_budget(
            CONTENT_WORK_BUDGET,
            &cancellation,
        )?;
        let delivered = host
            .put_content(
                &volume,
                &write,
                "result.json",
                b"original",
                "application/json",
                "original.json",
                64,
                &key,
                None,
                Some(&io),
            )
            .await
            .and_then(|_| Err::<(), _>(Error::Storage("lost content acknowledgement".into())));
        assert!(delivered.is_err());
        let io = acyclic_harness::filesystem::PreparationIo::from_budget(
            CONTENT_WORK_BUDGET,
            &cancellation,
        )?;
        host.put_content(
            &volume,
            &write,
            "result.json",
            b"later",
            "text/plain",
            "later.txt",
            64,
            &IdempotencyKey::new("later-content")?,
            None,
            Some(&io),
        )
        .await?;
    }
    let fs = Fs::local(options)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let host = FilesystemHost::new(fs, &provider, None)?;
    let io = acyclic_harness::filesystem::PreparationIo::from_budget(
        CONTENT_WORK_BUDGET,
        &cancellation,
    )?;
    let recovered = host
        .content_for_operation(&volume, &write, &key, "result.json", 64, Some(&io))
        .await?;

    let original =
        recovered.ok_or_else(|| Error::Storage("committed original upload was absent".into()))?;
    assert_eq!(original.display_name(), "original.json");
    assert_eq!(original.descriptor().media_type(), "application/json");
    let io = acyclic_harness::filesystem::PreparationIo::from_budget(
        CONTENT_WORK_BUDGET,
        &cancellation,
    )?;
    assert_eq!(
        host.read_content(&original, &read, 64, Some(&io))
            .await?
            .as_ref(),
        b"original"
    );
    let io = acyclic_harness::filesystem::PreparationIo::from_budget(
        CONTENT_WORK_BUDGET,
        &cancellation,
    )?;
    assert_eq!(
        host.put_content(
            &volume,
            &write,
            "result.json",
            b"original",
            "application/json",
            "original.json",
            64,
            &key,
            None,
            Some(&io)
        )
        .await?,
        original
    );
    for (path, media, display, bytes) in [
        (
            "changed.json",
            "application/json",
            "original.json",
            b"original".as_slice(),
        ),
        (
            "result.json",
            "text/plain",
            "original.json",
            b"original".as_slice(),
        ),
        (
            "result.json",
            "application/json",
            "changed.json",
            b"original".as_slice(),
        ),
        (
            "result.json",
            "application/json",
            "original.json",
            b"changed".as_slice(),
        ),
    ] {
        let io = acyclic_harness::filesystem::PreparationIo::from_budget(
            CONTENT_WORK_BUDGET,
            &cancellation,
        )?;
        assert!(
            host.put_content(
                &volume,
                &write,
                path,
                bytes,
                media,
                display,
                64,
                &key,
                None,
                Some(&io)
            )
            .await
            .is_err()
        );
    }
    let io = acyclic_harness::filesystem::PreparationIo::from_budget(
        CONTENT_WORK_BUDGET,
        &cancellation,
    )?;
    let missing = host
        .content_for_operation(
            &volume,
            &write,
            &IdempotencyKey::new("never-uploaded")?,
            "result.json",
            64,
            Some(&io),
        )
        .await?;
    assert_eq!(missing, None);
    assert!(
        host.content_for_operation(&volume, &write, &key, "changed.json", 64, Some(&io))
            .await
            .is_err()
    );
    let io = acyclic_harness::filesystem::PreparationIo::from_budget(
        CONTENT_WORK_BUDGET,
        &cancellation,
    )?;
    assert!(
        host.read_content(&original, &read, 1, Some(&io))
            .await
            .is_err()
    );
    let workspace = workspace_ref(provider.clone(), &volume.storage_name(&mut None)?)?;
    let head = host.resolve(&workspace, None).await?.generation;
    let cancelled = acyclic_fs::CancellationToken::new();
    let io =
        acyclic_harness::filesystem::PreparationIo::from_budget(CONTENT_WORK_BUDGET, &cancelled)?;
    cancelled.cancel();
    assert!(
        host.put_content(
            &volume,
            &write,
            "cancelled",
            b"never",
            "text/plain",
            "never",
            64,
            &IdempotencyKey::new("cancelled-content")?,
            None,
            Some(&io)
        )
        .await
        .is_err()
    );
    assert_eq!(host.resolve(&workspace, None).await?.generation, head);
    let mut denied = CONTENT_WORK_BUDGET;
    denied.peak_allocation_bytes = 0;
    let io = acyclic_harness::filesystem::PreparationIo::from_budget(denied, &cancellation)?;
    assert!(
        host.put_content(
            &volume,
            &write,
            "denied",
            b"never",
            "text/plain",
            "never",
            64,
            &IdempotencyKey::new("denied-content")?,
            None,
            Some(&io)
        )
        .await
        .is_err()
    );
    assert_eq!(host.resolve(&workspace, None).await?.generation, head);
    let io = acyclic_harness::filesystem::PreparationIo::from_budget(
        CONTENT_WORK_BUDGET,
        &cancellation,
    )?;
    assert_eq!(
        host.content_for_operation(
            &volume,
            &write,
            &IdempotencyKey::new("denied-content")?,
            "denied",
            64,
            Some(&io)
        )
        .await?,
        None
    );
    assert_eq!(
        host.read(&workspace, None, "/result.json", 64, None)
            .await?
            .as_ref(),
        b"later"
    );
    Ok(())
}
