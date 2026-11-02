use super::*;
use crate::{AgentId, conversation::VolumeOwner, core::Authority};

#[tokio::test]
async fn reopening_volume_preserves_stored_configuration_and_head() -> Result<()> {
    use acyclic_fs::model::ConcurrencyMode;
    let provider = ProviderRef::new("reopen".into(), "filesystem".into(), "1".into(), &mut None)?;
    let filesystem = Fs::memory();
    let host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
    let volume = |name: &str| {
        VolumeRef::new(
            provider.clone(),
            name.into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([62; 16])),
            &mut None,
        )
    };
    let saved = volume("saved")?;
    let (name, operation) = host.volume_creation(&saved)?;
    let mut initialization = host.volume_initialization();
    initialization.config.concurrency = ConcurrencyMode::Optimistic;
    filesystem
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            &name,
            initialization,
            operation,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
            None,
        )
        .await
        .map_err(|failure| map_error(failure.error))?;
    let reference = workspace_ref(provider.clone(), &name)?;
    let head = host
        .apply(
            &reference,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/retained".into(),
                bytes: b"saved content".to_vec().into(),
            }],
            &IdempotencyKey::new("saved-content")?,
        )
        .await?;
    // Explicit creation still refuses a changed configuration. Reopening must
    // use the stored one and retain the authored head, rather than recreate it.
    assert!(host.create_volume(&saved).await.is_err());
    let reopened_host = FilesystemHost::new(filesystem, &provider, None)?;
    assert_eq!(
        reopened_host
            .open_or_create_volume(&saved)
            .await?
            .generation,
        head
    );
    assert_eq!(
        reopened_host
            .open(&reference, None)
            .await?
            .volume_config()
            .concurrency,
        ConcurrencyMode::Optimistic
    );
    assert_eq!(
        reopened_host
            .read(&reference, None, "/retained", 64, None)
            .await?
            .as_ref(),
        b"saved content"
    );
    let fresh = volume("fresh")?;
    let fresh_head = reopened_host.open_or_create_volume(&fresh).await?;
    assert_eq!(
        reopened_host
            .open(&fresh_head.workspace, None)
            .await?
            .volume_config()
            .concurrency,
        ConcurrencyMode::SerializedAuthority
    );
    assert_eq!(
        reopened_host
            .open_or_create_volume(&fresh)
            .await?
            .generation,
        fresh_head.generation
    );
    Ok(())
}

#[tokio::test]
async fn revocation_during_staging_prevents_content_publication() -> Result<()> {
    let provider = ProviderRef::new(
        "revocation".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes([61; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "revocation".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let mut host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::new(
        "owner",
        [73; 32],
        Authority {
            kind: crate::core::AggregateKind::Agent,
            id: agent.to_string(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "write",
        &crate::Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]),
        Some(agent),
        &mut None,
    )?;
    let grant = ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let workspace = bounded_io::content_workspace(&provider, &volume, None)?;
    let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
    let opened = host.open(&workspace, Some(&io)).await?;
    let before = io.sdk(opened.head(&io, io.cancellation()).await)?.id();
    Arc::get_mut(&mut host).unwrap().publication_pause = Some(Arc::new(move || {
        let issuer = issuer.clone();
        let scope = scope.clone();
        Box::pin(async move {
            issuer.revoke_workload(&scope).unwrap();
        })
    }));
    let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
    let result = host
        .put_content(
            &volume,
            &grant,
            "private",
            b"secret",
            "text/plain",
            "private",
            64,
            &IdempotencyKey::new("revoked-publication")?,
            None,
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&io),
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        io.sdk(opened.head(&io, io.cancellation()).await)?.id(),
        before
    );
    Ok(())
}
