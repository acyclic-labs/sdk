use super::*;
use crate::{AgentId, conversation::VolumeOwner, core::Authority};

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
