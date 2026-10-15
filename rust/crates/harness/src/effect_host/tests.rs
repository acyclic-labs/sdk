use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use acyclic_stream::MemoryStream;
fn budget() -> WorkBudget {
    WorkBudget {
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        items_examined: 1024,
        allocation_operations: 1024,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}
fn host(
    metadata: &NativeMaterializationWork,
    work: &NativeMaterializationWork,
    foreign_scope: bool,
) -> Result<Arc<ConversationEffectHost<MemoryStream>>> {
    let authority = Authority {
        kind: crate::core::AggregateKind::Conversation,
        id: "native-effect-metadata-control".into(),
    };
    let issuer = AuthorityIssuer::new("original-effect-issuer", [41; 32], authority.clone());
    let foreign = AuthorityIssuer::new("foreign-effect-issuer", [43; 32], authority.clone());
    let scope = if foreign_scope { &foreign } else { &issuer }.issue(
        "original-effect-scope",
        &crate::Capabilities::new([capability::EFFECT_RUN]),
        Some(crate::AgentId::from_bytes([42; 16])),
        None,
        &mut None,
    )?;
    let files = crate::filesystem::FilesystemHost::new(
        acyclic_fs::Fs::memory(),
        &crate::resources::ProviderRef::new(
            "effect-metadata-control".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?,
        None,
    )?;
    let content = crate::filesystem::FilesystemContentVerifier::new(
        files,
        &issuer.verifier(&mut None)?,
        &scope,
        crate::conversation::Limits::default().file_bytes,
        None,
    )?;
    let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(
        metadata,
    )?);
    let authority = io.clone_authority(&authority)?;
    let issuer = issuer.copy(&mut Some(&mut io))?;
    let scope = scope.copy(&mut Some(&mut io))?;
    ConversationEffectHost::new(
        Arc::new(MemoryStream::default()),
        (authority, issuer, scope),
        SchemaRegistry::new(),
        content,
        EffectRegistry::new(Some(metadata), None)?,
        Some(io.into_custody()?),
        Some(work),
    )
}
#[test]
fn native_host_metadata_survives_shared_clone_until_physical_drop() -> Result<()> {
    let token = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let shared = host(&work, &work, false)?;
    let alias = shared.clone();
    drop(shared);
    assert!(work.live_allocation_bytes() > baseline);
    drop(alias);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
#[test]
fn native_host_rejects_foreign_metadata_scope_and_cancelled_source() -> Result<()> {
    let token = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let foreign = NativeMaterializationWork::new(budget(), &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    assert!(host(&work, &foreign, false).is_err());
    assert!(host(&work, &work, true).is_err());
    assert_eq!(work.live_allocation_bytes(), baseline);
    token.cancel();
    assert!(matches!(
        host(&work, &work, false),
        Err(Error::NativeWork { .. })
    ));
    Ok(())
}
