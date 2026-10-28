use super::*;

pub(in crate::filesystem::execution_journal) async fn journal() -> Result<(
    FilesystemExecutionJournal<
        acyclic_stream::MemoryStream,
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
    crate::core::AuthorityRoot,
)> {
    let provider = crate::resources::ProviderRef::new(
        "append-cost".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = crate::AgentId::from_bytes([81; 16]);
    let volume = VolumeRef::new(
        provider,
        "append-cost".into(),
        VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::new(
        "append-cost",
        [81; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::from_bytes([82; 16]).to_string(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        &mut None,
    )?;
    let journal = FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        crate::filesystem::FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            io: None,
        },
    )?;
    Ok((journal, issuer))
}
