use super::*;

pub(crate) fn live_publisher<P, A, O>(
    journal: &FilesystemExecutionJournal<P, A, O>,
) -> Result<Arc<dyn crate::conversation::ContentPublisher>>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    Ok(crate::filesystem::FilesystemContentPublisher::new(
        journal.host.clone(),
        &journal.volume,
        &journal.verifier,
        &journal.scope,
        16 * 1024 * 1024,
        None,
    )?)
}

pub(crate) async fn journal() -> Result<(
    FilesystemExecutionJournal<
        acyclic_stream::MemoryStream,
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
    crate::core::AuthorityRoot,
)> {
    journal_with_backends(
        Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_fs::Fs::memory(),
    )
    .await
}

pub(in crate::filesystem::execution_journal) async fn journal_with_backends<P, A, O>(
    stream: Arc<P>,
    filesystem: acyclic_fs::Fs<A, O>,
) -> Result<(
    FilesystemExecutionJournal<P, A, O>,
    crate::core::AuthorityRoot,
)>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let provider = crate::resources::ProviderRef::new(
        "append-cost".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let agent = crate::AgentId::from_bytes([81; 16]);
    let volume = VolumeRef::new(
        provider,
        "append-cost".into(),
        VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::host_owned(
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
        stream,
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
