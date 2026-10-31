use super::*;
use crate::{AgentId, conversation::VolumeOwner, resources::ProviderRef};

type Journal = FilesystemExecutionJournal<
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1234
    }
}

async fn fixture_with_filesystem(
    filesystem: acyclic_fs::MemoryFs,
) -> Result<(Journal, crate::core::AuthorityRoot)> {
    let provider = ProviderRef::new("future".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider,
        "future".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::new(
        "future",
        [2; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::from_bytes([3; 16]).to_string(),
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
        Arc::new(acyclic_stream::MemoryStream::new_with_clock(
            Default::default(),
            Arc::new(Clock),
        )),
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

#[tokio::test]
async fn separately_composed_journals_exclude_the_same_live_operation() -> Result<()> {
    use futures::FutureExt as _;
    let provider = acyclic_stream::MemoryStream::default();
    let objects = acyclic_objects::MemoryObjects::memory();
    let objects = Arc::new(objects);
    let first_fs =
        acyclic_fs::Fs::from_memory_providers(Arc::new(provider.clone()), objects.clone());
    let second_fs = acyclic_fs::Fs::from_memory_providers(Arc::new(provider), objects);
    let (first, _runtime) = fixture_with_filesystem(first_fs).await?;
    let second = FilesystemExecutionJournal::new(
        first.stream.clone(),
        FilesystemHost::new(second_fs, first.volume.provider(), None)?,
        crate::filesystem::FilesystemJournalBinding {
            volume: &first.volume,
            verifier: &first.verifier,
            scope: &first.scope,
            io: None,
        },
    )?;
    let operation = OperationId::new();
    let owner = first.claim_execution(operation, None)?;
    assert!(matches!(
        second.claim_execution(operation, None),
        Err(Error::Conflict(_))
    ));
    let observation = match second.acquire_execution(operation, None)? {
        crate::executor::ExecutionAttempt::Observed(observation) => observation,
        crate::executor::ExecutionAttempt::Owned(_) => panic!("duplicate producer admitted"),
    };
    let mut retired = Box::pin(observation.terminated());
    assert!(retired.as_mut().now_or_never().is_none());
    drop(owner);
    let successor = second.claim_execution(operation, None)?;
    // An occupied successor cannot redirect the pinned predecessor observation.
    assert_eq!(
        retired.now_or_never(),
        Some(acyclic_fs::OperationTermination::ProducerDropped)
    );
    let finished = successor.observe();
    successor.release();
    assert_eq!(
        finished.terminated().now_or_never(),
        Some(acyclic_fs::OperationTermination::Released)
    );
    Ok(())
}
