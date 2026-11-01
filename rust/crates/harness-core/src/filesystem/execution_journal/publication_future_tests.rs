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
    let first = first.with_turn(operation, OperationId::new());
    let second = second.with_turn(operation, OperationId::new());
    let parent = first.claim_execution(operation, None)?;
    let purpose = crate::executor::ModelPurpose::Summary;
    let summary = first
        .acquire_model_execution(operation, purpose, None)?
        .into_owner(None)?;
    let observed = match second.acquire_model_execution(operation, purpose, None)? {
        crate::executor::ExecutionAttempt::Observed(observation) => observation,
        crate::executor::ExecutionAttempt::Owned(_) => panic!("duplicate summary admitted"),
    };
    // The former call confuses the derived custody key with parent authority.
    assert!(matches!(
        first.acquire_execution(purpose.attempt_operation(operation, None)?, None),
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        first.acquire_model_execution(OperationId::new(), purpose, None),
        Err(Error::Unauthorized(_))
    ));
    drop(summary);
    assert_eq!(
        observed.terminated().now_or_never(),
        Some(acyclic_fs::OperationTermination::ProducerDropped)
    );
    let replacement = second
        .acquire_model_execution(operation, purpose, None)?
        .into_owner(None)?;
    let completed = replacement.observe();
    replacement.release();
    assert_eq!(
        completed.terminated().now_or_never(),
        Some(acyclic_fs::OperationTermination::Released)
    );
    // A summary never retires or replaces its original turn's claim.
    assert!(matches!(
        second.acquire_execution(operation, None)?,
        crate::executor::ExecutionAttempt::Observed(_)
    ));
    drop(parent);
    let model = Arc::new(crate::test_support::ScriptedModelProvider::new([
        crate::test_support::ScriptedTurn::Text("retained summary".into()),
    ]));
    let executor = crate::executor::StockExecutor::new(
        crate::model::Model::new("fixture".into(), "summary".into(), "1".into())?,
        model.clone(),
        crate::context::ContextPipeline::default(),
        crate::tool::ToolRegistry::default(),
    );
    let input = crate::executor::TurnInput {
        operation_id: operation,
        input: crate::model::ModelContent::Text("current input".into()),
        selected_context: None,
        max_steps: 1,
    };
    let source = crate::context::Context {
        messages: vec![crate::model::ModelMessage {
            role: crate::model::ModelRole::User,
            content: crate::model::ModelContent::Text("original source".into()),
        }],
        ..Default::default()
    };
    let instruction = crate::model::ModelContent::Text("Summarize the source".into());
    let result = executor
        .summarize_in_turn(&first, &input, 0, source.clone(), instruction.clone())
        .await?;
    assert_eq!(
        first.load(&result.output, None)?.await?.as_ref(),
        b"retained summary"
    );
    assert_eq!(
        executor
            .summarize_in_turn(&first, &input, 0, source, instruction)
            .await?,
        result
    );
    assert_eq!(model.requests().len(), 1);
    let (foreign, _) = fixture_with_filesystem(acyclic_fs::Fs::memory()).await?;
    let refused = first.claim_execution(operation, None)?;
    let refusal = refused.observe();
    assert!(foreign.retain_execution_claim(refused, None).is_err());
    assert_eq!(
        refusal.terminated().await,
        acyclic_fs::OperationTermination::ProducerDropped
    );
    drop(foreign);
    // A producer admitted before handoff can outlive both journal handles
    // without reacquiring exclusion on its first future poll.
    let claim = first.claim_execution(operation, None)?;
    let retained = first.retain_execution_claim(claim, None)?;
    let observed = retained.observe();
    let host = Arc::downgrade(&first.host);
    drop((first, second));
    assert!(host.upgrade().is_some());
    let unpolled = async move {
        let _original = retained;
        std::future::pending::<()>().await;
    };
    drop(unpolled);
    assert_eq!(
        observed.terminated().await,
        acyclic_fs::OperationTermination::ProducerDropped
    );
    assert!(host.upgrade().is_none());
    Ok(())
}
