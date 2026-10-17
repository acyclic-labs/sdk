use super::*;

#[tokio::test]
async fn stream_journal_keeps_model_body_in_pinned_private_files() -> Result<()> {
    let agent = AgentId::from_bytes([42; 16]);
    let (host, private, issuer) = filesystem("journal-e2e", "journal-owner", agent, 7).await?;
    let scope = scope(&issuer, &private, agent, &[])?;
    let stream = Arc::new(MemoryStream::default());
    let journal = journal(stream.clone(), host, &private, &issuer, &scope)?;
    let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
        "private answer".into(),
    )]));
    let executor = StockExecutor::new(
        Model::new("test".into(), "text".into(), "1".into(), Value::Null)?,
        model.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(acyclic_harness::context::CompactionPolicy::Disabled);
    let operation_id = OperationId::from_bytes([9; 16]);
    let input = TurnInput {
        operation_id,
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        max_steps: 2,
    };
    let first = executor.execute(input.clone(), &journal).await?;
    let staged_operation = OperationId::from_bytes([8; 16]);
    let staged = journal
        .stage(
            staged_operation,
            "original",
            b"original payload",
            "application/json",
            None,
        )?
        .await?;
    for index in 0..32 {
        let replacement = journal
            .stage(
                OperationId::new(),
                &format!("replacement-{index}"),
                &format!("replacement payload {index}").into_bytes(),
                "application/json",
                None,
            )?
            .await?;
        assert_ne!(replacement.path(), staged.path());
        assert_eq!(
            journal.load(&staged, None)?.await?.as_ref(),
            b"original payload"
        );
    }
    assert_eq!(
        journal
            .stage(
                staged_operation,
                "original",
                b"original payload",
                "application/json",
                None
            )?
            .await?,
        staged
    );
    assert!(matches!(
        journal
            .stage(
                staged_operation,
                "original",
                b"changed payload",
                "application/json",
                None
            )?
            .await,
        Err(Error::Conflict(_))
    ));
    let again = executor.execute(input, &journal).await?;
    assert_eq!(first, again);
    assert_eq!(first.text, "private answer");
    assert_eq!(model.requests().len(), 1);
    let records = journal.replay(operation_id, 0, 64, None)?.await?;
    assert_eq!(records.len(), 5);
    let stream = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&stream),
        format!("harness/v2/execution/{operation_id}"),
    )
    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    use futures::TryStreamExt as _;
    let raw = stream
        .read(0, 16)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert!(raw.iter().all(|record| {
        !record
            .value
            .windows(b"private answer".len())
            .any(|window| window == b"private answer")
    }));
    Ok(())
}
