use super::*;

#[tokio::test]
async fn interactions_are_ref_only_authenticated_and_single_resolution() -> Result<()> {
    let agent = AgentId::from_bytes([43; 16]);
    let (host, private, issuer) =
        filesystem("interaction-e2e", "interaction-owner", agent, 8).await?;
    let scope = scope(&issuer, &private, agent, &["interaction:open".into()])?;
    let id = InteractionId::from_bytes([11; 16]);
    let responder = issuer.issue(
        "human",
        &Capabilities::new([
            "interaction:resolve".to_owned(),
            format!("interaction:respond:{id}"),
        ]),
        None,
        None,
        &mut None,
    )?;
    let unauthorized =
        issuer.issue("bystander", &Capabilities::default(), None, None, &mut None)?;
    let stream = Arc::new(MemoryStream::default());
    bind_conversation(
        &stream,
        &issuer,
        "interaction-owner",
        AgentId::from_bytes([43; 16]),
    )
    .await?;
    let interaction_host = FilesystemInteractionHost::new(
        stream.clone(),
        Arc::clone(&host),
        &Authority {
            kind: AggregateKind::Conversation,
            id: "interaction-owner".into(),
        },
        &SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: &private.clone(),
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope.clone(),
            maximum_bytes: 4_096,
            io: None,
        },
    )?;
    let journal = journal(stream.clone(), host, &private, &issuer, &scope)?;
    let request = Interaction::question("The private prompt".into(), json!({"type":"string"}))?;
    journal.open_interaction(id, &request, None)?.await?;
    journal.open_interaction(id, &request, None)?.await?;
    assert!(journal.interaction_outcome(id, None)?.await?.is_none());
    let first_staged = interaction_host
        .stage_answer(
            id,
            1,
            OperationId::from_bytes([60; 16]),
            &InteractionResponse::Question {
                value: json!("orphaned first attempt"),
            },
            None,
        )
        .await?;
    let second_staged = interaction_host
        .stage_answer(
            id,
            1,
            OperationId::from_bytes([61; 16]),
            &InteractionResponse::Question {
                value: json!("competing answer"),
            },
            None,
        )
        .await?;
    assert_ne!(first_staged.path(), second_staged.path());
    let response = InteractionResponse::Question {
        value: json!("private answer"),
    };
    assert!(
        journal
            .resolve_interaction(id, response.clone(), &unauthorized)
            .await
            .is_err()
    );
    assert!(
        journal
            .resolve_interaction(
                id,
                InteractionResponse::Question { value: json!(17) },
                &responder
            )
            .await
            .is_err()
    );
    journal
        .resolve_interaction(id, response.clone(), &responder)
        .await?;
    assert!(matches!(
        journal.interaction_outcome(id, None)?.await?,
        Some(InteractionOutcome::Answered { .. })
    ));
    journal
        .resolve_interaction(id, response.clone(), &responder)
        .await?;
    assert!(
        journal
            .resolve_interaction(
                id,
                InteractionResponse::Question {
                    value: json!("different")
                },
                &responder
            )
            .await
            .is_err()
    );
    let raw = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&stream),
        "harness/v2/conversations/interaction-owner",
    )
    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
    .read(0, 3)
    .await
    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    use futures::TryStreamExt as _;
    let records = raw
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(records.len(), 3);
    for record in records {
        assert!(
            !record
                .value
                .windows(b"private prompt".len())
                .any(|window| window == b"private prompt")
        );
        assert!(
            !record
                .value
                .windows(b"private answer".len())
                .any(|window| window == b"private answer")
        );
    }
    Ok(())
}
