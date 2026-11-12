use super::*;
use acyclic_harness_core::conversation::ContentGrant;

#[tokio::test]
async fn interrupted_streams_keep_live_reply_on_the_journal() -> Result {
    use acyclic_harness_core::{model::ModelEvent, test_support::ScriptedEvent};
    let path = AgentPath::root(&id(214));
    let stream = Arc::new(MemoryStream::default());
    let (files, private) = private_in(&stream, &path).await?;
    let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::Content {
            delta: "saved prefix".into(),
        }),
        ScriptedEvent::Interrupted {
            error: "provider stopped".into(),
        },
    ])]));
    let host = FilesystemTurnHost::new(
        stream,
        within(),
        files.clone(),
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "agents".into(),
            },
        ),
        TurnAdmission::LocalRoot,
        executors(model.clone())?,
        LIMITS,
        Default::default(),
    );
    let mut adoption = adopted(path.id());
    adoption.volumes = vec![private.clone()];
    let state = AgentState::adopted(path, &adoption)?;
    let operation = OperationId::from_bytes([215; 16]);
    let input = ModelContent::Text("continue".into());
    let open = host
        .open(
            &state,
            operation,
            0,
            0,
            &input,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
    let result = open
        .executor
        .execute_progress(
            acyclic_harness_core::executor::TurnInput {
                operation_id: operation,
                input: open.input.clone().unwrap_or(input),
                selected_context: open.selected.clone(),
                max_steps: None,
            },
            open.journal.as_ref(),
        )
        .await;
    assert!(result.is_err());
    assert_eq!(open.outputs.partial(operation).await?, "saved prefix");
    let workspace = acyclic_harness_core::filesystem::workspace_ref(
        private.volume()?.provider().clone(),
        &private.volume()?.storage_name(&mut None)?,
    )?;
    assert!(
        matches!(files.stat(&workspace, None, "tools").await, Err(error)
        if error.code() == acyclic_harness_core::wire::ErrorCode::NotFound)
    );
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn journal_payloads_and_effects_replay_outside_model_writable_storage() -> Result {
    let path = AgentPath::root(&id(211));
    let stream = Arc::new(MemoryStream::default());
    let (files, private) = private_in(&stream, &path).await?;
    let journal = journal_of(private.volume()?)?;
    let issuer = AuthorityRoot::new(
        "agents",
        [7; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "agents".into(),
        },
    );
    let model = Arc::new(ScriptedModelProvider::new([]));
    let host = FilesystemTurnHost::new(
        stream,
        within(),
        files.clone(),
        issuer.clone(),
        TurnAdmission::LocalRoot,
        executors(model.clone())?,
        LIMITS,
        Default::default(),
    );
    let mut adoption = adopted(path.id());
    adoption.volumes = vec![private.clone()];
    let state = AgentState::adopted(path.clone(), &adoption)?;
    let operation = OperationId::from_bytes([212; 16]);
    let input = ModelContent::Text("continue".into());
    let open = host
        .open(
            &state,
            operation,
            0,
            0,
            &input,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
    let payload = open
        .journal
        .stage(operation, "context", b"{}", "application/json", None)?
        .await?;
    assert_eq!(payload.volume(), &journal);
    let call = call_key(1, operation);
    let effect = TurnEffect::Archive {
        target: path.child(&id(213))?,
        id: intent("archive"),
    };
    open.effects
        .as_ref()
        .ok_or("missing effects")?
        .record(&call, &effect)
        .await?;
    let effect_path = format!(".system/interactions/effects/v4/{operation}/0/{call}.msgpack");
    let workspace = |volume: &VolumeRef| {
        acyclic_harness_core::filesystem::workspace_ref(
            volume.provider().clone(),
            &volume.storage_name(&mut None)?,
        )
    };
    files
        .stat(&workspace(&journal)?, None, &effect_path)
        .await?;
    for hidden in [payload.path(), effect_path.as_str()] {
        let missing = files
            .stat(&workspace(private.volume()?)?, None, hidden)
            .await;
        assert!(matches!(missing, Err(error)
            if error.code() == acyclic_harness_core::wire::ErrorCode::NotFound));
    }
    drop(open);
    let reopened = host
        .open(
            &state,
            operation,
            0,
            0,
            &input,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        reopened
            .journal
            .stage(operation, "context", b"{}", "application/json", None)?
            .await?,
        payload
    );
    assert_eq!(
        reopened.journal.load(&payload, None)?.await?.as_ref(),
        b"{}"
    );
    assert_eq!(
        reopened
            .effects
            .as_ref()
            .ok_or("missing effects")?
            .recorded()
            .await?,
        [effect]
    );
    let authority = acyclic_harness_core::filesystem::agent_authority(
        &issuer.into(),
        &TurnAdmission::LocalRoot,
        &path,
        &state.volumes,
        &[],
    )
    .await?;
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        assert!(
            ContentGrant::verify(
                &authority.verifier,
                &authority.scope,
                &journal,
                operation,
                &mut None
            )
            .is_err()
        );
    }
    let write = ContentGrant::verify(
        &authority.verifier,
        &authority.scope,
        private.volume()?,
        VolumeOperation::Write,
        &mut None,
    )?;
    let ui = files
        .put_content(
            private.volume()?,
            &write,
            "ui/title",
            b"title",
            "text/plain",
            "title",
            1024,
            &acyclic_harness_core::IdempotencyKey::new("title")?,
            None,
            Default::default(),
        )
        .await?;
    reopened.journal.verify_input_file(&ui, None)?.await?;
    assert!(reopened.journal.load(&ui, None)?.await.is_err());
    assert!(
        reopened
            .journal
            .verify_input_file(&payload, None)?
            .await
            .is_err()
    );
    let output = reopened.outputs.stage(operation, b"{}").await?;
    assert_eq!(output.volume(), &journal);
    files
        .stat(&workspace(private.volume()?)?, None, "ui/title")
        .await?;
    assert!(model.requests().is_empty());
    Ok(())
}
