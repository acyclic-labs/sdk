use super::*;

#[tokio::test]
async fn indexed_publication_cancellation_precedes_codec_and_sdk() -> Result<()> {
    let reducer = Reducer::new(
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
    );
    let ApplyResult::Applied { event } = reducer.plan(&command(71)?)? else {
        return Err(Error::Invalid("fixture needs fresh event".into()));
    };
    let bytes = crate::wire_codec::encode_event(reducer.authority(), &event, &mut None)?;
    let cancellation = CancellationToken::new();
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        publication_budget(),
        &cancellation,
    )?);
    cancellation.cancel();
    assert!(
        operations::IndexedPublication::new(
            reducer.authority(),
            &event,
            bytes,
            &mut Some(&mut work),
        )
        .is_err()
    );
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn guarded_fork_reopen_and_child_replay_keep_original_identity_and_owner_fence() -> Result<()>
{
    use crate::core::SchemaRegistry;
    let mut setup = guarded_fork_fixture().await?;
    let cancellation = CancellationToken::new();
    let budget = guarded_publication_budget();
    let seed = setup.report.clone().into_seed(&mut None)?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let before = setup.parent.reducer().revision();
    assert!(
        publish(
            &mut setup.parent,
            setup.report.clone(),
            setup.parent_scope.clone(),
            &setup.owner,
            budget,
            &cancelled
        )
        .await
        .is_err()
    );
    assert!(
        publish(
            &mut setup.parent,
            setup.report.clone(),
            setup.parent_scope.clone(),
            &setup.owner,
            WorkBudget {
                peak_allocation_bytes: 1,
                ..budget
            },
            &cancellation
        )
        .await
        .is_err()
    );
    let wrong_parent_agent =
        crate::core::AuthorityIssuer::new("created-owner", [80; 32], seed.parent.clone())
            .issue(
                "not-original-admitted-parent",
                &Capabilities::new(["fork:publish"]),
                Some(AgentId::from_bytes([80; 16])),
                None,
                &mut None,
            )
            .expect("scope proof");
    assert!(
        publish(
            &mut setup.parent,
            setup.report.clone(),
            wrong_parent_agent,
            &setup.owner,
            budget,
            &cancellation
        )
        .await
        .is_err()
    );
    let mut hostile_report = setup.report.clone();
    hostile_report.request.child_agent = AgentId::from_bytes([96; 16]);
    assert!(
        publish(
            &mut setup.parent,
            hostile_report,
            setup.parent_scope.clone(),
            &setup.owner,
            budget,
            &cancellation
        )
        .await
        .is_err()
    );
    assert_eq!(setup.parent.reducer().revision(), before);
    publish(
        &mut setup.parent,
        setup.report.clone(),
        setup.parent_scope.clone(),
        &setup.owner,
        budget,
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    let mut reopened = StreamAggregate::open(
        &setup.fixture.stream,
        seed.parent.clone(),
        setup.parent_verifier.clone(),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_fork_verifier(
        setup
            .parent
            .fork_verifier
            .clone()
            .ok_or_else(|| Error::Invalid("original verifier missing".into()))?,
    );
    let replay = publish(
        &mut reopened,
        setup.report.clone(),
        setup.parent_scope.clone(),
        &setup.owner,
        budget,
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    assert!(matches!(replay.value, ApplyResult::Replayed { .. }));
    assert_eq!(reopened.reducer().revision(), before + 1);
    assert!(replay.work.backend_read_operations > 0);
    assert!(
        publish(
            &mut reopened,
            setup.report.clone(),
            setup.parent_scope.clone(),
            &setup.owner,
            WorkBudget {
                backend_read_operations: replay.work.backend_read_operations - 1,
                ..budget
            },
            &cancellation
        )
        .await
        .is_err()
    );
    assert_eq!(
        setup
            .fixture
            .stream
            .bounds(acyclic_stream::StreamPath::new(
                seed.parent.stream_path(&mut None)?.as_str()
            )?)
            .await?
            .tail,
        before + 1
    );
    let mut child = StreamAggregate::open(
        &setup.fixture.stream,
        seed.child.clone(),
        setup.child_verifier.clone(),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    let partial = bind(
        &mut child,
        &reopened,
        &seed,
        setup.child_scope.clone(),
        &setup.owner,
        WorkBudget {
            authority_bytes_read: 0,
            ..budget
        },
        &cancellation,
    )
    .await
    .expect_err("actual parent record must be charged before decoding");
    assert!(partial.work.authority_records_read > 0);
    assert!(partial.work.authority_bytes_read > 0);
    assert_eq!(child.reducer().revision(), 0);
    let mut hostile = seed.clone();
    hostile.parent_revision += 1;
    assert!(
        bind(
            &mut child,
            &reopened,
            &hostile,
            setup.child_scope.clone(),
            &setup.owner,
            budget,
            &cancellation
        )
        .await
        .is_err()
    );
    let wrong_agent =
        crate::core::AuthorityIssuer::new("created-owner", [80; 32], seed.child.clone())
            .issue(
                "wrong-child-agent",
                &Capabilities::new(["conversation:bind".into()]),
                Some(AgentId::from_bytes([96; 16])),
                None,
                &mut None,
            )
            .expect("scope proof");
    assert!(
        bind(
            &mut child,
            &reopened,
            &seed,
            wrong_agent,
            &setup.owner,
            budget,
            &cancellation
        )
        .await
        .is_err()
    );
    bind(
        &mut child,
        &reopened,
        &seed,
        setup.child_scope.clone(),
        &setup.owner,
        budget,
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    let mut reopened_child = StreamAggregate::open(
        &setup.fixture.stream,
        seed.child.clone(),
        setup.child_verifier.clone(),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    bind(
        &mut reopened_child,
        &reopened,
        &seed,
        setup.child_scope.clone(),
        &setup.owner,
        budget,
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    assert_eq!(reopened_child.reducer().revision(), 1);
    assert_eq!(
        setup
            .fixture
            .stream
            .bounds(acyclic_stream::StreamPath::new(
                seed.child.stream_path(&mut None)?.as_str()
            )?)
            .await?
            .tail,
        1
    );
    setup
        .fixture
        .host
        .settle_task(
            crate::TaskId::from_bytes([84; 16]),
            &(&setup.lease.reservation).into(),
            crate::Outcome::Failed {
                message: "test closes original publication lease".into(),
            },
            &mut None,
        )
        .await?;
    assert!(
        publish(
            &mut reopened,
            setup.report,
            setup.parent_scope,
            &setup.owner,
            budget,
            &cancellation
        )
        .await
        .is_err()
    );
    assert_eq!(
        setup
            .fixture
            .stream
            .bounds(acyclic_stream::StreamPath::new(
                seed.parent.stream_path(&mut None)?.as_str()
            )?)
            .await?
            .tail,
        before + 1
    );
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn guarded_child_rejects_original_event_one_with_wrong_binding_identity() -> Result<()> {
    use crate::core::SchemaRegistry;
    let mut setup = guarded_fork_fixture().await?;
    let cancellation = CancellationToken::new();
    let budget = guarded_publication_budget();
    let seed = setup.report.clone().into_seed(&mut None)?;
    publish(
        &mut setup.parent,
        setup.report,
        setup.parent_scope,
        &setup.owner,
        budget,
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    let mut child = StreamAggregate::open(
        &setup.fixture.stream,
        seed.child.clone(),
        setup.child_verifier,
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    child
        .execute(
            Command {
                operation_id: OperationId::from_bytes([97; 16]),
                idempotency_key: IdempotencyKey::new("hostile-original-child-binding")?,
                expected_revision: 0,
                scope: setup.child_scope.clone(),
                causal_parent: None,
                action: Action::BindConversation {
                    agent: seed.child_agent,
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(
        bind(
            &mut child,
            &setup.parent,
            &seed,
            setup.child_scope,
            &setup.owner,
            budget,
            &cancellation
        )
        .await
        .is_err()
    );
    assert_eq!(
        setup
            .fixture
            .stream
            .bounds(acyclic_stream::StreamPath::new(
                seed.child.stream_path(&mut None)?.as_str()
            )?)
            .await?
            .tail,
        1
    );
    Ok(())
}
