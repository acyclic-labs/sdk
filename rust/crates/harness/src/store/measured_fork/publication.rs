use super::*;

#[tokio::test]
async fn guarded_fork_reopen_and_child_replay_keep_original_identity_and_owner_fence() -> Result<()>
{
    let mut setup = guarded_fork_fixture().await?;
    let cancellation = CancellationToken::new();
    let budget = guarded_publication_budget();
    let seed = setup.report.clone().into_seed(&mut None)?;
    let before = setup.parent.reducer().revision();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
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
    let wrong_actor =
        crate::core::AuthorityIssuer::new("created-owner", [80; 32], seed.parent.clone()).issue(
            "wrong-parent",
            &Capabilities::new(["fork:publish"]),
            Some(AgentId::from_bytes([80; 16])),
            None,
            &mut None,
        )?;
    assert!(
        publish(
            &mut setup.parent,
            setup.report.clone(),
            wrong_actor,
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
        (
            seed.parent.clone(),
            setup.parent_verifier.clone(),
            SchemaRegistry::new(),
        ),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_fork_verifier(setup.parent.fork_verifier.clone().unwrap());
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
    let mut child = StreamAggregate::open(
        &setup.fixture.stream,
        (
            seed.child.clone(),
            setup.child_verifier.clone(),
            SchemaRegistry::new(),
        ),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    let failure = bind(
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
    .expect_err("parent read bound");
    assert!(failure.work.authority_bytes_read > 0);
    assert_eq!(child.reducer().revision(), 0);
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
        (
            seed.child.clone(),
            setup.child_verifier,
            SchemaRegistry::new(),
        ),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
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
    setup
        .fixture
        .host
        .settle_task(
            crate::TaskId::from_bytes([84; 16]),
            &(&setup.lease.reservation).into(),
            crate::Outcome::Failed {
                message: "closed".into(),
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
    assert_eq!(reopened.reducer().revision(), before + 1);
    Ok(())
}

#[tokio::test]
async fn guarded_child_rejects_original_event_one_with_wrong_binding_identity() -> Result<()> {
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
        (
            seed.child.clone(),
            setup.child_verifier,
            SchemaRegistry::new(),
        ),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    child
        .execute(
            Command {
                operation_id: OperationId::from_bytes([97; 16]),
                idempotency_key: IdempotencyKey::new("wrong-binding")?,
                expected_revision: 0,
                scope: setup.child_scope.clone(),
                causal_parent: None,
                action: Action::BindConversation {
                    agent: seed.child_agent,
                },
            },
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
    assert_eq!(child.reducer().revision(), 1);
    Ok(())
}
