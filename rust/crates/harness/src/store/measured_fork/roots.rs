use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_root_binding_keeps_original_actor_scope_fence_and_indexed_retry() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
    let fixture = crate::durable_host::tests::created_fixture::CreationFixture::new().await?;
    let owner = fixture
        .host
        .journal_owner(
            fixture.root,
            &((&fixture.root_lease.reservation).into()),
            None,
        )
        .await?;
    let agent = owner
        .authenticated_binding_agent(&mut None)?
        .ok_or_else(|| Error::Invalid("original ROOT fixture has no authenticated agent".into()))?;
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "original-native-root-conversation".into(),
    };
    let original_issuer =
        crate::core::AuthorityIssuer::new("created-owner", [80; 32], audience.clone());
    let scope = original_issuer
        .issue(
            "original-native-root-bind",
            &Capabilities::new(["conversation:bind".to_owned()]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let command = Command {
        operation_id: OperationId::from_bytes([124; 16]),
        idempotency_key: IdempotencyKey::new("original-native-root-bind")?,
        expected_revision: 0,
        scope,
        causal_parent: None,
        action: Action::BindConversation { agent },
    };
    let mut aggregate = StreamAggregate::open(
        &fixture.stream,
        audience.clone(),
        original_issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)
        .expect("original ROOT ledger");
    let owner = fixture
        .host
        .journal_owner(
            fixture.root,
            &(&fixture.root_lease.reservation).into(),
            Some(&crate::filesystem::PreparationIo::from_native_work(
                &native,
            )?),
        )
        .await?
        .into_native_shared(&native)?;
    let mut first_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let first = aggregate
        .execute(command.clone(), Some(&owner), &mut Some(&mut first_io))
        .await?;
    first_io.return_staged_output()?;
    let first_custody = first_io.into_custody()?;
    assert!(matches!(&first, ApplyResult::Applied { event }
        if event.revision == 1 && event.operation_id == command.operation_id));
    assert_eq!(
        aggregate
            .reducer()
            .conversation()
            .and_then(|state| state.agent),
        Some(agent)
    );
    assert!(first_custody.bytes() > 0);
    assert!(first_custody.work().shares_ledger(&native));
    owner.authenticate_fork_parent(&audience, &mut None)?;
    let canonical = aggregate
        .operation_event(command.operation_id)
        .await?
        .ok_or_else(|| Error::NotFound("original canonical ROOT event".into()))?;
    assert!(matches!(&first, ApplyResult::Applied { event } if event == &canonical));
    let mut retry_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let retry = aggregate
        .execute(command.clone(), Some(&owner), &mut Some(&mut retry_io))
        .await?;
    retry_io.return_staged_output()?;
    let retry_custody = retry_io.into_custody()?;
    assert!(matches!(&retry, ApplyResult::Replayed { event } if event == &canonical));
    assert!(retry_custody.work().shares_ledger(&native));
    assert_eq!(
        fixture
            .stream
            .bounds(acyclic_stream::StreamPath::new(
                audience.stream_path(&mut None)?.as_str()
            )?)
            .await?
            .tail,
        1
    );

    let independent = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)
        .expect("independent original query ledger");
    let mut independent_retry_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&independent)?,
    );
    let independent_retry = aggregate
        .execute(
            command.clone(),
            Some(&owner),
            &mut Some(&mut independent_retry_io),
        )
        .await?;
    independent_retry_io.return_staged_output()?;
    let independent_custody = independent_retry_io.into_custody()?;
    assert!(matches!(&independent_retry, ApplyResult::Replayed { event } if event == &canonical));
    assert!(independent_custody.work().shares_ledger(&independent));

    let mut wrong_actor = command.clone();
    wrong_actor.action = Action::BindConversation {
        agent: AgentId::from_bytes([125; 16]),
    };
    let mut missing_grant = command.clone();
    missing_grant.scope = original_issuer
        .issue(
            "original-native-root-bind",
            &Capabilities::default(),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let mut causal_binding = command.clone();
    causal_binding.causal_parent = Some(EventReference {
        authority: audience.clone(),
        revision: 1,
    });
    for invalid in [wrong_actor, missing_grant, causal_binding] {
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)?,
        );
        let error = aggregate
            .execute(invalid, Some(&owner), &mut Some(&mut io))
            .await
            .err()
            .expect("original ROOT actor, grant and no-parent policy remain required");
        assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
    }

    let wrong_issuer =
        crate::core::AuthorityIssuer::new("created-owner", [81; 32], audience.clone());
    let mut wrong_scope = command.clone();
    wrong_scope.scope = wrong_issuer
        .issue(
            "original-native-root-bind",
            &Capabilities::new(["conversation:bind".to_owned()]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let mut wrong_scope_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let wrong_scope_error = aggregate
        .execute(wrong_scope, Some(&owner), &mut Some(&mut wrong_scope_io))
        .await
        .err()
        .expect("original issuer key must authenticate the binding scope");
    assert_eq!(
        wrong_scope_error.code(),
        crate::wire::ErrorCode::Unauthorized
    );
    let denied = NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: 0,
            ..guarded_publication_budget()
        },
        &cancellation,
    )
    .expect("zero-copy original ledger");
    let mut denied_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&denied)?,
    );
    assert!(matches!(
        aggregate
            .execute(command.clone(), Some(&owner), &mut Some(&mut denied_io),)
            .await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }),
            ..
        })
    ));
    let cancelled_token = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(guarded_publication_budget(), &cancelled_token)
        .expect("original ledger before cancellation");
    cancelled_token.cancel();
    let mut cancelled_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&cancelled)?,
    );
    assert!(matches!(
        aggregate
            .execute(command, Some(&owner), &mut Some(&mut cancelled_io),)
            .await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(
        fixture
            .stream
            .bounds(acyclic_stream::StreamPath::new(
                audience.stream_path(&mut None)?.as_str()
            )?)
            .await?
            .tail,
        1
    );
    drop(first);
    drop(first_custody);
    drop(retry);
    drop(retry_custody);
    drop(independent_retry);
    drop(independent_custody);
    assert_eq!(aggregate.reducer().revision(), 1);
    owner.authenticate_fork_parent(&audience, &mut None)?;
    let bootstrap = owner
        .authenticated_root_binding_with_native_work(&native)
        .await?;
    let (original_root, _root_custody) = owner
        .original_root_admission_with_native_work(&native)
        .await?;
    owner
        .root_bootstrap_context(&original_root, &bootstrap.0, &native)
        .await?;
    assert_ne!(owner.root_bootstrap_owner(), &audience);
    let creator = crate::core::AuthorityIssuer::new(
        "created-owner",
        [80; 32],
        owner.root_bootstrap_owner().clone(),
    );
    owner
        .authenticate_root_bootstrap_creator_with_native_work(
            &creator.verifier(&mut None)?,
            &native,
        )
        .await?;
    let publication = owner
        .root_bootstrap_publication_lease_with_native_work(&native)
        .await?;
    let native_io = crate::filesystem::PreparationIo::from_native_work(&native)?;
    publication.verify(Some(&native_io))?.await?;
    let ordinary_io =
        crate::filesystem::PreparationIo::from_budget(guarded_publication_budget(), &cancellation)?;
    publication.verify(Some(&ordinary_io))?.await?;
    let denied_io = crate::filesystem::PreparationIo::from_budget(
        WorkBudget {
            backend_read_operations: 0,
            ..guarded_publication_budget()
        },
        &cancellation,
    )?;
    assert!(publication.verify(Some(&denied_io))?.await.is_err());
    let wrong_creator = crate::core::AuthorityIssuer::new(
        "created-owner",
        [81; 32],
        owner.root_bootstrap_owner().clone(),
    );
    assert!(
        matches!(owner.authenticate_root_bootstrap_creator_with_native_work(&wrong_creator.verifier(&mut None)?, &native).await, Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized)
    );
    let mut substituted_root = original_root.clone();
    substituted_root.input = serde_json::json!({"substituted_original_root":true});
    assert!(
        matches!(owner.root_bootstrap_context(&substituted_root, &bootstrap.0, &native).await, Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized)
    );
    let foreign = crate::durable_host::tests::created_fixture::CreationFixture::new_with_grants(&[
        "foreign:bootstrap",
    ])
    .await?;
    let foreign_owner = foreign
        .host
        .journal_owner(
            foreign.root,
            &(&foreign.root_lease.reservation).into(),
            None,
        )
        .await?;
    assert!(
        matches!(foreign_owner.root_bootstrap_context(&original_root, &bootstrap.0, &native).await, Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized)
    );
    assert!(matches!(
        owner
            .root_bootstrap_context(&original_root, &bootstrap.0, &cancelled)
            .await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    Ok(())
}
