use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_root_binding_keeps_original_actor_scope_fence_and_indexed_retry() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
    let fixture = crate::durable_host::tests::created_fixture::CreationFixture::new().await?;
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)?;
    let owner_io = crate::filesystem::PreparationIo::from_native_work(&native)?;
    let owner = fixture
        .host
        .journal_owner(
            fixture.root,
            &((&fixture.root_lease.reservation).into()),
            Some(&owner_io),
        )
        .await?
        .into_native_shared(&native)?;
    let agent = owner.authenticated_binding_agent(&mut None)?.unwrap();
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "original-native-root-conversation".into(),
    };
    let issuer = crate::core::AuthorityIssuer::new("created-owner", [80; 32], audience.clone());
    let command = Command {
        operation_id: OperationId::from_bytes([124; 16]),
        idempotency_key: IdempotencyKey::new("original-native-root-bind")?,
        expected_revision: 0,
        scope: issuer.issue(
            "root-bind",
            &Capabilities::new(["conversation:bind"]),
            Some(agent),
            None,
            &mut None,
        )?,
        causal_parent: None,
        action: Action::BindConversation { agent },
    };
    let mut aggregate = StreamAggregate::open(
        &fixture.stream,
        audience.clone(),
        issuer.verifier(&mut None)?,
        SchemaRegistry::new(),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    for replay in [false, true] {
        let work = &native;
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
        let result = aggregate
            .execute(command.clone(), Some(&owner), &mut Some(&mut io))
            .await?;
        io.return_staged_output()?;
        let custody = io.into_custody()?;
        let canonical = aggregate
            .operation_event(command.operation_id)
            .await?
            .unwrap();
        assert!(match &result {
            ApplyResult::Applied { event } => !replay && event == &canonical,
            ApplyResult::Replayed { event } => replay && event == &canonical,
        });
        assert_eq!(canonical.revision, 1);
        assert_eq!(
            aggregate.reducer().conversation().unwrap().agent,
            Some(agent)
        );
        assert!(custody.work().shares_ledger(work));
        drop(result);
        drop(custody);
    }
    let independent = NativeMaterializationWork::new(guarded_publication_budget(), &cancellation)?;
    let mut foreign_io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(
        &independent,
    )?);
    assert_eq!(
        aggregate
            .execute(command.clone(), Some(&owner), &mut Some(&mut foreign_io))
            .await
            .err()
            .unwrap()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    let mut ordinary = HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        guarded_publication_budget(),
        &cancellation,
    )?);
    let available = ordinary.remaining()?.peak_allocation_bytes;
    let replayed = aggregate
        .execute(command.clone(), Some(&owner), &mut Some(&mut ordinary))
        .await?;
    let ApplyResult::Replayed { event } = &replayed else {
        panic!("existing binding");
    };
    let held = crate::core::event_owned_bytes(event)?;
    drop(replayed);
    ordinary.release_owned(held)?;
    assert_eq!(ordinary.remaining()?.peak_allocation_bytes, available);
    owner.authenticate_fork_parent(&audience, &mut None)?;
    let mut wrong_actor = command.clone();
    wrong_actor.action = Action::BindConversation {
        agent: AgentId::from_bytes([125; 16]),
    };
    let mut missing_grant = command.clone();
    missing_grant.scope = issuer.issue(
        "root-bind",
        &Capabilities::default(),
        Some(agent),
        None,
        &mut None,
    )?;
    let mut causal_binding = command.clone();
    causal_binding.causal_parent = Some(EventReference {
        authority: audience.clone(),
        revision: 1,
    });
    let wrong_issuer =
        crate::core::AuthorityIssuer::new("created-owner", [81; 32], audience.clone());
    let mut wrong_scope = command.clone();
    wrong_scope.scope = wrong_issuer.issue(
        "root-bind",
        &Capabilities::new(["conversation:bind"]),
        Some(agent),
        None,
        &mut None,
    )?;
    for invalid in [wrong_actor, missing_grant, causal_binding, wrong_scope] {
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
        let error = aggregate
            .execute(invalid, Some(&owner), &mut Some(&mut io))
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
    }
    let denied = NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: 0,
            ..guarded_publication_budget()
        },
        &cancellation,
    )?;
    let mut denied_io =
        HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&denied)?);
    assert!(matches!(
        aggregate
            .execute(command.clone(), Some(&owner), &mut Some(&mut denied_io))
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
    let cancelled = NativeMaterializationWork::new(guarded_publication_budget(), &cancelled_token)?;
    cancelled_token.cancel();
    let mut cancelled_io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(
        &cancelled,
    )?);
    assert!(matches!(
        aggregate
            .execute(command, Some(&owner), &mut Some(&mut cancelled_io))
            .await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(
        fixture
            .stream
            .bounds(StreamPath::new(audience.stream_path(&mut None)?)?)
            .await?
            .tail,
        1
    );
    assert_eq!(aggregate.reducer().revision(), 1);

    let bootstrap = owner
        .authenticated_root_binding_with_native_work(&native)
        .await?;
    let (original, root_custody) = owner
        .original_root_admission_with_native_work(&native)
        .await?;
    owner
        .root_bootstrap_context(&original, &bootstrap.0, &native)
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
    for allowed in [true, false] {
        let io = crate::filesystem::PreparationIo::from_budget(
            WorkBudget {
                backend_read_operations: if allowed {
                    guarded_publication_budget().backend_read_operations
                } else {
                    0
                },
                ..guarded_publication_budget()
            },
            &cancellation,
        )?;
        assert_eq!(publication.verify(Some(&io))?.await.is_ok(), allowed);
    }
    drop(publication);
    let wrong_creator = crate::core::AuthorityIssuer::new(
        "created-owner",
        [81; 32],
        owner.root_bootstrap_owner().clone(),
    );
    let query = &native;
    assert_eq!(
        owner
            .authenticate_root_bootstrap_creator_with_native_work(
                &wrong_creator.verifier(&mut None)?,
                query
            )
            .await
            .err()
            .unwrap()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    let mut substituted = original.clone();
    substituted.input = serde_json::json!({"substituted_original_root":true});
    assert_eq!(
        owner
            .root_bootstrap_context(&substituted, &bootstrap.0, query)
            .await
            .err()
            .unwrap()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    let foreign = crate::durable_host::tests::created_fixture::CreationFixture::new_with_grants(&[
        "foreign:bootstrap",
    ])
    .await?;
    let foreign_owner = foreign
        .host
        .journal_owner(
            foreign.root,
            &((&foreign.root_lease.reservation).into()),
            None,
        )
        .await?;
    assert_eq!(
        foreign_owner
            .root_bootstrap_context(&original, &bootstrap.0, query)
            .await
            .err()
            .unwrap()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    assert!(matches!(
        owner
            .root_bootstrap_context(&original, &bootstrap.0, &cancelled)
            .await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    drop(substituted);
    drop(original);
    drop(root_custody);
    Ok(())
}
