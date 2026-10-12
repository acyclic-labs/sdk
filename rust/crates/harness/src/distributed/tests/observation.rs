use super::*;

#[tokio::test]
async fn declaration_point_observation_is_read_only_bounded_and_source_verified() -> Result<()>
{
    use std::sync::atomic::Ordering;
    let provider = Arc::new(LostSessionAck::default());
    let client = provider.clone();
    let operation = OperationId::from_bytes([1; 16]);
    let declaration = spec(operation, 0)?;
    let owner = declaration.owner.authority().clone();
    let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    let scope = issuer.issue("observe", &Capabilities::new(["operation:observe"]), None, None, &mut None).expect("scope proof");
    let verifier = issuer.verifier(&mut None).expect("verifier copy");
    let key = IdempotencyKey::new("point-declaration")?;
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    declare(&mut coordinator, declaration.clone(), key.as_str()).await?;
    coordinator
        .apply(
            IdempotencyKey::new("point-cancel")?,
            SchedulerEvent::CancellationRequested {
                operation_id: operation,
                recursive: false,
            }, &mut None,
        )
        .await?;
    for value in 2..82 {
        declare(
            &mut coordinator,
            spec(OperationId::from_bytes([value; 16]), 0)?,
            &format!("point-other-{value}"),
        )
        .await?;
    }
    drop(coordinator);
    provider.forbid_writes.store(true, Ordering::SeqCst);
    assert_eq!(
        observe_declaration(&client, &owner, &scope, &verifier, operation, &key, &mut None).await?,
        Some(declaration)
    );
    assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
    let denied = issuer.issue("denied", &Capabilities::default(), None, None, &mut None).expect("scope proof");
    assert!(matches!(
        observe_declaration(&client, &owner, &denied, &verifier, operation, &key, &mut None).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        observe_declaration(
            &client,
            &owner,
            &scope,
            &verifier,
            OperationId::from_bytes([99; 16]),
            &key
        , &mut None)
        .await,
        Err(Error::Conflict(_))
    ));
    let foreign = Authority {
        kind: AggregateKind::Task,
        id: "foreign".into(),
    };
    let foreign_issuer = AuthorityIssuer::new("foreign-observer", [7; 32], foreign.clone());
    let foreign_scope =
        foreign_issuer.issue("observe", &Capabilities::new(["operation:observe"]), None, None, &mut None).expect("scope proof");
    assert!(matches!(
        observe_declaration(
            &client,
            &foreign,
            &foreign_scope,
            &foreign_issuer.verifier(&mut None).expect("verifier copy"),
            operation,
            &key
        , &mut None)
        .await,
        Err(Error::Unauthorized(_))
    ));
    let missing = IdempotencyKey::new("point-missing")?;
    assert_eq!(
        observe_declaration(&client, &owner, &scope, &verifier, operation, &missing, &mut None).await?,
        None
    );
    assert!(matches!(
        observe_declaration(
            &client,
            &owner,
            &scope,
            &verifier,
            operation,
            &IdempotencyKey::new("point-cancel")?
        , &mut None)
        .await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn original_owner_proof_survives_reopen_and_rejects_replaced_key() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let operation_id = OperationId::from_bytes([91; 16]);
    let original = spec(operation_id, 0)?;
    let mut json = serde_json::to_value(&original).expect("declaration JSON");
    json.as_object_mut().expect("spec object").remove("owner_scope");
    assert!(serde_json::from_value::<OperationSpec>(json).is_err());
    let owner = original.owner.authority().clone();
    let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    let renewed = issuer.issue("renewed-current-grant", &Capabilities::new(["operation:declare", "operation:observe", "operation:cancel"]), None, None, &mut None).expect("scope proof");
    let mut tampered = serde_json::to_value(&original).expect("declaration JSON");
    tampered["owner_scope"]["id"] = serde_json::json!("tampered-original-grant");
    let tampered: OperationSpec = serde_json::from_value(tampered).expect("typed declaration");
    let mut insufficient = original.clone();
    insufficient.owner_scope = issuer.issue("observe-only", &Capabilities::new(["operation:observe"]), None, None, &mut None)?;
    for invalid in [tampered, insufficient] {
        assert!(matches!(invalid.verify_owner(&owner, &issuer.verifier(&mut None)?, &mut None), Err(Error::Unauthorized(_))));
    }
    let key = IdempotencyKey::new("original-owner-proof")?;
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    declare(&mut coordinator, original.clone(), key.as_str()).await?;
    let mut reopened =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let replaced = AuthorityIssuer::new("test-runtime", [8; 32], owner.clone());
    let replacement = replaced.issue("renewed-current-grant", renewed.capabilities(), None, None, &mut None).expect("scope proof");
    assert!(matches!(
        observe_declaration(
            &client,
            &owner,
            &replacement,
            &replaced.verifier(&mut None).expect("verifier copy"),
            operation_id,
            &key
        , &mut None)
        .await,
        Err(Error::Unauthorized(_))
    ));
    let worker = Worker {
        id: "owner-proof-worker".into(),
        available: ResourceRequest::default(),
        labels: BTreeMap::new(),
    };
    let revision = reopened.revision();
    assert!(matches!(
        reopened
            .pull_owned(&owner, &replacement, &replaced.verifier(&mut None).expect("verifier copy"), &worker, Some(operation_id), &mut None)
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(reopened.revision(), revision);
    assert!(
        reopened
            .scheduler()
            .operation(operation_id)
            .is_some_and(|state| state.reservation.is_none())
    );
    assert_eq!(
        observe_declaration(
            &client,
            &owner,
            &renewed,
            &issuer.verifier(&mut None).expect("verifier copy"),
            operation_id,
            &key
        , &mut None)
        .await?,
        Some(original.clone())
    );
    assert_eq!(
        reopened
            .declare_operation(&owner, &renewed, &issuer.verifier(&mut None).expect("verifier copy"), original, key, None, &mut None)
            .await?,
        false
    );
    assert!(matches!(
        reopened
            .pull_owned(&owner, &renewed, &issuer.verifier(&mut None).expect("verifier copy"), &worker, Some(operation_id), &mut None)
            .await?,
        WorkPull::Claimed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn initial_state_must_reside_before_declaration_is_published() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let operation_id = OperationId::from_bytes([51; 16]);
    let mut unbound =
        DistributedCoordinator::open(&client, Arc::new(DenyContentVerifier), None, &mut None).await?;
    assert!(matches!(
        declare(&mut unbound, spec(operation_id, 0)?, "unbound-state").await,
        Err(Error::Unsupported(_))
    ));
    assert!(unbound.scheduler().operation(operation_id).is_none());

    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let mut corrupt = spec(operation_id, 0)?;
    corrupt.state = FileRef::new(
        corrupt.state.volume().clone(),
        "state/initial.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(b"different", "application/json", &mut None)?,
        "initial.json".into(), &mut None,
    )?;
    assert!(matches!(
        declare(&mut coordinator, corrupt, "corrupt-state").await,
        Err(Error::Invalid(_))
    ));
    assert!(coordinator.scheduler().operation(operation_id).is_none());

    assert_eq!(
        declare(&mut coordinator, spec(operation_id, 0)?, "valid-state").await?,
        true
    );
    assert!(matches!(
        DistributedCoordinator::open(&client, Arc::new(DenyContentVerifier), None, &mut None).await,
        Err(Error::Unsupported(_))
    ));
    Ok(())
}

#[tokio::test]
async fn declarations_require_authenticated_matching_owner_and_capability() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let operation_id = OperationId::from_bytes([52; 16]);
    let declaration = spec(operation_id, 0)?;
    assert!(matches!(
        coordinator
            .apply(
                IdempotencyKey::new("generic-declare")?,
                coordinator.scheduler().declare(declaration.clone())?
            , &mut None)
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(coordinator.apply(IdempotencyKey::new("forged-reduction")?,
        SchedulerEvent::Orchestrated {
            operation_id, expected_revision: 0, outcome: crate::Outcome::Succeeded(state_ref()?),
            cancel: Vec::new(), reducer: Some(EntrypointRef {
                name: "example.reducer".into(), version: "1".into(), digest: [3; 32],
                result_schema: Value::Object(Default::default()),
            }), reduction_digest: Some([1; 32]),
        }, &mut None).await, Err(Error::Unauthorized(_))));
    let owner = Authority {
        kind: AggregateKind::Task,
        id: "owner".into(),
    };
    let other = Authority {
        kind: AggregateKind::Task,
        id: "other".into(),
    };
    let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    let granted = issuer.issue("declare", &Capabilities::new(["operation:declare"]), None, None, &mut None).expect("scope proof");
    let empty = issuer.issue("empty", &Capabilities::new([] as [&str; 0]), None, None, &mut None).expect("scope proof");
    assert!(matches!(
        coordinator
            .declare_operation(
                &owner,
                &empty,
                &issuer.verifier(&mut None).expect("verifier copy"),
                declaration.clone(),
                IdempotencyKey::new("missing-capability")?,
                None, &mut None,
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        coordinator
            .declare_operation(
                &other,
                &granted,
                &issuer.verifier(&mut None).expect("verifier copy"),
                declaration.clone(),
                IdempotencyKey::new("wrong-audience")?,
                None, &mut None,
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    let mut declaration = declaration;
    declaration.owner = DurableOwner::Detached { authority: other };
    assert!(matches!(
        coordinator
            .declare_operation(
                &owner,
                &granted,
                &issuer.verifier(&mut None).expect("verifier copy"),
                declaration,
                IdempotencyKey::new("wrong-owner")?,
                None, &mut None,
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert!(coordinator.scheduler().operation(operation_id).is_none());
    Ok(())
}

#[tokio::test]
async fn another_owner_cannot_attach_a_child_to_a_foreign_parent() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let parent = OperationId::from_bytes([54; 16]);
    let child = OperationId::from_bytes([55; 16]);
    declare(&mut coordinator, spec(parent, 0)?, "owned-parent").await?;
    let mut foreign_child = spec(child, 0)?;
    foreign_child.owner = DurableOwner::Detached {
        authority: Authority {
            kind: AggregateKind::Task,
            id: "foreign".into(),
        },
    };
    foreign_child.owner_scope = declaration_scope(foreign_child.owner.authority());
    foreign_child.parent = Some(ParentLink {
        operation_id: parent,
        slot: "uninvited-child".into(),
    });
    assert!(matches!(
        declare(&mut coordinator, foreign_child, "foreign-parent-link").await,
        Err(Error::NotFound(_))
    ));
    assert!(coordinator.scheduler().operation(child).is_none());
    assert_eq!(coordinator.scheduler().children(parent).count(), 0);
    Ok(())
}

#[tokio::test]
async fn owner_observes_bounded_children_with_revision_checked_continuation() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let (owner, issuer) = owner_issuer();
    let scope = issuer.issue("observe", &Capabilities::new(["operation:observe"]), None, None, &mut None).expect("scope proof");
    let parent = OperationId::from_bytes([56; 16]);
    declare(&mut coordinator, spec(parent, 0)?, "hierarchy-parent").await?;
    for (id, slot) in [(57_u8, "a"), (58_u8, "b")] {
        let mut child = spec(OperationId::from_bytes([id; 16]), 0)?;
        child.parent = Some(ParentLink {
            operation_id: parent,
            slot: slot.into(),
        });
        declare(&mut coordinator, child, &format!("hierarchy-{slot}")).await?;
    }
    let first = coordinator
        .observe_children(
            &owner,
            &scope,
            &issuer.verifier(&mut None).expect("verifier copy"),
            parent,
            None, None, 1,
         &mut None,)
        .await.map(|(page, _)| page)?;
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].slot, "a");
    assert_eq!(first.next_after.as_deref(), Some("a"));
    let second = coordinator
        .observe_children(
            &owner,
            &scope,
            &issuer.verifier(&mut None).expect("verifier copy"),
            parent,
            Some(first.revision), first.next_after.as_deref(), 1,
         &mut None,)
        .await.map(|(page, _)| page)?;
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].slot, "b");
    assert!(second.next_after.is_none());
    let mut late = spec(OperationId::from_bytes([59; 16]), 0)?;
    late.parent = Some(ParentLink {
        operation_id: parent,
        slot: "c".into(),
    });
    let mut peer = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    declare(&mut peer, late, "hierarchy-c").await?;
    assert!(matches!(
        coordinator
            .observe_children(
                &owner,
                &scope,
                &issuer.verifier(&mut None).expect("verifier copy"),
                parent,
                Some(first.revision), first.next_after.as_deref(), 1
            , &mut None)
            .await.map(|(page, _)| page),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn projector_reads_bounded_verified_coordinator_pages() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    assert!(read_coordinator_event_page(&client, 0, 1).await?.is_empty());
    let operation_id = OperationId::from_bytes([41; 16]);
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    declare(
        &mut coordinator,
        spec(operation_id, 1)?,
        "projector-declare",
    )
    .await?;
    let page = read_coordinator_event_page(&client, 0, 1).await?;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].operation_id(), operation_id);
    assert!(matches!(page[0], SchedulerEvent::Declared { .. }));
    let first_time = coordinator.last_committed_at_ms;
    assert!(first_time > 0);
    let second = OperationId::from_bytes([42; 16]);
    declare(&mut coordinator, spec(second, 1)?, "projector-second").await?;
    let next = read_coordinator_event_page(&client, 1, 1).await?;
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].operation_id(), second);
    assert!(coordinator.last_committed_at_ms > first_time);
    let mut reopened =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let third = OperationId::from_bytes([43; 16]);
    declare(&mut reopened, spec(third, 1)?, "projector-third").await?;
    let last = read_coordinator_event_page(&client, 2, 1).await?;
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].operation_id(), third);
    assert!(reopened.last_committed_at_ms > coordinator.last_committed_at_ms);
    assert!(read_coordinator_event_page(&client, 3, 1).await?.is_empty());
    assert!(read_coordinator_event_page(&client, 0, 0).await.is_err());
    let repeated = SchedulerEvent::Declared { spec: Box::new(spec(third, 1)?) };
    let canonical = crate::contract::canonical_json_bytes(&repeated)?;
    let bytes = encode(4, third, "projector-third", *blake3::hash(&canonical).as_bytes(),
        canonical, reopened.last_committed_at_ms + 1, &mut None)?;
    Stream::new(client.clone(), COORDINATOR_PATH)?.append_batch(vec![OwnedBytes::from(bytes)], Some(3), None).await?;
    assert!(matches!(reopened.refresh(&mut None).await, Err(Error::Conflict(_))));
    assert_eq!(reopened.revision, 3);
    Ok(())
}
