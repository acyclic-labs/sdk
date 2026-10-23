use super::*;
use acyclic_harness::filesystem::{PreparationIo, WorkspaceMutation, workspace_ref};

fn writer(fixture: &Fixture, scope: &Scope) -> Result<ContentGrant, Box<dyn Error>> {
    Ok(ContentGrant::verify(
        &fixture.issuer.verifier(&mut None)?,
        scope,
        &fixture.volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )?)
}

fn write_scope(
    fixture: &Fixture,
    actions: &[Action],
    path: &str,
    expired: bool,
) -> Result<Scope, Box<dyn Error>> {
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: actions.iter().copied().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::Head,
                path: path.into(),
            },
        }]],
        expires_at: expired.then_some(1),
    };
    Ok(fixture.issuer.issue(
        "write",
        &Capabilities::new([
            fixture
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
            capability.to_scope_label()?,
        ]),
        fixture.scope.agent(),
        None,
        &mut None,
    )?)
}

async fn put(
    fixture: &Fixture,
    grant: &ContentGrant,
    path: &str,
    key: &str,
    io: Option<&PreparationIo>,
) -> acyclic_harness::Result<FileRef> {
    fixture
        .host
        .put_content(
            &fixture.volume,
            grant,
            path,
            b"new",
            "text/plain",
            "new",
            4096,
            &IdempotencyKey::new(key)?,
            None,
            acyclic_harness::conversation::ContentAccess { ceiling: None, io },
        )
        .await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn writes_are_policy_intersection_capability_and_exact_operation(
        existing in any::<bool>(),
        create in any::<bool>(),
        write in any::<bool>(),
        path_granted in any::<bool>(),
        expired in any::<bool>(),
        denied in any::<bool>(),
        measured in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let fixture = Fixture::new().await?;
            if denied {
                let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
                policy.version = 2;
                let mut deny = policy.layers.first_mut().and_then(|layer| layer.first_mut()).ok_or(acyclic_fs::access::InvalidPolicy("missing fixture rule"))?.clone();
                deny.id = "no-mutations".into();
                deny.effect = Effect::Deny;
                deny.actions = [Action::Create, Action::Write].into_iter().collect();
                deny.resource.path = "secrets/**".into();
                policy.layers.first_mut().ok_or(acyclic_fs::access::InvalidPolicy("missing fixture layer"))?.push(deny);
                fixture.publish_policy(policy).await?;
            }
            let actions: Vec<_> = create.then_some(Action::Create).into_iter()
                .chain(write.then_some(Action::Write)).collect();
            let scope = write_scope(&fixture, &actions, if path_granted { "**" } else { "public/**" }, expired)?;
            let grant = writer(&fixture, &scope)?;
            let reference = workspace_ref(fixture.volume.provider().clone(), &fixture.volume.storage_name(&mut None)?)?;
            let before = fixture.host.resolve(&reference, None).await?.generation;
            let io = measured.then(|| PreparationIo::from_budget(
                acyclic_fs::performance::BOUNDARY_BUDGET, &CancellationToken::new(),
            )).transpose()?;
            let path = if existing { "secrets/key" } else { "secrets/new" };
            let result = put(&fixture, &grant, path, "attempt", io.as_ref()).await;
            let allowed = path_granted && !expired && !denied && if existing { write } else { create };
            assert_eq!(result.is_ok(), allowed, "{result:?}");
            if allowed {
                assert_eq!(fixture.host.read(&reference, None, &format!("/{path}"), 4096, None).await?.as_ref(), b"new");
            } else {
                assert_eq!(result.unwrap_err().code(), acyclic_harness::wire::ErrorCode::Unauthorized);
                assert_eq!(fixture.host.resolve(&reference, None).await?.generation, before);
                assert_eq!(fixture.host.read(&reference, None, "/secrets/key", 4096, None).await?.as_ref(), b"private");
            }
            Ok::<_, Box<dyn Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
async fn inbox_creation_is_idempotent_without_read_list_or_overwrite() -> Result<(), Box<dyn Error>>
{
    let fixture = Fixture::new().await?;
    let scope = write_scope(&fixture, &[Action::Create], "**", false)?;
    let grant = writer(&fixture, &scope)?;
    let created = put(&fixture, &grant, "inbox/message", "create", None).await?;
    assert_eq!(
        put(&fixture, &grant, "inbox/message", "create", None).await?,
        created
    );
    assert!(
        put(&fixture, &grant, "inbox/message", "replace", None)
            .await
            .is_err()
    );
    assert!(
        fixture
            .reader(&scope)?
            .read(&created, 4096, None, None)?
            .await
            .is_err()
    );
    // Permission on the child cannot implicitly grant creation of its parent.
    let scope = write_scope(&fixture, &[Action::Create], "missing/child", false)?;
    assert!(
        put(
            &fixture,
            &writer(&fixture, &scope)?,
            "missing/child",
            "missing",
            None
        )
        .await
        .is_err()
    );
    // Append authority does not authorize a complete-file replacement or creation.
    let scope = write_scope(&fixture, &[Action::Append], "**", false)?;
    for path in ["secrets/key", "secrets/new"] {
        assert!(
            put(&fixture, &writer(&fixture, &scope)?, path, "append", None)
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn revocation_blocks_legacy_writes_and_cached_operation_replies() -> Result<(), Box<dyn Error>>
{
    let fixture = Fixture::new().await?;
    let grant = writer(&fixture, &fixture.scope)?;
    let _ = put(&fixture, &grant, "secrets/new", "original", None).await?;
    let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
    policy.version = 2;
    let mut deny = policy
        .layers
        .first_mut()
        .and_then(|layer| layer.first_mut())
        .ok_or(acyclic_fs::access::InvalidPolicy("missing fixture rule"))?
        .clone();
    deny.id = "revoked".into();
    deny.effect = Effect::Deny;
    deny.actions = [Action::Create, Action::Write].into_iter().collect();
    deny.resource.path = "secrets/**".into();
    policy
        .layers
        .first_mut()
        .ok_or(acyclic_fs::access::InvalidPolicy("missing fixture layer"))?
        .push(deny);
    fixture.publish_policy(policy).await?;
    for key in ["original", "new-attempt"] {
        assert_eq!(
            put(&fixture, &grant, "secrets/new", key, None)
                .await
                .unwrap_err()
                .code(),
            acyclic_harness::wire::ErrorCode::Unauthorized
        );
    }
    let missing = Fixture::with_policy(false).await?;
    assert_eq!(
        put(
            &missing,
            &writer(&missing, &missing.scope)?,
            "new",
            "missing-policy",
            None
        )
        .await
        .unwrap_err()
        .code(),
        acyclic_harness::wire::ErrorCode::Unauthorized
    );
    Ok(())
}

#[tokio::test]
async fn publication_refuses_a_generation_older_than_its_transaction_head()
-> Result<(), Box<dyn Error>> {
    for measured in [false, true] {
        let fixture = Fixture::new().await?;
        let prior = fixture.generation()?;
        fixture.policy(false, 2).await?;
        let workspace = workspace_ref(
            fixture.volume.provider().clone(),
            &fixture.volume.storage_name(&mut None)?,
        )?;
        let before = fixture.host.resolve(&workspace, None).await?.generation;
        let grant = writer(&fixture, &fixture.scope)?;
        let io = measured
            .then(|| {
                PreparationIo::from_budget(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &CancellationToken::new(),
                )
            })
            .transpose()?;
        let result = fixture
            .host
            .put_content(
                &fixture.volume,
                &grant,
                "secrets/key",
                b"new",
                "text/plain",
                "key",
                4096,
                &IdempotencyKey::new("stale")?,
                Some(&prior),
                acyclic_harness::conversation::ContentAccess {
                    ceiling: None,
                    io: io.as_ref(),
                },
            )
            .await;
        assert_eq!(
            result.unwrap_err().code(),
            acyclic_harness::wire::ErrorCode::Conflict
        );
        let result = fixture
            .host
            .apply(
                &workspace,
                Some(&prior),
                &[WorkspaceMutation::PutFile {
                    path: "/secrets/key".into(),
                    bytes: bytes::Bytes::from_static(b"stale").into(),
                }],
                &IdempotencyKey::new("stale-direct")?,
            )
            .await;
        assert_eq!(
            result.unwrap_err().code(),
            acyclic_harness::wire::ErrorCode::Conflict
        );
        assert_eq!(
            fixture.host.resolve(&workspace, None).await?.generation,
            before
        );
    }
    Ok(())
}

#[tokio::test]
async fn publication_retry_verifies_the_original_operation_after_head_advance()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let workspace = workspace_ref(
        fixture.volume.provider().clone(),
        &fixture.volume.storage_name(&mut None)?,
    )?;
    let base = fixture.generation()?;
    let key = IdempotencyKey::new("verified-retry")?;
    let mutations = [WorkspaceMutation::PutFile {
        path: "/secrets/key".into(),
        bytes: bytes::Bytes::from_static(b"original operation").into(),
    }];
    let first = fixture
        .host
        .apply(&workspace, Some(&base), &mutations, &key)
        .await?;
    fixture.policy(false, 2).await?;
    let head = fixture.host.resolve(&workspace, None).await?.generation;
    assert_eq!(
        fixture
            .host
            .apply(&workspace, Some(&base), &mutations, &key)
            .await?,
        first
    );
    let changed = [WorkspaceMutation::PutFile {
        path: "/secrets/key".into(),
        bytes: bytes::Bytes::from_static(b"different operation").into(),
    }];
    assert_eq!(
        fixture
            .host
            .apply(&workspace, Some(&base), &changed, &key)
            .await
            .unwrap_err()
            .code(),
        acyclic_harness::wire::ErrorCode::Conflict
    );
    assert_eq!(
        fixture.host.resolve(&workspace, None).await?.generation,
        head
    );
    Ok(())
}
