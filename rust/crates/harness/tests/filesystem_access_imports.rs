use super::*;

async fn revoked_import(protected: bool, action: Action) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let target = VolumeRef::new(
        fixture.volume.provider().clone(),
        "import-cut".into(),
        VolumeClass::Project,
        VolumeOwner::Project("owner".into()),
        &mut None,
    )?;
    let source = copies::legacy(&fixture, &fixture.volume, VolumeOperation::Read)?;
    let writer = copies::legacy(&fixture, &fixture.volume, VolumeOperation::Write)?;
    let destination = copies::legacy(&fixture, &target, VolumeOperation::Write)?;
    fixture
        .host
        .fork_volume(
            &fixture.volume,
            &source,
            &fixture.generation()?,
            &target,
            &destination,
            None,
            &IdempotencyKey::new("import-base")?,
        )
        .await?;
    let added = fixture
        .host
        .put_content(
            &fixture.volume,
            &writer,
            "new-private",
            b"new-private",
            "text/plain",
            "new-private",
            4096,
            &IdempotencyKey::new("new-private")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let selected = GenerationRef::new(
        fixture.volume.provider(),
        &hex::decode(added.version())?,
        None,
        None,
    )?
    .0;
    let plan = fixture
        .host
        .prepare_volume_import(&fixture.volume, &source, &selected, &target, &destination)
        .await?;
    let cancel = CancellationToken::new();
    let source_workspace = fixture.workspace(&fixture.volume).await?;
    let target_workspace = fixture.workspace(&target).await?;
    let old_plan = if protected {
        None
    } else {
        Some(
            unchecked_join_plan(
                &source_workspace,
                &target_workspace,
                plan.source_head(),
                &cancel,
            )
            .await?,
        )
    };
    let policy = revoked_policy("revoked-after-planning", action)?;
    fixture.publish_policy(policy).await?;
    let options = acyclic_fs::ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: acyclic_fs::IdempotencyKey::new(),
    };
    let applied = match old_plan {
        Some(old) => old
            .apply(
                options,
                Default::default(),
                acyclic_fs::PublicationPermit::Unrestricted,
            )
            .await
            .map_err(|error| acyclic_harness::Error::Storage(error.to_string())),
        None => {
            plan.apply(
                options,
                Default::default(),
                acyclic_fs::PublicationPermit::Unrestricted,
            )
            .await
        }
    };
    let denied = matches!(
        applied,
        Err(acyclic_harness::Error::FilesystemAccessConfiguration(_))
    );
    check_import_result(&target_workspace, plan.target_head(), protected, denied).await
}

#[tokio::test]
async fn import_application_reauthorizes_the_prepared_source() -> Result<(), Box<dyn Error>> {
    for action in [Action::Read, Action::List] {
        revoked_import(true, action).await?;
    }
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "prepared import ignored source revocation")]
async fn raw_join_plan_negative_control() {
    revoked_import(false, Action::Read).await.unwrap();
}

#[tokio::test]
async fn imports_reject_expiring_source_authority() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let target = VolumeRef::new(
        fixture.volume.provider().clone(),
        "expiring-import".into(),
        VolumeClass::Project,
        VolumeOwner::Project("owner".into()),
        &mut None,
    )?;
    let source = copies::legacy(&fixture, &fixture.volume, VolumeOperation::Read)?;
    let destination = copies::legacy(&fixture, &target, VolumeOperation::Write)?;
    fixture
        .host
        .fork_volume(
            &fixture.volume,
            &source,
            &fixture.generation()?,
            &target,
            &destination,
            None,
            &IdempotencyKey::new("expiring-import-base")?,
        )
        .await?;
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: [Action::Read, Action::List].into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: "**".into(),
            },
        }]],
        expires_at: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
                + 60,
        ),
    };
    let scope = fixture.issuer.issue(
        "expiring-import",
        &Capabilities::new([capability.to_scope_label()?]),
        fixture.scope.agent(),
        None,
        &mut None,
    )?;
    let source = ContentGrant::verify(
        &fixture.issuer.verifier(&mut None)?,
        &scope,
        &fixture.volume,
        VolumeOperation::Read,
        65536,
        &mut None,
    )?;
    let result = fixture
        .host
        .prepare_volume_import(
            &fixture.volume,
            &source,
            &fixture.generation()?,
            &target,
            &destination,
        )
        .await;
    assert!(matches!(
        result,
        Err(acyclic_harness::Error::FilesystemAccessConfiguration(
            "whole-volume copy requires non-expiring authority"
        ))
    ));
    Ok(())
}

async fn unchecked_join_plan(
    source_workspace: &acyclic_fs::Workspace<
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
    target_workspace: &acyclic_fs::Workspace<
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
    source_head: acyclic_fs::GenerationId,
    cancel: &CancellationToken,
) -> Result<
    acyclic_fs::workspace::JoinPlan<
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
    Box<dyn Error>,
> {
    Ok(source_workspace
        .join_into(target_workspace)
        .plan_at(
            &source_workspace
                .generation(source_head, WorkBudget::UNBOUNDED, cancel)
                .await?
                .value,
            &target_workspace
                .head(WorkBudget::UNBOUNDED, cancel)
                .await?
                .value,
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await?
        .value)
}

async fn check_import_result(
    target_workspace: &acyclic_fs::Workspace<
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
    target_head: acyclic_fs::GenerationId,
    protected: bool,
    denied: bool,
) -> Result<(), Box<dyn Error>> {
    let head = target_workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let read = head
        .read(
            "/new-private",
            4096,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await;
    if !protected {
        assert_eq!(
            read.as_ref()
                .map_err(|_| "negative control import did not expose copied bytes")?
                .value
                .as_ref(),
            b"new-private"
        );
    }
    assert!(
        denied && head.id() == target_head && read.is_err(),
        "prepared import ignored source revocation"
    );
    Ok(())
}
