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
            None,
            None,
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
    let source_workspace = fixture
        .fs
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let target_workspace = fixture
        .fs
        .open_workspace(
            target.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let old_plan = if protected {
        None
    } else {
        Some(
            source_workspace
                .join_into(&target_workspace)
                .plan_at(
                    &source_workspace
                        .generation(plan.source_head(), WorkBudget::UNBOUNDED, &cancel)
                        .await?
                        .value,
                    &target_workspace
                        .head(WorkBudget::UNBOUNDED, &cancel)
                        .await?
                        .value,
                    WorkBudget::UNBOUNDED,
                    &cancel,
                )
                .await?
                .value,
        )
    };
    let mut policy: Policy = serde_json::from_slice(INITIAL_POLICY)?;
    policy.version += 1;
    let mut deny = policy.layers[0][0].clone();
    deny.id = "revoked-after-planning".into();
    deny.effect = Effect::Deny;
    deny.actions = [action].into_iter().collect();
    policy.layers[0].push(deny);
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
    let head = target_workspace
        .head(WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    let read = head
        .read("/new-private", 4096, WorkBudget::UNBOUNDED, &cancel)
        .await;
    if !protected {
        assert_eq!(read.as_ref().unwrap().value.as_ref(), b"new-private");
    }
    assert!(
        matches!(
            applied,
            Err(acyclic_harness::Error::FilesystemAccessConfiguration(_))
        ) && head.id() == plan.target_head()
            && read.is_err(),
        "prepared import ignored source revocation"
    );
    Ok(())
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
