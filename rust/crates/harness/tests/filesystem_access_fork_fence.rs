use super::*;

fn assert_fenced(revocation: &Result<(), Box<dyn Error>>) {
    assert!(
        revocation.as_ref().err().is_some_and(|error| matches!(
            error.downcast_ref::<acyclic_fs::OperationFailure<acyclic_fs::FsError>>(),
            Some(failure) if matches!(failure.error, acyclic_fs::FsError::Publication(
                acyclic_fs::kernel::PublicationError::ActiveProjection
            ))
        )),
        "source policy changed while fork destination was being published: {revocation:?}"
    );
}

async fn racing_fork(protected: bool, action: Action) -> Result<(), Box<dyn Error>> {
    let objects = objects::CountingObjects::default();
    let fs = Fs::new(
        acyclic_fs::StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default())),
        objects.clone(),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let fixture = Fixture::with_filesystem(fs, true).await?;
    let target = VolumeRef::new(
        fixture.volume.provider().clone(),
        "fork-target".into(),
        VolumeClass::Project,
        VolumeOwner::Project("owner".into()),
        &mut None,
    )?;
    let source = copies::legacy(&fixture, &fixture.volume, VolumeOperation::Read)?;
    let destination = copies::legacy(&fixture, &target, VolumeOperation::Write)?;
    let workspace = fixture
        .fs
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let (entered, release) = objects.pause_write(acyclic_fs::ObjectKind::GenerationRoot);
    let copy = async {
        if protected {
            fixture
                .host
                .fork_volume(
                    &fixture.volume,
                    &source,
                    &fixture.generation()?,
                    &target,
                    &destination,
                    None,
                    &IdempotencyKey::new("fenced-fork")?,
                )
                .await?;
        } else {
            // Deliberately run the former unfenced publication path. The same
            // checker below must reject the observable policy change.
            let pinned = workspace
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await?
                .value;
            workspace
                .fork::<_, _, acyclic_fs::WorkspaceError>(
                    target.storage_name(&mut None)?,
                    acyclic_fs::ForkOptions::from_generation(
                        pinned,
                        acyclic_fs::IdempotencyKey::new(),
                    ),
                    acyclic_fs::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                    &mut |_| {
                        std::future::ready(Ok(acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::UNCHARGED,
                        }))
                    },
                )
                .await?;
        }
        Ok::<_, Box<dyn Error>>(())
    };
    let publish = async {
        entered.await?;
        let mut policy: Policy = serde_json::from_slice(INITIAL_POLICY)?;
        policy.version += 1;
        let mut deny = policy.layers[0][0].clone();
        deny.id = "revoked-during-fork".into();
        deny.effect = Effect::Deny;
        deny.actions = [action].into_iter().collect();
        policy.layers[0].push(deny);
        let result = fixture.publish_policy(policy.clone()).await;
        release
            .send(())
            .map_err(|_| "fork abandoned its storage read")?;
        Ok::<_, Box<dyn Error>>((result, policy))
    };
    let (copied, publication) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        futures::join!(copy, publish)
    })
    .await?;
    copied?;
    let (publication, policy) = publication?;
    let forked = fixture
        .fs
        .open_workspace(
            target.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let bytes = forked
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value
        .read(
            "/secrets/key",
            4096,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    assert_eq!(bytes.as_ref(), b"private");
    assert_fenced(&publication);
    // Retirement releases exclusion; a completed fork does not pin source policy.
    fixture.publish_policy(policy).await?;
    Ok(())
}

#[tokio::test]
async fn source_policy_cannot_change_during_fork_publication() -> Result<(), Box<dyn Error>> {
    for action in [Action::Read, Action::List, Action::Fork] {
        racing_fork(true, action).await?;
    }
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "source policy changed while fork destination")]
async fn unfenced_fork_negative_control() {
    racing_fork(false, Action::Read).await.unwrap();
}

async fn expiring_fork(offset: u64) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: [Action::Read, Action::List, Action::Fork]
                .into_iter()
                .collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: "**".into(),
            },
        }]],
        expires_at: Some(now.saturating_add(offset)),
    };
    let scope = fixture.issuer.issue(
        "expiring",
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
    let target = VolumeRef::new(
        fixture.volume.provider().clone(),
        "no-expiring-copy".into(),
        VolumeClass::Project,
        VolumeOwner::Project("owner".into()),
        &mut None,
    )?;
    let destination = copies::legacy(&fixture, &target, VolumeOperation::Write)?;
    let result = fixture
        .host
        .fork_volume(
            &fixture.volume,
            &source,
            &fixture.generation()?,
            &target,
            &destination,
            None,
            &IdempotencyKey::new("expiring-fork")?,
        )
        .await;
    assert!(matches!(
        result,
        Err(acyclic_harness::Error::FilesystemAccessConfiguration(
            "whole-volume copy requires non-expiring authority"
        ))
    ));
    assert!(
        fixture
            .fs
            .open_workspace(
                target.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn expiring_fork_authority_never_creates_a_destination(offset in 60u64..=u64::MAX) {
        tokio::runtime::Builder::new_current_thread().build()?.block_on(expiring_fork(offset))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
