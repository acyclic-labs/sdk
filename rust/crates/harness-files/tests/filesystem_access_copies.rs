use super::*;

fn agrees(allowed: bool, succeeded: bool, contents: Option<&[u8]>) -> bool {
    succeeded == allowed && contents == allowed.then_some(b"private".as_slice())
}

pub(super) fn legacy<O>(
    fixture: &Fixture<O>,
    volume: &VolumeRef,
    operation: VolumeOperation,
) -> Result<ContentGrant, Box<dyn Error>> {
    let scope = fixture.issuer.bootstrap_scope(
        "whole-volume",
        &Capabilities::new([volume.capability(operation, &mut None)?]),
        fixture.scope.agent(),
        &mut None,
    )?;
    Ok(ContentGrant::verify(
        &fixture.issuer.verifier(&mut None)?,
        &scope,
        volume,
        operation,
        &mut None,
    )?)
}

fn signed(
    fixture: &Fixture,
    volume: &VolumeRef,
    operation: VolumeOperation,
    actions: &[Action],
    path: &str,
    resource: Option<String>,
) -> Result<ContentGrant, Box<dyn Error>> {
    let scope = fixture.issuer.bootstrap_scope(
        "copy",
        &Capabilities::new([Capability {
            layers: vec![vec![Grant {
                actions: actions.iter().copied().collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: resource,
                    generations: Generations::All,
                    path: path.into(),
                },
            }]],
            expires_at: None,
        }
        .to_scope_label()?]),
        fixture.scope.agent(),
        &mut None,
    )?;
    Ok(ContentGrant::verify(
        &fixture.issuer.verifier(&mut None)?,
        &scope,
        volume,
        operation,
        &mut None,
    )?)
}

async fn copies(
    action: bool,
    volume: bool,
    path: bool,
    clock_ahead: bool,
) -> Result<(), Box<dyn Error>> {
    let fixture = if clock_ahead {
        use acyclic_stream::UnixMillisClock as _;
        let start = acyclic_stream::SystemUnixMillisClock.now_unix_millis() + 5_000;
        let (clock, _control) = acyclic_actors::local::ControlledClock::new(start);
        Fixture::with_clock(Arc::new(clock)).await?
    } else {
        Fixture::new().await?
    };
    let destination = VolumeRef::new(
        fixture.volume.provider().clone(),
        "copy-target".into(),
        VolumeClass::Project,
        VolumeOwner::Project("owner".into()),
        &mut None,
    )?;
    let destination_grant = legacy(&fixture, &destination, VolumeOperation::Write)?;
    let source = signed(
        &fixture,
        &fixture.volume,
        VolumeOperation::Read,
        if action {
            &[Action::List, Action::Read, Action::Fork]
        } else {
            &[Action::Write]
        },
        if path { "**" } else { "public/**" },
        (!volume).then(|| "another-volume".into()),
    )?;
    let result = fixture
        .host
        .fork_volume(
            &fixture.volume,
            &source,
            &fixture.generation()?,
            &destination,
            &destination_grant,
            None,
            &IdempotencyKey::new("copy")?,
        )
        .await;
    let opened = fixture
        .fs
        .open_workspace(
            destination.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await;
    let contents = match opened {
        Ok(opened) => Some(
            opened
                .value
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
                .value
                .as_ref()
                .to_vec(),
        ),
        Err(_) => None,
    };
    assert!(
        agrees(
            action && volume && path,
            result.is_ok(),
            contents.as_deref()
        ),
        "copy decision or bytes differ from signed authority"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn signed_forks_require_actions_volume_and_every_path(
        action in any::<bool>(), volume in any::<bool>(), path in any::<bool>(),
        clock_ahead in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().build()?.block_on(copies(action, volume, path, clock_ahead))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
async fn imports_check_both_signed_grants_through_the_policy_compiler() -> Result<(), Box<dyn Error>>
{
    for (source_ok, destination_ok) in [(true, true), (false, true), (true, false)] {
        let fixture = Fixture::new().await?;
        let destination = VolumeRef::new(
            fixture.volume.provider().clone(),
            "import-target".into(),
            VolumeClass::Project,
            VolumeOwner::Project("owner".into()),
            &mut None,
        )?;
        // A merge target must share lineage with its source. This setup uses
        // the public, already-authorized fork before testing narrowed imports.
        fixture
            .host
            .fork_volume(
                &fixture.volume,
                &legacy(&fixture, &fixture.volume, VolumeOperation::Read)?,
                &fixture.generation()?,
                &destination,
                &legacy(&fixture, &destination, VolumeOperation::Write)?,
                None,
                &IdempotencyKey::new("import-base")?,
            )
            .await?;
        let source = signed(
            &fixture,
            &fixture.volume,
            VolumeOperation::Read,
            &[Action::Read, Action::List],
            if source_ok { "**" } else { "public/**" },
            None,
        )?;
        let target = signed(
            &fixture,
            &destination,
            VolumeOperation::Write,
            &[Action::MergeInto],
            if destination_ok { "**" } else { "artifacts/**" },
            None,
        )?;
        let result = fixture
            .host
            .prepare_volume_import(
                &fixture.volume,
                &source,
                &fixture.generation()?,
                &destination,
                &target,
            )
            .await;
        assert_eq!(result.is_ok(), source_ok && destination_ok);
    }
    Ok(())
}
