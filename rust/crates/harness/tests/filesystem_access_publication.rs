use super::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn cached_metadata_requires_list_independently_of_read(
        list in any::<bool>(), read in any::<bool>(),
        path in any::<bool>(), expired in any::<bool>(), denied in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let fixture = Fixture::new().await?;
            let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
            policy.version = 2;
            if denied {
                let mut deny = policy.layers[0][0].clone();
                deny.id = "hide-cached-metadata".into();
                deny.effect = Effect::Deny;
                deny.actions = [Action::List].into_iter().collect();
                deny.resource.path = "secrets/**".into();
                policy.layers[0].push(deny);
            }
            fixture.publish_policy(policy).await?;
            let capability = Capability {
                layers: vec![vec![Grant {
                    actions: std::iter::once(Action::ReadHistory)
                        .chain(list.then_some(Action::List)).chain(read.then_some(Action::Read)).collect(),
                    resource: ResourcePattern {
                        lineage_root: None, volume: None, generations: Generations::All,
                        path: if path { "secrets/**" } else { "public/**" }.into(),
                    },
                }]], expires_at: expired.then_some(1),
            };
            let scope = fixture.issuer.issue("observe", &Capabilities::new([
                fixture.volume.capability(VolumeOperation::Write, &mut None)?,
                capability.to_scope_label()?,
            ]), fixture.scope.agent(), None, &mut None)?;
            let grant = ContentGrant::verify(&fixture.issuer.verifier(&mut None)?, &scope,
                &fixture.volume, VolumeOperation::Write, 4096, &mut None)?;
            let result = fixture.host.content_for_operation(&fixture.volume, &grant,
                &IdempotencyKey::new("file")?, "secrets/key", 4096, None, None).await;
            let allowed = list && path && !expired && !denied;
            assert_eq!(result.is_ok(), allowed, "{result:?}");
            if allowed {
                assert_eq!(result?, Some(fixture.file));
            } else {
                assert_eq!(result.unwrap_err().code(), acyclic_harness::wire::ErrorCode::Unauthorized);
            }
            Ok::<_, Box<dyn Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
async fn legacy_write_grant_does_not_disclose_cached_metadata() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    for read in [false, true] {
        let grants = std::iter::once(
            fixture
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        )
        .chain(
            read.then(|| fixture.volume.capability(VolumeOperation::Read, &mut None))
                .transpose()?,
        );
        let scope = fixture.issuer.issue(
            "legacy-observe",
            &Capabilities::new(grants),
            fixture.scope.agent(),
            None,
            &mut None,
        )?;
        let grant = ContentGrant::verify(
            &fixture.issuer.verifier(&mut None)?,
            &scope,
            &fixture.volume,
            VolumeOperation::Write,
            4096,
            &mut None,
        )?;
        let result = fixture
            .host
            .content_for_operation(
                &fixture.volume,
                &grant,
                &IdempotencyKey::new("file")?,
                "secrets/key",
                4096,
                None,
                None,
            )
            .await;
        assert_eq!(result.is_ok(), read);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn cached_metadata_releases_original_ledger_custody_on_allow_and_deny()
-> Result<(), Box<dyn Error>> {
    use acyclic_harness::conversation::ContentPublisher;
    let fixture = Fixture::new().await?;
    let writer = acyclic_harness::filesystem::FilesystemContentPublisher::new(
        fixture.host.clone(),
        &fixture.volume,
        &fixture.issuer.verifier(&mut None)?,
        &fixture.scope,
        4096,
        None,
    )?;
    let operation = acyclic_harness::OperationId::new();
    let _ = writer
        .stage_once(
            operation,
            "secrets/cached",
            b"cached",
            "text/plain",
            "cached",
            None,
            None,
        )?
        .await?;
    let cancel = CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancel,
    )
    .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let _ = work.receipt();
    let baseline = work.live_allocation_bytes();
    for denied in [false, true] {
        let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
        policy.version = if denied { 3 } else { 2 };
        if denied {
            let mut deny = policy.layers[0][0].clone();
            deny.id = "hide-cache".into();
            deny.effect = Effect::Deny;
            deny.actions = [Action::List].into_iter().collect();
            deny.resource.path = "secrets/**".into();
            policy.layers[0].push(deny);
        }
        fixture.publish_policy(policy).await?;
        let mut retained = None;
        for _ in 0..2 {
            let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?;
            let result = writer
                .observe_staged_once(operation, 4096, None, Some(&io))?
                .await;
            assert_eq!(result.is_ok(), !denied);
            drop(result);
            drop(io);
            let _ = work.receipt();
            if let Some(previous) = retained.replace(work.live_allocation_bytes()) {
                assert_eq!(
                    work.live_allocation_bytes(),
                    previous,
                    "warm metadata retains no request storage"
                );
            }
        }
    }
    drop(writer);
    drop(fixture);
    let _ = work.receipt();
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
