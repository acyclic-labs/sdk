use super::*;
use acyclic_harness::runtime::RuntimeScope;

fn query_scope(
    fixture: &Fixture,
    list: bool,
    read: bool,
    expired: bool,
) -> Result<Scope, Box<dyn Error>> {
    let resource = |path: &str| ResourcePattern {
        lineage_root: None,
        volume: None,
        generations: Generations::All,
        path: path.into(),
    };
    let capability = Capability {
        layers: vec![vec![
            Grant {
                actions: [Action::List, Action::ReadHistory].into_iter().collect(),
                resource: resource("secrets"),
            },
            Grant {
                actions: std::iter::once(Action::ReadHistory)
                    .chain(list.then_some(Action::List))
                    .chain(read.then_some(Action::Read))
                    .collect(),
                resource: resource("secrets/key"),
            },
        ]],
        expires_at: expired.then_some(1),
    };
    Ok(fixture.issuer.issue(
        "query",
        &Capabilities::new([
            fixture
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            capability.to_scope_label()?,
        ]),
        fixture.scope.agent(),
        None,
        &mut None,
    )?)
}

#[tokio::test]
async fn runtime_scope_cannot_drop_a_ceiling_and_keep_legacy_authority()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let scoped = fixture.narrowed(false, false)?;
    let legacy = Capabilities::new([fixture
        .volume
        .capability(VolumeOperation::Read, &mut None)?]);
    assert!(!legacy.is_subset_of(scoped.capabilities()));
    assert!(
        fixture
            .issuer
            .attenuate(&scoped, "escape", &legacy)
            .is_err()
    );
    let runtime = RuntimeScope::new(scoped.capabilities().clone(), Default::default())?;
    assert!(runtime.narrow(legacy.clone(), runtime.limits()).is_err());
    let fs = Capabilities::new(
        scoped
            .capabilities()
            .iter()
            .filter(|label| label.starts_with(SCOPE_PREFIX)),
    );
    for reduced in [
        scoped.capabilities().intersect(&legacy),
        scoped.capabilities().without(&fs),
    ] {
        assert!(reduced.iter().all(|label| !is_filesystem_capability(label)));
    }
    // Completely relinquishing filesystem access remains a valid attenuation.
    assert!(Capabilities::default().is_subset_of(scoped.capabilities()));
    Ok(())
}

#[tokio::test]
async fn task_ceiling_reaches_the_broader_bound_reader() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let reader = fixture.reader(&fixture.scope)?;
    for (path, expired, expected) in [
        (true, false, true),
        (false, false, false),
        (true, true, false),
    ] {
        let caller = fixture.narrowed(path, expired)?;
        let task =
            context::content_context(reader.clone(), None, caller.capabilities().clone()).await?;
        assert_eq!(task.read_file(&fixture.file, None).await.is_ok(), expected);
    }
    // The additional caller cannot replace the signed reader's narrower ceiling.
    let bound = fixture.narrowed(false, false)?;
    let task = context::content_context(
        fixture.reader(&bound)?,
        None,
        fixture.scope.capabilities().clone(),
    )
    .await?;
    assert!(task.read_file(&fixture.file, None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn structured_only_runtime_authority_uses_the_filesystem_owner() -> Result<(), Box<dyn Error>>
{
    let fixture = Fixture::new().await?;
    let all = fixture.narrowed(true, false)?;
    let grants = Capabilities::new(
        all.capabilities()
            .iter()
            .filter(|label| label.starts_with(SCOPE_PREFIX)),
    );
    let task = context::content_context(fixture.reader(&fixture.scope)?, None, grants).await?;
    assert_eq!(
        task.read_file(&fixture.file, None).await?.as_ref(),
        b"private"
    );
    fixture.policy(true, 2).await?;
    assert!(task.read_file(&fixture.file, None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn empty_signed_or_caller_ceiling_stays_empty() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let empty = fixture.issuer.issue(
        "empty",
        &Capabilities::new([
            fixture
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            Capability {
                layers: vec![],
                expires_at: None,
            }
            .to_scope_label()?,
        ]),
        fixture.scope.agent(),
        None,
        &mut None,
    )?;
    let all = fixture.narrowed(true, false)?;
    for (signed, caller) in [(&empty, &all), (&all, &empty)] {
        let task =
            context::content_context(fixture.reader(signed)?, None, caller.capabilities().clone())
                .await?;
        assert!(task.read_file(&fixture.file, None).await.is_err());
    }
    Ok(())
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn caller_ceiling_and_its_storage_use_the_original_native_ledger()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let reader = fixture.reader(&fixture.scope)?;
    let cancellation = CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let _ = work.receipt();
    let baseline = work.live_allocation_bytes();
    for allowed in [true, false] {
        let caller = fixture.narrowed_for(
            &[Action::List, Action::Read, Action::ReadHistory],
            allowed,
            false,
        )?;
        let mut retained = None;
        for _ in 0..2 {
            let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?;
            let result = reader
                .read(&fixture.file, 4096, Some(caller.capabilities()), Some(&io))?
                .await;
            assert_eq!(result.is_ok(), allowed);
            drop(result);
            let selected = fixture.generation()?;
            let prefix = reader
                .read_private_prefix(
                    &fixture.volume,
                    "secrets",
                    "secrets/key",
                    &selected,
                    2,
                    acyclic_harness::conversation::ContentAccess {
                        ceiling: Some(caller.capabilities()),
                        io: Some(&io),
                    },
                )?
                .await;
            assert_eq!(prefix.is_ok(), allowed);
            drop(prefix);
            let resolved = reader
                .read_private_path(
                    &fixture.volume,
                    "secrets",
                    "secrets/key",
                    Some(&selected),
                    4096,
                    acyclic_harness::conversation::ContentAccess {
                        ceiling: Some(caller.capabilities()),
                        io: Some(&io),
                    },
                )?
                .await;
            assert_eq!(resolved.is_ok(), allowed);
            drop(resolved);
            drop(io);
            let _ = work.receipt();
            if let Some(previous) = retained.replace(work.live_allocation_bytes()) {
                assert_eq!(
                    work.live_allocation_bytes(),
                    previous,
                    "warm reads retain no request storage"
                );
            }
        }
    }
    // Decoded pages legitimately retain their birth ledger until the cache
    // owner retires. Request allocations must plateau above and all cache
    // custody must return to the exact baseline when that owner is dropped.
    drop(reader);
    drop(fixture);
    let _ = work.receipt();
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn queries_intersect_policy_and_both_caller_ceilings(
        signed_list in any::<bool>(), caller_list in any::<bool>(),
        signed_read in any::<bool>(), caller_read in any::<bool>(),
        signed_expired in any::<bool>(), caller_expired in any::<bool>(),
        denied in any::<bool>(), structured_only in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let fixture = Fixture::new().await?;
            fixture.policy(denied, 2).await?;
            let signed = query_scope(&fixture, signed_list, signed_read, signed_expired)?;
            let caller = query_scope(&fixture, caller_list, caller_read, caller_expired)?;
            let reader = fixture.reader(&signed)?;
            let grants = Capabilities::new(caller.capabilities().iter().filter(|label|
                !structured_only || label.starts_with(SCOPE_PREFIX)));
            let task = context::content_context(reader.clone(), None, grants.clone()).await?;
            let live = !signed_expired && !caller_expired;
            let page = task.list_private_directory(&fixture.volume, "secrets", "secrets", None, None, 8).await;
            assert_eq!(page.is_ok(), live);
            if let Ok(page) = page {
                assert_eq!(page.entries.iter().map(|entry| entry.name.as_str()).collect::<Vec<_>>(),
                    if signed_list && caller_list { vec!["key"] } else { vec![] });
            }
            let selected = fixture.generation()?;
            let resolved = task.read_private_path(&fixture.volume, "secrets", "secrets/key", Some(&selected)).await;
            let readable = signed_read && caller_read && !denied && live;
            assert_eq!(resolved.is_ok(), readable && signed_list && caller_list);
            let prefix = reader.read_private_prefix(&fixture.volume, "secrets", "secrets/key", &selected, 2, acyclic_harness::conversation::ContentAccess { ceiling: Some(&grants), io: None })?.await;
            assert_eq!(prefix.is_ok(), readable);
            if let Ok(prefix) = prefix { assert_eq!(prefix.as_ref(), b"pr"); }
            Ok::<_, Box<dyn Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }

    #[test]
    fn direct_reader_checks_the_presented_legacy_grant(kind in 0usize..6, denied in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let fixture = Fixture::new().await?;
            fixture.policy(denied, 2).await?;
            let grants = match kind {
                0 => Capabilities::default(),
                1 => Capabilities::new([fixture.volume.capability(VolumeOperation::Write, &mut None)?]),
                2 => Capabilities::new([fixture.volume.directory_read_capability("public", &mut None)?]),
                3 => Capabilities::new([fixture.volume.capability(VolumeOperation::Read, &mut None)?]),
                4 => Capabilities::new([fixture.file.read_capability(&mut None)?]),
                _ => Capabilities::new([fixture.volume.directory_read_capability("secrets", &mut None)?]),
            };
            let reader = fixture.reader(&fixture.scope)?;
            assert_eq!(reader.read(&fixture.file, 4096, Some(&grants), None)?.await.is_ok(), kind >= 3 && !denied);
            let directory = kind == 3 || kind == 5;
            let generation = fixture.generation()?;
            assert_eq!(reader.read_private_prefix(&fixture.volume, "secrets", "secrets/key", &generation, 2, acyclic_harness::conversation::ContentAccess { ceiling: Some(&grants), io: None })?.await.is_ok(), directory && !denied);
            assert_eq!(reader.read_private_path(&fixture.volume, "secrets", "secrets/key", Some(&generation), 4096, acyclic_harness::conversation::ContentAccess { ceiling: Some(&grants), io: None })?.await.is_ok(), directory && !denied);
            assert_eq!(ContentResidencyVerifier::list_private_directory(reader.as_ref(), &fixture.volume, "secrets", "secrets", None, None, 8, Some(&grants), None)?.await.is_ok(), directory);
            Ok::<_, Box<dyn Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }

    #[test]
    fn runtime_read_is_bound_policy_and_both_capability_ceilings(
        signed_path in any::<bool>(), caller_path in any::<bool>(),
        signed_expired in any::<bool>(), caller_expired in any::<bool>(), denied in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let fixture = Fixture::new().await?;
            fixture.policy(denied, 2).await?;
            let signed = fixture.narrowed(signed_path, signed_expired)?;
            let caller = fixture.narrowed(caller_path, caller_expired)?;
            let task = context::content_context(fixture.reader(&signed)?, None, caller.capabilities().clone()).await?;
            assert_eq!(task.read_file(&fixture.file, None).await.is_ok(),
                signed_path && caller_path && !signed_expired && !caller_expired && !denied);
            Ok::<_, Box<dyn Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
