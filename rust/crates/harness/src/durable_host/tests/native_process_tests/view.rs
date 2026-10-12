use super::*;

#[tokio::test]
async fn issued_mixed_working_view_retains_actual_lazy_project_and_materialized_scratch()
-> Result<()> {
    use std::path::Path;
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000_000, 100_000_000),
        &cancellation,
    )
    .map_err(|(cause, _)| Error::NativeWork {
        cause,
        custody: None,
    })?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "mixed-issued-view",
        true,
        Some(&work),
    )
    .await?;
    assert!(matches!(
        fixture.view.manifest().volumes[0].namespace,
        NativeVolumeNamespace::LazyMount { .. }
    ));
    assert_eq!(
        fixture.view.manifest().volumes[0].volume.class(),
        VolumeClass::Project
    );
    assert!(matches!(
        fixture.view.manifest().volumes[1].namespace,
        NativeVolumeNamespace::MaterializedDirectory
    ));
    let issued = fixture
        .view
        .task_working_view_with_native_work(&fixture.owner, &work)?
        .await?;
    let operation = OperationId::from_bytes([216; 16]);
    let before = work.receipt().source_bytes_read;
    let project = issued
        .read_builtin_file(operation, Path::new("src/cold.txt"), 64, &work)?
        .await?;
    assert_eq!(project.as_slice(), b"genuine cold project");
    assert!(
        project.matches_meter(
            &fixture
                .lazy_work
                .as_ref()
                .expect("original seeded volume work")
                .runtime_meter()
        )
    );
    assert!(work.receipt().source_bytes_read - before >= project.len() as u64);
    drop(project);
    issued.verify_with_native_work(&work)?.await?;
    issued
        .write_builtin_file(
            operation,
            Path::new("dst/issued.txt"),
            "scratch",
            false,
            None,
            &work,
        )?
        .await?;
    let scratch = issued
        .read_builtin_file(operation, Path::new("dst/issued.txt"), 16, &work)?
        .await?;
    assert_eq!(scratch.as_slice(), b"scratch");
    drop(scratch);
    let before = work.receipt().backend_write_operations;
    assert!(
        issued
            .write_builtin_file(
                operation,
                Path::new("src/denied.txt"),
                "denied",
                false,
                None,
                &work
            )?
            .await
            .is_err()
    );
    assert!(
        issued
            .write_builtin_file(
                operation,
                Path::new("results/denied.txt"),
                "denied",
                true,
                None,
                &work
            )?
            .await
            .is_err()
    );
    assert_eq!(work.receipt().backend_write_operations, before);
    let foreign = test_work(
        acyclic_fs::performance::work_budget(100_000_000, 100_000_000),
        &cancellation,
    )?;
    assert!(
        issued
            .read_builtin_file(operation, Path::new("src/cold.txt"), 64, &foreign)
            .is_err()
    );
    assert_eq!(foreign.receipt().source_bytes_read, 0);
    fixture
        .owner
        .verify(
            false,
            &mut Some(&mut crate::store::history::native_work_io(&work)?),
        )
        .await?;
    fixture
        .lazy_mount
        .as_ref()
        .expect("actual fixture lazy mount")
        .retire(
            fixture
                .lazy_work
                .as_ref()
                .expect("original seeded volume work")
                .cancellation(),
        )?;
    Ok(())
}

#[tokio::test]
async fn genuine_view_retirement_preserves_cached_proof_on_admission_and_wait_denial() -> Result<()>
{
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cancellation,
    )?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&original),
    )
    .await?;
    let process = fixture.provider(&original).await?;
    let (status, status_custody) = fixture
        .effects(process.clone())?
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, &original)
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    let cleanup_wait = acyclic_fs::CancellationToken::new();
    let cleanup = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cleanup_wait,
    )?;
    let proof = process
        .drain_with_native_work(
            process.original_attempt(),
            &fixture.view,
            &cleanup,
            &cleanup_wait,
        )?
        .await?;
    let mut cached_process = Some(proof);
    let mut cached_partial = None;
    let mut denied_budget = acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000);
    denied_budget.allocation_operations = 1;
    let denied = test_work(denied_budget, &cleanup_wait)?;
    assert!(
        fixture
            .view
            .cleanup_mounts_with_native_work(
                &fixture.owner,
                &mut cached_process,
                &mut cached_partial,
                &original,
                &denied,
                &cleanup_wait,
            )
            .is_err()
    );
    assert!(cached_process.is_some());
    assert!(cached_partial.is_none());
    let refused_wait = acyclic_fs::CancellationToken::new();
    refused_wait.cancel();
    let source_before = original.receipt();
    assert!(
        fixture
            .view
            .cleanup_mounts_with_native_work(
                &fixture.owner,
                &mut cached_process,
                &mut cached_partial,
                &original,
                &cleanup,
                &refused_wait,
            )?
            .await
            .is_err()
    );
    assert!(cached_process.is_some());
    assert!(cached_partial.is_none());
    assert_eq!(original.receipt(), source_before);
    cancellation.cancel();
    let result = fixture
        .view
        .cleanup_mounts_with_native_work(
            &fixture.owner,
            &mut cached_process,
            &mut cached_partial,
            &original,
            &cleanup,
            &cleanup_wait,
        )?
        .await?;
    let receipt = match result {
        Ok(receipt) => receipt,
        Err(failure) => {
            cached_partial = Some(failure.partial_receipt);
            assert!(cached_partial.is_some());
            return Err(failure.cause);
        }
    };
    assert!(cached_process.is_none());
    assert!(cached_partial.is_none());
    receipt.process_drain_proof().require_view_binding(
        fixture.view.as_ref(),
        fixture.owner.as_ref(),
        &original,
    )?;
    assert!(original.cancellation().is_cancelled());
    assert_eq!(cleanup.receipt().backend_write_operations, 0);
    drop(receipt);
    drop(status);
    drop(status_custody);
    Ok(())
}

#[tokio::test]
async fn issued_working_view_denies_legacy_foreign_work_full_fence_and_stale_owner() -> Result<()> {
    use crate::runtime::DurableTaskHost as _;
    use std::path::Path;
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000_000, 100_000_000),
        &cancellation,
    )
    .map_err(|(cause, _)| Error::NativeWork {
        cause,
        custody: None,
    })?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&work),
    )
    .await?;
    let issued = fixture
        .view
        .task_working_view_with_native_work(&fixture.owner, &work)?
        .await?;
    let foreign_work = test_work(
        acyclic_fs::performance::work_budget(100_000_000, 100_000_000),
        &cancellation,
    )?;
    assert!(matches!(
        issued.verify_with_native_work(&foreign_work),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ..
        })
    ));
    assert_eq!(foreign_work.receipt().backend_write_operations, 0);
    let foreign = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&work),
    )
    .await?;
    assert_ne!(
        fixture.owner.task_binding().1,
        foreign.owner.task_binding().1
    );
    assert!(
        fixture
            .view
            .task_working_view_with_native_work(&foreign.owner, &work)?
            .await
            .is_err()
    );
    let context = fixture
        .task
        .task_context_with_native_work(
            fixture.owner.task_binding().0,
            fixture.owner.task_binding().1,
            &work,
        )
        .await?;
    let guard = context
        .volume_callback_guard_with_native_work(&fixture.owner, &work)
        .await?;
    let callback = guard
        .for_volume(&fixture.owner, &fixture.source, false, &work)?
        .0;
    let writable_callback = guard
        .for_volume(&fixture.owner, &fixture.destination, true, &work)?
        .0;
    let mount_work = work.clone();
    assert!(
        callback
            .check(&mount_work)
            .expect("original callback admission")
            .await
            .is_ok()
    );
    assert!(
        writable_callback
            .check(&mount_work)
            .expect("original writable callback admission")
            .await
            .is_ok()
    );
    let foreign_mount_work = foreign_work.clone();
    assert!(
        callback
            .check(&foreign_mount_work)
            .expect("paid foreign callback frame")
            .await
            .is_err()
    );
    let before_guards = work.receipt();
    assert!(
        guard
            .for_volume(&fixture.owner, &fixture.source, true, &work)
            .is_err()
    );
    assert!(
        guard
            .for_volume(&foreign.owner, &fixture.source, false, &work)
            .is_err()
    );
    assert!(
        guard
            .for_volume(&fixture.owner, &fixture.source, false, &foreign_work)
            .is_err()
    );
    let ungranted = VolumeRef::new(
        fixture.source.provider().clone(),
        "ungranted",
        VolumeClass::AgentPrivate,
        fixture.source.owner().clone(),
    )?;
    assert!(
        guard
            .for_volume(&fixture.owner, &ungranted, false, &work)
            .is_err()
    );
    assert_eq!(
        work.receipt().backend_read_operations,
        before_guards.backend_read_operations
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        before_guards.backend_write_operations
    );
    let legacy = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    assert!(
        legacy
            .view
            .task_working_view_with_native_work(&legacy.owner, &work)?
            .await
            .is_err()
    );
    fixture
        .task
        .task_host()
        .cancel(fixture.owner.task_binding().0, None)
        .await?;
    let before = work.receipt().backend_write_operations;
    assert!(issued.verify_with_native_work(&work)?.await.is_err());
    assert!(
        issued
            .write_builtin_file(
                OperationId::from_bytes([215; 16]),
                Path::new("dst/stale.txt"),
                "denied",
                true,
                None,
                &work
            )?
            .await
            .is_err()
    );
    assert_eq!(work.receipt().backend_write_operations, before);
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/stale.txt")
            .exists()
    );
    let pending = callback
        .check(&mount_work)
        .expect("pre-cancellation callback admission");
    cancellation.cancel();
    let error = pending
        .await
        .expect_err("original cancellation denies callback");
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut original_cancelled = false;
    while let Some(cause) = source {
        if matches!(
            cause.downcast_ref::<Error>(),
            Some(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ) {
            original_cancelled = true;
            break;
        }
        source = cause.source();
    }
    assert!(
        original_cancelled,
        "callback preserves original typed harness cancellation"
    );
    Ok(())
}

#[tokio::test]
async fn publication_races_preserve_output_and_do_not_claim_joint_volume_success() -> Result<()> {
    for mode in ["pause-write", "partial-publication"] {
        let fixture = process_fixture(Arc::new(MemoryStream::default()), mode, true, None).await?;
        let process = fixture.provider(fixture.view.work()).await?;
        let effects = Arc::new(fixture.effects(process.clone())?);
        let running = {
            let effects = effects.clone();
            let owner = fixture.owner.clone();
            let command = fixture.command;
            let plan = fixture.plan.clone();
            tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await })
        };
        let root = &fixture.view.manifest().options.root;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !root.join("publication-ready").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        let changed = if mode == "pause-write" {
            &fixture.destination
        } else {
            &fixture.second_destination
        };
        let grant = ContentGrant::verify(
            &fixture.issuer.verifier(&mut None).expect("verifier copy"),
            &fixture.scope,
            changed,
            VolumeOperation::Write,
        )?;
        fixture
            .files
            .put_content(
                changed,
                &grant,
                "concurrent.txt",
                b"concurrent writer",
                "text/plain",
                "concurrent.txt",
                128,
                &IdempotencyKey::new("concurrent-destination")?,
                None,
                None,
            )
            .await?;
        std::fs::write(root.join("publication-release"), b"release")
            .map_err(|error| Error::Storage(error.to_string()))?;
        let failure_error = running
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .expect_err("actual typed publication failure");
        let Error::NativePublication { failure, .. } = &failure_error else {
            panic!("publication failure keeps its original typed cause and custody");
        };
        assert!(matches!(
            &failure.cause,
            Error::NativeView {
                cause: acyclic_fs::MountLifecycleError::Source(acyclic_fs::MountSourceError::Stale),
                ..
            }
        ));
        assert!(failure.accounting_failure.is_none());
        let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
        let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
        let output = failure
            .diagnostic
            .as_ref()
            .ok_or_else(|| Error::Storage("publication fault lost staged native output".into()))?;
        assert!(process.output(attempt).await?.is_none());
        let receipt = process
            .receipt(attempt)
            .await?
            .ok_or_else(|| Error::Storage("publication fault lost typed receipt".into()))?;
        assert_native_receipt_binding(
            &fixture.owner,
            fixture.command,
            &fixture.plan,
            &receipt,
            &EffectStatus::Indeterminate,
            false,
        )?;
        assert_eq!(
            process.reconcile(attempt).await?,
            Some(receipt.effect.clone()),
        );
        assert!(receipt.diagnostic.is_none());
        assert!(receipt.invocation_failure.is_none());
        assert!(receipt.publication_failure.is_none());
        assert!(receipt.published.is_empty());
        assert!(failure.work.backend_read_operations > 0);
        if mode == "partial-publication" {
            assert!(failure.work.backend_write_operations > 0);
            assert!(failure.work.source_entries_visited > 0);
            assert!(failure.work.source_bytes_read > 0);
        }
        let mapping = |volume: &VolumeRef| {
            fixture
                .view
                .manifest()
                .volumes
                .iter()
                .find(|mapping| &mapping.volume == volume)
                .cloned()
                .ok_or_else(|| Error::Storage("missing admitted destination mapping".into()))
        };
        assert_eq!(failure.destination.as_ref(), Some(&mapping(changed)?));
        assert_eq!(
            failure.completed,
            if mode == "partial-publication" {
                vec![mapping(&fixture.destination)?]
            } else {
                Vec::new()
            }
        );
        let destination_index = fixture
            .view
            .manifest()
            .volumes
            .iter()
            .position(|mapping| mapping.volume == fixture.destination)
            .expect("pinned destination");
        let changed_index = fixture
            .view
            .manifest()
            .volumes
            .iter()
            .position(|mapping| &mapping.volume == changed)
            .expect("pinned failed destination");
        assert!(
            !failure
                .published
                .iter()
                .any(|published| published.index == changed_index),
            "failed destination has no acknowledged final snapshot"
        );
        if mode == "partial-publication" {
            let published = failure
                .published
                .iter()
                .find(|published| published.index == destination_index)
                .expect("actual first destination acknowledgement is retained");
            assert!(published.snapshot.operation().is_some());
        }
        let output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(output).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(output.success && output.stop.is_none());
        assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
        let first = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&first, None, "/output.txt", 64)
                .await
                .is_ok(),
            mode == "partial-publication"
        );
        let changed_ref = workspace_ref(changed.provider().clone(), &changed.storage_name()?)?;
        assert_eq!(
            fixture
                .files
                .read(&changed_ref, None, "/concurrent.txt", 64)
                .await?
                .as_ref(),
            b"concurrent writer"
        );
        assert!(
            fixture
                .files
                .read(&changed_ref, None, "/output.txt", 64)
                .await
                .is_err()
        );
        std::fs::remove_file(root.join("dst/output.txt"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            EffectStatus::Indeterminate,
        );
        assert!(!root.join("dst/output.txt").exists());
        assert_eq!(process.receipt(attempt).await?.as_ref(), Some(&receipt));
        let weak = Arc::downgrade(&process);
        drop((effects, process));
        assert!(weak.upgrade().is_none());
        let recovered = Arc::new(
            Process::recover(
                fixture.owner.clone(),
                fixture.command,
                fixture.plan.clone(),
                fixture.approval(),
                fixture.authority(),
            )
            .await?,
        );
        let effects = fixture.effects(recovered.clone())?;
        assert_eq!(recovered.receipt(attempt).await?.as_ref(), Some(&receipt));
        assert_eq!(
            crate::contract::canonical_json_bytes(&recovered.receipt(attempt).await?)?,
            crate::contract::canonical_json_bytes(&Some(&receipt))?,
        );
        assert_eq!(recovered.output(attempt).await?, receipt.diagnostic,);
        assert_eq!(
            effects
                .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
                .await?,
            EffectStatus::Indeterminate,
        );
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            EffectStatus::Indeterminate
        );
        assert!(!root.join("dst/output.txt").exists());
        assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
        assert_eq!(
            recovered
                .receipt(crate::EffectAttemptId::from_bytes([255; 16]))
                .await?,
            None,
        );
        assert_eq!(
            fixture
                .task
                .task_host()
                .outcome(fixture.owner.task_binding().0)
                .await?,
            None
        );
        assert!(fixture.owner.verify(false, &mut None).await.is_ok());
    }
    Ok(())
}

#[tokio::test]
async fn native_publication_factory_pins_original_generation_and_retains_denied_cleanup()
-> Result<()> {
    for stale in [false, true] {
        let fixture =
            process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let cleanup_cancellation = acyclic_fs::CancellationToken::new();
        let budget = fixture.view.manifest().validation_budget()?;
        let work = test_work(budget, &cancellation)?;
        let destination = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        let workspace = fixture
            .files
            .open_volume_with_native_work(&fixture.destination, &work)
            .await?;
        let mapping = fixture
            .view
            .manifest()
            .volumes
            .iter()
            .find(|mapping| mapping.volume == fixture.destination)
            .ok_or_else(|| Error::NotFound("original destination mapping".into()))?;
        let selected = fixture
            .files
            .generation_with_native_work(&workspace, &mapping.generation, &work)
            .await?;
        let original_generation = selected.id();
        drop(selected);
        let operation = acyclic_fs::OperationId::from_bytes([91; 16]);
        let before_query = work.receipt().backend_write_operations;
        let missing = fixture
            .owner
            .recover_publication(
                &workspace,
                &fixture.destination,
                original_generation,
                operation,
                &work,
                &cancellation,
                &cleanup_cancellation,
            )
            .await?;
        assert!(missing.is_none());
        assert_eq!(work.receipt().backend_write_operations, before_query);
        let recovery_workspace = workspace.clone();
        if stale {
            fixture
                .files
                .write(&destination, "/concurrent.txt", b"new original head")
                .await?;
        }
        let reserved = fixture
            .owner
            .reserve_publication(
                &workspace,
                &fixture.destination,
                original_generation,
                operation,
                &work,
                &cancellation,
                &cleanup_cancellation,
            )
            .await;
        if stale {
            let failed = reserved.err().ok_or_else(|| {
                Error::Storage("stale mapped generation received a permit".into())
            })?;
            let failed = crate::filesystem::NativePublicationReservationFailure::<
                MemoryAuthorityBackend,
                MemoryObjectBackend,
            >::from_error(&failed)
            .expect("whole stale-generation reservation failure");
            assert!(
                failed
                    .error
                    .to_string()
                    .contains("differs from original selection")
            );
            assert!(failed.cleanup.is_none() && failed.cleanup_error.is_none());
            assert!(failed.unwitnessed_workspace.is_none());
            assert!(failed.work.backend_read_operations > 0);
            fixture
                .files
                .write(&destination, "/release-check.txt", b"settled")
                .await?;
        } else {
            let receipt = reserved?;
            assert!(
                matches!(receipt.0, acyclic_fs::PublicationPermit::Reservation {
                operation_id, ..
            } if operation_id == operation.to_bytes())
            );
            let before_query = work.receipt().backend_write_operations;
            let source_mapping = fixture
                .view
                .manifest()
                .volumes
                .iter()
                .find(|mapping| mapping.volume == fixture.source)
                .ok_or_else(|| Error::NotFound("original source generation mapping".into()))?;
            let source_digest = source_mapping
                .generation
                .as_resource()
                .key()
                .try_into()
                .map_err(|_| Error::Invalid("original source generation digest width".into()))?;
            let other_original_generation =
                acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(source_digest));
            assert_ne!(other_original_generation, original_generation);
            let mismatched = fixture
                .owner
                .recover_publication(
                    &recovery_workspace,
                    &fixture.destination,
                    other_original_generation,
                    operation,
                    &work,
                    &cancellation,
                    &cleanup_cancellation,
                )
                .await
                .err()
                .ok_or_else(|| {
                    Error::Storage("readonly query adopted a different original generation".into())
                })?;
            let mismatched_failure = crate::filesystem::NativePublicationReservationFailure::<
                MemoryAuthorityBackend,
                MemoryObjectBackend,
            >::from_error(&mismatched)
            .expect("whole readonly selection failure");
            assert!(matches!(
                mismatched_failure.error,
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    ..
                }
            ));
            assert!(mismatched_failure.cleanup.is_some());
            assert_eq!(
                mismatched_failure.work.backend_write_operations,
                before_query
            );
            drop(mismatched);
            let reopened = fixture
                .owner
                .recover_publication(
                    &recovery_workspace,
                    &fixture.destination,
                    original_generation,
                    operation,
                    &work,
                    &cancellation,
                    &cleanup_cancellation,
                )
                .await?;
            let recovered = reopened
                .ok_or_else(|| Error::NotFound("actual original SDK gate after query".into()))?;
            assert_eq!(recovered.0, receipt.0);
            assert_eq!(work.receipt().backend_write_operations, before_query);
            drop(recovered);
            let foreign = fixture
                .owner
                .recover_publication(
                    &recovery_workspace,
                    &fixture.destination,
                    original_generation,
                    acyclic_fs::OperationId::from_bytes([92; 16]),
                    &work,
                    &cancellation,
                    &cleanup_cancellation,
                )
                .await?;
            assert!(foreign.is_none());
            assert_eq!(work.receipt().backend_write_operations, before_query);
            let denied_work = test_work(budget, &cleanup_cancellation)?;
            let mut failed = receipt
                .1
                .release(&fixture.owner, &denied_work, &cleanup_cancellation)
                .await
                .err()
                .ok_or_else(|| {
                    Error::Storage("foreign cleanup family denial was not exercised".into())
                })?;
            let retained = crate::filesystem::NativePublicationReservationFailure::<
                MemoryAuthorityBackend,
                MemoryObjectBackend,
            >::from_error(&failed)
            .expect("whole denied cleanup owner and cause");
            assert_eq!(retained.work.backend_write_operations, 0);
            assert!(matches!(
                retained.error,
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    ..
                }
            ));
            assert!(retained.cleanup.is_some());
            let original_failed_work = retained.work;
            let original_before_retry = work.receipt();
            let foreign_before_retry = denied_work.receipt();
            assert!(matches!(
                crate::filesystem::NativePublicationReservationFailure::<
                    MemoryAuthorityBackend,
                    MemoryObjectBackend,
                >::retry_cleanup_with_native_work(
                    &mut failed,
                    &fixture.owner,
                    &denied_work,
                    &cleanup_cancellation
                )
                .await,
                Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    ..
                })
            ));
            assert_eq!(denied_work.receipt(), foreign_before_retry);
            assert_eq!(work.receipt(), original_before_retry);
            assert!(
                crate::filesystem::NativePublicationReservationFailure::<
                    MemoryAuthorityBackend,
                    MemoryObjectBackend,
                >::retry_cleanup_with_native_work(
                    &mut failed,
                    &fixture.owner,
                    &work,
                    &cleanup_cancellation
                )
                .await
                .expect("actual original gate cleanup")
            );
            let retained = crate::filesystem::NativePublicationReservationFailure::<
                MemoryAuthorityBackend,
                MemoryObjectBackend,
            >::from_error(&failed)
            .expect("original failure retained after actual cleanup");
            assert!(retained.cleanup.is_none());
            assert_eq!(retained.work, original_failed_work);
            fixture
                .files
                .write(&destination, "/release-check.txt", b"settled")
                .await?;
        }
    }
    Ok(())
}
