use super::*;

#[tokio::test]
async fn native_builtin_current_volume_grants_and_unmounted_results_deny_before_physical_writes()
-> Result<()> {
    use std::path::Path;
    let seed = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let fixture = preparation_fixture_on(Arc::new(MemoryStream::default()), &seed).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let view = PreparationView::prepare(
        &fixture.owner,
        fixture.allocation.clone(),
        fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings),
        &cancellation,
    )
    .await
    .map_err(|(cause, _)| cause)?
    .value;
    let work = test_work(
        fixture
            .options
            .budget_for_volume_count(fixture.bindings.len())?,
        &cancellation,
    )?;
    assert!(
        fixture
            .owner
            .require_volume_grant(
                &seed.source,
                VolumeOperation::Write,
                &mut Some(&mut crate::store::history::native_work_io(&work)?)
            )
            .is_err()
    );
    for path in [
        "a-source/input.txt",
        "results/unmounted.txt",
        "../results/escape.txt",
    ] {
        assert!(
            view.write_builtin_file(
                &fixture.owner,
                Path::new(path),
                "forbidden",
                true,
                None,
                &work
            )
            .await
            .is_err()
        );
    }
    assert_eq!(work.receipt().output_bytes, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(
        std::fs::read(fixture.allocation.root.join("a-source/input.txt"))
            .map_err(acyclic_fs::NativeTreeMaterializationError::from)?,
        b"pinned input"
    );
    assert!(!fixture.allocation.root.join("results").exists());
    Ok(())
}

#[tokio::test]
async fn native_builtin_rejects_same_uuid_foreign_original_owner_and_cancelled_current_task()
-> Result<()> {
    use crate::runtime::DurableTaskHost as _;
    use std::path::Path;
    let seed = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let fixture = preparation_fixture_on(Arc::new(MemoryStream::default()), &seed).await?;
    let foreign = preparation_fixture_on(Arc::new(MemoryStream::default()), &seed).await?;
    assert_eq!(
        fixture.owner.task_binding().0,
        foreign.owner.task_binding().0
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let view = PreparationView::prepare(
        &fixture.owner,
        fixture.allocation.clone(),
        fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings),
        &cancellation,
    )
    .await
    .map_err(|(cause, _)| cause)?
    .value;
    let work = test_work(
        fixture
            .options
            .budget_for_volume_count(fixture.bindings.len())?,
        &cancellation,
    )?;
    let operation = OperationId::from_bytes([213; 16]);
    assert!(
        view.read_builtin_file(&foreign.owner, Path::new("a-source/input.txt"), 32, &work)
            .await
            .is_err()
    );
    assert_eq!(work.receipt().source_bytes_read, 0);
    fixture
        .task
        .task_host()
        .cancel(fixture.owner.task_binding().0, None)
        .await?;
    assert!(
        view.write_builtin_file(
            &fixture.owner,
            Path::new("b-destination/after-cancel"),
            "forbidden",
            true,
            None,
            &work
        )
        .await
        .is_err()
    );
    assert_eq!(work.receipt().output_bytes, 0);
    assert!(
        !fixture
            .allocation
            .root
            .join("b-destination/after-cancel")
            .exists()
    );
    assert_eq!(
        std::fs::read(fixture.allocation.root.join("a-source/input.txt"))
            .map_err(acyclic_fs::NativeTreeMaterializationError::from)?,
        b"pinned input"
    );
    Ok(())
}

#[tokio::test]
async fn original_native_preparation_resumes_physical_partial_and_lost_acknowledgements()
-> Result<()> {
    use acyclic_fs::MaterializationPhase::{Applied, Applying, Preparing};
    let seed = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let original = PreparationView::inspect_preparation(
        &seed.owner,
        &seed.preparation_root,
        seed.view.manifest().options.work_per_volume,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map_err(|(error, _)| error)?
    .value
    .ok_or_else(|| Error::NotFound("completed original preparation journal".into()))?;
    assert_eq!(original.journal.plan, seed.view.preparation().journal.plan);
    assert_eq!(original.journal.phase, Applied);
    assert_eq!(
        original.journal.plan.operation_id,
        NativePreparationRoot::operation(seed.owner.task_binding().0, seed.view.work())?
    );
    let acyclic_fs::MaterializationIntent::NativePreparation {
        selection,
        original: request,
    } = &original.journal.plan.intent
    else {
        return Err(Error::Storage(
            "native root used a synthetic generation transition".into(),
        ));
    };
    assert!(!request.is_empty());
    assert_eq!(selection.len(), original.manifest.volumes.len());
    for (selected, mapping) in selection.iter().zip(&original.manifest.volumes) {
        assert_eq!(
            selected.provider_identity,
            crate::contract::canonical_json_bytes(mapping.volume.provider())?
        );
        assert_eq!(
            selected.generation.digest().as_bytes().as_slice(),
            mapping.generation.as_resource().key()
        );
    }
    for (phase, preimages, applied, lost_reply) in [
        (Preparing, 0, 0, false),
        (Preparing, 0, 0, true),
        (Preparing, 1, 0, false),
        (Preparing, 1, 0, true),
        (Applying, 3, 1, false),
        (Applying, 3, 1, true),
        (Applied, 3, 3, false),
        (Applied, 3, 3, true),
    ] {
        let stream = Arc::new(ReceiptFaultStream::default());
        let fixture = preparation_fixture_on(stream.clone(), &seed).await?;
        *stream.preparation_fault.lock().await = Some(PreparationCommitFault {
            phase,
            preimages,
            applied,
            lost_reply,
        });
        let failed = PreparationView::prepare(
            &fixture.owner,
            fixture.allocation.clone(),
            fixture.options.clone(),
            borrow_preparation_bindings(&fixture.bindings),
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .err()
        .ok_or_else(|| Error::Storage("native preparation fault was not exercised".into()))?;
        assert!(stream.preparation_fault.lock().await.is_none());
        assert!(failed.1.backend_read_operations > 0);
        let before = PreparationView::inspect_preparation(
            &fixture.owner,
            &fixture.allocation,
            fixture.options.work_per_volume,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|(error, _)| error)?
        .value;
        let original_allocation = PreparationView::recover_allocation(
            &fixture.owner,
            &fixture.options,
            fixture.options.work_per_volume,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|(error, _)| error)?
        .value;
        assert_eq!(original_allocation.is_some(), before.is_some());
        if let Some(allocation) = &original_allocation {
            assert_eq!(allocation.root, fixture.allocation.root);
            assert_eq!(
                allocation.operation_directory,
                fixture.allocation.operation_directory
            );
            assert_eq!(
                allocation.expected_identity,
                fixture.allocation.expected_identity
            );
            assert_eq!(
                allocation.expected_preparation_identity,
                fixture.allocation.expected_preparation_identity
            );
        }
        if phase == Applying {
            assert_eq!(
                std::fs::read(fixture.allocation.root.join("a-source/input.txt"))
                    .map_err(|error| Error::Storage(error.to_string()))?,
                b"pinned input"
            );
            assert!(!fixture.allocation.root.join("b-destination").exists());
            assert!(failed.1.materializations > 0);
        }
        if let Some(receipt) = &before {
            assert_eq!(receipt.task, fixture.owner.task_binding().0);
            assert_eq!(&receipt.original_fence, fixture.owner.task_binding().1);
            assert_eq!(
                receipt.journal.root_identity,
                Some(fixture.allocation.expected_identity.to_bytes())
            );
            assert_eq!(
                receipt.journal.preparation_identity.as_ref(),
                Some(&fixture.allocation.expected_preparation_identity)
            );
        } else {
            assert_eq!(
                (phase, preimages, applied, lost_reply),
                (Preparing, 0, 0, false)
            );
        }
        let recovered = PreparationView::prepare(
            &fixture.owner,
            original_allocation.unwrap_or_else(|| fixture.allocation.clone()),
            fixture.options.clone(),
            borrow_preparation_bindings(&fixture.bindings),
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|(cause, _)| cause)?;
        let receipt = &recovered.value.preparation();
        assert_eq!(receipt.journal.phase, Applied);
        assert_eq!(receipt.journal.applied, 3);
        assert_eq!(
            receipt.journal.plan.operation_id,
            NativePreparationRoot::operation(fixture.owner.task_binding().0, seed.view.work())?
        );
        if let Some(before) = before {
            assert_eq!(before.journal.plan, receipt.journal.plan);
            assert_eq!(before.original_fence, receipt.original_fence);
            assert!(
                receipt.journal.work.backend_read_operations
                    >= before.journal.work.backend_read_operations
            );
        }
        recovered
            .value
            .validate_for_dispatch(recovered.value.manifest().validation_budget()?)
            .await
            .map_err(|(cause, _)| cause)?;
        for mapping in &recovered.value.manifest().volumes {
            assert!(mapping.path.is_dir());
        }
    }
    Ok(())
}

#[tokio::test]
#[cfg(feature = "filesystem-local")]
async fn native_preparation_incomplete_journal_reopens_on_actual_local_stream() -> Result<()> {
    let seed = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let storage = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let path = storage.path().join("coordinator");
    let provider = acyclic_stream::LocalStream::open(&path, Default::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let stream = Arc::new(ReceiptFaultStream::new(provider));
    let fixture = preparation_fixture_on(stream.clone(), &seed).await?;
    *stream.preparation_fault.lock().await = Some(PreparationCommitFault {
        phase: acyclic_fs::MaterializationPhase::Applying,
        preimages: 3,
        applied: 1,
        lost_reply: true,
    });
    let failed = PreparationView::prepare(
        &fixture.owner,
        fixture.allocation.clone(),
        fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings),
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or_else(|| {
        Error::Storage("local preparation lost acknowledgement was not exercised".into())
    })?;
    assert!(failed.1.materializations > 0);
    let PreparationFixture {
        _directory,
        task,
        owner,
        allocation,
        options,
        bindings,
    } = fixture;
    let (task_id, fence) = owner.task_binding();
    let fence = fence.clone();
    drop(owner);
    drop(task);
    drop(stream);
    let reopened = acyclic_stream::LocalStream::open(&path, Default::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let runtime = native_task_fixture(
        Arc::new(reopened),
        seed.files.clone(),
        seed.results.volume().clone(),
        &seed.issuer,
        &seed.scope,
        seed.owner.input_grants.clone(),
    )
    .await?;
    let owner = Arc::new(
        runtime
            .task_host()
            .journal_owner(task_id, fence.clone())
            .await?,
    );
    let before = PreparationView::inspect_preparation(
        &owner,
        &allocation,
        options.work_per_volume,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map_err(|(error, _)| error)?
    .value
    .ok_or_else(|| Error::NotFound("reopened actual preparation journal".into()))?;
    assert_eq!(
        before.journal.phase,
        acyclic_fs::MaterializationPhase::Applying
    );
    assert_eq!(before.journal.applied, 1);
    assert_eq!(before.original_fence, fence);
    let prepared = PreparationView::prepare(
        &owner,
        allocation.clone(),
        options,
        borrow_preparation_bindings(&bindings),
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map_err(|(cause, _)| cause)?;
    assert_eq!(
        prepared.value.preparation().journal.plan,
        before.journal.plan
    );
    assert_eq!(
        prepared.value.preparation().journal.phase,
        acyclic_fs::MaterializationPhase::Applied
    );
    prepared
        .value
        .validate_for_dispatch(prepared.value.manifest().validation_budget()?)
        .await
        .map_err(|(cause, _)| cause)?;
    drop(_directory);
    Ok(())
}

#[tokio::test]
async fn native_preparation_bounds_shared_codec_before_physical_root_open() -> Result<()> {
    let seed = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    assert!(seed.view.manifest().volumes.iter().all(|mapping| {
        matches!(
            mapping.namespace,
            NativeVolumeNamespace::MaterializedDirectory
        )
    }));
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(stream, &seed).await?;
    let mut options = fixture.options.clone();
    options.work_per_volume.bytes_encoded = 0;
    let failed = PreparationView::prepare(
        &fixture.owner,
        fixture.allocation.clone(),
        options,
        borrow_preparation_bindings(&fixture.bindings[..1]),
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or_else(|| Error::Storage("unadmitted canonical preparation intent".into()))?;
    assert_eq!(failed.1.backend_read_operations, 0);
    assert_eq!(failed.1.backend_write_operations, 0);
    assert!(
        std::fs::read_dir(&fixture.allocation.root)
            .map_err(|error| Error::Storage(error.to_string()))?
            .next()
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn native_preparation_decode_failure_retains_original_journal_read_work() -> Result<()> {
    let seed = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = preparation_fixture_on(stream, &seed).await?;
    let prepared = PreparationView::prepare(
        &fixture.owner,
        fixture.allocation.clone(),
        fixture.options.clone(),
        borrow_preparation_bindings(&fixture.bindings),
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map_err(|(error, _)| error)?;
    let budget = acyclic_fs::WorkBudget {
        items_examined: 0,
        ..prepared.value.manifest().validation_budget()?
    };
    let failed = PreparationView::inspect_preparation(
        &fixture.owner,
        &fixture.allocation,
        budget,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or_else(|| Error::Storage("unadmitted original preparation decode".into()))?;
    assert_eq!(failed.1.backend_write_operations, 0);
    let read_budget = acyclic_fs::WorkBudget {
        authority_bytes_read: acyclic_stream::MAX_RECORD_BYTES as u64 - 1,
        ..prepared.value.manifest().validation_budget()?
    };
    let denied = PreparationView::inspect_preparation(
        &fixture.owner,
        &fixture.allocation,
        read_budget,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or_else(|| Error::Storage("unadmitted bounded journal fetch".into()))?;
    assert_eq!(denied.1.backend_write_operations, 0);
    Ok(())
}
