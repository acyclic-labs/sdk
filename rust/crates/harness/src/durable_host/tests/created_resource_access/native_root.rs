use super::*;

#[tokio::test]
async fn uncertain_root_choice_is_denial_only_and_rejects_identity_substitution() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let owner = fixture
        .host
        .journal_owner(fixture.root, (&fixture.root_lease.reservation).into())
        .await?;
    let operation = OperationId::from_bytes([113; 16]);
    let intent = [114; 32];
    let audience = [115; 32];
    owner.pin_root_binding_choice(operation, intent, audience, &mut None)?;
    owner.pin_root_binding_choice(operation, intent, audience, &mut None)?;
    for offset in 0..32 {
        let mut changed_intent = intent;
        changed_intent[offset] ^= 1;
        assert!(matches!(
            owner.pin_root_binding_choice(operation, changed_intent, audience, &mut None),
            Err(Error::Conflict(_))
        ));
        let mut changed_audience = audience;
        changed_audience[offset] ^= 1;
        assert!(matches!(
            owner.pin_root_binding_choice(operation, intent, changed_audience, &mut None),
            Err(Error::Conflict(_))
        ));
    }
    assert!(matches!(
        owner.pin_root_binding_choice(
            OperationId::from_bytes([116; 16]),
            intent,
            audience,
            &mut None
        ),
        Err(Error::Conflict(_))
    ));
    assert!(
        owner
            .root_binding
            .as_ref()
            .and_then(|state| state.proof.get())
            .is_none()
    );
    assert!(
        owner
            .authenticate_fork_parent(fixture.host.verifier.audience(), &mut None)
            .is_err()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = test_work(
            acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
            &cancellation,
        )?;
        assert!(matches!(
            owner
                .authenticated_root_binding_with_native_work(&work)
                .await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            })
        ));
        assert_eq!(work.receipt().backend_read_operations, 0);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_cancel_retains_original_retry_and_never_claims_terminal_cleanup() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )?;
    fixture.host.cancel(fixture.root, Some(&work)).await?;
    fixture.host.cancel(fixture.root, Some(&work)).await?;
    let state = fixture.host.observe_task(fixture.root).await?;
    assert!(state.cancellation_requested);
    assert!(state.outcome.is_none());
    assert!(work.receipt().backend_read_operations > 0);
    let before = work.receipt().backend_read_operations;
    cancellation.cancel();
    assert!(matches!(
        fixture.host.cancel(fixture.root, Some(&work)).await,
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(work.receipt().backend_read_operations, before);

    let fresh_cancellation = acyclic_fs::CancellationToken::new();
    let mut minimal = acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024);
    minimal.peak_allocation_bytes = acyclic_fs::NativeMaterializationWork::accounting_state_bytes();
    let denied = test_work(minimal, &fresh_cancellation)?;
    assert!(matches!(
        fixture.host.cancel(fixture.root, Some(&denied)).await,
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(denied.receipt().backend_read_operations, 0);
    Ok(())
}

#[tokio::test]
async fn canonical_root_preparation_witness_has_no_fabricated_scratch() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )?;
    let witness = fixture
        .host
        .task_admission_readonly_witness_with_native_work(fixture.root, &work)
        .await?;
    assert_eq!(witness.task(), fixture.root);
    assert_eq!(
        witness.admission_operation(),
        fixture.root_lease.operation.operation_id
    );
    assert!(witness.scratch_proof().is_none());
    assert!(witness._native_custody.bytes() > 0);
    witness.verify_with_native_work(&work).await?;
    let foreign = test_work(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )?;
    let before = foreign.receipt();
    assert!(matches!(
        witness.verify_with_native_work(&foreign).await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(foreign.receipt(), before);
    cancellation.cancel();
    let before = work.receipt();
    assert!(matches!(
        witness.verify_with_native_work(&work).await,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    drop(witness);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

fn native_registered_root_input(
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    use crate::store::history::HistoryIo;
    #[derive(Serialize)]
    struct Input<'a> {
        payload: &'a str,
    }
    let mut io = crate::store::history::native_work_io(work)?;
    let value = crate::contract::json_value_bounded(
        &Input {
            payload: "original registered root",
        },
        crate::contract::JsonBounds {
            maximum_bytes: 4096,
            maximum_items: 64,
            maximum_depth: 8,
        },
        Some(crate::wire::ErrorCode::Invalid),
        &mut |charge| io.codec_charge(charge),
    )?;
    Ok((value, io.into_custody()))
}

#[tokio::test]
async fn registered_native_root_keeps_configured_scope_and_same_ack_identity() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let definition = fixture.host.tasks.get::<Value, Value>("test.created@1")?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )?;
    let operation = OperationId::from_bytes([220; 16]);
    let expected = TaskId::from_bytes(operation.into_bytes());
    for _ in 0..2 {
        let record = fixture.harness.registered_root_admission_with_native_work(
            operation,
            &definition,
            native_registered_root_input(&work)?,
            &work,
        )?;
        assert!(record.0.parent.is_none());
        assert_eq!(record.0.task, fixture.task);
        assert_eq!(record.0.grants, *fixture.root_scope.grants());
        assert_eq!(record.0.run_limits, fixture.root_scope.run_limits());
        let (admitted, custody) = fixture.host.admit_with_native_work(record, &work)?.await?;
        assert!(matches!(admitted, Admission::Accepted(task) if task == expected));
        assert!(custody.work().shares_allocation_context(&work));
        drop(admitted);
        drop(custody);
    }
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn registered_native_root_rejects_foreign_input_and_child_without_fence_before_io()
-> Result<()> {
    let fixture = CreationFixture::new().await?;
    let definition = fixture.host.tasks.get::<Value, Value>("test.created@1")?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )?;
    let foreign = test_work(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )?;
    let before = foreign.receipt();
    assert!(matches!(
        fixture.harness.registered_root_admission_with_native_work(
            OperationId::from_bytes([221; 16]),
            &definition,
            native_registered_root_input(&work)?,
            &foreign,
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(
        foreign.receipt().backend_read_operations,
        before.backend_read_operations
    );
    assert_eq!(
        foreign.receipt().backend_write_operations,
        before.backend_write_operations
    );
    let mut record = fixture.harness.registered_root_admission_with_native_work(
        OperationId::from_bytes([221; 16]),
        &definition,
        native_registered_root_input(&work)?,
        &work,
    )?;
    record.0.parent = Some(fixture.root);
    let before = work.receipt();
    assert!(matches!(
        fixture.host.admit_with_native_work(record, &work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        before.backend_write_operations
    );
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn registered_native_root_cancellation_precedes_metadata_copy_and_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let definition = fixture.host.tasks.get::<Value, Value>("test.created@1")?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )?;
    let input = native_registered_root_input(&work)?;
    cancellation.cancel();
    let before = work.receipt();
    assert!(matches!(
        fixture.harness.registered_root_admission_with_native_work(
            OperationId::from_bytes([222; 16]),
            &definition,
            input,
            &work,
        ),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        before.backend_write_operations
    );
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn original_read_source_cannot_become_native_directory_execution_owner() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )?;
    let (context, custody) = fixture
        .harness
        .durable_context_with_native_work(
            fixture.root,
            fixture.root_lease.operation.operation_id,
            &work,
        )
        .await?;
    let before = work.receipt();
    assert!(matches!(
        context.verify_original_execution_owner_with_native_work(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(work.receipt(), before);
    drop(context);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_admission_exhaustion_precedes_source_io_and_preserves_old_ack() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let operation = OperationId::from_bytes(fixture.root.into_bytes());
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut budget = acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024);
    budget.allocation_operations = 1;
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .expect("only the original ledger allocation");
    let mut io = crate::store::history::native_work_io(&work)?;
    let before = work.receipt();
    assert!(matches!(
        fixture
            .host
            .admission_with_work(operation, &mut Some(&mut io))
            .await,
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    let retained = fixture
        .host
        .admission_with_work(operation, &mut None)
        .await?;
    assert_eq!(retained.operation_id, operation);
    assert_eq!(retained.parent, None);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_admission_json_metadata_denial_precedes_reader_and_hash() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let operation = OperationId::from_bytes(fixture.root.into_bytes());
    let declaration = fixture
        .host
        .retained_declaration_with_work(operation, &mut None)
        .await?
        .expect("original acknowledged declaration");
    let original = declaration.state;
    let descriptor = crate::conversation::FileDescriptor::new(
        *original.descriptor().sha256(),
        original.descriptor().byte_length(),
        "text/plain".into(),
    )?;
    let reference = FileRef::new(
        original.volume().clone(),
        original.path().into(),
        original.version().into(),
        descriptor,
        original.display_name().into(),
        &mut None,
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )
    .expect("original native metadata ledger");
    let mut io = crate::store::history::native_work_io(&work)?;
    assert!(matches!(
        fixture.host.read_json(&reference, &mut Some(&mut io)).await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(
        fixture
            .host
            .admission_with_work(operation, &mut None)
            .await?
            .operation_id,
        operation
    );
    Ok(())
}
