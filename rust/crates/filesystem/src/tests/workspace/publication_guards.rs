use super::*;

#[tokio::test]
async fn readonly_creation_lookup_keeps_authentic_initial_generation_and_failed_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let operation = IdempotencyKey::from_bytes([0xd4; 16]).operation_id();
    let cancellation = CancellationToken::new();
    let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    for allowance in allowances
        .as_object_mut()
        .expect("budget fields")
        .values_mut()
    {
        *allowance = serde_json::json!(100_000_000_u64);
    }
    let budget: WorkBudget = serde_json::from_value(allowances)?;
    let created = fs
        .create_workspace_with_operation(
            "readonly-original-creation",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?
        .value;
    let workspace = unbounded!(fs, open_workspace("readonly-original-creation"))?;
    workspace.write_text("/later", "advanced head").await?;
    let advanced = unbounded!(workspace, head())?.id();
    assert_ne!(advanced, created.id());
    let original = fs
        .workspace_for_creation_operation(
            "readonly-original-creation",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?;
    original.work.verify(budget)?;
    assert_eq!(
        original.value.as_ref().map(|generation| generation.id()),
        Some(created.id())
    );
    assert_eq!(
        original
            .value
            .as_ref()
            .map(|generation| generation.workspace_id()),
        Some(workspace.id())
    );

    assert_eq!(unbounded!(workspace, head())?.id(), advanced);

    let wrong_operation = IdempotencyKey::from_bytes([0xd5; 16]).operation_id();
    let mut wrong_config = config;
    wrong_config.lifecycle = Lifecycle::Durable;
    for (requested_config, requested_operation) in
        [(config, wrong_operation), (wrong_config, operation)]
    {
        let failure = fs
            .workspace_for_creation_operation(
                "readonly-original-creation",
                requested_config,
                requested_operation,
                budget,
                &cancellation,
            )
            .await
            .err()
            .expect("mismatched original creation is rejected");

        failure.work.verify(budget)?;
    }
    let missing = fs
        .workspace_for_creation_operation(
            "readonly-missing-creation",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?;
    assert!(missing.value.is_none());

    missing.work.verify(budget)?;
    assert!(unbounded!(fs, open_workspace("readonly-missing-creation")).is_err());

    let mut denied = budget;
    denied.backend_read_operations = 0;
    let failure = fs
        .workspace_for_creation_operation(
            "readonly-original-creation",
            config,
            operation,
            denied,
            &cancellation,
        )
        .await
        .err()
        .expect("finite denial retains name admission prefix");

    failure.work.verify(denied)?;

    assert_eq!(unbounded!(workspace, head())?.id(), advanced);
    Ok(())
}

#[tokio::test]
async fn ordinary_owner_denial_preserves_whole_error_work_and_candidate()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("whole-ordinary-owner"))?;
    let base = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let mut candidate = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        candidate,
        write("/file", bytes::Bytes::from_static(b"candidate"))
    )?;
    let callback_work = crate::WorkCounters {
        backend_read_operations: 3,
        ..crate::WorkCounters::UNCHARGED
    };
    let failure = candidate
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            selected_transaction_budget(),
            &cancellation,
            |_| {
                std::future::ready(Err(crate::OperationFailure::new(
                    OrdinaryOwnerError::Denied {
                        task: [117; 16],
                        revision: 912,
                        expired: true,
                    },
                    callback_work,
                )))
            },
        )
        .await
        .err()
        .ok_or("expired owner published")?;
    assert!(matches!(failure.error,
        OrdinaryOwnerError::Denied { task, revision: 912, expired: true } if task == [117; 16]));
    assert!(failure.work.backend_read_operations >= callback_work.backend_read_operations);
    assert!(failure.work.bytes_hashed > 0);

    assert_eq!(unbounded!(workspace, head())?.id(), base.id());
    let failure = value!(
        candidate
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                |_| std::future::ready(Err(crate::OperationFailure::before_work(
                    OrdinaryOwnerError::Denied {
                        task: [118; 16],
                        revision: 913,
                        expired: false,
                    }
                ))),
            )
            .await
    )
    .err()
    .ok_or("denied ordinary owner published")?;
    assert!(matches!(failure,
        OrdinaryOwnerError::Denied { task, revision: 913, expired: false } if task == [118; 16]));
    assert_eq!(unbounded!(workspace, head())?.id(), base.id());
    assert!(matches!(
        commit(&mut candidate).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file", 32))?.as_ref(),
        b"candidate"
    );
    let mut cancelled_candidate = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        cancelled_candidate,
        write("/cancelled", bytes::Bytes::from_static(b"not published"))
    )?;
    cancellation.cancel();
    let sdk_failure = value!(
        cancelled_candidate
            .commit::<OrdinaryOwnerError, _, _>(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                allow,
            )
            .await
    )
    .err()
    .ok_or("cancelled SDK work published")?;
    assert!(matches!(
        sdk_failure,
        OrdinaryOwnerError::Sdk(WorkspaceError::Cancelled(_))
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_creation_preserves_original_callback_error_and_prepared_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let guard_original = original.clone();
    let mut deny = move |observed: &crate::NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_original));
        std::future::ready(Err(OrdinaryOwnerError::Denied {
            task: [23; 16],
            revision: 23,
            expired: true,
        }))
    };
    let failure = fs
        .create_workspace_with_operation_guarded_with_native_work(
            "native-guarded-creation-denied",
            VolumeConfig::portable(Lifecycle::Durable),
            IdempotencyKey::new().operation_id(),
            &original,
            &cancellation,
            &mut deny,
        )
        .await
        .err()
        .ok_or("the owner denial unexpectedly created a workspace")?;
    assert!(
        matches!(failure, OrdinaryOwnerError::Denied { task, revision: 23, expired: true } if task == [23; 16])
    );
    assert!(original.receipt().bytes_hashed > before.bytes_hashed);

    assert!(unbounded!(fs, open_workspace("native-guarded-creation-denied")).is_err());
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_fork_denial_keeps_prepared_work_without_destination_authority()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("native-guarded-fork-source"))?;
    workspace
        .write("/selected", bytes::Bytes::from_static(b"original"))
        .await?;
    let frozen = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let guard_original = original.clone();
    let mut deny = move |observed: &crate::NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_original));
        std::future::ready(Err(WorkspaceError::from(
            crate::FsError::MutationNotAllowed,
        )))
    };
    let failure = workspace
        .fork_with_native_work(
            "native-guarded-fork-denied",
            ForkOptions::from_generation(frozen, IdempotencyKey::new())
                .inherit_paths(vec!["/selected".to_owned()]),
            crate::PublicationPermit::Unrestricted,
            &original,
            &cancellation,
            &mut deny,
        )
        .await
        .err()
        .ok_or("the original owner denial unexpectedly published a destination")?;
    assert!(matches!(
        failure,
        WorkspaceError::Filesystem(crate::FsError::MutationNotAllowed)
    ));
    assert!(original.receipt().bytes_hashed > before.bytes_hashed);
    assert!(unbounded!(fs, open_workspace("native-guarded-fork-denied")).is_err());
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_fork_rejects_an_unrelated_source_reservation_without_rebinding_it()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("native-fork-reserved-source"))?;
    workspace
        .write("/value", bytes::Bytes::from_static(b"frozen"))
        .await?;
    let frozen = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let reservation = workspace
        .reserve_publication(
            IdempotencyKey::new().operation_id(),
            &original,
            &cancellation,
        )
        .await?;
    let before = original.receipt().authority_records_appended;
    let permit = crate::PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    };
    let mut allow =
        |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(()));
    let failed = workspace
        .fork_with_native_work(
            "native-fork-reserved-destination",
            ForkOptions::from_generation(frozen, IdempotencyKey::new()),
            permit,
            &original,
            &cancellation,
            &mut allow,
        )
        .await
        .err()
        .ok_or("a source reservation was incorrectly rebound to a new destination")?;
    assert!(matches!(
        failed,
        WorkspaceError::Filesystem(crate::FsError::Authority(
            crate::AuthorityStoreError::RejectedStatic(_)
        ))
    ));

    assert!(unbounded!(fs, open_workspace("native-fork-reserved-destination")).is_err());
    workspace
        .release_publication(reservation, &original, &cancellation)
        .await?;
    Ok(())
}
