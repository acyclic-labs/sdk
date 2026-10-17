use super::*;
use crate::PublicationPermit;

#[derive(Debug, thiserror::Error)]
enum OwnerError {
    #[error(transparent)]
    Sdk(WorkspaceError),
    #[error("owner denied publication: {0}")]
    Denied(u64),
}

impl From<WorkspaceError> for OwnerError {
    fn from(error: WorkspaceError) -> Self {
        Self::Sdk(error)
    }
}

impl From<FsError> for OwnerError {
    fn from(error: FsError) -> Self {
        Self::Sdk(error.into())
    }
}

#[tokio::test]
async fn creation_lookup_is_readonly_exact_and_bounded() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let operation = IdempotencyKey::from_bytes([0xd4; 16]).operation_id();
    let cancellation = CancellationToken::new();
    let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    for allowance in allowances
        .as_object_mut()
        .ok_or("budget fields missing")?
        .values_mut()
    {
        *allowance = serde_json::json!(100_000_000_u64);
    }
    let budget: WorkBudget = serde_json::from_value(allowances)?;
    let initial = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "creation",
            config,
            operation,
            budget,
            &cancellation,
            None,
        )
        .await?
        .value;
    let workspace = unbounded!(fs, open_workspace("creation"))?;
    workspace.write_text("/later", "advanced head").await?;
    let head = unbounded!(workspace, head())?;
    assert_ne!(head.id(), initial.id());
    let original = fs
        .workspace_for_creation_operation("creation", config, operation, budget, &cancellation)
        .await?;
    original.work.verify(budget)?;
    let original = original.value.ok_or("creation missing")?;
    assert_eq!(original.id(), initial.id());
    assert_eq!(original.workspace_id(), workspace.id());
    let mut wrong_config = config;
    wrong_config.lifecycle = Lifecycle::Durable;
    for (config, operation) in [
        (
            config,
            IdempotencyKey::from_bytes([0xd5; 16]).operation_id(),
        ),
        (wrong_config, operation),
    ] {
        let failure = fs
            .workspace_for_creation_operation("creation", config, operation, budget, &cancellation)
            .await
            .err()
            .ok_or("mismatched creation accepted")?;
        failure.work.verify(budget)?;
    }
    assert!(
        unbounded!(
            fs,
            workspace_for_creation_operation("missing", config, operation)
        )?
        .is_none()
    );
    assert!(unbounded!(fs, open_workspace("missing")).is_err());
    let mut denied = budget;
    denied.backend_read_operations = 0;
    let failure = fs
        .workspace_for_creation_operation("creation", config, operation, denied, &cancellation)
        .await
        .err()
        .ok_or("read budget ignored")?;
    failure.work.verify(denied)?;
    assert_eq!(unbounded!(workspace, head())?.id(), head.id());
    Ok(())
}

#[tokio::test]
async fn owner_errors_and_cancellation_do_not_publish_or_discard_candidate()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("owner"))?;
    let head = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let mut candidate = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    candidate.write_text("/file", "candidate").await?;
    let callback_work = crate::WorkCounters {
        backend_read_operations: 3,
        ..crate::WorkCounters::UNCHARGED
    };
    for ticket in [912, 913] {
        let failure = candidate
            .commit::<OwnerError, _, _>(
                None,
                PublicationPermit::Unrestricted,
                selected_transaction_budget(),
                &cancellation,
                |_| {
                    std::future::ready(Err(crate::OperationFailure::new(
                        OwnerError::Denied(ticket),
                        callback_work,
                    )))
                },
            )
            .await
            .err()
            .ok_or("denied owner published")?;
        assert!(matches!(failure.error, OwnerError::Denied(observed) if observed == ticket));
        assert!(failure.work.backend_read_operations >= callback_work.backend_read_operations);
        failure.work.verify(selected_transaction_budget())?;
        assert_eq!(unbounded!(workspace, head())?.id(), head.id());
    }
    assert!(matches!(
        commit(&mut candidate).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file", 32))?.as_ref(),
        b"candidate"
    );
    let mut cancelled = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    cancelled.write_text("/cancelled", "not published").await?;
    cancellation.cancel();
    let failure = cancelled
        .commit::<OwnerError, _, _>(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow,
        )
        .await
        .err()
        .ok_or("cancelled candidate published")?;
    assert!(matches!(
        failure.error,
        OwnerError::Sdk(WorkspaceError::Cancelled(_))
    ));
    assert!(unbounded!(workspace, stat("/cancelled")).is_err());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_creation_and_fork_call_original_guard_without_publishing()
-> Result<(), Box<dyn Error>> {
    for create in [true, false] {
        let fs = Fs::memory();
        let source = unbounded!(fs, create_workspace("source"))?;
        source.write_text("/selected", "original").await?;
        let frozen = unbounded!(source, head())?;
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
                .map_err(|(error, _)| error)?;
        let guard_work = original.clone();
        let mut deny =
            crate::NativeAuthorityGuard(move |observed: &crate::NativeMaterializationWork| {
                assert!(observed.shares_ledger(&guard_work));
                std::future::ready(Err(OwnerError::Denied(23)))
            });
        let result = if create {
            fs.create_workspace_with_operation::<_, _, OwnerError>(
                "denied",
                source.volume.config,
                IdempotencyKey::new().operation_id(),
                &original,
                &cancellation,
                Some(&mut deny),
            )
            .await
            .map(|_| ())
        } else {
            source
                .fork::<_, _, OwnerError>(
                    "denied",
                    ForkOptions::from_generation(frozen, IdempotencyKey::new())
                        .inherit_paths(vec!["/selected".to_owned()]),
                    PublicationPermit::Unrestricted,
                    &original,
                    &cancellation,
                    deny,
                )
                .await
                .map(|_| ())
        };
        assert!(
            matches!(&result, Err(OwnerError::Denied(23))),
            "create={create}: {result:?}"
        );
        assert!(unbounded!(fs, open_workspace("denied")).is_err());
        original.receipt().verify(selected_transaction_budget())?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn fork_cannot_rebind_a_source_reservation_to_destination() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source = unbounded!(fs, create_workspace("reserved-source"))?;
    source.write_text("/value", "frozen").await?;
    let frozen = unbounded!(source, head())?;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let reservation = source
        .reserve_publication(
            IdempotencyKey::new().operation_id(),
            &original,
            &cancellation,
        )
        .await?;
    let permit = PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    };
    let result = source
        .fork::<_, _, WorkspaceError>(
            "destination",
            ForkOptions::from_generation(frozen, IdempotencyKey::new()),
            permit,
            &original,
            &cancellation,
            crate::NativeAuthorityGuard(|_: &crate::NativeMaterializationWork| {
                std::future::ready(Ok::<_, WorkspaceError>(()))
            }),
        )
        .await;
    // The authority admits only Unrestricted or a matching source Lease to a fork; a source Reservation is
    // rejected as a creation (lineage.rs), before any destination exists.
    assert!(
        matches!(
            result,
            Err(WorkspaceError::Filesystem(FsError::CreationRejected))
        ),
        "{:?}",
        result.as_ref().err()
    );
    assert!(unbounded!(fs, open_workspace("destination")).is_err());
    source
        .release_publication(reservation, &original, &cancellation)
        .await?;
    Ok(())
}
