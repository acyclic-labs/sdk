//! Retained authentication never retains a lease or a trust decision.
use super::*;
use crate::distributed::JournalWrite;

async fn fixture() -> Result<Fixture> {
    Fixture::new(
        Arc::new(MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await
}

#[tokio::test]
async fn journal_guards_reuse_immutable_authentication_with_original_work_limit() -> Result<()> {
    let f = fixture().await?;
    let owner = f.running(1).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024);
    let io = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
    for _ in 0..32 {
        owner
            .condition(JournalWrite::Fresh, &mut Some(&mut io.history_io()))
            .await?;
    }
    // Exact former call shape: each live guard re-authenticated both mutable
    // scope wires. The same ledger and count must expose that repeated work.
    let old = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
    let coordinator = f.host.coordinator.lock().await;
    let mut denied = false;
    for _ in 0..32 {
        if let Err(error) = coordinator.observe_operation(
            &f.host.owner,
            &f.host.owner_scope,
            &f.host.verifier,
            owner.operation_id(),
            &mut Some(&mut old.history_io()),
        ) {
            assert!(error.to_string().contains("items_examined"), "{error}");
            denied = true;
            break;
        }
    }
    assert!(
        denied,
        "former journal guards did not exercise their repeated wire cost"
    );
    Ok(())
}

#[tokio::test]
async fn journal_authentication_rejects_other_provider_operation_and_revocation() -> Result<()> {
    let f = fixture().await?;
    let owner = f.running(1).await?;
    let other = fixture().await?;
    let other_owner = other.running(1).await?;
    let mut coordinator = f.host.coordinator.lock().await;
    for (authority, operation) in [
        (&other_owner.authority, owner.operation_id()),
        (&owner.authority, OperationId::from_bytes([2; 16])),
    ] {
        assert!(matches!(
            coordinator
                .journal_condition(
                    authority,
                    operation,
                    &owner.fence,
                    JournalWrite::Fresh,
                    &mut None
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
    }
    drop(coordinator);
    owner.condition(JournalWrite::Fresh, &mut None).await?;
    f.issuer.revoke_workload(&f.host.owner_scope)?;
    assert!(matches!(
        owner.condition(JournalWrite::Fresh, &mut None).await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn journal_authentication_does_not_retain_cancellation_or_lease() -> Result<()> {
    let f = fixture().await?;
    let owner = f.running(1).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = crate::executor::PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024),
        &cancellation,
    )?;
    owner
        .condition(JournalWrite::Fresh, &mut Some(&mut io.history_io()))
        .await?;
    cancellation.cancel();
    assert!(
        owner
            .condition(JournalWrite::Fresh, &mut Some(&mut io.history_io()))
            .await
            .is_err()
    );
    let _replacement = f.replace(1).await?;
    assert!(
        owner
            .condition(JournalWrite::Fresh, &mut None)
            .await
            .is_err()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn journal_authentication_retires_its_native_custody() -> Result<()> {
    let f = fixture().await?;
    let owner = f.running(1).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let native = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::executor::PreparationIo::from_native_work(&native)?;
    let coordinator = f.host.coordinator.lock().await;
    let authenticate = || {
        coordinator.authenticate_journal(
            &f.host.owner,
            &f.host.owner_scope,
            &f.host.verifier,
            owner.operation_id(),
            &mut Some(&mut io.history_io()),
        )
    };
    // The public verifier owns its separate retained raw-wire cache. Warm it
    // before measuring the journal proof's independent allocation lifetime.
    drop(authenticate()?);
    let baseline = native.live_allocation_bytes();
    let authority = authenticate()?;
    assert!(
        native.live_allocation_bytes() > baseline,
        "live proof has no custody"
    );
    drop(authority);
    assert_eq!(native.live_allocation_bytes(), baseline);
    Ok(())
}
