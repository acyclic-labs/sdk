use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ForkOptions, Fs, FsError,
    IdempotencyKey, PublicationPermit, PublicationReservation, Transaction, TransactionCommit,
    WorkspaceError,
};
use acyclic_fs_format::identity::{Digest, OperationId};
use acyclic_fs_work::performance::{OperationFailure, WorkBudget};
use acyclic_stream::OwnedBytes;
use std::error::Error;

use crate::workspace_transactions::fixtures::fork_workspace;
use crate::workspace_transactions::selected_budget::selected_transaction_budget;
use crate::workspace_transactions::transaction;

fn permit(reservation: &PublicationReservation) -> PublicationPermit {
    PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    }
}

async fn contextual_commit<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    transaction: &mut Transaction<A, O>,
    evidence: &'static [u8],
    permit: PublicationPermit,
) -> Result<TransactionCommit<A, O>, WorkspaceError> {
    transaction
        .commit::<WorkspaceError, _, _>(
            Some((
                Digest::from_bytes([0xc8; 32]),
                OwnedBytes::from_static(evidence),
                PublicationPermit::Unrestricted,
            )),
            permit,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            transaction::allow,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
}

#[tokio::test]
async fn fork_retry_keeps_original_creation_after_both_heads_advance() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("fork-parent"))?;
    parent.write_text("/file", "original").await?;
    let frozen = unbounded!(parent, head())?;
    let key = IdempotencyKey::new();
    let child = fork_workspace(
        &parent,
        "fork-child",
        ForkOptions::from_generation(frozen.clone(), key),
    )
    .await?;
    let original = unbounded!(child, head())?;
    parent.write_text("/file", "later parent").await?;
    child.write_text("/file", "later child").await?;
    let advanced = unbounded!(child, head())?;
    let retry = parent
        .fork::<_, _, WorkspaceError>(
            "fork-child",
            ForkOptions::from_generation(frozen, key),
            PublicationPermit::Unrestricted,
            selected_transaction_budget(),
            &CancellationToken::new(),
            &mut |_| {
                std::future::ready(Err(OperationFailure::before_work(
                    FsError::CreationRejected,
                )))
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    assert_eq!(unbounded!(retry, head())?.id(), advanced.id());
    assert_eq!(
        unbounded!(retry, read("/file", 32))?.as_ref(),
        b"later child"
    );
    let creation = unbounded!(
        fs,
        workspace_for_creation_operation(
            "fork-child",
            child.volume_config(),
            OperationId::from_bytes(key.into_bytes())
        )
    )?
    .ok_or("creation missing")?;
    assert_eq!(creation.id(), original.id());
    assert_eq!(
        unbounded!(creation, read("/file", 32))?.as_ref(),
        b"original"
    );
    Ok(())
}

#[tokio::test]
async fn operation_recovery_survives_head_and_reservation_changes_but_rejects_changed_input()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("context-operation"))?;
    let key = IdempotencyKey::new();
    let mut transaction = unbounded!(workspace, begin_transaction(key))?;
    transaction
        .write_text("/original", "original content")
        .await?;
    let first = unbounded!(workspace, reserve_publication(OperationId::new()))?;
    let TransactionCommit::Committed(original) =
        contextual_commit(&mut transaction, b"witness", permit(&first)).await?
    else {
        return Err("publication failed".into());
    };
    unbounded!(workspace, release_publication(first))?;
    workspace.write_text("/later", "later content").await?;
    let head = unbounded!(workspace, head())?;
    assert_ne!(head.id(), original.id());
    assert_eq!(
        unbounded!(workspace, operation_generation(key))?
            .ok_or("operation missing")?
            .id(),
        original.id()
    );
    assert!(unbounded!(workspace, operation_generation(IdempotencyKey::new()))?.is_none());
    let rotated = unbounded!(workspace, reserve_publication(OperationId::new()))?;
    assert!(
        matches!(contextual_commit(&mut transaction, b"witness", permit(&rotated)).await?, TransactionCommit::AlreadyCommitted(generation) if generation.id() == original.id())
    );
    assert!(matches!(
        contextual_commit(&mut transaction, b"changed witness", permit(&rotated)).await?,
        TransactionCommit::IdempotencyConflict
    ));
    transaction
        .write_text("/original", "changed candidate content")
        .await?;
    assert!(matches!(
        contextual_commit(&mut transaction, b"witness", permit(&rotated)).await?,
        TransactionCommit::IdempotencyConflict
    ));
    assert_eq!(unbounded!(workspace, head())?.id(), head.id());
    unbounded!(workspace, release_publication(rotated))?;
    Ok(())
}

#[tokio::test]
async fn historical_context_never_authorizes_a_foreign_reservation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let target = unbounded!(fs, create_workspace("context-target"))?;
    let foreign = unbounded!(fs, create_workspace("context-foreign"))?;
    let head = unbounded!(target, head())?;
    let key = IdempotencyKey::new();
    let mut transaction = unbounded!(target, begin_transaction(key))?;
    transaction.write_text("/value", "candidate").await?;
    let target_reservation = unbounded!(target, reserve_publication(OperationId::new()))?;
    let foreign_reservation = unbounded!(foreign, reserve_publication(OperationId::new()))?;
    assert!(matches!(
        contextual_commit(&mut transaction, b"witness", permit(&foreign_reservation)).await?,
        TransactionCommit::Fenced
    ));
    assert_eq!(unbounded!(target, head())?.id(), head.id());
    assert!(unbounded!(target, operation_generation(key))?.is_none());
    unbounded!(target, release_publication(target_reservation))?;
    unbounded!(foreign, release_publication(foreign_reservation))?;
    Ok(())
}
