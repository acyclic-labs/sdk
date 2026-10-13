use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_selected_restore_honors_the_real_retained_authority_reservation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("native-selected-reservation"))?;
    workspace
        .write("/selected", bytes::Bytes::from_static(b"frozen"))
        .await?;
    let frozen = unbounded!(workspace, head())?;
    workspace
        .write("/selected", bytes::Bytes::from_static(b"current"))
        .await?;
    let current = unbounded!(workspace, head())?;
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
    let before = original.receipt();
    let key = IdempotencyKey::new();
    let paths = vec!["/selected".to_owned()];
    let mut allow =
        |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(()));
    let denied = workspace
        .restore_selected_paths_from_with_permit_guarded_with_native_work(
            &frozen,
            &paths,
            current.id(),
            key,
            crate::PublicationPermit::Unrestricted,
            &original,
            &cancellation,
            &mut allow,
        )
        .await;
    assert!(!matches!(
        denied,
        Ok(TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_))
    ));
    assert_eq!(unbounded!(workspace, head())?.id(), current.id());
    assert_eq!(
        unbounded!(workspace, read("/selected", 7))?,
        OwnedBytes::from_static(b"current")
    );

    let permit = crate::PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    };
    let restored = workspace
        .restore_selected_paths_from_with_permit_guarded_with_native_work(
            &frozen,
            &paths,
            current.id(),
            key,
            permit,
            &original,
            &cancellation,
            &mut allow,
        )
        .await?;
    assert!(matches!(
        restored,
        TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/selected", 6))?,
        OwnedBytes::from_static(b"frozen")
    );
    workspace
        .release_publication(reservation, &original, &cancellation)
        .await?;
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}
