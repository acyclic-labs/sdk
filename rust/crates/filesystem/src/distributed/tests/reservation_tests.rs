use super::*;

#[tokio::test]
async fn rejected_reservation_attempt_can_retry_the_same_operation_at_the_new_head() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(stream.clone());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let id = AuthorityId::from_bytes([101; 16]);
    store
        .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let original = store.head(id, &mut work).await?;
    let operation = OperationId::from_bytes([102; 16]);
    let mut spy = ReleaseSpy::new(stream);
    spy.before_commit = vec![crate::GuardedAppend {
        authority_id: id,
        epoch: original.epoch,
        expected: original,
        commit: proposed(103),
        permit: PublicationPermit::Unrestricted,
    }];
    let racing = StreamAuthorityStore::new(Arc::new(spy));
    let ReservationOutcome::Conflict { actual } = racing
        .reserve_publication(id, original, operation, &mut work)
        .await?
    else {
        return Err("reservation must lose the injected head race".into());
    };
    assert_ne!(actual, original);
    let ReservationOutcome::Reserved(reservation) = racing
        .reserve_publication(id, actual, operation, &mut work)
        .await?
    else {
        return Err("rejected attempt poisoned reservation retry".into());
    };
    assert_eq!(reservation.expected, actual);
    assert_eq!(
        racing
            .publication_reservation(id, operation, &mut work)
            .await?,
        Some(reservation)
    );
    assert_eq!(
        racing
            .reserve_publication(id, actual, operation, &mut work)
            .await?,
        ReservationOutcome::AlreadyReserved(reservation)
    );
    Ok(())
}

#[tokio::test]
async fn lost_reservation_ack_recovers_the_exact_original_admission() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(stream.clone());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let id = AuthorityId::from_bytes([104; 16]);
    store
        .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let expected = store.head(id, &mut work).await?;
    let operation = OperationId::from_bytes([105; 16]);
    let mut spy = ReleaseSpy::new(stream);
    spy.lose_ack = true;
    let lossy = StreamAuthorityStore::new(Arc::new(spy));
    assert!(
        lossy
            .reserve_publication(id, expected, operation, &mut work)
            .await
            .is_err()
    );
    let reservation = lossy
        .publication_reservation(id, operation, &mut work)
        .await?
        .ok_or("lost reservation ack hid admitted head")?;
    assert_eq!(reservation.expected, expected);
    assert_eq!(reservation.gate_tail, 2);
    assert_eq!(
        lossy
            .reserve_publication(id, expected, operation, &mut work)
            .await?,
        ReservationOutcome::AlreadyReserved(reservation)
    );
    assert!(
        lossy
            .release_publication(reservation, &mut work)
            .await
            .is_err()
    );
    lossy.release_publication(reservation, &mut work).await?;
    assert_eq!(
        lossy
            .publication_reservation(id, operation, &mut work)
            .await?,
        None
    );
    Ok(())
}

#[tokio::test]
async fn reservation_recovery_uses_bounded_reads_and_hashing_for_long_histories() -> TestResult {
    for native in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream);
        let token = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(BUDGET, &token).map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(BUDGET, &token)
        };
        let id = AuthorityId::from_bytes([106; 16]);
        store
            .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
        let expected = store.head(id, &mut work).await?;
        let operation = OperationId::from_bytes([107; 16]);
        let ReservationOutcome::Reserved(reservation) = store
            .reserve_publication(id, expected, operation, &mut work)
            .await?
        else {
            return Err("reservation denied".into());
        };
        let mut initial = None;
        for index in 0..=64_u8 {
            if index > 0 {
                let expected = store.head(id, &mut work).await?;
                let result = store
                    .compare_and_append(
                        crate::GuardedAppend {
                            authority_id: id,
                            epoch: expected.epoch,
                            expected,
                            commit: proposed(index),
                            permit: PublicationPermit::Reservation {
                                expected,
                                operation_id: reservation.operation_id.into_bytes(),
                                gate_tail: reservation.gate_tail,
                            },
                        },
                        &mut work,
                        None::<&mut ()>,
                    )
                    .await?;
                assert!(matches!(result, FsAppendOutcome::Committed(_)));
            }
            if index % 2 == 0 && index > 0 {
                let head = store.head(id, &mut work).await?;
                assert!(matches!(
                    store.fence(id, head, &mut work).await?,
                    FenceOutcome::Advanced(_)
                ));
            }
            if matches!(index, 0 | 32 | 64) {
                let mut budget = BUDGET;
                budget.items_examined = 1024;
                let native_work = crate::NativeMaterializationWork::new(budget, &token)
                    .map_err(|(error, _)| error)?;
                let mut recovery = if native {
                    AuthorityWork::native(&native_work, &token)
                } else {
                    AuthorityWork::new(budget, &token)
                };
                assert_eq!(
                    store
                        .publication_reservation(id, operation, &mut recovery)
                        .await?,
                    Some(reservation)
                );
                let spent = recovery.spent();
                // The provider charges its retained-key index probes separately.
                // Recovery performs a fixed number of reads and hashes one request.
                assert!(spent.items_examined <= 1024);
                let measured = (
                    spent.authority_records_read,
                    spent.backend_read_operations,
                    spent.bytes_hashed,
                );
                if let Some(initial) = initial {
                    assert_eq!(measured, initial);
                } else {
                    initial = Some(measured);
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn reservation_recovery_rejects_every_forged_original_request_component() -> TestResult {
    let (stream, reservation) = reserved().await?;
    let token = CancellationToken::new();
    for corrupt in (1..=10).chain([12, 13]) {
        let mut spy = ReleaseSpy::new(stream.clone());
        spy.corrupt_observation = corrupt;
        let store = StreamAuthorityStore::new(Arc::new(spy));
        let mut work = AuthorityWork::new(BUDGET, &token);
        assert!(
            matches!(
                store
                    .publication_reservation(
                        reservation.authority_id,
                        reservation.operation_id,
                        &mut work
                    )
                    .await,
                Err(AuthorityStoreError::ReservationOriginalHeadUnavailable { .. })
            ),
            "accepted forged reservation component {corrupt}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn refused_or_cancelled_reservations_leave_the_gate_free() -> TestResult {
    for native in [false, true] {
        for cancel in [false, true] {
            let stream = Arc::new(MemoryStream::default());
            let store = StreamAuthorityStore::new(stream.clone());
            let fresh = CancellationToken::new();
            let mut setup = AuthorityWork::new(BUDGET, &fresh);
            let id = AuthorityId::from_bytes([111; 16]);
            store
                .create_authority(id, Epoch::GENESIS, &mut setup, None::<&mut ()>)
                .await?;
            let expected = store.head(id, &mut setup).await?;
            let operation = OperationId::from_bytes([112; 16]);
            let token = CancellationToken::new();
            let mut budget = BUDGET;
            if !cancel {
                budget.authority_records_appended = 0;
            }
            let original = crate::NativeMaterializationWork::new(budget, &token)
                .map_err(|(error, _)| error)?;
            let mut work = if native {
                AuthorityWork::native(&original, &token)
            } else {
                AuthorityWork::new(budget, &token)
            };
            if cancel {
                token.cancel();
            }
            assert!(
                store
                    .reserve_publication(id, expected, operation, &mut work)
                    .await
                    .is_err()
            );
            assert_eq!(stream.tail(stream_path(id, "/publication-gate")?).await?, 1);
            assert_eq!(
                store
                    .publication_reservation(id, operation, &mut setup)
                    .await?,
                None
            );
            assert_eq!(store.head(id, &mut setup).await?, expected);
        }
    }
    Ok(())
}
