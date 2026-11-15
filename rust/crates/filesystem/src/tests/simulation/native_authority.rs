use super::*;

#[tokio::test]
async fn reservation_ack_loss_and_query_partition_preserve_actual_reservation()
-> Result<(), Box<dyn std::error::Error>> {
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    let (authority, _) = simulation.stores();
    let cancel = CancellationToken::new();
    let id = AuthorityId::from_bytes([0xc1; 16]);
    let operation = OperationId::from_bytes([0xc2; 16]);
    work!(authority, create_authority(id, Epoch::GENESIS), B, &cancel).await?;
    let head = work!(authority, head(id), B, &cancel).await?.value;
    schedule.inject_next(
        sites::AUTHORITY_RESERVE,
        SimulationFault::AmbiguousAuthorityAppend,
    );
    assert!(matches!(
        authority
            .reserve_publication(id, head, operation, &mut AuthorityWork::new(B, &cancel))
            .await,
        Err(AuthorityStoreError::Indeterminate { .. })
    ));
    let reservation = authority
        .publication_reservation(id, operation, &mut AuthorityWork::new(B, &cancel))
        .await?
        .ok_or("committed reservation missing")?;
    assert_eq!(reservation.authority_id, id);
    assert_eq!(reservation.operation_id, operation);
    assert_eq!(reservation.expected, head);
    schedule.inject_next(
        sites::AUTHORITY_RESERVATION,
        SimulationFault::PartitionBefore,
    );
    let mut read_work = AuthorityWork::new(B, &cancel);
    assert!(matches!(
        authority
            .publication_reservation(id, operation, &mut read_work)
            .await,
        Err(AuthorityStoreError::RejectedStatic(_))
    ));
    assert_eq!(read_work.spent(), WorkCounters::UNCHARGED);
    schedule.inject_next(
        sites::AUTHORITY_RELEASE,
        SimulationFault::AmbiguousAuthorityAppend,
    );
    assert!(matches!(
        authority
            .release_publication(reservation, &mut AuthorityWork::new(B, &cancel))
            .await,
        Err(AuthorityStoreError::Indeterminate { .. })
    ));
    authority
        .release_publication(reservation, &mut AuthorityWork::new(B, &cancel))
        .await?;
    assert!(
        authority
            .publication_reservation(id, operation, &mut AuthorityWork::new(B, &cancel))
            .await?
            .is_none()
    );
    assert!(
        schedule
            .site_calls()
            .iter()
            .any(|call| call.site == sites::AUTHORITY_RESERVATION
                && call.fault() == Some(&SimulationFault::PartitionBefore))
    );
    Ok(())
}

fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 1_024,
        authority_records_appended: 1_024,
        authority_bytes_read: 1_048_576,
        authority_bytes_written: 1_048_576,
        backend_read_operations: 1_024,
        backend_write_operations: 1_024,
        durability_operations: 1_024,
        bytes_hashed: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        items_examined: 1_024,
        items_returned: 1_024,
        allocation_operations: 1_024,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

#[tokio::test]
async fn opaque_native_reads_and_mutations_never_fall_back_to_ordinary_io_or_owner()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::{NativeAuthorityGuard, NativeMaterializationWork};
    let (schedule, _events) = Schedule::new();
    let simulation = Simulation::wrap(
        &schedule,
        crate::facade::test_authority::OpaqueAuthority::default(),
        (),
    );
    let (authority, _) = simulation.stores();
    let cancel = CancellationToken::new();
    let original = NativeMaterializationWork::new(budget(), &cancel).map_err(|(error, _)| error)?;
    let id = AuthorityId::from_bytes([0xc1; 16]);
    let head = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([0xc2; 32]),
    };
    let mut context = AuthorityWork::native(&original, &cancel);
    assert!(matches!(
        authority.head(id, &mut context).await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        authority
            .replay(
                id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 64
                },
                &mut context
            )
            .await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        authority.fence(id, head, &mut context).await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        authority
            .find_operation(id, OperationId::from_bytes([0xf3; 16]), &mut context)
            .await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    let proposed = proposal(0xc3, 0xc4);
    let append = crate::GuardedAppend {
        authority_id: id,
        epoch: head.epoch,
        expected: head,
        commit: proposed.clone(),
        permit: crate::PublicationPermit::Unrestricted,
    };
    let mut forbidden = NativeAuthorityGuard(
        |_: &NativeMaterializationWork| -> std::future::Ready<Result<(), AuthorityStoreError>> {
            panic!("opaque or rejected native operation invoked owner")
        },
    );
    for fault in [
        SimulationFault::RejectBefore,
        SimulationFault::PartitionBefore,
    ] {
        schedule.inject_next(sites::AUTHORITY_CREATE, fault);
        assert!(matches!(
            authority
                .create_first_record(id, proposed.clone(), &mut context, None::<&mut ()>)
                .await
                .map_err(|error| error.into_error(std::convert::identity)),
            Err(AuthorityStoreError::RejectedStatic(_))
        ));
        for guarded in [false, true] {
            schedule.inject_next(sites::AUTHORITY_APPEND, fault);
            let result = if guarded {
                authority
                    .compare_and_append(append.clone(), &mut context, Some(&mut forbidden))
                    .await
                    .map_err(|error| error.into_error(std::convert::identity))
            } else {
                authority
                    .compare_and_append(append.clone(), &mut context, None::<&mut ()>)
                    .await
                    .map_err(|error| error.into_error(std::convert::identity))
            };
            assert!(matches!(
                result,
                Err(AuthorityStoreError::RejectedStatic(_))
            ));
        }
    }
    assert!(matches!(
        authority
            .create_first_record(id, proposed, &mut context, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity)),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    for guarded in [false, true] {
        let result = if guarded {
            authority
                .compare_and_append(append.clone(), &mut context, Some(&mut forbidden))
                .await
                .map_err(|error| error.into_error(std::convert::identity))
        } else {
            authority
                .compare_and_append(append.clone(), &mut context, None::<&mut ()>)
                .await
                .map_err(|error| error.into_error(std::convert::identity))
        };
        assert!(matches!(
            result,
            Err(AuthorityStoreError::NativeOriginalUnsupported)
        ));
    }
    assert_eq!(
        simulation
            .authority
            .ordinary_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    Ok(())
}
