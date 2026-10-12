use super::*;

#[tokio::test]
async fn reservation_lost_ack_and_readonly_query_faults_use_original_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([0xc1; 16]);
    let operation_id = OperationId::from_bytes([0xc2; 16]);
    ({
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .create_authority(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?;
    let original = ({
        let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation);
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
        let __authority_result = authority
            .head(__authority_args.0, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })?
    .value;
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityReserve,
        occurrence: 1,
        fault: SimulationFault::AmbiguousAuthorityAppend,
    })?;
    let lost = ({
        let __authority_args = (
            authority_id,
            original,
            operation_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.3, __authority_args.4);
        let __authority_result = authority
            .reserve_publication(
                __authority_args.0,
                __authority_args.1,
                __authority_args.2,
                &mut __authority_work,
            )
            .await;
        __authority_work.receipt(__authority_result)
    })
    .expect_err("real acquisition committed before the scheduled acknowledgement loss");
    assert!(matches!(
        lost.error,
        AuthorityStoreError::Indeterminate {
            operation: "reserve-publication",
            ..
        }
    ));
    assert_eq!(lost.work.backend_write_operations, 1);
    let reservation = ({
        let __authority_args = (
            authority_id,
            operation_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .publication_reservation(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
            )
            .await;
        __authority_work.receipt(__authority_result)
    })?
    .value
    .ok_or("original reservation evidence missing")?;
    assert_eq!(reservation.authority_id, authority_id);
    assert_eq!(reservation.operation_id, operation_id);
    assert_eq!(reservation.expected, original);
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityReservation,
        occurrence: 2,
        fault: SimulationFault::PartitionBefore,
    })?;
    let partitioned = ({
        let __authority_args = (
            authority_id,
            operation_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .publication_reservation(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
            )
            .await;
        __authority_work.receipt(__authority_result)
    })
    .expect_err("scheduled actual readonly query is intercepted before provider I/O");
    assert!(matches!(
        partitioned.error,
        AuthorityStoreError::RejectedStatic(_)
    ));
    assert_eq!(*partitioned.work, WorkCounters::UNCHARGED);
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityRelease,
        occurrence: 1,
        fault: SimulationFault::AmbiguousAuthorityAppend,
    })?;
    let released = ({
        let __authority_args = (reservation, WorkBudget::UNBOUNDED, &cancellation);
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
        let __authority_result = authority
            .release_publication(__authority_args.0, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })
    .expect_err("real release committed before acknowledgement loss");
    assert!(matches!(
        released.error,
        AuthorityStoreError::Indeterminate {
            operation: "release-publication",
            ..
        }
    ));
    ({
        let __authority_args = (reservation, WorkBudget::UNBOUNDED, &cancellation);
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
        let __authority_result = authority
            .release_publication(__authority_args.0, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })?;
    assert_eq!(
        ({
            let __authority_args = (
                authority_id,
                operation_id,
                WorkBudget::UNBOUNDED,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
            let __authority_result = authority
                .publication_reservation(
                    __authority_args.0,
                    __authority_args.1,
                    &mut __authority_work,
                )
                .await;
            __authority_work.receipt(__authority_result)
        })?
        .value,
        None
    );
    let trace = simulation.trace()?;
    assert_eq!(
        trace
            .iter()
            .filter(|entry| entry.operation == SimulationOperation::AuthorityReserve)
            .count(),
        1
    );
    assert_eq!(
        trace
            .iter()
            .filter(|entry| entry.operation == SimulationOperation::AuthorityRelease)
            .count(),
        2
    );
    assert!(trace.iter().any(
        |entry| entry.operation == SimulationOperation::AuthorityReservation
            && entry.fault == Some(SimulationFault::PartitionBefore)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_simulation_propagates_opaque_backend_rejection_before_ordinary_io()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::wrap(
        OpaqueOriginalAuthority {
            inner: crate::MemoryAuthorityStore::default(),
            ordinary_calls: std::sync::atomic::AtomicUsize::new(0),
        },
        (),
        SimulationOptions::default(),
    )?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let work = crate::materializer::NativeMaterializationWork::new(
        original_authority_budget(),
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let before = work.receipt();
    let id = AuthorityId::from_bytes([0xf1; 16]);
    let expected = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([0xf2; 32]),
    };
    assert!(matches!(
        ({
            let __authority_args = (id, &work, &cancellation);
            let mut __authority_work =
                crate::AuthorityWork::native(__authority_args.1, __authority_args.2);
            let __authority_result = authority
                .head(__authority_args.0, &mut __authority_work)
                .await;
            __authority_result
        }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        ({
            let __authority_args = (
                id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 64,
                },
                &work,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::native(__authority_args.3, __authority_args.4);
            let __authority_result = authority
                .replay(
                    __authority_args.0,
                    __authority_args.1,
                    __authority_args.2,
                    &mut __authority_work,
                )
                .await
                .map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                });
            __authority_result
        }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        ({
            let __authority_args = (id, expected, &work, &cancellation);
            let mut __authority_work =
                crate::AuthorityWork::native(__authority_args.2, __authority_args.3);
            let __authority_result = authority
                .fence(
                    __authority_args.0,
                    __authority_args.1,
                    &mut __authority_work,
                )
                .await;
            __authority_result
        }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        ({
            let __authority_args = (
                id,
                OperationId::from_bytes([0xf3; 16]),
                &work,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::native(__authority_args.2, __authority_args.3);
            let __authority_result = authority
                .find_operation(
                    __authority_args.0,
                    __authority_args.1,
                    &mut __authority_work,
                )
                .await
                ;
            __authority_result
        }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert_eq!(
        simulation
            .authority
            .ordinary_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(work.receipt(), before);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_simulation_mutation_faults_and_unsupported_providers_never_call_ordinary_io()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::wrap(
        OpaqueOriginalAuthority {
            inner: crate::MemoryAuthorityStore::default(),
            ordinary_calls: std::sync::atomic::AtomicUsize::new(0),
        },
        (),
        SimulationOptions::default(),
    )?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(original_authority_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let id = AuthorityId::from_bytes([0xc1; 16]);
    let expected = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([0xc2; 32]),
    };
    let proposed = original_proposal(0xc3, 0xc4, &original, &cancellation)?;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let mut guard = move |_root: &crate::NativeMaterializationWork| {
        callback_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Ok::<(), SimulationOwnerFailure>(()))
    };
    for fault in [
        SimulationFault::RejectBefore,
        SimulationFault::PartitionBefore,
    ] {
        simulation.schedule_next(SimulationOperation::AuthorityCreate, fault)?;
        assert!(matches!(
            ({
                let __authority_args = (id, proposed.clone(), &original, &cancellation);
                let mut __authority_work =
                    crate::AuthorityWork::native(__authority_args.2, __authority_args.3);
                let __authority_result = authority
                    .create_first_record(
                        __authority_args.0,
                        __authority_args.1,
                        &mut __authority_work,
                        None::<&mut ()>,
                    )
                    .await
                    .map_err(|error| error.into_error(std::convert::identity));
                __authority_result
            }),
            Err(AuthorityStoreError::RejectedStatic(_))
        ));
        simulation.schedule_next(SimulationOperation::AuthorityAppend, fault)?;
        assert!(matches!(
            ({
                let __authority_args = (
                    original_append(id, expected, proposed.clone()),
                    &original,
                    &cancellation,
                );
                let mut __authority_work =
                    crate::AuthorityWork::native(__authority_args.1, __authority_args.2);
                let __authority_result = authority
                    .compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>)
                    .await
                    .map_err(|error| error.into_error(std::convert::identity))
                    ;
                __authority_result
            }),
            Err(AuthorityStoreError::RejectedStatic(_))
        ));
        simulation.schedule_next(SimulationOperation::AuthorityAppend, fault)?;
        assert!(matches!(
            ({
                let __authority_args = (
                    original_append(id, expected, proposed.clone()),
                    &original,
                    &cancellation,
                    &mut guard,
                );
                let mut __authority_work =
                    crate::AuthorityWork::native(__authority_args.1, __authority_args.2);
                let __authority_result = authority
                    .compare_and_append(
                        __authority_args.0,
                        &mut __authority_work,
                        Some(&mut crate::NativeAuthorityGuard(
                            (__authority_args.3).clone(),
                        )),
                    )
                    .await
                    .map_err(|error| error.into_error(std::convert::Into::into))
                    ;
                __authority_result
            }),
            Err(SimulationOwnerFailure::Authority(
                AuthorityStoreError::RejectedStatic(_)
            ))
        ));
    }
    assert!(matches!(
        ({
            let __authority_args = (id, proposed.clone(), &original, &cancellation);
            let mut __authority_work =
                crate::AuthorityWork::native(__authority_args.2, __authority_args.3);
            let __authority_result = authority
                .create_first_record(
                    __authority_args.0,
                    __authority_args.1,
                    &mut __authority_work,
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity));
            __authority_result
        }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        ({
            let __authority_args = (
                original_append(id, expected, proposed.clone()),
                &original,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::native(__authority_args.1, __authority_args.2);
            let __authority_result = authority
                .compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>)
                .await
                .map_err(|error| error.into_error(std::convert::identity))
                ;
            __authority_result
        }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        ({
            let __authority_args = (
                original_append(id, expected, proposed),
                &original,
                &cancellation,
                &mut guard,
            );
            let mut __authority_work =
                crate::AuthorityWork::native(__authority_args.1, __authority_args.2);
            let __authority_result = authority
                .compare_and_append(
                    __authority_args.0,
                    &mut __authority_work,
                    Some(&mut crate::NativeAuthorityGuard(
                        (__authority_args.3).clone(),
                    )),
                )
                .await
                .map_err(|error| error.into_error(std::convert::Into::into))
                ;
            __authority_result
        }),
        Err(SimulationOwnerFailure::Authority(
            AuthorityStoreError::NativeOriginalUnsupported
        ))
    ));
    assert_eq!(
        simulation
            .authority
            .ordinary_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    Ok(())
}
