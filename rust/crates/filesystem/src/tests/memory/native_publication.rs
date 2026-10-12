use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authority_checks_both_cancellation_roots_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::materializer::NativeMaterializationWork;
    let (store, id, committed) = native_authority_fixture().await?;
    let root = crate::CancellationToken::new();
    let supplied = crate::CancellationToken::new();
    let active = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &root)
        .map_err(|(error, _)| error)?;
    let head = ({
        let authority_cancellation = &crate::CancellationToken::new();

        store
            .head(
                id,
                &mut crate::AuthorityWork::new(NATIVE_AUTHORITY_BUDGET, authority_cancellation),
            )
            .await
    })?;
    let before = work.receipt();
    supplied.cancel();
    for cancellation in [&supplied, &active] {
        if std::ptr::eq(cancellation, &active) {
            root.cancel();
        }
        assert!(
            ({
                AsyncAuthorityStore::head(
                    &store,
                    id,
                    &mut crate::AuthorityWork::native(&work, cancellation),
                )
                .await
            })
            .is_err()
        );
        assert!(
            ({
                AsyncAuthorityStore::replay(
                    &store,
                    id,
                    Sequence::GENESIS,
                    ReplayLimit {
                        records: 1,
                        payload_bytes: 8_192,
                    },
                    &mut crate::AuthorityWork::native(&work, cancellation),
                )
                .await
                .map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                })
            })
            .is_err()
        );
        assert!(
            ({
                AsyncAuthorityStore::fence(
                    &store,
                    id,
                    head,
                    &mut crate::AuthorityWork::native(&work, cancellation),
                )
                .await
            })
            .is_err()
        );
        assert!(
            ({
                AsyncAuthorityStore::find_operation(
                    &store,
                    id,
                    committed.operation_id,
                    &mut crate::AuthorityWork::native(&work, cancellation),
                )
                .await
            })
            .is_err()
        );
        assert_eq!(work.receipt(), before);
    }
    assert_eq!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            store
                .head(
                    id,
                    &mut crate::AuthorityWork::new(NATIVE_AUTHORITY_BUDGET, authority_cancellation),
                )
                .await
        })?,
        head
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_first_record_is_atomic_exact_and_ordinary_byte_compatible()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let native = std::sync::Arc::new(MemoryAuthorityStore::default());
    let ordinary = std::sync::Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x90; 16]);
    let proposed = native_mutation_request(id, Head::genesis(Epoch::GENESIS)).commit;
    assert!(
        (async {
            AsyncAuthorityStore::create_first_record(
                &native,
                id,
                proposed.clone(),
                &mut crate::AuthorityWork::native(&work, &cancellation),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
        })
        .await?
    );
    let created = work.receipt();
    assert_eq!(created.backend_write_operations, 1);
    assert_eq!(created.authority_records_appended, 1);
    assert_eq!(created.allocation_operations, 4); // Root, history vector, two real map growth attempts.
    let state = NativeMaterializationWork::accounting_state_bytes();
    let retained = (size_of::<(AuthorityId, AuthorityState)>()
        + size_of::<DurableCommit>()
        + size_of::<(OperationId, (usize, KernelOwnedAllocation))>()) as u64;
    assert_eq!(created.peak_allocation_bytes, state + retained);
    let ordinary_receipt = (async {
        let mut __authority_work =
            crate::AuthorityWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation);
        let __authority_result = AsyncAuthorityStore::create_first_record(
            &ordinary,
            id,
            proposed.clone(),
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(ordinary_receipt.value);
    assert_eq!(ordinary_receipt.work.backend_write_operations, 1);
    let limit = ReplayLimit {
        records: 1,
        payload_bytes: 1024,
    };
    assert_eq!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            AsyncAuthorityStore::replay(
                native.as_ref(),
                id,
                Sequence::GENESIS,
                limit,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            )
            .await
            .map(|(value, _allocation)| value)
        })?,
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            AsyncAuthorityStore::replay(
                ordinary.as_ref(),
                id,
                Sequence::GENESIS,
                limit,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            )
            .await
            .map(|(value, _allocation)| value)
        })?
    );
    assert!(
        (async {
            AsyncAuthorityStore::create_first_record(
                &native,
                id,
                proposed.clone(),
                &mut crate::AuthorityWork::native(&work, &cancellation),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
        })
        .await?
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        created.backend_write_operations
    );
    assert_eq!(
        work.receipt().allocation_operations,
        created.allocation_operations
    );
    for changed in [
        ProposedCommit {
            payload: bytes::Bytes::from_static(b"different configuration bytes"),
            ..proposed.clone()
        },
        ProposedCommit {
            operation_id: OperationId::from_bytes([0x93; 16]),
            ..proposed.clone()
        },
        ProposedCommit {
            fingerprint: Digest::from_bytes([0x94; 32]),
            ..proposed
        },
    ] {
        assert!(
            !(async {
                AsyncAuthorityStore::create_first_record(
                    &native,
                    id,
                    changed,
                    &mut crate::AuthorityWork::native(&work, &cancellation),
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))
            })
            .await?
        );
    }
    assert_eq!(work.receipt().backend_write_operations, 1);
    // Persistent table/history buffers retain their original root through state drop.
    let live = work
        .allocations(|allocations, _, _, _| {
            Ok::<_, crate::materializer::NativeTreeMaterializationError>(allocations.live_bytes())
        })
        .map(|(value, _allocation)| value)?;
    assert_eq!(live, state + retained);
    drop(native);
    assert_eq!(
        work.allocations(|allocations, _, _, _| {
            Ok::<_, crate::materializer::NativeTreeMaterializationError>(allocations.live_bytes())
        })
        .map(|(value, _allocation)| value)?,
        state
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_owner_failure_wins_after_planning_outside_every_lock()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let store = std::sync::Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x96; 16]);
    let head = Head::genesis(Epoch::GENESIS);
    ({
        let authority_cancellation = &crate::CancellationToken::new();

        AsyncAuthorityStore::create_authority(
            store.as_ref(),
            id,
            head.epoch,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))
    })?;
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let callback_work = work.clone();
    let callback_store = store.clone();
    let callback_cancellation = cancellation.clone();
    let mut guard = move |original: &NativeMaterializationWork| {
        callback_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert!(original.shares_ledger(&callback_work));
        let store = callback_store.clone();
        let cancellation = callback_cancellation.clone();
        let original = original.clone();
        async move {
            let actual = ({
                AsyncAuthorityStore::head(
                    store.as_ref(),
                    id,
                    &mut crate::AuthorityWork::native(&original, &cancellation),
                )
                .await
            })
            .expect("owner callback must hold no provider or root lock");
            assert_eq!(actual, head);
            original
                .charge(WorkCounters {
                    items_examined: 5,
                    ..WorkCounters::default()
                })
                .expect("owner work admitted");
            Err::<(), _>(OriginalMutationOwnerFailure::Owner(0xa7))
        }
    };
    assert!(matches!(
        (async {
            AsyncAuthorityStore::compare_and_append(
                &store,
                native_mutation_request(id, head),
                &mut crate::AuthorityWork::native(&work, &cancellation),
                Some(&mut crate::NativeAuthorityGuard((&mut guard).clone())),
            )
            .await
            .map_err(|error| error.into_error(std::convert::Into::into))
        })
        .await,
        Err(OriginalMutationOwnerFailure::Owner(0xa7))
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            AsyncAuthorityStore::head(
                store.as_ref(),
                id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            )
            .await
        })?,
        head
    );
    let prefix = work.receipt();
    assert!(prefix.bytes_hashed > 0);
    assert!(prefix.items_examined >= 5);
    assert_eq!(prefix.allocation_operations, 3);
    assert_eq!(prefix.backend_write_operations, 0);
    assert_eq!(prefix.authority_records_appended, 0);
    assert_eq!(
        work.allocations(|allocations, _, _, _| {
            Ok::<_, crate::materializer::NativeTreeMaterializationError>(allocations.live_bytes())
        })
        .map(|(value, _allocation)| value)?,
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_owner_revalidates_head_permit_and_cancellation_at_publication()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let store = std::sync::Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x97; 16]);
    let head = Head::genesis(Epoch::GENESIS);
    ({
        let authority_cancellation = &crate::CancellationToken::new();

        AsyncAuthorityStore::create_authority(
            store.as_ref(),
            id,
            head.epoch,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))
    })?;
    let root = crate::CancellationToken::new();
    let supplied = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &root)
        .map_err(|(error, _)| error)?;
    let callback_store = store.clone();
    let callback_supplied = supplied.clone();
    let mut guard = move |original: &NativeMaterializationWork| {
        let store = callback_store.clone();
        let supplied = callback_supplied.clone();
        let original = original.clone();
        async move {
            ({
                AsyncAuthorityStore::fence(
                    store.as_ref(),
                    id,
                    head,
                    &mut crate::AuthorityWork::native(&original, &supplied),
                )
                .await
            })
            .map(|_| ())
        }
    };
    let outcome = (async {
        AsyncAuthorityStore::compare_and_append(
            &store,
            native_mutation_request(id, head),
            &mut crate::AuthorityWork::native(&work, &supplied),
            Some(&mut crate::NativeAuthorityGuard((&mut guard).clone())),
        )
        .await
        .map_err(|error| error.into_error(std::convert::Into::into))
    })
    .await?;
    assert!(matches!(outcome, AppendOutcome::Fenced { actual_epoch } if actual_epoch.get() == 2));
    assert_eq!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            AsyncAuthorityStore::head(
                store.as_ref(),
                id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            )
            .await
        })?
        .sequence,
        Sequence::GENESIS
    );
    let actual = ({
        let authority_cancellation = &crate::CancellationToken::new();

        AsyncAuthorityStore::head(
            store.as_ref(),
            id,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
        )
        .await
    })?;
    let mut request = native_mutation_request(id, actual);
    request.permit = crate::storage::PublicationPermit::Reservation {
        operation_id: request.commit.operation_id.into_bytes(),
        gate_tail: 7,
        expected: actual,
    };
    let prefix = work.receipt();
    let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_called = called.clone();
    let mut forbidden = move |_: &NativeMaterializationWork| {
        callback_called.store(true, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Ok::<(), AuthorityStoreError>(()))
    };
    assert!(matches!(
        (async {
            AsyncAuthorityStore::compare_and_append(
                &store,
                request,
                &mut crate::AuthorityWork::native(&work, &supplied),
                Some(&mut crate::NativeAuthorityGuard((&mut forbidden).clone())),
            )
            .await
            .map_err(|error| error.into_error(std::convert::Into::into))
        })
        .await,
        Err(AuthorityStoreError::RejectedStatic(_))
    ));
    assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(work.receipt(), prefix);
    let callback_supplied = supplied.clone();
    let mut cancel = move |_: &NativeMaterializationWork| {
        callback_supplied.cancel();
        std::future::ready(Ok::<(), AuthorityStoreError>(()))
    };
    assert!(matches!(
        (async {
            AsyncAuthorityStore::compare_and_append(
                &store,
                native_mutation_request(id, actual),
                &mut crate::AuthorityWork::native(&work, &supplied),
                Some(&mut crate::NativeAuthorityGuard((&mut cancel).clone())),
            )
            .await
            .map_err(|error| error.into_error(std::convert::Into::into))
        })
        .await,
        Err(AuthorityStoreError::Cancelled)
    ));
    assert_eq!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            AsyncAuthorityStore::head(
                store.as_ref(),
                id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            )
            .await
        })?,
        actual
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        prefix.backend_write_operations
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_first_record_and_append_denial_never_create_a_partial_authority()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::materializer::NativeMaterializationWork;
    let id = AuthorityId::from_bytes([0x98; 16]);
    let store = MemoryAuthorityStore::default();
    let root = crate::CancellationToken::new();
    let supplied = crate::CancellationToken::new();
    let active = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
            ..NATIVE_AUTHORITY_BUDGET
        },
        &root,
    )
    .map_err(|(error, _)| error)?;
    let request = native_mutation_request(id, Head::genesis(Epoch::GENESIS));
    assert!(matches!(
        ({
            AsyncAuthorityStore::create_first_record(
                &store,
                id,
                request.commit.clone(),
                &mut crate::AuthorityWork::native(&work, &supplied),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
        }),
        Err(AuthorityStoreError::NativeOriginal(_))
    ));
    assert!(matches!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result = store.head(id, &mut __authority_work).await;
            __authority_work.receipt(__authority_result)
        }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    assert_eq!(work.receipt().allocation_operations, 1);
    assert_eq!(work.receipt().backend_write_operations, 0);
    for cancel_root in [false, true] {
        if cancel_root {
            root.cancel();
        } else {
            supplied.cancel();
        }
        let cancellation = if cancel_root { &active } else { &supplied };
        let prefix = work.receipt();
        assert!(
            ({
                AsyncAuthorityStore::create_first_record(
                    &store,
                    id,
                    request.commit.clone(),
                    &mut crate::AuthorityWork::native(&work, cancellation),
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))
            })
            .is_err()
        );
        assert!(
            ({
                AsyncAuthorityStore::compare_and_append(
                    &store,
                    request.clone(),
                    &mut crate::AuthorityWork::native(&work, cancellation),
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))
            })
            .is_err()
        );
        assert_eq!(work.receipt(), prefix);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_guarded_first_record_seals_atomic_creation_and_successful_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    let store = std::sync::Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x9a; 16]);
    let proposed = native_mutation_request(id, Head::genesis(Epoch::GENESIS)).commit;
    let budget = NATIVE_AUTHORITY_BUDGET;
    let cancellation = crate::CancellationToken::new();
    let mut failed = |remaining: WorkBudget| {
        assert!(remaining.bytes_hashed < budget.bytes_hashed);
        let store = store.clone();
        async move {
            assert!(matches!(
                ({
                    let authority_cancellation = &crate::CancellationToken::new();

                    let mut __authority_work =
                        crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
                    let __authority_result =
                        AsyncAuthorityStore::head(store.as_ref(), id, &mut __authority_work).await;
                    __authority_work.receipt(__authority_result)
                }),
                Err(AuthorityFailure {
                    error: AuthorityStoreError::Missing,
                    ..
                })
            ));
            Err::<crate::OperationReceipt<()>, _>(crate::performance::OperationFailure::new(
                crate::facade::FsError::Cancelled(crate::CancellationError),
                WorkCounters {
                    items_examined: 3,
                    ..WorkCounters::default()
                },
            ))
        }
    };
    let failure = (async {
        let authority_input_4 = &mut failed;

        let mut __authority_work = crate::AuthorityWork::new(budget, &cancellation);
        let __authority_result = AsyncAuthorityStore::create_first_record(
            &store,
            id,
            proposed.clone(),
            &mut __authority_work,
            Some(&mut (&mut *authority_input_4)),
        )
        .await
        .map_err(|error| error.into_error(crate::FsError::from));
        __authority_work.receipt(__authority_result)
    })
    .await
    .err()
    .ok_or("ordinary guard rejection did not win")?;
    assert!(matches!(
        failure.error,
        crate::facade::FsError::Cancelled(_)
    ));
    assert!(failure.work.bytes_hashed > 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    assert_eq!(failure.work.items_examined, 3);
    assert!(matches!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result =
                AsyncAuthorityStore::head(store.as_ref(), id, &mut __authority_work).await;
            __authority_work.receipt(__authority_result)
        }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    let mut calls = 0;
    let mut successful = |_: WorkBudget| {
        calls += 1;
        let store = store.clone();
        async move {
            // Also exercises recovery's unlocked seal; no synchronous mutex can
            // remain held when the actual caller performs its own head proof.
            let _ = ({
                let authority_cancellation = &crate::CancellationToken::new();

                let mut __authority_work =
                    crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
                let __authority_result =
                    AsyncAuthorityStore::head(store.as_ref(), id, &mut __authority_work).await;
                __authority_work.receipt(__authority_result)
            });
            Ok::<_, crate::performance::OperationFailure<crate::facade::FsError>>(
                crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::default(),
                },
            )
        }
    };
    let first = (async {
        let authority_input_4 = &mut successful;

        let mut __authority_work = crate::AuthorityWork::new(budget, &cancellation);
        let __authority_result = AsyncAuthorityStore::create_first_record(
            &store,
            id,
            proposed.clone(),
            &mut __authority_work,
            Some(&mut (&mut *authority_input_4)),
        )
        .await
        .map_err(|error| error.into_error(crate::FsError::from));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(first.value);
    assert_eq!(first.work.backend_write_operations, 1);
    let retry = (async {
        let authority_input_4 = &mut successful;

        let mut __authority_work = crate::AuthorityWork::new(budget, &cancellation);
        let __authority_result = AsyncAuthorityStore::create_first_record(
            &store,
            id,
            proposed,
            &mut __authority_work,
            Some(&mut (&mut *authority_input_4)),
        )
        .await
        .map_err(|error| error.into_error(crate::FsError::from));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(retry.value);
    assert_eq!(retry.work.backend_write_operations, 0);
    assert_eq!(calls, 2);
    Ok(())
}
