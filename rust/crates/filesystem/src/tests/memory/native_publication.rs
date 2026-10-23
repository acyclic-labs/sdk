use super::*;
use crate::{GuardedAppend, NativeAuthorityGuard, NativeMaterializationWork, PublicationPermit};

const BUDGET: WorkBudget = WorkBudget {
    authority_records_read: 32,
    authority_records_appended: 32,
    authority_bytes_read: 131_072,
    authority_bytes_written: 131_072,
    backend_read_operations: 32,
    backend_write_operations: 32,
    bytes_hashed: 131_072,
    bytes_copied: 131_072,
    items_examined: 128,
    allocation_operations: 32,
    peak_allocation_bytes: 131_072,
    ..WorkCounters::UNCHARGED
};

fn request(id: AuthorityId, head: Head) -> GuardedAppend {
    GuardedAppend {
        authority_id: id,
        epoch: head.epoch,
        expected: head,
        commit: ProposedCommit {
            publication: None,
            operation_id: OperationId::from_bytes([0x91; 16]),
            fingerprint: Digest::from_bytes([0x92; 32]),
            payload: bytes::Bytes::from_static(b"canonical configuration bytes"),
        },
        permit: PublicationPermit::Unrestricted,
    }
}

#[tokio::test]
async fn both_cancellation_roots_deny_all_authority_surfaces() -> Result<(), Box<dyn Error>> {
    let store = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0xe1; 16]);
    let CreateAuthorityOutcome::Created(head) =
        provider!(store, create_authority(id, Epoch::GENESIS), BUDGET)?.value
    else {
        return Err("authority exists".into());
    };
    let mut append = request(id, head);
    append.commit.payload = bytes::Bytes::from(vec![0x5a; 8_192]);
    let operation = append.commit.operation_id;
    let cancel = CancellationToken::new();
    store
        .compare_and_append(
            append,
            &mut AuthorityWork::new(BUDGET, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let head = provider!(store, head(id), BUDGET)?.value;
    for cancel_root in [false, true] {
        let root = CancellationToken::new();
        let supplied = CancellationToken::new();
        let original = NativeMaterializationWork::new(BUDGET, &root).map_err(|(error, _)| error)?;
        let before = original.receipt();
        if cancel_root {
            root.cancel();
        } else {
            supplied.cancel();
        }
        let mut work = AuthorityWork::native(&original, &supplied);
        assert!(store.head(id, &mut work).await.is_err());
        assert!(
            store
                .replay(
                    id,
                    Sequence::GENESIS,
                    ReplayLimit {
                        records: 1,
                        payload_bytes: 8_192
                    },
                    &mut work
                )
                .await
                .is_err()
        );
        assert!(store.fence(id, head, &mut work).await.is_err());
        assert!(
            store
                .find_operation(id, operation, &mut work)
                .await
                .is_err()
        );
        let append = request(id, head);
        assert!(
            store
                .create_first_record(id, append.commit.clone(), &mut work, None::<&mut ()>)
                .await
                .is_err()
        );
        assert!(
            store
                .compare_and_append(append, &mut work, None::<&mut ()>)
                .await
                .is_err()
        );
        assert_eq!(original.receipt(), before);
    }
    assert_eq!(provider!(store, head(id), BUDGET)?.value, head);
    Ok(())
}

#[tokio::test]
async fn first_record_is_atomic_idempotent_and_matches_ordinary_publication()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let original = NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
    let native = MemoryAuthorityStore::default();
    let ordinary = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0x90; 16]);
    let proposed = request(id, Head::genesis(Epoch::GENESIS)).commit;
    let mut work = AuthorityWork::native(&original, &cancel);
    assert!(
        native
            .create_first_record(id, proposed.clone(), &mut work, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity))?
    );
    assert!(
        ordinary
            .create_first_record(
                id,
                proposed.clone(),
                &mut AuthorityWork::new(BUDGET, &cancel),
                None::<&mut ()>
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))?
    );
    let limit = ReplayLimit {
        records: 1,
        payload_bytes: 1024,
    };
    assert_eq!(
        provider!(native, replay(id, Sequence::GENESIS, limit), BUDGET)?.value,
        provider!(ordinary, replay(id, Sequence::GENESIS, limit), BUDGET)?.value
    );
    let head = provider!(native, head(id), BUDGET)?.value;
    assert!(
        native
            .create_first_record(id, proposed.clone(), &mut work, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity))?
    );
    for changed in [
        ProposedCommit {
            publication: None,
            payload: bytes::Bytes::from_static(b"different configuration bytes"),
            ..proposed.clone()
        },
        ProposedCommit {
            publication: None,
            operation_id: OperationId::from_bytes([0x93; 16]),
            ..proposed.clone()
        },
        ProposedCommit {
            publication: None,
            fingerprint: Digest::from_bytes([0x94; 32]),
            ..proposed
        },
    ] {
        assert!(
            !native
                .create_first_record(id, changed, &mut work, None::<&mut ()>)
                .await
                .map_err(|error| error.into_error(std::convert::identity))?
        );
    }
    assert_eq!(provider!(native, head(id), BUDGET)?.value, head);
    original.receipt().verify(BUDGET)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum OwnerError {
    #[error(transparent)]
    Authority(#[from] AuthorityStoreError),
    #[error("owner rejected publication: {0}")]
    Owner(u8),
}

#[tokio::test]
async fn native_owner_reenters_provider_and_preserves_its_rejection() -> Result<(), Box<dyn Error>>
{
    let store = Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x96; 16]);
    provider!(
        store,
        create_authority(id, Epoch::GENESIS),
        WorkBudget::UNBOUNDED
    )?;
    let head = Head::genesis(Epoch::GENESIS);
    let cancel = CancellationToken::new();
    let original = NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
    let guard_store = store.clone();
    let guard_cancel = cancel.clone();
    let guard_work = original.clone();
    let mut guard = NativeAuthorityGuard(move |observed: &NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_work));
        let store = guard_store.clone();
        let cancel = guard_cancel.clone();
        let original = observed.clone();
        async move {
            assert_eq!(
                store
                    .head(id, &mut AuthorityWork::native(&original, &cancel))
                    .await?,
                head
            );
            original
                .charge(WorkCounters {
                    items_examined: 5,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(AuthorityStoreError::from)?;
            Err::<(), OwnerError>(OwnerError::Owner(0xa7))
        }
    });
    let failure = store
        .compare_and_append(
            request(id, head),
            &mut AuthorityWork::native(&original, &cancel),
            Some(&mut guard),
        )
        .await
        .err()
        .ok_or("owner rejection ignored")?;
    assert!(matches!(
        failure.into_error(OwnerError::from),
        OwnerError::Owner(0xa7)
    ));
    assert!(original.receipt().items_examined >= 5);
    assert_eq!(
        provider!(store, head(id), WorkBudget::UNBOUNDED)?.value,
        head
    );
    original.receipt().verify(BUDGET)?;
    Ok(())
}

#[tokio::test]
async fn provider_revalidates_fence_permit_and_cancellation_after_owner_callback()
-> Result<(), Box<dyn Error>> {
    for cancel_owner in [false, true] {
        let store = Arc::new(MemoryAuthorityStore::default());
        let id = AuthorityId::from_bytes([0x97; 16]);
        provider!(
            store,
            create_authority(id, Epoch::GENESIS),
            WorkBudget::UNBOUNDED
        )?;
        let head = Head::genesis(Epoch::GENESIS);
        let root = CancellationToken::new();
        let supplied = CancellationToken::new();
        let original = NativeMaterializationWork::new(BUDGET, &root).map_err(|(error, _)| error)?;
        let guard_store = store.clone();
        let guard_cancel = supplied.clone();
        let mut guard = NativeAuthorityGuard(move |observed: &NativeMaterializationWork| {
            let store = guard_store.clone();
            let cancel = guard_cancel.clone();
            let work = observed.clone();
            async move {
                if cancel_owner {
                    cancel.cancel();
                } else {
                    store
                        .fence(id, head, &mut AuthorityWork::native(&work, &cancel))
                        .await?;
                }
                Ok::<(), AuthorityStoreError>(())
            }
        });
        let outcome = store
            .compare_and_append(
                request(id, head),
                &mut AuthorityWork::native(&original, &supplied),
                Some(&mut guard),
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        if cancel_owner {
            assert!(matches!(outcome, Err(AuthorityStoreError::Cancelled)));
        } else {
            assert!(
                matches!(outcome, Ok(AppendOutcome::Fenced { actual_epoch }) if actual_epoch.get() == 2)
            );
        }
        let actual = provider!(store, head(id), WorkBudget::UNBOUNDED)?.value;
        assert_eq!(actual.sequence, Sequence::GENESIS);
        if cancel_owner {
            assert_eq!(actual, head);
        }
        let mut foreign = request(id, actual);
        foreign.permit = PublicationPermit::Reservation {
            operation_id: foreign.commit.operation_id.into_bytes(),
            gate_tail: 7,
            expected: actual,
        };
        let mut forbidden = NativeAuthorityGuard(
            |_: &NativeMaterializationWork| -> std::future::Ready<Result<(), AuthorityStoreError>> {
                panic!("foreign permit invoked owner")
            },
        );
        let active = CancellationToken::new();
        assert!(matches!(
            store
                .compare_and_append(
                    foreign,
                    &mut AuthorityWork::native(&original, &active),
                    Some(&mut forbidden)
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity)),
            Ok(AppendOutcome::Conflict { actual: observed }) if observed == actual
        ));
        original.receipt().verify(BUDGET)?;
    }
    Ok(())
}

#[tokio::test]
async fn first_record_allocation_denial_leaves_no_partial_authority() -> Result<(), Box<dyn Error>>
{
    let store = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0x98; 16]);
    let cancel = CancellationToken::new();
    let budget = WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
        ..BUDGET
    };
    let original = NativeMaterializationWork::new(budget, &cancel).map_err(|(error, _)| error)?;
    let proposed = request(id, Head::genesis(Epoch::GENESIS)).commit;
    assert!(matches!(
        store
            .create_first_record(
                id,
                proposed,
                &mut AuthorityWork::native(&original, &cancel),
                None::<&mut ()>
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity)),
        Err(AuthorityStoreError::NativeOriginal(_))
    ));
    assert!(matches!(
        provider!(store, head(id), WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    original.receipt().verify(budget)?;
    Ok(())
}

#[tokio::test]
async fn ordinary_first_record_guard_runs_unlocked_on_creation_and_recovery()
-> Result<(), Box<dyn Error>> {
    let store = Arc::new(MemoryAuthorityStore::default());
    let id = AuthorityId::from_bytes([0x9a; 16]);
    let proposed = request(id, Head::genesis(Epoch::GENESIS)).commit;
    let cancel = CancellationToken::new();
    let mut rejected = |remaining: WorkBudget| {
        assert!(remaining.bytes_hashed < BUDGET.bytes_hashed);
        let store = store.clone();
        async move {
            assert!(matches!(
                provider!(store, head(id), WorkBudget::UNBOUNDED),
                Err(AuthorityFailure {
                    error: AuthorityStoreError::Missing,
                    ..
                })
            ));
            Err::<crate::OperationReceipt<()>, _>(crate::OperationFailure::new(
                crate::FsError::Cancelled(crate::CancellationError),
                WorkCounters {
                    items_examined: 3,
                    ..WorkCounters::UNCHARGED
                },
            ))
        }
    };
    let mut work = AuthorityWork::new(BUDGET, &cancel);
    let failure = store
        .create_first_record(id, proposed.clone(), &mut work, Some(&mut (&mut rejected)))
        .await
        .err()
        .ok_or("ordinary owner rejection ignored")?;
    assert!(matches!(
        failure.into_error(crate::FsError::from),
        crate::FsError::Cancelled(_)
    ));
    assert!(work.spent().items_examined >= 3);
    assert!(matches!(
        provider!(store, head(id), WorkBudget::UNBOUNDED),
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
            let _ = provider!(store, head(id), WorkBudget::UNBOUNDED);
            Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt {
                value: (),
                work: WorkCounters::UNCHARGED,
            })
        }
    };
    for _ in 0..2 {
        assert!(
            store
                .create_first_record(
                    id,
                    proposed.clone(),
                    &mut AuthorityWork::new(BUDGET, &cancel),
                    Some(&mut (&mut successful))
                )
                .await
                .map_err(|error| error.into_error(crate::FsError::from))?
        );
    }
    assert_eq!(calls, 2);
    Ok(())
}

#[tokio::test]
async fn opaque_native_publication_rejects_without_owner_or_io_and_keeps_ordinary_fallback()
-> Result<(), Box<dyn Error>> {
    let store = crate::facade::test_authority::OpaqueAuthority::default();
    let id = AuthorityId::from_bytes([0x99; 16]);
    let cancel = CancellationToken::new();
    let original = NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let append = request(id, Head::genesis(Epoch::GENESIS));
    let mut work = AuthorityWork::native(&original, &cancel);
    assert!(matches!(
        store
            .create_first_record(id, append.commit.clone(), &mut work, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity)),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        store
            .compare_and_append(append.clone(), &mut work, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity)),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    let mut forbidden = NativeAuthorityGuard(
        |_: &NativeMaterializationWork| -> std::future::Ready<Result<(), AuthorityStoreError>> {
            panic!("opaque provider invoked native owner")
        },
    );
    assert!(matches!(
        store
            .compare_and_append(append.clone(), &mut work, Some(&mut forbidden))
            .await
            .map_err(|error| error.into_error(std::convert::identity)),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert_eq!(original.receipt(), before);
    assert!(matches!(
        provider!(store, head(id), WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    assert!(
        store
            .create_first_record(
                id,
                append.commit,
                &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel),
                None::<&mut ()>
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))?
    );
    Ok(())
}
