use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AuthorityFailure, AuthorityStoreError, AuthorityWork,
    CancellationError, CancellationToken, CreateAuthorityOutcome, Epoch, FsError, Head,
    MemoryAuthorityStore, NativeAuthorityGuard, NativeMaterializationWork, ProposedCommit,
    PublicationPermit, ReplayLimit, Sequence,
};
use acyclic_fs_format::identity::{AuthorityId, Digest, OperationId};
use acyclic_fs_work::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{error::Error, sync::Arc};

#[path = "../../../support/native_authority_publication.rs"]
mod fixture;
use fixture::{BUDGET, request};

#[tokio::test]
async fn both_cancellation_roots_deny_all_authority_surfaces() -> Result<(), Box<dyn Error>> {
    let store = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0xe1; 16]);
    let CreateAuthorityOutcome::Created(head) =
        provider_receipt!(store, create_authority(id, Epoch::GENESIS), BUDGET)?.value
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
    let head = provider_receipt!(store, head(id), BUDGET)?.value;
    for cancel_root in [false, true] {
        let root = CancellationToken::new();
        let supplied = CancellationToken::new();
        let original = NativeMaterializationWork::new(BUDGET, &root).map_err(|(error, _)| error)?;
        let before = original.receipt()?;
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
        assert_eq!(original.receipt()?, before);
    }
    assert_eq!(provider_receipt!(store, head(id), BUDGET)?.value, head);
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
        provider_receipt!(native, replay(id, Sequence::GENESIS, limit), BUDGET)?.value,
        provider_receipt!(ordinary, replay(id, Sequence::GENESIS, limit), BUDGET)?.value
    );
    let head = provider_receipt!(native, head(id), BUDGET)?.value;
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
    assert_eq!(provider_receipt!(native, head(id), BUDGET)?.value, head);
    original.receipt()?.verify(BUDGET)?;
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
        provider_receipt!(store, head(id), WorkBudget::UNBOUNDED),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    original.receipt()?.verify(budget)?;
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
                provider_receipt!(store, head(id), WorkBudget::UNBOUNDED),
                Err(AuthorityFailure {
                    error: AuthorityStoreError::Missing,
                    ..
                })
            ));
            Err::<OperationReceipt<()>, _>(OperationFailure::new(
                FsError::Cancelled(CancellationError),
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
        failure.into_error(FsError::from),
        FsError::Cancelled(_)
    ));
    assert!(work.spent()?.items_examined >= 3);
    assert!(matches!(
        provider_receipt!(store, head(id), WorkBudget::UNBOUNDED),
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
            let _ = provider_receipt!(store, head(id), WorkBudget::UNBOUNDED);
            Ok::<_, OperationFailure<FsError>>(OperationReceipt {
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
                .map_err(|error| error.into_error(FsError::from))?
        );
    }
    assert_eq!(calls, 2);
    Ok(())
}

#[tokio::test]
async fn provider_revalidates_fence_permit_and_cancellation_after_owner_callback()
-> Result<(), Box<dyn Error>> {
    for cancel_owner in [false, true] {
        let store = Arc::new(MemoryAuthorityStore::default());
        let id = AuthorityId::from_bytes([0x97; 16]);
        provider_receipt!(
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
        let actual = provider_receipt!(store, head(id), WorkBudget::UNBOUNDED)?.value;
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
        let owner_called = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&owner_called);
        let mut forbidden = NativeAuthorityGuard(move |_: &NativeMaterializationWork| {
            flag.store(true, Ordering::Relaxed);
            std::future::ready(Ok::<(), AuthorityStoreError>(()))
        });
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
        assert!(!owner_called.load(Ordering::Relaxed));
        original.receipt()?.verify(BUDGET)?;
    }
    Ok(())
}
