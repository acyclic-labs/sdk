use super::*;
use crate::{NativeAuthorityGuard, NativeMaterializationWork, PublicationPermit};

#[path = "../../../tests/support/native_authority_publication.rs"]
mod fixture;
use fixture::{BUDGET, request};

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
    provider_receipt!(
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
    assert!(original.receipt().unwrap().items_examined >= 5);
    assert_eq!(
        provider_receipt!(store, head(id), WorkBudget::UNBOUNDED)?.value,
        head
    );
    original.receipt().unwrap().verify(BUDGET)?;
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
        original.receipt().unwrap().verify(BUDGET)?;
    }
    Ok(())
}

#[tokio::test]
async fn opaque_native_publication_rejects_without_owner_or_io_and_keeps_ordinary_fallback()
-> Result<(), Box<dyn Error>> {
    let store = crate::facade::test_authority::OpaqueAuthority::default();
    let id = AuthorityId::from_bytes([0x99; 16]);
    let cancel = CancellationToken::new();
    let original = NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(error, _)| error)?;
    let before = original.receipt().unwrap();
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
    assert_eq!(original.receipt().unwrap(), before);
    assert!(matches!(
        provider_receipt!(store, head(id), WorkBudget::UNBOUNDED),
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
