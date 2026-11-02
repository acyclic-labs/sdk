use super::*;
use crate::kernel::allocation::retained::{OwnedAccounting, RetainedAllocation};

#[tokio::test]
async fn scoped_authority_restores_original_work_and_custody_on_withdrawal()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut original = KernelAccounting::standalone(
        WorkBudget {
            items_examined: 2,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    original.charge_items(1)?;
    let first = RetainedAllocation::claim(16, 1, &mut original)?;
    let before = original.spent();
    let live = original
        .standalone_parts()
        .ok_or(WorkError::Overflow)?
        .0
        .live_bytes();
    let second;
    {
        let mut borrowed = AuthorityFrame::borrow(&mut original)?;
        // The former remaining-budget frame starts at zero and fails here.
        assert_eq!(borrowed.spent(), before);
        assert_eq!(borrowed.allocations.live_bytes(), live);
        borrowed.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        second = RetainedAllocation::claim(32, 1, &mut borrowed.accounting())?;
        assert!(matches!(
            borrowed.admit(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            }),
            Err(AuthorityStoreError::Work(WorkError::BudgetExceeded {
                counter: "items_examined",
                ..
            }))
        ));
        let held = OwnedAccounting::take_branch(&mut borrowed.accounting(), &cancellation)?;
        let before = borrowed.spent();
        assert!(matches!(
            borrowed.check(),
            Err(AuthorityStoreError::AccountingHandoffActive)
        ));
        assert!(matches!(
            borrowed.admit(WorkCounters::UNCHARGED),
            Err(AuthorityStoreError::AccountingHandoffActive)
        ));
        assert!(matches!(
            borrowed.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            }),
            Err(AuthorityStoreError::AccountingHandoffActive)
        ));
        assert!(matches!(
            borrowed.record_completed(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            }),
            Err(AuthorityStoreError::AccountingHandoffActive)
        ));
        assert!(matches!(
            borrowed.accept::<_, AuthorityStoreError>(Ok(OperationReceipt {
                value: (),
                work: WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                },
            })),
            Err(AuthorityStoreError::AccountingHandoffActive)
        ));
        assert!(matches!(
            borrowed.scope(),
            Err(AuthorityStoreError::AccountingHandoffActive)
        ));
        assert!(matches!(
            borrowed
                .remaining()
                .map_err(FsError::from)
                .map_err(crate::async_storage::object_error),
            Err(ObjectStoreError::AccountingHandoffActive)
        ));
        assert!(matches!(
            borrowed.operation(),
            Err(AuthorityStoreError::AccountingHandoffActive)
        ));
        assert_eq!(borrowed.spent(), before);
        held.restore(&mut borrowed.accounting())?;
        crate::performance::VERIFICATION_PASSES.with(|passes| passes.set(0));
        borrowed.admit(WorkCounters::UNCHARGED)?;
        crate::performance::VERIFICATION_PASSES.with(|passes| assert_eq!(passes.get(), 0));
        let remaining = borrowed.remaining()?;
        assert_eq!(remaining.items_examined, 0);
        assert_eq!(
            remaining.peak_allocation_bytes,
            borrowed.budget.peak_allocation_bytes - borrowed.allocations.live_bytes()
        );
    }
    assert_eq!(original.spent().items_examined, 2);
    assert_eq!(
        original
            .standalone_parts()
            .ok_or(WorkError::Overflow)?
            .0
            .live_bytes(),
        live + 32
    );

    // Dropping the same suspended operation restores its original frame,
    // without a completion callback or fresh cancellation admission.
    let (producer, receiver) = tokio::sync::oneshot::channel::<()>();
    let before = original.spent();
    let mut operation = Box::pin(async {
        let borrowed = AuthorityFrame::borrow(&mut original)?;
        assert_eq!(borrowed.spent(), before);
        let _ = receiver.await;
        drop(borrowed);
        Ok::<_, FsError>(())
    });
    assert!(futures::poll!(operation.as_mut()).is_pending());
    cancellation.cancel();
    drop(operation);
    drop(producer);
    assert_eq!(original.spent(), before);
    assert!(AuthorityFrame::borrow(&mut original).is_err());
    let mut stopped = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert!(matches!(
        stopped.remaining(),
        Err(AuthorityStoreError::Cancelled)
    ));
    assert_eq!(stopped.spent(), WorkCounters::UNCHARGED);
    assert!(matches!(
        stopped.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        }),
        Err(AuthorityStoreError::Cancelled)
    ));
    assert_eq!(stopped.spent().items_examined, 1);
    stopped.accept::<_, AuthorityStoreError>(Ok(OperationReceipt {
        value: (),
        work: WorkCounters {
            items_examined: 2,
            ..WorkCounters::UNCHARGED
        },
    }))?;
    assert_eq!(stopped.spent().items_examined, 3);
    assert!(matches!(
        stopped.accept::<(), _>(Err(OperationFailure::new(
            AuthorityStoreError::Cancelled,
            WorkCounters {
                items_examined: 3,
                ..WorkCounters::UNCHARGED
            },
        ))),
        Err(AuthorityStoreError::Cancelled)
    ));
    assert_eq!(stopped.spent().items_examined, 6);
    let completed_before = stopped.spent();
    assert!(matches!(
        stopped.accept::<_, FsError>(Ok(OperationReceipt {
            value: (),
            work: WorkCounters {
                items_examined: u64::MAX,
                ..WorkCounters::UNCHARGED
            },
        })),
        Err(FsError::Work(WorkError::Overflow))
    ));
    assert_eq!(stopped.spent(), completed_before);
    assert_eq!(original.spent(), before);
    drop(first);
    drop(second);
    assert_eq!(
        original
            .standalone_parts()
            .ok_or(WorkError::Overflow)?
            .0
            .live_bytes(),
        live - 16,
        "the original ledger control remains until its issuer is destroyed"
    );

    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = CancellationToken::new();
        let native = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = native.live_allocation_bytes();
        let mut original = KernelAccounting::native(&native, &cancellation);
        original.charge_items(1)?;
        let mut borrowed = AuthorityFrame::borrow(&mut original)?;
        assert_eq!(borrowed.spent(), native.receipt());
        let held = RetainedAllocation::claim(32, 1, &mut borrowed.accounting())?;
        borrowed.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        assert_eq!(native.receipt().items_examined, 2);
        let caller_cancel = CancellationToken::new();
        let mut caller = AuthorityWork::native(&native, &caller_cancel);
        let before = native.receipt();
        caller_cancel.cancel();
        assert!(matches!(
            caller.remaining(),
            Err(AuthorityStoreError::Cancelled)
        ));
        assert!(matches!(
            caller.operation(),
            Err(AuthorityStoreError::Cancelled)
        ));
        assert_eq!(native.receipt(), before);
        assert!(borrowed.remaining().is_ok());
        cancellation.cancel();
        drop(borrowed);
        assert_eq!(original.spent(), native.receipt());
        drop(held);
        assert_eq!(native.live_allocation_bytes(), baseline);
    }
    Ok(())
}
