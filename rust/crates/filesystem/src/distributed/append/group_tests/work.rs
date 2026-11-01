//! The actual group guard observes the original counters and retained credit.
use super::*;
use crate::kernel::allocation::retained::RetainedAllocation;

struct OriginalWork {
    address: usize,
    prior_items: u64,
    prior_live: u64,
    observed_items: Option<u64>,
    retained: Option<RetainedAllocation>,
    refuse: bool,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    pause: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl AuthorityGuard for OriginalWork {
    type Error = AuthorityStoreError;

    async fn seal(
        &mut self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityError<Self::Error>> {
        assert_eq!(
            work as *mut AuthorityWork<'_> as usize, self.address,
            "group admission replaced the original work frame"
        );
        assert!(work.spent().items_examined >= self.prior_items);
        assert!(work.allocations.live_bytes() >= self.prior_live);
        if self.observed_items.is_none() {
            work.charge(WorkCounters {
                items_examined: 17,
                ..WorkCounters::UNCHARGED
            })?;
            self.retained = Some(RetainedAllocation::claim(64, 1, &mut work.accounting())?);
            self.observed_items = Some(work.spent().items_examined);
        }
        if self.refuse {
            return Err(AuthorityError::Owner(AuthorityStoreError::RejectedStatic(
                "original group guard refused",
            )));
        }
        if let Some(entered) = self.entered.take() {
            entered.send(()).map_err(|_| {
                AuthorityError::Owner(AuthorityStoreError::RejectedStatic(
                    "original group withdrawal observer lost",
                ))
            })?;
        }
        if let Some(pause) = self.pause.take() {
            pause.await.map_err(|_| {
                AuthorityError::Owner(AuthorityStoreError::RejectedStatic(
                    "original group guard producer lost",
                ))
            })?;
        }
        Ok(())
    }
}

pub(super) async fn original_work_survives_group_guard_and_withdrawal() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for exit in 0..4 {
            let (store, authority) = fixture().await?;
            let requests = requests(authority, 2)?;
            let cancellation = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(
                crate::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let mut work = if native {
                AuthorityWork::native(&original, &cancellation)
            } else {
                AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            };
            work.charge(WorkCounters {
                items_examined: 109,
                ..WorkCounters::UNCHARGED
            })?;
            let prior = work.vec_owned::<u8>(64)?;
            let (producer, pause) = tokio::sync::oneshot::channel();
            let (entered, entered_rx) = tokio::sync::oneshot::channel();
            let producer = (exit != 3).then_some(producer);
            let mut guard = OriginalWork {
                address: &mut work as *mut AuthorityWork<'_> as usize,
                prior_items: work.spent().items_examined,
                prior_live: work.allocations.live_bytes(),
                observed_items: None,
                retained: None,
                refuse: exit == 1,
                entered: (exit == 2).then_some(entered),
                pause: (exit >= 2).then_some(pause),
            };
            let result = {
                let future =
                    store.compare_and_append_group(&requests, &[], &mut work, Some(&mut guard));
                if exit == 2 {
                    let mut future = std::pin::pin!(future);
                    tokio::select! {
                        _ = &mut future => return Err("paused group completed before withdrawal".into()),
                        entered = entered_rx => entered?,
                    }
                    None
                } else {
                    Some(future.await)
                }
            };
            drop(producer);
            let observed = guard
                .observed_items
                .ok_or("original guard was not entered")?;
            assert!(observed >= guard.prior_items + 17);
            assert!(
                work.spent().items_examined >= observed,
                "completed group work disappeared on refusal or withdrawal"
            );
            assert!(guard.retained.is_some());
            assert!(work.allocations.live_bytes() >= guard.prior_live);
            if exit == 0 {
                assert!(matches!(
                    result,
                    Some(Ok(AppendGroupOutcome::Published { .. }))
                ));
                assert_eq!(work.spent().backend_write_operations, 1);
            } else {
                if exit != 2 {
                    assert!(matches!(result, Some(Err(AuthorityError::Owner(_)))));
                }
                assert_eq!(work.spent().backend_write_operations, 0);
                let mut observer = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
                assert_eq!(
                    store.head(authority, &mut observer).await?,
                    Head::genesis(Epoch::GENESIS)
                );
            }
            drop(result);
            drop(guard);
            drop(prior);
            drop(work);
            drop(store);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}
