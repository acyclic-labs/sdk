//! Futures built on the heap, so callers hold a pointer instead of them.
//!
//! A future holds every future it is awaiting, so an operation whose layers
//! await each other is one value as large as its deepest path, and each poll
//! frame reserves room for the futures it builds and awaits. A native mount
//! callback polls a whole operation on the driver's thread, so that size
//! becomes stack depth: before this boundary existed, one lazy rename used
//! 1.8 MiB of a 2 MiB FUSE thread in an unoptimized build.
//!
//! [`in_heap`] builds a future in a short frame of its own and moves it into
//! a box. An async function whose body runs through it is a pointer to its
//! callers, and none of its state enters their futures or frames.

use std::future::Future;
use std::pin::Pin;

/// Builds `create`'s future directly in a heap allocation.
///
/// The future is built in this function's own frame, which returns before the
/// future is first polled, so no poll frame ever holds it.
#[inline(never)]
pub(crate) fn in_heap<F: Future>(create: impl FnOnce() -> F) -> Pin<Box<F>> {
    Box::pin(create())
}

/// Admits a box into an existing ownership ledger before constructing its future.
/// The caller keeps that ledger in `state` and releases `bytes` after dropping
/// the completed box; this does not create or reset an operation receipt.
pub(crate) fn in_heap_admitted<C, F: Future, E>(
    mut state: C,
    admit: impl FnOnce(&mut C, u64) -> Result<(), E>,
    create: impl FnOnce(C) -> F,
) -> Result<(Pin<Box<F>>, u64), E> {
    let bytes = crate::foundation::usize_to_u64(std::mem::size_of::<F>());
    admit(&mut state, bytes)?;
    Ok((in_heap(move || create(state)), bytes))
}

/// Known logical ownership of the existing boxed future, not proof DATA.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HeapFutureEvent {
    BeforeFrame { bytes: u64, budget: crate::WorkBudget },
    Admitted(crate::WorkCounters),
    Released(u64),
}

type HeapFutureObserver<'a> =
    &'a (dyn Fn(HeapFutureEvent) -> Result<u64, crate::WorkError> + Sync);

struct HeapFutureRelease<'a> {
    observer: Option<HeapFutureObserver<'a>>,
    bytes: u64,
}

impl Drop for HeapFutureRelease<'_> {
    fn drop(&mut self) {
        if let Some(observer) = self.observer {
            observer(HeapFutureEvent::Released(self.bytes))
                .expect("the actual admitted SDK future must release once");
        }
    }
}

/// Preserves the short-frame boundary while admitting its actual future body.
/// The nested permit reserves the live box until the operation has completed.
pub(crate) async fn in_heap_measured<F, T, E>(
    create: impl FnOnce(crate::WorkBudget) -> F,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    cancelled: E,
    observer: Option<HeapFutureObserver<'_>>,
) -> crate::MeasuredResult<crate::OperationReceipt<T>, E>
where
    F: Future<Output = crate::MeasuredResult<crate::OperationReceipt<T>, E>>,
    E: From<crate::WorkError>,
{
    in_heap_measured_with_error(create, budget, cancellation, cancelled, observer, E::from).await
}

/// Maps internal admission errors through the same SDK error as the operation.
pub(crate) async fn in_heap_measured_with_error<F, T, E>(
    create: impl FnOnce(crate::WorkBudget) -> F,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    cancelled: E,
    observer: Option<HeapFutureObserver<'_>>,
    map_work: impl Fn(crate::WorkError) -> E,
) -> crate::MeasuredResult<crate::OperationReceipt<T>, E>
where
    F: Future<Output = crate::MeasuredResult<crate::OperationReceipt<T>, E>>,
{
    let mut work = crate::WorkCounters::default();
    if cancellation.check().is_err() {
        return Err(crate::OperationFailure::new(cancelled, work));
    }
    let bytes = crate::foundation::usize_to_u64(std::mem::size_of::<F>());
    let standing = match observer {
        Some(observer) => observer(HeapFutureEvent::BeforeFrame { bytes, budget })
            .map_err(|error| crate::OperationFailure::before_work(map_work(error)))?,
        None => 0,
    };
    let mut frame_budget = budget;
    frame_budget.peak_allocation_bytes = frame_budget.peak_allocation_bytes.checked_sub(standing)
        .ok_or_else(|| crate::OperationFailure::before_work(map_work(crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", observed: standing, maximum: budget.peak_allocation_bytes,
        })))?;
    work.charge_allocation(u64::from(bytes != 0), bytes, &frame_budget)
        .map_err(|error| crate::OperationFailure::new(map_work(error), work))?;
    if let Some(observer) = observer {
        observer(HeapFutureEvent::Admitted(work))
            .map_err(|error| crate::OperationFailure::before_work(map_work(error)))?;
    }
    // Declared before all fallible bookkeeping and the box so admission retires
    // on errors and unwinding, after the whole future has been destroyed.
    let frame = HeapFutureRelease { observer, bytes };
    work.peak_allocation_bytes = work.peak_allocation_bytes.checked_add(standing)
        .ok_or_else(|| crate::OperationFailure::new(map_work(crate::WorkError::Overflow), work))?;
    let mut remaining = work.remaining(budget)
        .map_err(|error| crate::OperationFailure::new(map_work(error), work))?;
    // The existing cache leaf reserves its own backing. Only this new frame
    // is delegated here, otherwise the very same cache would be charged twice.
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(bytes)
        .ok_or_else(|| crate::OperationFailure::new(map_work(crate::WorkError::Overflow), work))?;
    let mut future = in_heap(move || create(remaining));
    let result = future.as_mut().await;
    drop(future);
    drop(frame);
    match result {
        Err(mut failure) => {
            // Ordinary failures already own their receipt. Retain that same
            // allocation and the whole caller error while joining frame work.
            *failure.work = work.with_backend(*failure.work, bytes).unwrap_or(work);
            Err(failure)
        }
        Ok(receipt) => {
            work = work.with_backend(receipt.work, bytes)
                .map_err(|error| crate::OperationFailure::new(map_work(error), work))?;
            work.verify(budget).map_err(|error| crate::OperationFailure::new(map_work(error), work))?;
            Ok(crate::OperationReceipt { value: receipt.value, work })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HeapFutureEvent, in_heap_admitted, in_heap_measured};
    use crate::kernel::allocation::AllocationLedger;
    use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Debug, Eq, PartialEq)]
    enum TestError {
        Cancelled,
        Work(WorkError),
    }

    impl From<WorkError> for TestError {
        fn from(error: WorkError) -> Self {
            Self::Work(error)
        }
    }

    struct CopyFuture {
        budget: WorkBudget,
        copied: [u8; 8],
    }

    impl Future for CopyFuture {
        type Output = MeasuredResult<OperationReceipt<[u8; 8]>, TestError>;

        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let mut work = WorkCounters::default();
            let result = (|| {
                work.charge_items(2, &self.budget)?;
                let delta = WorkCounters { bytes_copied: 4, ..WorkCounters::default() };
                work.charge(&delta, &self.budget)?;
                self.copied[..4].copy_from_slice(b"heap");
                work.charge(&delta, &self.budget)?;
                self.copied[4..].copy_from_slice(b"body");
                Ok(self.copied)
            })();
            Poll::Ready(result.map(|value| OperationReceipt { value, work })
                .map_err(|error: WorkError| OperationFailure::new(error.into(), work)))
        }
    }

    fn copy_budget() -> WorkBudget {
        WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: crate::foundation::usize_to_u64(std::mem::size_of::<CopyFuture>()),
            items_examined: 2,
            bytes_copied: 8,
            ..WorkBudget::default()
        }
    }

    #[tokio::test]
    async fn cold_cancellation_and_future_denial_do_not_construct_body() {
        let created = Cell::new(0);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let failure = in_heap_measured(|budget| {
            created.set(created.get() + 1);
            CopyFuture { budget, copied: [0; 8] }
        }, copy_budget(), &cancelled, TestError::Cancelled, None).await.expect_err("cold cancellation");
        assert_eq!(failure.error, TestError::Cancelled);
        assert_eq!(*failure.work, WorkCounters::default());

        let cancellation = CancellationToken::new();
        for budget in [
            WorkBudget { allocation_operations: 0, ..copy_budget() },
            WorkBudget { peak_allocation_bytes: copy_budget().peak_allocation_bytes - 1, ..copy_budget() },
        ] {
            let failure = in_heap_measured(|budget| {
                created.set(created.get() + 1);
                CopyFuture { budget, copied: [0; 8] }
            }, budget, &cancellation, TestError::Cancelled, None).await.expect_err("future admission");
            assert!(matches!(failure.error, TestError::Work(WorkError::BudgetExceeded { .. })));
            assert_eq!(*failure.work, WorkCounters::default());
        }
        assert_eq!(created.get(), 0);
    }

    #[tokio::test]
    async fn exact_future_permit_counts_envelope_once_and_preserves_copy_failure() {
        let cancellation = CancellationToken::new();
        let receipt = in_heap_measured(|budget| {
            assert_eq!(budget.allocation_operations, 0);
            assert_eq!(budget.peak_allocation_bytes, 0);
            CopyFuture { budget, copied: [0; 8] }
        }, copy_budget(), &cancellation, TestError::Cancelled, None).await.expect("exact permit");
        assert_eq!(receipt.value, *b"heapbody");
        assert_eq!(receipt.work, copy_budget());

        let budget = WorkBudget { bytes_copied: 7, ..copy_budget() };
        let failure = in_heap_measured(|budget| CopyFuture { budget, copied: [0; 8] }, budget, &cancellation, TestError::Cancelled, None).await.expect_err("second copy denied");
        assert_eq!(failure.error, TestError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied", observed: 8, maximum: 7,
        }));
        assert_eq!(*failure.work, WorkCounters { bytes_copied: 4, ..copy_budget() });
    }

    #[tokio::test]
    async fn cache_standing_and_actual_frame_share_exact_peak_without_double_delegation() {
        let cancellation = CancellationToken::new();
        let cache = Box::new([0_u8; 13]);
        let admitted = std::sync::atomic::AtomicU64::new(0);
        let released = std::sync::atomic::AtomicU64::new(0);
        let trace = |event| {
            match event {
                HeapFutureEvent::BeforeFrame { .. } => return Ok(cache.len() as u64),
                HeapFutureEvent::Admitted(work) => {
                    admitted.fetch_add(work.allocation_operations, std::sync::atomic::Ordering::AcqRel);
                }
                HeapFutureEvent::Released(bytes) => {
                    released.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
                }
            }
            Ok(0)
        };
        let budget = WorkBudget {
            peak_allocation_bytes: copy_budget().peak_allocation_bytes + cache.len() as u64,
            ..copy_budget()
        };
        let receipt = in_heap_measured(|remaining| {
            assert_eq!(remaining.peak_allocation_bytes, cache.len() as u64);
            CopyFuture { budget: remaining, copied: [0; 8] }
        }, budget, &cancellation, TestError::Cancelled, Some(&trace))
            .await.expect("exact standing plus frame");
        assert_eq!(receipt.value, *b"heapbody");
        assert_eq!(receipt.work, budget);
        assert_eq!(admitted.load(std::sync::atomic::Ordering::Acquire), 1);
        assert_eq!(released.load(std::sync::atomic::Ordering::Acquire),
            copy_budget().peak_allocation_bytes);

        let created = Cell::new(false);
        let failure = in_heap_measured(|remaining| {
            created.set(true);
            CopyFuture { budget: remaining, copied: [0; 8] }
        }, WorkBudget { peak_allocation_bytes: budget.peak_allocation_bytes - 1, ..budget },
            &cancellation, TestError::Cancelled, Some(&trace))
            .await.expect_err("cache and frame overlap denied");
        assert!(!created.get());
        assert_eq!(*failure.work, WorkCounters::UNCHARGED);
        assert_eq!(admitted.load(std::sync::atomic::Ordering::Acquire), 1);
        assert_eq!(released.load(std::sync::atomic::Ordering::Acquire),
            copy_budget().peak_allocation_bytes);
    }

    #[tokio::test]
    async fn shared_future_uses_existing_ledger_without_resetting_work_or_live_bytes() {
        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        allocations.claim_bytes(17, 1, &mut work, WorkBudget::UNBOUNDED).expect("parent allocation");
        let parent = Box::new([0_u8; 17]);
        work.charge_items(3, &WorkBudget::UNBOUNDED).expect("parent prefix");
        let created = Cell::new(false);
        let admitted_bytes = Cell::new(0);
        let (mut future, bytes) = in_heap_admitted(
            (&mut work, &mut allocations),
            |(work, allocations), bytes| {
                admitted_bytes.set(bytes);
                allocations.claim_bytes(bytes, u64::from(bytes != 0), work, WorkBudget::UNBOUNDED)
            },
            |(work, allocations)| {
                created.set(true);
                let admitted_bytes = &admitted_bytes;
                async move {
                    assert_eq!(allocations.live_bytes(), 17 + admitted_bytes.get());
                    assert_eq!(work.items_examined, 3);
                    work.charge_items(1, &WorkBudget::UNBOUNDED)
                }
            },
        ).expect("shared admission");
        assert!(created.get());
        assert_eq!(bytes, crate::foundation::usize_to_u64(std::mem::size_of_val(future.as_ref().get_ref())));
        future.as_mut().await.expect("shared body");
        drop(future);
        assert_eq!(allocations.live_bytes(), 17 + bytes);
        allocations.release(bytes).expect("release completed box");
        assert_eq!(allocations.live_bytes(), 17);
        assert_eq!(work.items_examined, 4);
        assert_eq!(work.allocation_operations, 2);
        assert_eq!(work.peak_allocation_bytes, 17 + bytes);
        assert_eq!(parent.len(), 17);
        drop(parent);
        allocations.release(17).expect("release parent");
    }

    #[test]
    fn shared_future_denial_keeps_the_original_prefix_and_never_constructs_body() {
        let mut work = WorkCounters::default();
        let mut allocations = AllocationLedger::default();
        allocations.claim_bytes(17, 1, &mut work, WorkBudget::UNBOUNDED).expect("parent allocation");
        let parent = Box::new([0_u8; 17]);
        work.charge_items(3, &WorkBudget::UNBOUNDED).expect("parent prefix");
        let prior = work;
        let created = Cell::new(false);
        let result = in_heap_admitted(
            (&mut work, &mut allocations),
            |(work, allocations), bytes| {
                let budget = WorkBudget {
                    allocation_operations: 1,
                    peak_allocation_bytes: 17 + bytes,
                    items_examined: 3,
                    ..WorkBudget::default()
                };
                allocations.claim_bytes(bytes, u64::from(bytes != 0), work, budget)
            },
            |_| {
                created.set(true);
                std::future::ready(())
            },
        );
        assert!(result.is_err());
        assert!(!created.get());
        assert_eq!(work, prior);
        assert_eq!(allocations.live_bytes(), 17);
        assert_eq!(parent.len(), 17);
        drop(parent);
        allocations.release(17).expect("release parent");
    }

    #[test]
    fn observer_future_panic_preserves_exact_frame_and_data_prefix_after_box_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct PanicDataFuture<'a> {
            owner: &'a crate::kernel::ProofDataCustody,
            dropped: &'a AtomicBool,
            budget: WorkBudget,
        }
        impl Future for PanicDataFuture<'_> {
            type Output = MeasuredResult<OperationReceipt<()>, TestError>;

            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                let mut work = WorkCounters::default();
                let mut allocations = AllocationLedger::default();
                allocations.claim_bytes(3, 1, &mut work, self.budget).expect("actual DATA admission");
                self.owner.record_data_work(work).expect("admitted DATA prefix");
                let mut data = Vec::new();
                data.try_reserve_exact(3).expect("three actual DATA bytes");
                let copy = WorkCounters { bytes_copied: 3, ..WorkCounters::default() };
                work.charge(&copy, &self.budget).expect("actual copy admission");
                self.owner.record_data_work(WorkCounters {
                    peak_allocation_bytes: 3, ..copy
                }).expect("completed copy prefix");
                data.extend_from_slice(b"cow");
                panic!("backend unwind after actual SDK DATA copy");
            }
        }
        impl Drop for PanicDataFuture<'_> {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::Release);
            }
        }

        let owner = crate::kernel::ProofDataCustody::default();
        let dropped = AtomicBool::new(false);
        let observer = |event| {
            if matches!(event, super::HeapFutureEvent::Released(_)) {
                assert!(dropped.load(Ordering::Acquire), "the entire box must die before release");
            }
            owner.record_heap_future_event(event)
        };
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::async_storage::poll_immediate(in_heap_measured(
                |budget| PanicDataFuture { owner: &owner, dropped: &dropped, budget },
                WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), TestError::Cancelled,
                Some(&observer),
            ))
        }));
        assert!(panic.is_err());
        assert_eq!(owner.retained_bytes(), 0, "frames are not proof DATA adoption");
        let prefix = owner.pending_data_work();
        assert_eq!(prefix.allocation_operations, 2);
        assert_eq!(prefix.bytes_copied, 3);
        assert_eq!(prefix.peak_allocation_bytes,
            crate::foundation::usize_to_u64(std::mem::size_of::<PanicDataFuture<'_>>()) + 3);
        assert_eq!(owner.take_completed_data_work(), prefix);
        assert_eq!(owner.pending_data_work(), WorkCounters::default());
        assert_eq!(owner.completed_data_work(), prefix);

        // A later segment must not inherit the retired frame's live bytes.
        owner.record_data_work(WorkCounters {
            allocation_operations: 1, peak_allocation_bytes: 1, ..WorkCounters::default()
        }).expect("later actual DATA");
        assert_eq!(owner.pending_data_work().peak_allocation_bytes, 1);
    }
}
