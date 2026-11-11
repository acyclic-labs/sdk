//! One native future allocation and its original-ledger custody.

use crate::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// An admitted future whose custody outlives its physical allocation.
#[must_use = "native futures must be awaited or dropped"]
pub struct NativeFutureBundle<'a, T, E = NativeTreeMaterializationError> {
    future: Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>,
    custody: NativeAllocation,
}

impl<'a, T, E> NativeFutureBundle<'a, T, E> {
    /// Borrows the future's original ledger.
    #[must_use]
    pub const fn work(&self) -> &NativeMaterializationWork {
        self.custody.work()
    }

    /// Admits the concrete allocation before invoking its builder.
    pub fn new<F>(work: &NativeMaterializationWork, build: impl FnOnce() -> F) -> Result<Self, E>
    where
        F: Future<Output = Result<T, E>> + Send + 'a,
        E: From<NativeTreeMaterializationError>,
    {
        work.remaining()?;
        let bytes = std::mem::size_of::<F>() as u64;
        let custody = work.claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)?;
        Ok(Self {
            future: Box::pin(build()),
            custody,
        })
    }
}

impl<T, E> Future for NativeFutureBundle<'_, T, E> {
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CancellationToken, WorkBudget};
    type Result<T> = std::result::Result<T, NativeTreeMaterializationError>;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn budget(allocation_operations: u64) -> WorkBudget {
        WorkBudget {
            allocation_operations,
            peak_allocation_bytes: 4096,
            ..WorkBudget::default()
        }
    }

    #[test]
    fn exhausted_allocator_and_cancelled_work_reject_before_future_builder() {
        let cancellation = CancellationToken::new();
        let exhausted = NativeMaterializationWork::new(budget(1), &cancellation)
            .expect("admitted original ledger");
        let built = AtomicBool::new(false);
        let result = NativeFutureBundle::new(&exhausted, || {
            built.store(true, Ordering::SeqCst);
            std::future::ready(Ok(()))
        });
        assert!(matches!(
            result,
            Err(NativeTreeMaterializationError::Work(_))
        ));
        assert!(!built.load(Ordering::SeqCst));

        let cancelled = NativeMaterializationWork::new(budget(2), &cancellation)
            .expect("admitted original ledger");
        cancellation.cancel();
        let result = NativeFutureBundle::new(&cancelled, || {
            built.store(true, Ordering::SeqCst);
            std::future::ready(Ok(()))
        });
        assert!(matches!(
            result,
            Err(NativeTreeMaterializationError::Cancelled)
        ));
        assert!(!built.load(Ordering::SeqCst));
    }

    struct DropProbe<'a> {
        work: &'a NativeMaterializationWork,
        dropped: &'a AtomicBool,
        baseline: u64,
    }

    impl Future for DropProbe<'_> {
        type Output = Result<()>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for DropProbe<'_> {
        fn drop(&mut self) {
            assert!(
                self.work
                    .remaining()
                    .expect("live original ledger")
                    .peak_allocation_bytes
                    < self.baseline
            );
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    struct ZeroSizedFuture;

    impl Future for ZeroSizedFuture {
        type Output = Result<()>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn zero_sized_future_fits_an_exhausted_allocation_budget() -> Result<()> {
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(1), &cancellation).map_err(|(cause, _)| cause)?;
        let future = NativeFutureBundle::new(&work, || ZeroSizedFuture)?;
        drop(future);
        Ok(())
    }

    #[test]
    fn actual_boxed_future_drops_under_original_live_custody() -> Result<()> {
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(2), &cancellation).map_err(|(cause, _)| cause)?;
        let baseline = work.remaining()?.peak_allocation_bytes;
        let dropped = AtomicBool::new(false);
        let future = NativeFutureBundle::new(&work, || DropProbe {
            work: &work,
            dropped: &dropped,
            baseline,
        })?;
        assert!(future.work().shares_ledger(&work));
        drop(future);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }
}
