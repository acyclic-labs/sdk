//! One native future allocation and its original-ledger custody.

use crate::Result;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_stream::BoxProviderFuture;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An admitted native future whose allocation custody outlives its physical box.
#[must_use = "native futures must be awaited or dropped"]
pub struct NativeFutureBundle<'a, T> {
    future: BoxProviderFuture<'a, Result<T>>,
    _custody: NativeAllocation,
}

impl<'a, T> NativeFutureBundle<'a, T> {
    /// Borrows this future's original ledger without a clone or a new allowance.
    #[must_use]
    pub const fn work(&self) -> &NativeMaterializationWork {
        self._custody.work()
    }

    /// Admits the concrete future's real allocation before invoking its builder.
    pub fn new<F, B>(work: &NativeMaterializationWork, build: B) -> Result<Self>
    where
        F: Future<Output = Result<T>> + Send + 'a,
        B: FnOnce() -> F,
    {
        work.remaining()?;
        let bytes = std::mem::size_of::<F>() as u64;
        let custody = if bytes == 0 {
            work.retain_owned(0)?
        } else {
            work.claim(bytes)?
        };
        let future: BoxProviderFuture<'a, Result<T>> = Box::pin(build());
        Ok(Self { future, _custody: custody })
    }
}

impl<T> Future for NativeFutureBundle<'_, T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, WorkBudget};
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
        // The original ledger itself uses its one genuinely attempted allocation.
        let exhausted = NativeMaterializationWork::new(budget(1), &cancellation)
            .expect("admitted original ledger");
        let built = AtomicBool::new(false);
        let result = NativeFutureBundle::new(&exhausted, || {
            built.store(true, Ordering::SeqCst);
            std::future::ready(Ok(()))
        });
        assert!(matches!(result, Err(crate::Error::NativeWork { .. })));
        assert!(!built.load(Ordering::SeqCst));
        assert_eq!(exhausted.receipt().allocation_operations, 1);
        assert_eq!(exhausted.receipt().backend_read_operations, 0);

        let cancelled = NativeMaterializationWork::new(budget(2), &cancellation)
            .expect("admitted original ledger");
        cancellation.cancel();
        let result = NativeFutureBundle::new(&cancelled, || {
            built.store(true, Ordering::SeqCst);
            std::future::ready(Ok(()))
        });
        assert!(matches!(result, Err(crate::Error::NativeWork { .. })));
        assert!(!built.load(Ordering::SeqCst));
        assert_eq!(cancelled.receipt().allocation_operations, 1);
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
            assert!(self.work.remaining().expect("live original ledger")
                .peak_allocation_bytes < self.baseline);
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
    fn zero_sized_future_requires_no_heap_allocation_attempt() -> Result<()> {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(1), &cancellation)
            .expect("admitted original ledger");
        let future = NativeFutureBundle::new(&work, || ZeroSizedFuture)?;
        assert_eq!(work.receipt().allocation_operations, 1);
        drop(future);
        assert_eq!(work.receipt().allocation_operations, 1);
        Ok(())
    }

    #[test]
    fn actual_boxed_future_drops_under_original_live_custody() -> Result<()> {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(2), &cancellation)
            .expect("admitted original ledger");
        let baseline = work.remaining()?.peak_allocation_bytes;
        let dropped = AtomicBool::new(false);
        let future = NativeFutureBundle::new(&work, || DropProbe {
            work: &work,
            dropped: &dropped,
            baseline,
        })?;
        assert!(future.work().shares_ledger(&work));
        assert_eq!(work.receipt().allocation_operations, 2);
        drop(future);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }
}
