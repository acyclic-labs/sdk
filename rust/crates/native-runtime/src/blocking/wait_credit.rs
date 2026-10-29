use crate::*;
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};

// Resource waits cannot occupy the last worker needed to release their holder.
// This is service credit, not a limit on callers: excess waiters stay in the
// semaphore's intrusive FIFO before submitting a native job.
static WAIT_CREDIT: LazyLock<io::Result<Semaphore>> = LazyLock::new(|| {
    let workers = crate::workers(&BLOCKING_WORKERS, false)?;
    let credit = workers
        .threads
        .checked_sub(1)
        .filter(|credit| *credit != 0)
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    Ok(Semaphore::new(credit))
});

/// Credit retained by one accepted blocking resource wait.
///
/// Move this into the original native closure so caller cancellation cannot
/// refund it before that physical operation finishes.
pub struct BlockingWaitCredit {
    _permit: SemaphorePermit<'static>,
}

/// Admits a potentially blocking resource wait on the existing host service.
///
/// Admission waits asynchronously, leaving a physical worker available for
/// operations that release held resources. The accepted closure owns its
/// credit through physical completion, even if its caller disappears. A
/// cancelled admission submits no job. The semaphore's wait node lives in this
/// future; it creates no task, thread or heap allocation.
///
/// The cancellation future is polled only under contention, so its original
/// funded registration can be lazy. Its errors retain the caller's type. A
/// credit holder must not await another such credit. All ordinary host
/// operations use [`run_blocking_io`] without this resource-wait credit.
pub async fn blocking_wait_credit<E: From<io::Error>>(
    cancellation: impl Future<Output = Result<(), E>>,
) -> Result<BlockingWaitCredit, E> {
    let credit = WAIT_CREDIT
        .as_ref()
        .map_err(|error| E::from(io::Error::new(error.kind(), error.to_string())))?;
    let permit = match credit.try_acquire() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            let mut acquire = std::pin::pin!(credit.acquire());
            let mut cancellation = std::pin::pin!(cancellation);
            std::future::poll_fn(|context| {
                if let Poll::Ready(cancelled) = cancellation.as_mut().poll(context) {
                    cancelled?;
                    return Poll::Ready(Err(E::from(io::ErrorKind::Interrupted.into())));
                }
                acquire
                    .as_mut()
                    .poll(context)
                    .map(|result| result.map_err(|_| E::from(io::ErrorKind::BrokenPipe.into())))
            })
            .await?
        }
        Err(TryAcquireError::Closed) => {
            return Err(E::from(io::ErrorKind::BrokenPipe.into()));
        }
    };
    Ok(BlockingWaitCredit { _permit: permit })
}
