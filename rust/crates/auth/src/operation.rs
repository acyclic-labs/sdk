//! Caller-owned deadline and cancellation shared across an entire operation.
//!
//! Hosts supply the clock's expiry future once. Credential resolution, connection,
//! response reading and any explicitly classified retry share that same stop.
//! Stopping a wait does not establish whether a remote mutation committed.

use futures::{Future, FutureExt, future::BoxFuture, future::Shared};

/// Why the caller stopped waiting. Neither variant proves remote cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Stopped {
    /// The operation's original deadline elapsed.
    #[error("operation deadline elapsed")]
    Deadline,
    /// The caller cancelled its wait.
    #[error("operation wait cancelled")]
    Cancelled,
}

/// One absolute budget and monotonic cancellation signal, shared by clones.
#[derive(Clone)]
pub struct Context {
    deadline_unix_millis: u64,
    stopped: Shared<BoxFuture<'static, Stopped>>,
}

impl Context {
    /// Bind the host's absolute-deadline timer and caller cancellation once.
    /// The timer must correspond to `deadline_unix_millis`, rather than a fresh
    /// duration for each stage or retry. Dropping a host cancellation sender
    /// should be mapped according to that host's cancellation contract.
    pub fn new(
        deadline_unix_millis: u64,
        expires: impl Future<Output = ()> + Send + 'static,
        cancelled: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        let stopped = async move {
            futures::pin_mut!(expires, cancelled);
            match futures::future::select(cancelled, expires).await {
                futures::future::Either::Left(_) => Stopped::Cancelled,
                futures::future::Either::Right(_) => Stopped::Deadline,
            }
        }
        .boxed()
        .shared();
        Self {
            deadline_unix_millis,
            stopped,
        }
    }

    /// Original absolute deadline for transports that admit it on the wire.
    pub fn deadline_unix_millis(&self) -> u64 {
        self.deadline_unix_millis
    }

    /// Run a stage under the original budget. A ready stop is checked before
    /// polling work, so a cancelled operation cannot begin another callback.
    pub async fn run<T>(&self, work: impl Future<Output = T>) -> Result<T, Stopped> {
        let stopped = self.stopped.clone();
        futures::pin_mut!(stopped, work);
        match futures::future::select(stopped, work).await {
            futures::future::Either::Left((reason, _)) => Err(reason),
            futures::future::Either::Right((value, _)) => Ok(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{
        executor::block_on,
        future::{pending, ready},
    };
    use std::cell::Cell;

    #[test]
    fn expired_budget_never_polls_credentials_or_retry_work() {
        block_on(async {
            let context = Context::new(1, ready(()), pending());
            let calls = Cell::new(0);
            for _ in 0..2 {
                let result = context
                    .clone()
                    .run(async { calls.set(calls.get() + 1) })
                    .await;
                assert_eq!(result, Err(Stopped::Deadline));
            }
            assert_eq!(calls.get(), 0);
        });
    }

    #[test]
    fn cancellation_during_credentials_prevents_transport_and_survives_clone() {
        block_on(async {
            let (cancel, cancelled) = futures::channel::oneshot::channel();
            let context = Context::new(10, pending(), async {
                let _ = cancelled.await;
            });
            let sent = Cell::new(false);
            let operation = context.run(async {
                let _ = cancel.send(());
                pending::<()>().await;
                sent.set(true);
            });
            assert_eq!(operation.await, Err(Stopped::Cancelled));
            assert_eq!(
                context.clone().run(ready(())).await,
                Err(Stopped::Cancelled)
            );
            assert!(!sent.get());
        });
    }

    #[test]
    fn a_completed_stage_does_not_restart_the_deadline() {
        block_on(async {
            let (expire, expired) = futures::channel::oneshot::channel();
            let context = Context::new(
                10,
                async {
                    let _ = expired.await;
                },
                pending(),
            );
            assert_eq!(context.run(ready(7)).await, Ok(7));
            let _ = expire.send(());
            assert_eq!(context.run(pending::<()>()).await, Err(Stopped::Deadline));
        });
    }
}
