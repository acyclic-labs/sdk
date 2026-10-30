//! Caller-owned deadline and cancellation shared across an entire operation.
//!
//! Hosts supply the clock's expiry future once. Credential resolution, connection,
//! response reading and any explicitly classified retry share that same stop.
//! Stopping a wait does not establish whether a remote mutation committed.

use futures::{Future, FutureExt, future::Shared};
use std::{sync::Arc, time::Duration};

#[cfg(not(target_arch = "wasm32"))]
/// Drop-owned native stop registration.
pub type StopFuture = futures::future::BoxFuture<'static, Stopped>;
#[cfg(target_arch = "wasm32")]
/// Drop-owned browser stop registration on the original event loop.
pub type StopFuture = futures::future::LocalBoxFuture<'static, Stopped>;

/// Native clock ownership can cross executor threads.
#[cfg(not(target_arch = "wasm32"))]
pub trait ClockOwner: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> ClockOwner for T {}
/// Browser clock ownership stays on its original event loop.
#[cfg(target_arch = "wasm32")]
pub trait ClockOwner {}
#[cfg(target_arch = "wasm32")]
impl<T> ClockOwner for T {}

/// The ORIGINAL invocation's clock, supplied only by its trusted custodian.
/// Its wall-time projection must advance monotonically from the original clock
/// anchor. Reading time performs no IO. Registering an earlier expiry uses that
/// same clock, eagerly, and dropping the returned future unregisters it. Loss of
/// its producer must wake the future with `Cancelled`, never leave it pending.
/// A request or resource transport must not supply a replacement clock.
pub trait Clock: ClockOwner {
    /// Observe the retained clock, or refuse after its owner is lost.
    fn now_unix_millis(&self) -> Result<u64, Stopped>;
    /// Register exactly one absolute expiry on the retained clock.
    fn expires_at(&self, deadline_unix_millis: u64) -> Result<StopFuture, Stopped>;
}

/// Budget qualification refuses owners without an original clock projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BudgetError {
    /// An event-only context cannot manufacture remaining time or a child timer.
    #[error("original operation clock unavailable")]
    ClockUnavailable,
    /// The original invocation or its earlier child deadline stopped.
    #[error(transparent)]
    Stopped(#[from] Stopped),
}

/// A host event accepted by the platform executor.
#[cfg(not(target_arch = "wasm32"))]
pub trait OperationEvent: Future + Send + 'static {}
#[cfg(not(target_arch = "wasm32"))]
impl<F: Future + Send + 'static> OperationEvent for F {}
/// A browser event retains its owning event loop.
#[cfg(target_arch = "wasm32")]
pub trait OperationEvent: Future + 'static {}
#[cfg(target_arch = "wasm32")]
impl<F: Future + 'static> OperationEvent for F {}

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
    stopped: Shared<StopFuture>,
    clock: Option<Arc<dyn Clock>>,
    expiry: Option<Shared<StopFuture>>,
}

impl Context {
    /// Bind the host's absolute-deadline timer and caller cancellation once.
    /// The timer must correspond to `deadline_unix_millis`, rather than a fresh
    /// duration for each stage or retry. Dropping a host cancellation sender
    /// should be mapped according to that host's cancellation contract.
    pub fn new(
        deadline_unix_millis: u64,
        expires: impl OperationEvent<Output = ()>,
        cancelled: impl OperationEvent<Output = ()>,
    ) -> Self {
        let stopped = async move {
            futures::pin_mut!(expires, cancelled);
            match futures::future::select(cancelled, expires).await {
                futures::future::Either::Left(_) => Stopped::Cancelled,
                futures::future::Either::Right(_) => Stopped::Deadline,
            }
        };
        Self::from_stopped(deadline_unix_millis, stopped)
    }

    /// Bind one original host stop event. Adapters preserve its typed reason;
    /// losing the event's producer must resolve it as cancellation.
    pub fn from_stopped(
        deadline_unix_millis: u64,
        stopped: impl OperationEvent<Output = Stopped>,
    ) -> Self {
        let stopped: StopFuture = Box::pin(stopped);
        Self {
            deadline_unix_millis,
            stopped: stopped.shared(),
            clock: None,
            expiry: None,
        }
    }

    /// Retain the original custodian's clock beside its original stop event.
    /// `stopped` already contains this absolute deadline and cancellation. This
    /// constructor registers no timer, reads no ambient clock and starts no work.
    pub fn from_clock(
        deadline_unix_millis: u64,
        stopped: impl OperationEvent<Output = Stopped>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            clock: Some(clock),
            ..Self::from_stopped(deadline_unix_millis, stopped)
        }
    }

    /// Narrow ONCE before credentials or dispatch. Neither absent argument adds
    /// a fallback timeout. Clones and subsequent stages retain this same child.
    /// Work is O(1): one clock read and, only for an earlier deadline, one expiry
    /// registration. Nested narrowing replaces that registration rather than
    /// retaining a chain of previous budgets or cancellation owners.
    pub fn narrow(
        &self,
        timeout: Option<Duration>,
        authenticated_expiry_unix_millis: Option<u64>,
    ) -> Result<Self, BudgetError> {
        self.check()?;
        let clock = self.clock.as_ref().ok_or(BudgetError::ClockUnavailable)?;
        let now = clock.now_unix_millis()?;
        let mut deadline = authenticated_expiry_unix_millis
            .map_or(self.deadline_unix_millis, |expiry| {
                expiry.min(self.deadline_unix_millis)
            });
        if let Some(timeout) = timeout {
            // Compare in u128 first: even an unrepresentably large duration
            // cannot overflow, refuse a valid original budget or extend it.
            let remaining = deadline.saturating_sub(now);
            if timeout.as_millis() < u128::from(remaining) {
                deadline = now + timeout.as_millis() as u64;
            }
        }
        if deadline <= now {
            return Err(Stopped::Deadline.into());
        }
        let child = if deadline == self.deadline_unix_millis {
            self.clone()
        } else {
            Self {
                deadline_unix_millis: deadline,
                stopped: self.stopped.clone(),
                clock: self.clock.clone(),
                expiry: Some(clock.expires_at(deadline)?.shared()),
            }
        };
        child.check()?;
        Ok(child)
    }

    /// Remaining transport metadata from the SAME retained clock. This starts
    /// no registration and never renews the absolute deadline.
    pub fn remaining(&self) -> Result<Duration, BudgetError> {
        self.check()?;
        let clock = self.clock.as_ref().ok_or(BudgetError::ClockUnavailable)?;
        let remaining = self
            .deadline_unix_millis
            .saturating_sub(clock.now_unix_millis()?);
        self.check()?;
        if remaining == 0 {
            return Err(Stopped::Deadline.into());
        }
        Ok(Duration::from_millis(remaining))
    }

    /// Original absolute deadline for transports that admit it on the wire.
    pub fn deadline_unix_millis(&self) -> u64 {
        self.deadline_unix_millis
    }

    /// Observe the original stop before a synchronous admission boundary.
    /// This polls only the existing host event and starts no work or timer.
    pub fn check(&self) -> Result<(), Stopped> {
        if let Some(reason) = self.stopped.clone().now_or_never() {
            return Err(reason);
        }
        if let Some(reason) = self
            .expiry
            .as_ref()
            .and_then(|expiry| expiry.clone().now_or_never())
        {
            return Err(reason);
        }
        Ok(())
    }

    /// Run a stage under the original budget. A ready stop is checked before
    /// polling work and before accepting its result. A callback that stops its
    /// own operation cannot publish success in the same poll.
    pub async fn run<T>(&self, work: impl Future<Output = T>) -> Result<T, Stopped> {
        let stopped = async {
            let original = self.stopped.clone();
            let expiry = async {
                match &self.expiry {
                    Some(expiry) => expiry.clone().await,
                    None => futures::future::pending().await,
                }
            };
            futures::pin_mut!(original, expiry);
            match futures::future::select(original, expiry).await {
                futures::future::Either::Left((reason, _))
                | futures::future::Either::Right((reason, _)) => reason,
            }
        };
        futures::pin_mut!(stopped, work);
        match futures::future::select(stopped, work).await {
            futures::future::Either::Left((reason, _)) => Err(reason),
            futures::future::Either::Right((value, stopped)) => match stopped.now_or_never() {
                Some(reason) => Err(reason),
                None => Ok(value),
            },
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
    fn narrowed_budget_preserves_original_stop_and_constant_registration_ownership() {
        use futures::channel::oneshot;
        use std::{
            collections::BTreeMap,
            sync::{
                Mutex,
                atomic::{AtomicU64, Ordering},
            },
        };
        type Registrations = BTreeMap<u64, (u64, oneshot::Sender<Stopped>)>;
        #[derive(Default)]
        struct State {
            now: AtomicU64,
            next: AtomicU64,
            registrations: Mutex<Registrations>,
        }
        #[derive(Clone, Default)]
        struct SimClock(Arc<State>);
        impl SimClock {
            fn advance(&self, now: u64) {
                self.0.now.store(now, Ordering::SeqCst);
                let mut registrations = self.0.registrations.lock().unwrap();
                let due: Vec<_> = registrations
                    .iter()
                    .filter_map(|(id, (deadline, _))| (*deadline <= now).then_some(*id))
                    .collect();
                for id in due {
                    let (_, sender) = registrations.remove(&id).unwrap();
                    let _ = sender.send(Stopped::Deadline);
                }
            }
        }
        struct Registration(SimClock, u64);
        impl Drop for Registration {
            fn drop(&mut self) {
                self.0.0.registrations.lock().unwrap().remove(&self.1);
            }
        }
        impl Clock for SimClock {
            fn now_unix_millis(&self) -> Result<u64, Stopped> {
                Ok(self.0.now.load(Ordering::SeqCst))
            }
            fn expires_at(&self, deadline: u64) -> Result<StopFuture, Stopped> {
                let id = self.0.next.fetch_add(1, Ordering::SeqCst);
                let (send, receive) = oneshot::channel();
                self.0
                    .registrations
                    .lock()
                    .unwrap()
                    .insert(id, (deadline, send));
                let guard = Registration(self.clone(), id);
                Ok(Box::pin(async move {
                    let _guard = guard;
                    receive.await.unwrap_or(Stopped::Cancelled)
                }))
            }
        }
        block_on(async {
            for cause in ["expiry", "cancel", "loss", "completing-expiry"] {
                let clock = SimClock::default();
                clock.advance(10);
                let (cancel, cancelled) = oneshot::channel();
                let root = Context::from_clock(
                    1000,
                    async {
                        let _ = cancelled.await;
                        Stopped::Cancelled
                    },
                    Arc::new(clock.clone()),
                );
                let parent = root
                    .narrow(Some(Duration::from_millis(50)), Some(80))
                    .unwrap();
                let child = parent.narrow(None, Some(40)).unwrap();
                drop(parent);
                assert_eq!(clock.0.registrations.lock().unwrap().len(), 1);
                let mut child = child;
                // Arbitrarily many stages retain the same registration. Huge
                // relative durations neither overflow nor extend the grant.
                for _ in 0..1000 {
                    child = child.narrow(Some(Duration::MAX), None).unwrap();
                    assert_eq!(child.remaining(), Ok(Duration::from_millis(30)));
                }
                assert_eq!(clock.0.next.load(Ordering::SeqCst), 2);
                assert_eq!(child.run(ready(7)).await, Ok(7));
                assert!(matches!(
                    child.narrow(None, Some(10)),
                    Err(BudgetError::Stopped(Stopped::Deadline))
                ));
                let reason = match cause {
                    "expiry" => {
                        clock.advance(40);
                        Stopped::Deadline
                    }
                    "completing-expiry" => {
                        assert_eq!(
                            child
                                .run(async {
                                    clock.advance(40);
                                    7
                                })
                                .await,
                            Err(Stopped::Deadline)
                        );
                        Stopped::Deadline
                    }
                    "cancel" => {
                        cancel.send(()).unwrap();
                        Stopped::Cancelled
                    }
                    _ => {
                        clock.0.registrations.lock().unwrap().clear();
                        Stopped::Cancelled
                    }
                };
                // Await the actual channel event, including loss of its sender.
                assert_eq!(child.run(pending::<()>()).await, Err(reason));
                assert_eq!(child.run(ready(7)).await, Err(reason));
                if reason == Stopped::Deadline {
                    // Negative control: observing only the original invocation
                    // admits work after the child's authenticated expiry.
                    assert_eq!(root.run(ready(7)).await, Ok(7));
                }
                drop(child);
                assert!(clock.0.registrations.lock().unwrap().is_empty());
            }
            let event_only = Context::from_stopped(100, pending());
            assert!(matches!(
                event_only.narrow(None, None),
                Err(BudgetError::ClockUnavailable)
            ));
        });
    }

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

    #[test]
    fn a_stop_produced_by_completing_work_refuses_its_result() {
        block_on(async {
            for lose_producer in [false, true] {
                let (cancel, cancelled) = futures::channel::oneshot::channel();
                let context = Context::new(10, pending(), async {
                    let _ = cancelled.await;
                });
                let result = context
                    .run(async {
                        if lose_producer {
                            drop(cancel);
                        } else {
                            cancel.send(()).unwrap();
                        }
                        7
                    })
                    .await;
                assert_eq!(result, Err(Stopped::Cancelled));
                assert_eq!(context.clone().run(ready(8)).await, Err(Stopped::Cancelled));
            }
            let (expire, expired) = futures::channel::oneshot::channel();
            let context = Context::new(
                10,
                async {
                    let _ = expired.await;
                },
                pending(),
            );
            assert_eq!(
                context
                    .run(async {
                        expire.send(()).unwrap();
                        7
                    })
                    .await,
                Err(Stopped::Deadline)
            );
            // An unchanged owner still admits a ready completion.
            assert_eq!(
                Context::new(10, pending(), pending()).run(ready(7)).await,
                Ok(7)
            );
        });
    }
}
