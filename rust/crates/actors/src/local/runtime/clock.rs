//! The original host scheduling clock and its deadline event.
use acyclic_stream::runtime::{Executor as _, PlatformExecutor};
use acyclic_stream::{BoxProviderFuture, ProviderPlatform, UnixMillisClock};
use tokio::sync::watch;

/// Failure of the original scheduling clock or its controller.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ClockError {
    /// A future deadline cannot be reached after its sole producer is dropped.
    #[error("actor clock producer dropped")]
    ProducerDropped,
    /// Refused advancement leaves the original time and deadline events intact.
    #[error("actor clock cannot advance backwards")]
    Reversed,
    /// The shared native deadline producer refused or failed this wait.
    #[error("actor clock: {0}")]
    Native(#[source] std::sync::Arc<std::io::Error>),
}

/// The host clock. The dispatcher owns the only timer, armed at the earliest
/// committed, undelivered alarm.
pub trait Clock: UnixMillisClock + ProviderPlatform + 'static {
    /// Resolve no earlier than `deadline_unix_ms`, or return the original
    /// clock failure. Loss of the deadline producer must terminate the wait.
    fn sleep_until(
        &self,
        deadline_unix_ms: u64,
    ) -> BoxProviderFuture<'static, Result<(), ClockError>>;
}

/// Wall clock over the shared native absolute alarm or browser event loop.
pub struct SystemClock;
impl UnixMillisClock for SystemClock {
    fn is_system_clock(&self) -> bool {
        true
    }

    fn now_unix_millis(&self) -> u64 {
        acyclic_stream::SystemUnixMillisClock.now_unix_millis()
    }
}

impl Clock for SystemClock {
    fn sleep_until(
        &self,
        deadline_unix_ms: u64,
    ) -> BoxProviderFuture<'static, Result<(), ClockError>> {
        #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
        {
            // Capture the original clock producer before this future can move
            // to another executor or outlive its creating scope.
            let alarm = (self.now_unix_millis() < deadline_unix_ms)
                .then(|| acyclic_native_runtime::WallAlarm::wait(deadline_unix_ms));
            Box::pin(async move {
                if let Some(alarm) = alarm {
                    alarm
                        .map_err(|error| ClockError::Native(std::sync::Arc::new(error)))?
                        .await
                        .map_err(|error| ClockError::Native(std::sync::Arc::new(error)))?;
                }
                Ok(())
            })
        }
        #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
        {
            let milliseconds = deadline_unix_ms.saturating_sub(self.now_unix_millis());
            Box::pin(async move {
                platform_sleep(milliseconds).await;
                Ok(())
            })
        }
    }
}

/// A monotonic host clock advanced by its original controller, without an OS
/// timer or background task. Inject this same clock into the actual runtime.
/// One shared event retains time; only actual deadline waiters own futures.
pub struct ControlledClock {
    time: watch::Receiver<u64>,
    registered: watch::Sender<Option<u64>>,
    registrations: watch::Receiver<Option<u64>>,
}

/// Sole advancement producer for a [`ControlledClock`]. Dropping it ends
/// pending and late waits for future deadlines with [`ClockError::ProducerDropped`].
/// Already reached deadlines succeed, including after this producer is dropped.
pub struct ClockControl {
    time: watch::Sender<u64>,
    registered: watch::Sender<Option<u64>>,
}

impl ControlledClock {
    /// Create one clock and its sole advancement producer at `unix_ms`.
    pub fn new(unix_ms: u64) -> (Self, ClockControl) {
        let (producer, time) = watch::channel(unix_ms);
        let (registered, registrations) = watch::channel(None);
        (
            Self {
                time,
                registered: registered.clone(),
                registrations,
            },
            ClockControl {
                time: producer,
                registered,
            },
        )
    }

    /// Resolves once a deadline at or after `at_least` has been registered on
    /// this clock, returning that deadline. A world advances time only after
    /// this event, never on a scheduler tick that might still be preparing the
    /// wait. An earlier deadline that is already due does not satisfy it.
    pub fn registered_at_least(&self, at_least: u64) -> BoxProviderFuture<'static, u64> {
        let mut registrations = self.registrations.clone();
        Box::pin(async move {
            loop {
                if let Some(deadline) = *registrations.borrow_and_update()
                    && deadline >= at_least
                {
                    return deadline;
                }
                if registrations.changed().await.is_err() {
                    return std::future::pending().await;
                }
            }
        })
    }
}

impl ClockControl {
    /// Publish a monotonic time change to the original deadline waiters.
    /// Equal time is a no-op; reversal refuses without changing time or waking.
    /// Work is constant plus notification of actual registered waiters.
    pub fn clear_registered(&mut self) {
        self.registered.send_replace(None);
    }

    /// Advance the shared time to `unix_ms`, waking every reached waiter.
    /// Equal time is a no-op; reversal refuses without changing time or waking.
    pub fn advance_to(&mut self, unix_ms: u64) -> Result<(), ClockError> {
        let mut result = Ok(());
        self.time.send_if_modified(|current| {
            if unix_ms < *current {
                result = Err(ClockError::Reversed);
                return false;
            }
            if unix_ms == *current {
                return false;
            }
            *current = unix_ms;
            true
        });
        result
    }
}

impl UnixMillisClock for ControlledClock {
    fn now_unix_millis(&self) -> u64 {
        *self.time.borrow()
    }
}

impl Clock for ControlledClock {
    fn sleep_until(
        &self,
        deadline_unix_ms: u64,
    ) -> BoxProviderFuture<'static, Result<(), ClockError>> {
        let mut time = self.time.clone();
        let registered = self.registered.clone();
        Box::pin(async move {
            // Keep the highest deadline until the controller clears it: an
            // earlier waiter must not hide a later one from the observer.
            registered.send_if_modified(|current| match current {
                Some(at) if *at >= deadline_unix_ms => false,
                _ => {
                    *current = Some(deadline_unix_ms);
                    true
                }
            });
            loop {
                if *time.borrow_and_update() >= deadline_unix_ms {
                    return Ok(());
                }
                time.changed()
                    .await
                    .map_err(|_| ClockError::ProducerDropped)?;
            }
        })
    }
}

/// A platform sleep of `milliseconds`, in steps the platform timer can express.
pub(super) fn sleep(milliseconds: u64) -> BoxProviderFuture<'static, ()> {
    Box::pin(platform_sleep(milliseconds))
}

async fn platform_sleep(milliseconds: u64) {
    let mut remaining = milliseconds;
    while remaining > 0 {
        let step = u32::try_from(remaining).unwrap_or(u32::MAX);
        PlatformExecutor.sleep(step).await;
        remaining -= u64::from(step);
    }
}
