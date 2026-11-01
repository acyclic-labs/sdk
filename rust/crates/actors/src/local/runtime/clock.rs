//! The original host scheduling clock and its deadline event.
use acyclic_stream::runtime::{Executor as _, PlatformExecutor, SleepError};
use acyclic_stream::{BoxProviderFuture, ProviderPlatform, UnixMillisClock as _};
use tokio::sync::watch;

/// Failure of the original scheduling clock or its controller.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ClockError {
    /// A future deadline cannot be reached after its sole producer is dropped.
    #[error("actor clock producer dropped")]
    ProducerDropped,
    /// Refused advancement leaves the original time and deadline events intact.
    #[error("actor clock cannot advance backwards")]
    Reversed,
    /// The original platform timer failed; this is not a manifest timeout.
    #[error(transparent)]
    Platform(#[from] SleepError),
}

/// The host clock. The dispatcher owns the only timer, armed at the earliest
/// committed, undelivered alarm.
pub trait Clock: ProviderPlatform + 'static {
    /// Last observed Unix milliseconds. This read does not wait for advancement.
    fn now_unix_ms(&self) -> u64;
    /// Resolve no earlier than `deadline_unix_ms`, or return the original
    /// clock failure. Loss of the deadline producer must terminate the wait.
    fn sleep_until(
        &self,
        deadline_unix_ms: u64,
    ) -> BoxProviderFuture<'static, Result<(), ClockError>>;
}

/// Wall clock over the platform timer: Tokio natively, the event loop in a browser.
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        acyclic_stream::SystemUnixMillisClock.now_unix_millis()
    }
    fn sleep_until(
        &self,
        deadline_unix_ms: u64,
    ) -> BoxProviderFuture<'static, Result<(), ClockError>> {
        let milliseconds = deadline_unix_ms.saturating_sub(self.now_unix_ms());
        sleep(milliseconds)
    }
}

/// A monotonic host clock advanced by its original controller, without an OS
/// timer or background task. Inject this same clock into the actual runtime.
/// One shared event retains time; only actual deadline waiters own futures.
pub struct ControlledClock {
    time: watch::Receiver<u64>,
}

/// Sole advancement producer for a [`ControlledClock`]. Dropping it ends
/// pending and late waits for future deadlines with [`ClockError::ProducerDropped`].
/// Already reached deadlines succeed, including after this producer is dropped.
pub struct ClockControl {
    time: watch::Sender<u64>,
}

impl ControlledClock {
    /// Create one clock and its sole advancement producer at `unix_ms`.
    pub fn new(unix_ms: u64) -> (Self, ClockControl) {
        let (producer, time) = watch::channel(unix_ms);
        (Self { time }, ClockControl { time: producer })
    }
}

impl ClockControl {
    /// Publish a monotonic time change to the original deadline waiters.
    /// Equal time is a no-op; reversal refuses without changing time or waking.
    /// Work is constant plus notification of actual registered waiters.
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

impl Clock for ControlledClock {
    fn now_unix_ms(&self) -> u64 {
        *self.time.borrow()
    }

    fn sleep_until(
        &self,
        deadline_unix_ms: u64,
    ) -> BoxProviderFuture<'static, Result<(), ClockError>> {
        let mut time = self.time.clone();
        Box::pin(async move {
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

/// The original platform deadline, including registration and producer failure.
pub(super) fn sleep(milliseconds: u64) -> BoxProviderFuture<'static, Result<(), ClockError>> {
    Box::pin(async move {
        PlatformExecutor
            .sleep(milliseconds)
            .await
            .map_err(ClockError::from)
    })
}
