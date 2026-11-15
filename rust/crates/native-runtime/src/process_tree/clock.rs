//! The one clock every deadline of a process tree reads.
//!
//! A deadline's consumer binds its clock when it is constructed, on the
//! launching side. A worker, reaper or waiter thread then reads that clock
//! instead of looking one up, so it never falls back to the operating system's
//! clock outside a simulation scope. Each deadline is woken by its own clock:
//! the OS clock through the events hub's wake time, a seeded simulation's
//! paused clock through a [`ProcessWake`] notified by that runtime's timer.
//! Paused time does not elapse in real time, so an OS timeout would re-arm
//! forever. That timer runs only on the runtime's own thread, so a wait on it
//! yields to the simulation host rather than parking its worker.
use super::events::{self, Source, Wait};
use super::*;

/// The clock of one deadline consumer.
pub(crate) enum CaptureClock {
    /// The operating system's monotonic clock.
    Os,
    /// A seeded simulation's paused monotonic clock.
    #[cfg(feature = "test-support")]
    Simulated(crate::testing::CapturedClock),
}

impl CaptureClock {
    /// The active seeded simulation's clock, otherwise the operating system's.
    pub(crate) fn current() -> Self {
        #[cfg(feature = "test-support")]
        if let Some(clock) = crate::testing::CapturedClock::capture() {
            return Self::Simulated(clock);
        }
        Self::Os
    }

    pub(crate) fn now(&self) -> Instant {
        match self {
            Self::Os => Instant::now(),
            #[cfg(feature = "test-support")]
            Self::Simulated(clock) => clock.instant(),
        }
    }

    /// Arms the wake of `deadline` on this clock. Create one per consumer and
    /// retain it across that consumer's waits.
    pub(crate) fn alarm(&self, deadline: Instant) -> io::Result<Alarm> {
        match self {
            Self::Os => Ok(Alarm::Os(deadline)),
            #[cfg(feature = "test-support")]
            Self::Simulated(clock) => {
                let wake = ProcessWake::new()?;
                let notify = wake.clone();
                let at = tokio::time::Instant::from_std(deadline);
                // The task holds a clone of the signal, so both ends stay open
                // and the nonblocking notification can only coalesce.
                let timer = clock.runtime().spawn(async move {
                    tokio::time::sleep_until(at).await;
                    notify.notify()
                });
                Ok(Alarm::Simulated {
                    wake,
                    _timer: Timer(timer.abort_handle()),
                })
            }
        }
    }
}

/// The wake of one deadline, fired by the clock the deadline reads.
pub(crate) enum Alarm {
    /// The events hub wakes at this OS instant.
    Os(Instant),
    /// Notified once the simulated clock reaches the deadline. The signal
    /// stays readable after that, and the deadline stays expired.
    #[cfg(feature = "test-support")]
    Simulated { wake: ProcessWake, _timer: Timer },
}

/// Arms one wait on `sources` that `alarm`, if any, also wakes: an OS alarm
/// as the wait's wake time, a simulated one as a source whose wait yields to
/// the simulation host while it blocks.
pub(super) fn arm(sources: &[Source<'_>], alarm: Option<&Alarm>) -> io::Result<Wait<'static>> {
    match alarm {
        None => events::arm(sources, None),
        Some(Alarm::Os(deadline)) => events::arm(sources, Some(*deadline)),
        #[cfg(feature = "test-support")]
        Some(Alarm::Simulated { wake, .. }) => {
            let sources = sources.iter().copied().chain([wake.source()]);
            events::arm(&sources.collect::<Vec<_>>(), None).map(Wait::simulated)
        }
    }
}

/// Cancels the simulated timer when its alarm is dropped.
#[cfg(feature = "test-support")]
pub(crate) struct Timer(tokio::task::AbortHandle);

#[cfg(feature = "test-support")]
impl Drop for Timer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
#[cfg(feature = "test-support")]
#[cfg(not(loom))]
pub(super) mod tests {
    use super::super::ProcessTree;
    use super::super::scale_tests::{Blocked, Quiet, blocked};
    use crate::CaptureSchedule;
    use crate::testing::Schedule;
    use std::io;
    use std::time::Duration;

    /// A deadline far beyond any real elapsed time a test can take, so only
    /// simulated time decides it.
    pub(in crate::process_tree) const HOUR: Duration = Duration::from_secs(60 * 60);

    /// Runs `work` in a seeded scope on a current-thread runtime whose clock
    /// is paused, releasing every native boundary the work reaches.
    pub(in crate::process_tree) fn seeded<T>(
        work: impl AsyncFnOnce(&Schedule) -> io::Result<T>,
    ) -> io::Result<T> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            tokio::time::pause();
            let (schedule, mut boundaries) = Schedule::seeded(71);
            let operation = schedule.scope(work(&schedule));
            let mut operation = std::pin::pin!(operation);
            let outcome = loop {
                tokio::select! {
                    biased;
                    boundary = boundaries.recv() => {
                        boundary.expect("lost process boundary").release();
                    },
                    outcome = &mut operation => break outcome,
                }
            };
            schedule.drained().await;
            outcome
        })
    }

    /// Starts `work` on Tokio's blocking pool, outside the scope and its
    /// controlled workers, with only the simulation's inherited sources. Call
    /// it inside [`seeded`]. Tokio holds off the paused clock's automatic
    /// advance while a blocking task runs, so only an explicit advance moves
    /// time.
    pub(in crate::process_tree) fn unscoped<T: Send + 'static>(
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = io::Result<T>> {
        let waiting = tokio::task::spawn_blocking(crate::testing::inherit_host(work));
        async move { waiting.await.map_err(io::Error::other) }
    }

    /// A quiet child, which only this test can release, waited on by `wait`
    /// in a seeded scope. Nothing but the simulated deadline can end the
    /// wait: once its worker yields, the idle paused runtime advances to it.
    fn quiet<T>(wait: impl AsyncFnOnce(ProcessTree) -> io::Result<T>) -> io::Result<T> {
        crate::initialize_native_io_services()?;
        let Blocked { tree, release } = blocked()?;
        let outcome = seeded(async move |_: &Schedule| wait(tree).await);
        drop(release);
        outcome
    }

    /// A capture on the normal controlled worker of a paused current-thread
    /// runtime times out on simulated time. Its worker blocks between turns
    /// while that runtime's thread drives it, and only that thread can fire
    /// the deadline's timer, so the wait must yield to its host.
    ///
    /// Negative control: a wait that parks its worker holds the runtime's
    /// thread too, so the timer never fires and the test hangs until
    /// nextest's per-test limit fails it.
    #[test]
    fn a_controlled_capture_deadline_is_woken_by_simulated_time() -> io::Result<()> {
        let outcome = quiet(async |tree| {
            tree.capture(
                &crate::TaskSpawner::current(),
                CaptureSchedule::FromLaunch(HOUR),
                0,
                None,
                Quiet,
            )
            .await
        })?;
        let failure = outcome
            .result
            .expect_err("the simulated deadline never ended the capture");
        assert_eq!(failure.error.kind(), io::ErrorKind::TimedOut);
        assert!(failure.cleanup_completed);
        Ok(())
    }

    /// A direct-child exit wait on a controlled worker is woken by the same
    /// alarm, over the events hub, on every OS. Its deadline is fixed on the
    /// simulator side; an OS timeout would park it for a real hour.
    #[test]
    fn a_controlled_exit_deadline_is_woken_by_simulated_time() -> io::Result<()> {
        let error = quiet(async |mut tree| {
            let schedule = CaptureSchedule::FromLaunch(HOUR).launched();
            crate::testing::spawn_blocking(move || tree.wait(schedule)).await
        })?
        .expect_err("the simulated deadline never ended the exit wait");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        Ok(())
    }
}
