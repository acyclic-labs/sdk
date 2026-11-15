//! The one clock every deadline of a process tree reads.
//!
//! A deadline's consumer binds its clock when it is constructed, on the
//! launching side. A worker, reaper or waiter thread then reads that clock
//! instead of looking one up, so it never falls back to the operating system's
//! clock outside a simulation scope. Each deadline is woken by its own clock:
//! the OS clock through the events hub's wake time, a seeded simulation's
//! paused clock through a [`ProcessWake`] notified by that runtime's timer.
//! Paused time does not elapse in real time, so an OS timeout would re-arm
//! forever.
use super::events::Source;
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

impl Alarm {
    /// The OS wake time of a wait, which a simulated alarm never sets.
    pub(super) fn wake_at(&self) -> Option<Instant> {
        match self {
            Self::Os(deadline) => Some(*deadline),
            #[cfg(feature = "test-support")]
            Self::Simulated { .. } => None,
        }
    }

    /// The readiness source a simulated alarm adds to a wait.
    pub(super) fn source(&self) -> Option<Source<'_>> {
        match self {
            Self::Os(_) => None,
            #[cfg(feature = "test-support")]
            Self::Simulated { wake, .. } => Some(wake.source()),
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
    use super::super::scale_tests::{Blocked, blocked};
    use crate::testing::Schedule;
    use crate::{CaptureSchedule, ProcessInput};
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

    /// Starts `work` on the seeded runtime's blocking pool, outside the scope
    /// and with only the simulation's inherited sources, as every native
    /// worker has. Call it inside [`seeded`]. Tokio holds off the paused
    /// clock's automatic advance while a blocking task runs, so only an
    /// explicit advance moves time.
    pub(in crate::process_tree) fn unscoped<T: Send + 'static>(
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = io::Result<T>> {
        let waiting = tokio::task::spawn_blocking(crate::testing::inherit_host(work));
        async move { waiting.await.map_err(io::Error::other) }
    }

    /// Runs `wait` against a child that only this test can release, then
    /// advances simulated time past its deadline. `schedule` is evaluated
    /// inside the seeded scope. With `armed`, the advance waits until the
    /// capture has armed its first wait.
    fn expire<T: Send + 'static>(
        armed: bool,
        schedule: impl FnOnce() -> CaptureSchedule,
        wait: impl FnOnce(&mut ProcessTree, CaptureSchedule) -> T + Send + 'static,
    ) -> io::Result<T> {
        crate::initialize_native_io_services()?;
        let Blocked { mut tree, release } = blocked()?;
        let (sender, receipt) = tokio::sync::oneshot::channel();
        if armed {
            tree.capture_armed = Some(sender);
        }
        let outcome = seeded(async move |simulation: &Schedule| {
            let schedule = schedule();
            let waiting = unscoped(move || {
                let mut tree = tree;
                wait(&mut tree, schedule)
            });
            if armed {
                receipt.await.map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "capture ended before arming")
                })?;
            }
            simulation.advance(2 * HOUR).await;
            waiting.await
        });
        drop(release);
        outcome
    }

    /// A capture deadline on a seeded schedule's paused clock expires when
    /// simulated time reaches it, woken by that clock's own alarm.
    ///
    /// The child blocks until its stdin closes and only this test holds the
    /// writer, so the deadline is the capture's only way to end. Its
    /// launch-relative hour starts on the worker, which binds the simulated
    /// clock. The capture parks on its armed wait before the clock moves;
    /// advancing two simulated hours must end it as `TimedOut` at once.
    ///
    /// Negative control: a pump that reads `Instant::now()` for its deadline
    /// arms an OS wake an hour of real time away. Paused time never reaches
    /// it, so the test hangs until nextest's 10 s limit fails it.
    #[test]
    fn a_simulated_capture_deadline_is_woken_by_simulated_time() -> io::Result<()> {
        let failure = expire(
            true,
            || CaptureSchedule::FromLaunch(HOUR),
            |tree, schedule| {
                tree.wait_with_output(
                    schedule,
                    0,
                    None,
                    || Ok(()),
                    |_| Ok(ProcessInput::default()),
                    None,
                    None,
                    None,
                )
            },
        )?
        .expect_err("the simulated deadline never ended the capture");
        assert_eq!(failure.error.kind(), io::ErrorKind::TimedOut);
        assert!(failure.cleanup_completed);
        Ok(())
    }

    /// A direct-child exit wait on a simulated deadline is woken by the same
    /// alarm, over the events hub, on every OS. The deadline is fixed on the
    /// simulator side, so the wait expires whether it parks before or after
    /// the clock moves; an OS timeout would park it for a real hour.
    #[test]
    fn a_simulated_exit_deadline_is_woken_by_simulated_time() -> io::Result<()> {
        let error = expire(
            false,
            || CaptureSchedule::FromLaunch(HOUR).launched(),
            |tree, schedule| tree.wait(schedule),
        )?
        .expect_err("the simulated deadline never ended the exit wait");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        Ok(())
    }
}
