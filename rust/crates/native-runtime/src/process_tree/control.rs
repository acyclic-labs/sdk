//! Shared command cancellation and authenticated wall-clock expiry.
use super::{
    CaptureHandler, CaptureProgress, CaptureWakes, ProcessInput, ProcessOutput, ProcessWake, io,
};
use crate::WallAlarm;
use std::sync::Arc;

struct State {
    wake: ProcessWake,
    expiry: Option<Expiry>,
}

type ReadClock = Box<dyn Fn() -> u64 + Send + Sync>;

struct Expiry {
    unix_ms: u64,
    clock: Option<ReadClock>,
    _alarm: Alarm,
}

enum Alarm {
    Wall { _registration: WallAlarm },
    Bound { task: crate::AbortHandle },
}

impl Drop for Alarm {
    fn drop(&mut self) {
        if let Self::Bound { task } = self {
            task.abort();
        }
    }
}

struct StopOnDrop(ProcessWake);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.0.stop();
    }
}

/// One command's irreversible stop signal and original authority expiry.
/// The physical owner retains a clone through reaping. All commands use the
/// same wall alarm and kernel readiness services, including quiet commands.
#[derive(Clone)]
pub struct ProcessControl(Arc<State>);

impl ProcessControl {
    /// Retains this original stop/expiry source beside a capture's callbacks.
    pub fn bind<H: CaptureHandler>(self, handler: H) -> ControlledCapture<H> {
        ControlledCapture {
            inner: handler,
            control: self,
        }
    }
    /// Registers the original absolute authority expiry before dispatch.
    pub fn open(expiry_unix_ms: u64) -> io::Result<Self> {
        let wake = ProcessWake::new()?;
        let signal = wake.clone();
        let registration = WallAlarm::register(
            expiry_unix_ms,
            Arc::new(move |_| {
                let _ = signal.stop();
            }),
        )?;
        #[cfg(feature = "test-support")]
        let clock = crate::testing::CapturedClock::capture()
            .map(|clock| Box::new(move || clock.unix_millis()) as ReadClock);
        #[cfg(not(feature = "test-support"))]
        let clock = None;
        Ok(Self(Arc::new(State {
            wake,
            expiry: Some(Expiry {
                unix_ms: expiry_unix_ms,
                clock,
                _alarm: Alarm::Wall {
                    _registration: registration,
                },
            }),
        })))
    }

    /// Uses the authority owner's clock and its expiry event. The event must
    /// resolve on expiry or producer loss. Callback checks retain the same
    /// clock when capture moves to a native thread.
    ///
    /// # Errors
    /// Returns an error if the process wake or its event task cannot be opened.
    pub fn open_with_clock(
        expiry_unix_ms: u64,
        clock: impl Fn() -> u64 + Send + Sync + 'static,
        expiry: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> io::Result<Self> {
        let wake = ProcessWake::new()?;
        let stop = StopOnDrop(wake.clone());
        let task = crate::try_spawn(async move {
            let _stop = stop;
            expiry.await;
        })
        .map_err(|_| io::Error::other("no executor for native command expiry"))?;
        Ok(Self(Arc::new(State {
            wake,
            expiry: Some(Expiry {
                unix_ms: expiry_unix_ms,
                clock: Some(Box::new(clock)),
                _alarm: Alarm::Bound {
                    task: task.abort_handle(),
                },
            }),
        })))
    }

    /// A command controlled by owner loss and its native relative deadline,
    /// with no additional authenticated absolute expiry to register.
    pub fn unbounded() -> io::Result<Self> {
        Ok(Self(Arc::new(State {
            wake: ProcessWake::new()?,
            expiry: None,
        })))
    }

    /// Retains the original signal in the process pump's kernel wait.
    #[must_use]
    pub fn wake(&self) -> ProcessWake {
        self.0.wake.clone()
    }

    /// Irreversibly stops this command, including a dropped invoking task.
    pub fn stop(&self) {
        let _ = self.0.wake.stop();
    }

    /// Refuses stopped or expired authority before any further process IO.
    pub fn check(&self) -> io::Result<()> {
        if self.0.wake.is_stopped() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "native command was stopped",
            ));
        }
        let Some(expiry) = &self.0.expiry else {
            return Ok(());
        };
        // The alarm and this synchronous check share the clock that admitted
        // the command, including when the pump runs on a different thread.
        let now = expiry
            .clock
            .as_deref()
            .map_or_else(crate::wall_alarm::now_unix_ms, |clock| clock());
        if now >= expiry.unix_ms {
            self.stop();
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "native command scope expired",
            ));
        }
        Ok(())
    }
}

/// A capture retaining its original stop source and callback custody together.
pub struct ControlledCapture<H> {
    inner: H,
    control: ProcessControl,
}

impl<H> ControlledCapture<H> {
    /// Returns the original callbacks after the capture's cleanup join.
    pub fn into_inner(self) -> H {
        self.inner
    }
}

impl<H: CaptureHandler> CaptureHandler for ControlledCapture<H> {
    fn take_wakes(&mut self) -> io::Result<CaptureWakes> {
        let control = self.control.wake();
        Ok(match self.inner.take_wakes()? {
            CaptureWakes::One(None) => Some(control).into(),
            CaptureWakes::One(Some(owner)) => vec![control, owner].into(),
            CaptureWakes::Many(mut owners) => {
                owners.push(control);
                owners.into()
            }
        })
    }
    fn check(&mut self) -> io::Result<()> {
        self.control.check()?;
        self.inner.check()
    }
    fn observe(&mut self, output: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        self.inner.observe(output)
    }
    fn input(&mut self) -> io::Result<ProcessInput> {
        self.inner.input()
    }
    fn progress(&mut self, progress: CaptureProgress) {
        self.inner.progress(progress);
    }
    fn retired(&mut self, cleanup_completed: bool) {
        self.inner.retired(cleanup_completed);
    }
    fn observation(&mut self) -> Option<&mut crate::obs::CaptureObservation> {
        self.inner.observation()
    }
}

#[cfg(test)]
mod tests {
    use super::super::events;
    use super::*;

    #[test]
    fn expiry_is_an_event_and_elapsed_authority_refuses_dispatch() -> io::Result<()> {
        let now = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_millis(),
        )
        .map_err(io::Error::other)?;
        let control = ProcessControl::open(now + 50)?;
        let wake = control.wake();
        // Only the actual expiry producer can complete this wait.
        events::arm(&[wake.source()], None)?.block().into_result()?;
        assert_eq!(
            control.check().unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        let elapsed = ProcessControl::open(0)?;
        assert_eq!(
            elapsed.check().unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        // Omitting the original control wake would leave a quiet command alive.
        assert!(control.wake().is_stopped());
        assert!(!ProcessWake::new()?.is_stopped());
        Ok(())
    }
}
