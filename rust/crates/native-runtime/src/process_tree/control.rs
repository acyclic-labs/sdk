//! Shared command cancellation and authenticated wall-clock expiry.
use super::{CaptureHandler, CaptureWakes, ProcessInput, ProcessOutput, ProcessWake, io};
use crate::WallAlarm;
use std::sync::Arc;

struct State {
    wake: ProcessWake,
    expiry_unix_ms: Option<u64>,
    _alarm: Option<WallAlarm>,
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
        Self::new(Some(expiry_unix_ms))
    }

    /// A command controlled by owner loss and its native relative deadline,
    /// with no additional authenticated absolute expiry to register.
    pub fn unbounded() -> io::Result<Self> {
        Self::new(None)
    }

    fn new(expiry_unix_ms: Option<u64>) -> io::Result<Self> {
        let wake = ProcessWake::new()?;
        let alarm = expiry_unix_ms
            .map(|expiry| {
                WallAlarm::register(expiry, {
                    let wake = wake.clone();
                    Arc::new(move || {
                        let _ = wake.stop();
                    })
                })
            })
            .transpose()?;
        Ok(Self(Arc::new(State {
            wake,
            expiry_unix_ms,
            _alarm: alarm,
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
        let Some(expiry) = self.0.expiry_unix_ms else {
            return Ok(());
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        if now >= expiry {
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
    fn retired(&mut self, cleanup_completed: bool) {
        self.inner.retired(cleanup_completed);
    }
}

#[cfg(test)]
mod tests {
    use super::super::events::{self, Source};
    use super::*;

    #[test]
    fn expiry_is_an_event_and_elapsed_authority_refuses_dispatch() -> io::Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_millis() as u64;
        let control = ProcessControl::open(now + 50)?;
        let wake = control.wake();
        #[cfg(unix)]
        let source = {
            use std::os::fd::AsFd;
            Source::Readable(wake.as_fd())
        };
        #[cfg(windows)]
        // SAFETY: wake retains this event until the registered wait completes.
        let source =
            Source::Exit(unsafe { std::os::windows::io::BorrowedHandle::borrow_raw(wake.raw()) });
        // Only the actual expiry producer can complete this wait.
        events::arm(&[source], None)?.block();
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
