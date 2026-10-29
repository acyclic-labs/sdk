//! Shared command cancellation and authenticated wall-clock expiry.
use super::{ProcessWake, io};
use crate::WallAlarm;
use std::sync::Arc;

struct State {
    wake: ProcessWake,
    expiry_unix_ms: u64,
    _alarm: WallAlarm,
}

/// One command's irreversible stop signal and original authority expiry.
/// The physical owner retains a clone through reaping. All commands use the
/// same wall alarm and kernel readiness services, including quiet commands.
#[derive(Clone)]
pub struct ProcessControl(Arc<State>);

impl ProcessControl {
    /// Registers the original absolute authority expiry before dispatch.
    pub fn open(expiry_unix_ms: u64) -> io::Result<Self> {
        let wake = ProcessWake::new()?;
        let alarm = WallAlarm::register(expiry_unix_ms, {
            let wake = wake.clone();
            Arc::new(move || {
                let _ = wake.stop();
            })
        })?;
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        if now >= self.0.expiry_unix_ms {
            self.stop();
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "native command scope expired",
            ));
        }
        Ok(())
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
