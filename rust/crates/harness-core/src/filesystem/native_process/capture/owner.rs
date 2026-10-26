//! Stops a running command when its signed scope expires or its caller goes
//! away. Expiry is an absolute wall-clock alarm shared by every command, so a
//! quiet command needs no timer, poll or journal change of its own.
use super::*;
use acyclic_native_runtime::{ProcessWake, WallAlarm};
use std::sync::atomic::{AtomicBool, Ordering};

struct State {
    wake: ProcessWake,
    stopped: Arc<AtomicBool>,
    expiry_unix_ms: u64,
    _alarm: WallAlarm,
}

/// One running command's stop control. The native worker retains a clone
/// through reaping, even if its awaiting task exits.
#[doc(hidden)]
#[derive(Clone)]
pub struct OwnerControl(Arc<State>);

impl OwnerControl {
    /// Arms the stop for `expiry_unix_ms`, the command's signed-scope expiry.
    pub fn open(expiry_unix_ms: u64) -> Result<Self> {
        let wake = ProcessWake::new().map_err(storage)?;
        let stopped = Arc::new(AtomicBool::new(false));
        let alarm = WallAlarm::register(expiry_unix_ms, {
            let (wake, stopped) = (wake.clone(), Arc::clone(&stopped));
            Arc::new(move || stop(&wake, &stopped))
        })
        .map_err(storage)?;
        Ok(Self(Arc::new(State {
            wake,
            stopped,
            expiry_unix_ms,
            _alarm: alarm,
        })))
    }

    /// Signal passed to the kernel readiness wait owned by the native worker.
    #[must_use]
    pub fn wake(&self) -> ProcessWake {
        self.0.wake.clone()
    }

    /// Stops the command, as when the turn awaiting it is dropped.
    pub fn stop(&self) {
        stop(&self.0.wake, &self.0.stopped);
    }

    /// Fails once the command was stopped or its scope expired. The clock is
    /// read here too, so a dispatch after expiry cannot race the alarm.
    pub fn check(&self) -> std::io::Result<()> {
        if self.0.stopped.load(Ordering::Acquire) {
            return Err(control_error("native command was stopped"));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        if now >= self.0.expiry_unix_ms {
            self.stop();
            return Err(control_error("native command scope expired"));
        }
        Ok(())
    }
}

fn stop(wake: &ProcessWake, stopped: &AtomicBool) {
    stopped.store(true, Ordering::Release);
    // A wake that cannot signal leaves the flag, which the next check of the
    // worker observes.
    let _ = wake.stop();
}

fn storage(error: std::io::Error) -> Error {
    Error::Storage(format!("native command control: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_unix_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            })
    }

    #[test]
    fn signed_scope_expiry_wakes_a_quiet_owner_without_a_task_deadline() -> Result<()> {
        let control = OwnerControl::open(now_unix_ms() + 50)?;
        control.check().map_err(storage)?;
        // Nothing checks or publishes: only the shared alarm can stop it.
        let started = std::time::Instant::now();
        while !control.0.stopped.load(Ordering::Acquire) {
            assert!(
                started.elapsed().as_secs() < 30,
                "expiry never stopped the command"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(control.check().is_err());
        Ok(())
    }

    #[test]
    fn an_expired_scope_refuses_dispatch_before_its_alarm_runs() -> Result<()> {
        let control = OwnerControl::open(0)?;
        let failure = control.check().err();
        assert_eq!(
            failure.map(|failure| failure.kind()),
            Some(std::io::ErrorKind::Interrupted)
        );
        Ok(())
    }
}
