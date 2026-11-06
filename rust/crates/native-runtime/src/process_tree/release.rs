//! An admitted host action survives loss of its owner, using the original
//! static process runner rather than forking a multithreaded host.

use super::{ProcessCommand, command::Stdio, platform::Child, runner};
use crate::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
};
use std::fs::File;
use std::io::{self, Write as _};
use std::sync::Arc;

type Control = NativeOperationReservation<Arc<dyn NativeReadMeter>>;

/// A command admitted before an external effect, executed after this owner
/// releases it or exits. Its activity descriptors delay execution until every
/// admitted producer has closed its descriptor. All owner endpoints are
/// close-on-exec; only explicitly transferred activity endpoints may escape.
///
/// The command must terminate and must have explicit, non-piped standard
/// streams. An original namespace lock can be its standard input, keeping that
/// very lock through the command's physical exit. No volume device is needed.
pub struct ProcessExitAction {
    release: Option<File>,
    activity: Option<File>,
    child: Child,
    cancel: Option<Control>,
    cancel_byte: Option<NativeByteReservation<Arc<dyn NativeReadMeter>>>,
    wait: Option<Control>,
    terminal: Option<std::process::ExitStatus>,
    meter: Arc<dyn NativeReadMeter>,
}

impl ProcessExitAction {
    /// Admits `command` and waits for its original runner acknowledgement.
    /// Failure precedes the caller's external effect. No shell is involved.
    pub fn spawn(command: &ProcessCommand, meter: &Arc<dyn NativeReadMeter>) -> io::Result<Self> {
        if command.boundary.is_some()
            || [&command.stdin, &command.stdout, &command.stderr]
                .into_iter()
                .any(|stream| {
                    !matches!(
                        stream.as_ref().map(|s| &s.0),
                        Some(Stdio::Null | Stdio::Owned(_))
                    )
                })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owner-exit action requires explicit non-piped host streams",
            ));
        }
        let original = meter
            .allocation_owner()
            .unwrap_or_else(|| Arc::clone(meter));
        let cancel = Control::new(Arc::clone(&original), NativeOperationKind::Write, 1)?;
        let cancel_byte =
            NativeByteReservation::new(Arc::clone(&original), NativeOperationKind::Write, 1)?;
        let wait = Control::new(Arc::clone(&original), NativeOperationKind::Read, 1)?;
        let (released, release) = super::process_control_attempt(
            Some(meter.as_ref()),
            NativeOperationKind::Write,
            runner::pipe,
        )?;
        let (producers, activity) = super::process_control_attempt(
            Some(meter.as_ref()),
            NativeOperationKind::Write,
            runner::pipe,
        )?;
        let (started, recorded) = super::pipe_operation_attempt(
            Some(meter.as_ref()),
            NativeOperationKind::Write,
            || {
                runner::start_with_release(
                    command,
                    &File::from(released),
                    Some(&File::from(producers)),
                )
            },
        )?;
        let (child, admitted) = started?;
        let action = Self {
            release: Some(release.into()),
            activity: Some(activity.into()),
            child,
            cancel: Some(cancel),
            cancel_byte: Some(cancel_byte),
            wait: Some(wait),
            terminal: None,
            meter: original,
        };
        admitted?;
        recorded?;
        Ok(action)
    }

    /// Transfers one activity endpoint to an original producer. The action
    /// cannot run until this endpoint physically closes, even if the owner
    /// dies during that producer's external effect.
    pub fn activity(&self) -> io::Result<File> {
        super::process_control_attempt(Some(self.meter.as_ref()), NativeOperationKind::Read, || {
            self.activity
                .as_ref()
                .ok_or(io::ErrorKind::BrokenPipe)?
                .try_clone()
        })
    }

    /// The admitted action's PID, retained until this owner reaps it.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// Cancels an action whose external effect has already been retired,
    /// then waits for its original physical exit before ownership is released.
    pub fn cancel(&mut self) -> io::Result<()> {
        if let (Some(release), Some(mut credit)) = (self.release.as_mut(), self.cancel.take()) {
            let (written, recorded) = credit.attempt(|| release.write_all(&[0]));
            let bytes = self
                .cancel_byte
                .as_mut()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "owner-exit cancellation byte credit was lost",
                    )
                })?
                .record(usize::from(written.is_ok()));
            written?;
            recorded?;
            bytes?;
        }
        self.finish()
    }

    /// Releases the action, waits for accepted producers and then its actual
    /// exit. A nonzero command status is a named error, never accepted cleanup.
    pub fn finish(&mut self) -> io::Result<()> {
        drop(self.activity.take());
        drop(self.release.take());
        let status = match self.terminal {
            Some(status) => status,
            None => {
                let mut credit = self.wait.take().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "owner-exit physical completion was lost",
                    )
                })?;
                let (status, recorded) = credit.attempt(|| self.child.wait());
                let status = status?;
                self.terminal = Some(status);
                recorded?;
                status
            }
        };
        if status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "owner-exit action failed: {status}"
            )))
        }
    }
}

impl Drop for ProcessExitAction {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            tracing::error!(%error, "owner-exit action did not complete");
        }
    }
}
