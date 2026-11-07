//! An owned future over the original process-wide absolute alarm.
use super::{WallAlarm, io, retained_error};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
};
use tokio::sync::oneshot;

/// One absolute alarm, cancelled when its original waiter is dropped.
/// No executor task or runtime timer driver owns this registration.
pub struct AlarmWait {
    // Cancel the receiver before the registration releases its callback.
    // Callback destruction can therefore never wake this cancelled waiter
    // while the native alarm index is locked.
    receiver: oneshot::Receiver<Result<(), Arc<io::Error>>>,
    _registration: WallAlarm,
}

impl WallAlarm {
    /// Register one absolute deadline on the shared native wall-clock driver.
    /// The returned future retains native failure, including producer loss.
    /// Dropping it unregisters only this deadline, even before its first poll.
    pub fn wait(deadline_unix_ms: u64) -> io::Result<AlarmWait> {
        let (sender, receiver) = oneshot::channel();
        let sender = Mutex::new(Some(sender));
        let registration = Self::register(
            deadline_unix_ms,
            Arc::new(move |outcome| {
                let sender = sender.lock().unwrap_or_else(PoisonError::into_inner).take();
                if let Some(sender) = sender {
                    let _cancelled = sender.send(outcome);
                }
            }),
        )?;
        Ok(AlarmWait {
            receiver,
            _registration: registration,
        })
    }
}

impl Future for AlarmWait {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver).poll(context).map(|outcome| {
            outcome
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "step=wall-alarm-wait; recovery=restart the lost alarm owner",
                    )
                })?
                .map_err(|error| retained_error(&error))
        })
    }
}
