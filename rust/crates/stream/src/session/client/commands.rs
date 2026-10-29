//! One ordered command queue. Opens wait for dispatch credit; synchronous
//! credential updates always enqueue, including revocations.
use super::{Bytes, Command, StreamError};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

struct Queued {
    command: Command,
    _credit: Option<OwnedSemaphorePermit>,
}
#[derive(Clone)]
pub(super) struct Sender {
    sender: mpsc::UnboundedSender<Queued>,
    credit: Arc<Semaphore>,
}
pub(super) struct Receiver {
    receiver: mpsc::UnboundedReceiver<Queued>,
    credit: Arc<Semaphore>,
}

pub(super) fn channel(credit: usize) -> (Sender, Receiver) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let credit = Arc::new(Semaphore::new(credit));
    (
        Sender {
            sender,
            credit: credit.clone(),
        },
        Receiver { receiver, credit },
    )
}
impl Sender {
    pub(super) fn credential(&self, slot: u32, credential: Bytes) -> Result<(), StreamError> {
        self.sender
            .send(Queued {
                command: Command::Credential { slot, credential },
                _credit: None,
            })
            .map_err(|_| StreamError::Unavailable)
    }
    pub(super) async fn send(&self, command: Command) -> Result<(), StreamError> {
        let credit = self
            .credit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StreamError::Unavailable)?;
        self.sender
            .send(Queued {
                command,
                _credit: Some(credit),
            })
            .map_err(|_| StreamError::Unavailable)
    }
}
impl Receiver {
    pub(super) async fn recv(&mut self) -> Option<Command> {
        self.receiver.recv().await.map(|queued| queued.command)
    }
    #[cfg(test)]
    pub(super) fn try_recv(&mut self) -> Result<Command, mpsc::error::TryRecvError> {
        self.receiver.try_recv().map(|queued| queued.command)
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        // Wake open admissions even if dispatch ends while the queue is full.
        self.credit.close();
    }
}
