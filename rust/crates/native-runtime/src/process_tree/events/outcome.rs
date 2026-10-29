use std::{io, sync::Arc};

/// The first terminal event of a native process readiness wait.
#[derive(Clone, Debug)]
#[must_use]
pub enum ProcessWaitOutcome {
    /// At least one registered source can make progress.
    Ready,
    /// The requested wake time elapsed; the owner must check its deadline.
    Deadline,
    /// The owner cancelled this wait.
    Cancelled,
    /// The shared readiness producer stopped. The original error is retained.
    ServiceFailed(Arc<io::Error>),
}

impl ProcessWaitOutcome {
    /// Continues on readiness or a scheduled deadline; propagates cancellation
    /// and producer failure without rearming a dead service.
    pub fn into_result(self) -> io::Result<()> {
        match self {
            Self::Ready | Self::Deadline => Ok(()),
            Self::Cancelled => Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "process wait cancelled",
            )),
            Self::ServiceFailed(error) => Err(io::Error::new(error.kind(), error)),
        }
    }
}
