//! Native namespace synchronization shared with the Loom model.

#[cfg(loom)]
pub(crate) use loom::{sync::*, thread};
#[cfg(not(loom))]
pub(crate) use std::{sync::*, thread};

#[cfg(loom)]
pub(crate) type RetirementSender = loom::sync::mpsc::Sender<()>;
#[cfg(not(loom))]
pub(crate) type RetirementSender = std::sync::mpsc::SyncSender<()>;

// Retirement consumes its sender, including constructor rejection. There
// is exactly one notification for the one worker receive.
pub(crate) fn retirement_channel() -> (RetirementSender, mpsc::Receiver<()>) {
    #[cfg(loom)]
    return loom::sync::mpsc::channel();
    #[cfg(not(loom))]
    std::sync::mpsc::sync_channel(1)
}
