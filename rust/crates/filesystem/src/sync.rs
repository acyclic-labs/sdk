#[cfg(loom)]
pub(crate) use loom::sync::*;
#[cfg(not(loom))]
pub(crate) use std::sync::*;

// The lock itself is inline: first use cannot attach an unaccounted allocation
// to an already funded owner. Use where callers recover after unwinding;
// poisoning does not protect an invariant in these protocols.
#[cfg(not(loom))]
pub(crate) use parking_lot::Mutex as InlineMutex;

#[cfg(loom)]
#[derive(Default)]
pub(crate) struct InlineMutex<T>(loom::sync::Mutex<T>);

#[cfg(loom)]
impl<T> InlineMutex<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(loom::sync::Mutex::new(value))
    }

    pub(crate) fn lock(&self) -> loom::sync::MutexGuard<'_, T> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

// Native namespace and helper coordination has a dedicated model domain.
// The unchanged ledger uses the separate filesystem accounting proofs.
#[cfg(feature = "native-mount")]
pub(crate) mod native {
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
}

// Type identity checks compile in ordinary builds, including --all-features.
#[cfg(not(loom))]
const _: fn(Mutex<()>) -> std::sync::Mutex<()> = |mutex| mutex;
#[cfg(loom)]
const _: fn(Mutex<()>) -> loom::sync::Mutex<()> = |mutex| mutex;
