#[cfg(loom)]
pub(crate) use loom::sync::*;
#[cfg(not(loom))]
pub(crate) use std::sync::*;

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

#[cfg(not(loom))]
#[test]
fn ordinary_build_keeps_standard_synchronization() {
    assert!(
        !cfg!(loom),
        "ordinary builds must not enable the dedicated model cfg"
    );
}
