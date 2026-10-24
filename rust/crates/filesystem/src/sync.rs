#[cfg(acyclic_fs_loom)]
pub(crate) use loom::sync::*;
#[cfg(not(acyclic_fs_loom))]
pub(crate) use std::sync::*;

// Native namespace and helper coordination has a dedicated model domain.
// The unchanged ledger uses the separate filesystem accounting proofs.
#[cfg(feature = "native-mount")]
pub(crate) mod native {
    #[cfg(any(acyclic_fs_loom, acyclic_fs_native_loom))]
    pub(crate) use loom::{sync::*, thread};
    #[cfg(not(any(acyclic_fs_loom, acyclic_fs_native_loom)))]
    pub(crate) use std::{sync::*, thread};

    #[cfg(any(acyclic_fs_loom, acyclic_fs_native_loom))]
    pub(crate) type RetirementSender = loom::sync::mpsc::Sender<()>;
    #[cfg(not(any(acyclic_fs_loom, acyclic_fs_native_loom)))]
    pub(crate) type RetirementSender = std::sync::mpsc::SyncSender<()>;

    // Retirement consumes its sender, including constructor rejection. There
    // is exactly one notification for the one worker receive.
    pub(crate) fn retirement_channel() -> (RetirementSender, mpsc::Receiver<()>) {
        #[cfg(any(acyclic_fs_loom, acyclic_fs_native_loom))]
        return loom::sync::mpsc::channel();
        #[cfg(not(any(acyclic_fs_loom, acyclic_fs_native_loom)))]
        std::sync::mpsc::sync_channel(1)
    }
}

// Type identity checks compile in ordinary builds, including --all-features.
#[cfg(not(acyclic_fs_loom))]
const _: fn(Mutex<()>) -> std::sync::Mutex<()> = |mutex| mutex;
#[cfg(acyclic_fs_loom)]
const _: fn(Mutex<()>) -> loom::sync::Mutex<()> = |mutex| mutex;

#[cfg(not(acyclic_fs_loom))]
#[test]
fn ordinary_build_keeps_standard_synchronization() {
    assert!(
        !cfg!(acyclic_fs_loom),
        "ordinary builds must not enable the dedicated model cfg"
    );
}
