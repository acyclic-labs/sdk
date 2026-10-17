#[cfg(acyclic_fs_loom)]
pub(crate) use loom::sync::*;
#[cfg(not(acyclic_fs_loom))]
pub(crate) use std::sync::*;

#[cfg(all(feature = "native-mount", acyclic_fs_loom))]
pub(crate) use loom::thread;
#[cfg(all(feature = "native-mount", not(acyclic_fs_loom)))]
pub(crate) use std::thread;

#[cfg(all(feature = "native-mount", acyclic_fs_loom))]
pub(crate) type RetirementSender = loom::sync::mpsc::Sender<()>;
#[cfg(all(feature = "native-mount", not(acyclic_fs_loom)))]
pub(crate) type RetirementSender = std::sync::mpsc::SyncSender<()>;

// The owner sends once on last drop. Constructor rejection waits for retirement
// before last drop, so a second notification can only see a closed receiver.
#[cfg(feature = "native-mount")]
pub(crate) fn retirement_channel() -> (RetirementSender, mpsc::Receiver<()>) {
    #[cfg(acyclic_fs_loom)]
    return loom::sync::mpsc::channel();
    #[cfg(not(acyclic_fs_loom))]
    std::sync::mpsc::sync_channel(1)
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
