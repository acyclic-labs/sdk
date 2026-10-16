#[cfg(acyclic_fs_loom)]
pub(crate) use loom::sync::*;
#[cfg(not(acyclic_fs_loom))]
pub(crate) use std::sync::*;

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
