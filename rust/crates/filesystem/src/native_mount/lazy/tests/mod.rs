use super::*;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(any(not(unit_test_split), unit_test_mount_lazy))]
#[derive(Default)]
struct Recorded(Mutex<Vec<ViewStamp>>);

pub(in crate::native_mount) mod fixtures;
#[cfg(unix)]
#[cfg(any(not(unit_test_split), unit_test_mount_lazy))]
mod hard_links;
#[cfg(any(not(unit_test_split), unit_test_mount_lazy))]
mod live_mount;
#[cfg(any(not(unit_test_split), unit_test_mount_lazy))]
mod publication;
mod source_changes;
use fixtures::*;

#[cfg(any(not(unit_test_split), unit_test_mount_lazy))]
mod access;
#[cfg(any(not(unit_test_split), unit_test_mount_lazy))]
mod demand_errors;

#[cfg(any(not(unit_test_split), unit_test_mount_lazy))]
mod handle_entry;
