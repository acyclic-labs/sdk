//! Native shared view ownership and serial contracts.

#![cfg(all(not(loom), any(target_os = "linux", target_os = "macos", windows)))]

#[path = "native_view_table/native_mount.rs"]
mod native_mount;

#[path = "../src/sync.rs"]
mod sync;
#[path = "../src/native_mount/shared/view_table.rs"]
mod table;
