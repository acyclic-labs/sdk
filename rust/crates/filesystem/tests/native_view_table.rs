//! Native shared view ownership and serial contracts.

#![cfg(all(not(loom), any(target_os = "linux", target_os = "macos", windows)))]

#[path = "native_view_table/native_mount.rs"]
mod native_mount;

#[path = "../src/native_sync.rs"]
mod native_sync;
