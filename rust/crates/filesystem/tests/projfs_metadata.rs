//! ProjFS metadata capture regressions exercised through the mounted filesystem.

#![cfg(windows)]

use acyclic_fs::MountFilesystem;
use acyclic_fs::kernel;
use bytes::Bytes;

mod projfs_metadata_support;
use projfs_metadata_support::*;
#[path = "../src/native_mount/projfs/tests/metadata.rs"]
mod metadata;
