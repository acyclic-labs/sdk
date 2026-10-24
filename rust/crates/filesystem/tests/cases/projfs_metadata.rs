//! ProjFS metadata capture regressions exercised through the mounted filesystem.

#![cfg(windows)]

use acyclic_fs::MountFilesystem;
use bytes::Bytes;

#[path = "../projfs_metadata_support.rs"]
mod projfs_metadata_support;
use projfs_metadata_support::*;
#[path = "../../src/native_mount/projfs/tests/metadata.rs"]
mod metadata;
