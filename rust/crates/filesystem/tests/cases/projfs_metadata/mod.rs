//! ProjFS metadata capture regressions exercised through the mounted filesystem.

#![cfg(windows)]

use acyclic_fs::MountFilesystem;
use bytes::Bytes;

mod support;
use support::*;
mod metadata;
