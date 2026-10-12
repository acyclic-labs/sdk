//! Capability-rooted native filesystem access.
#![allow(unsafe_code, reason = "isolates native filesystem calls")]
use cap_std::fs::{Dir, OpenOptions};
use std::{ffi::{OsStr, OsString}, fs::File, io, path::Path};
#[cfg(unix)]
use std::os::fd::AsRawFd as _;
use crate::demand::{DemandError, DemandWork};
use crate::performance::{WorkBudget, WorkCounters};
use crate::NativeRootIdentity;
#[cfg(all(windows, any(feature = "native-mount", test)))]
use windows::Win32::Storage::FileSystem::FILE_BASIC_INFO;
mod record;
pub(crate) use record::*;
mod work;
pub(crate) use work::*;
mod cwd;
pub(crate) use cwd::*;
mod unix_path;
pub(crate) use unix_path::*;
mod open;
pub(crate) use open::*;
mod mutation;
pub(crate) use mutation::*;
mod metadata_sync;
pub(crate) use metadata_sync::*;
mod metadata;
pub(crate) use metadata::*;
mod macos_metadata;
pub(crate) use macos_metadata::*;
mod stat;
pub(crate) use stat::*;
mod reader;
pub(crate) use reader::*;
mod windows_path;
pub(crate) use windows_path::*;
mod root;
pub(crate) use root::*;
mod linux_metadata;
pub(crate) use linux_metadata::*;
mod links;
pub(crate) use links::*;
mod unix_stat;
pub(crate) use unix_stat::*;
mod root_path;
pub(crate) use root_path::*;
mod identity;
pub(crate) use identity::*;
mod windows_metadata;
pub(crate) use windows_metadata::*;
mod windows_file;
pub(crate) use windows_file::*;
mod copy;
pub(crate) use copy::*;
mod tree;
pub(crate) use tree::*;
mod sparse;
pub(crate) use sparse::*;
mod yielding;
pub(crate) use yielding::*;
mod ranges;
pub(crate) use ranges::*;
#[cfg(all(any(target_os = "linux", target_os = "macos", windows), any(feature = "native-mount", test)))]
pub use metadata::NativeMetadataError;
pub use root::HostRoot;
pub use stat::{HostDataRange, HostFileType, HostListedEntry, HostStat};
pub use reader::HostStatReader;
pub use yielding::{Yielded, YieldingFile};
#[cfg(unix)]
pub use sparse::punch_hole;
#[cfg(all(test, target_os = "macos"))]
#[path = "tests_macos_metadata.rs"]
mod macos_metadata_tests;
#[cfg(all(test, windows))]
#[path = "tests_yielding.rs"]
mod yielding_tests;
#[cfg(all(test, unix))]
#[path = "tests_unix.rs"]
mod tests;
#[cfg(all(test, windows))]
#[path = "tests_windows_clone.rs"]
mod windows_clone_tests;
#[cfg(all(test, any(windows, target_os = "linux", target_os = "macos")))]
#[path = "tests_bounded_source.rs"]
mod bounded_source_tests;
#[cfg(all(test, any(windows, target_os = "linux", target_os = "macos")))]
#[path = "tests_bounded_record.rs"]
mod bounded_record_tests;
