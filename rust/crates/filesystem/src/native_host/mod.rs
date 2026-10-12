//! Capability-rooted native filesystem access.
#![allow(unsafe_code, reason = "isolates native filesystem calls")]
use crate::NativeRootIdentity;
use crate::demand::{DemandError, DemandWork, SourceDemandReadWork};
use crate::materializer::{
    NativeAllocation, NativeCleanupWrite, NativeMaterializationWork,
    NativeTreeMaterializationError, NativeWorkReservation,
};
use crate::performance::{WorkBudget, WorkCounters};
use cap_std::fs::{Dir, OpenOptions};
#[cfg(unix)]
use std::os::fd::AsRawFd as _;
use std::{
    ffi::{OsStr, OsString},
    fs::File,
    io,
    path::Path,
};
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
#[cfg(windows)]
mod metadata_sync;
#[cfg(windows)]
pub(crate) use metadata_sync::*;
mod metadata;
use metadata::*;
#[cfg(all(target_os = "macos", any(feature = "native-mount", test)))]
mod macos_metadata;
#[cfg(all(target_os = "macos", any(feature = "native-mount", test)))]
pub(crate) use macos_metadata::*;
mod stat;
pub(crate) use stat::*;
mod reader;
pub(crate) use reader::*;
#[cfg(windows)]
mod windows_path;
#[cfg(windows)]
pub(crate) use windows_path::*;
mod root;
pub(crate) use root::*;
#[cfg(unix)]
mod linux_metadata;
#[cfg(unix)]
pub(crate) use linux_metadata::*;
mod links;
pub(crate) use links::*;
mod unix_stat;
pub(crate) use unix_stat::*;
mod root_path;
pub(crate) use root_path::*;
mod identity;
pub(crate) use identity::*;
#[cfg(all(windows, any(feature = "native-mount", test)))]
mod windows_metadata;
#[cfg(all(windows, any(feature = "native-mount", test)))]
pub(crate) use windows_metadata::*;
#[cfg(windows)]
mod windows_file;
#[cfg(windows)]
pub(crate) use windows_file::*;
#[cfg(windows)]
mod copy;
#[cfg(windows)]
use copy::*;
mod sparse;
mod tree;
pub(crate) use sparse::*;
mod ranges;
#[cfg(windows)]
mod yielding;
#[cfg(all(
    any(target_os = "linux", target_os = "macos", windows),
    any(feature = "native-mount", test)
))]
pub use metadata::NativeMetadataError;
pub(crate) use ranges::*;
pub use reader::HostStatReader;
pub use root::HostRoot;
#[cfg(unix)]
pub use sparse::punch_hole;
pub use stat::{HostDataRange, HostFileType, HostListedEntry, HostStat};
#[cfg(windows)]
pub use yielding::{Yielded, YieldingFile};
#[cfg(all(test, any(windows, target_os = "linux", target_os = "macos")))]
#[path = "tests_bounded_record.rs"]
mod bounded_record_tests;
#[cfg(all(test, any(windows, target_os = "linux", target_os = "macos")))]
#[path = "tests_bounded_source.rs"]
mod bounded_source_tests;
#[cfg(all(test, target_os = "macos"))]
#[path = "tests_macos_metadata.rs"]
mod macos_metadata_tests;
#[cfg(all(test, unix))]
#[path = "tests_unix.rs"]
mod tests;
#[cfg(all(test, windows))]
#[path = "tests_windows_clone.rs"]
mod windows_clone_tests;
#[cfg(all(test, windows))]
#[path = "tests_yielding.rs"]
mod yielding_tests;
