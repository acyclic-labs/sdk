//! Capability-rooted native filesystem access.
#![allow(unsafe_code, reason = "isolates native filesystem calls")]
use crate::NativeRootIdentity;
#[cfg(feature = "local")]
use crate::NativeWorkReservation;
use crate::demand::{DemandError, DemandWork, SourceDemandReadWork};
use crate::materializer::{
    NativeAllocation, NativeCleanupWrite, NativeMaterializationWork, NativeTreeMaterializationError,
};
#[cfg(any(test, feature = "local", feature = "native-watch"))]
use crate::performance::WorkBudget;
use crate::performance::WorkCounters;
use cap_std::fs::Dir;
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
#[cfg(windows)]
use windows::{
    Wdk::Storage::FileSystem as nt,
    Win32::{Foundation as win, Storage::FileSystem as fs},
};
#[cfg(windows)]
mod windows_journal;
#[cfg(windows)]
pub(crate) use windows_journal::*;
#[cfg(unix)]
pub(crate) type NativeUnit = u8;
#[cfg(windows)]
pub(crate) type NativeUnit = u16;
#[cfg(unix)]
type NativePath = Path;
#[cfg(windows)]
type NativePath = [u16];
#[cfg(unix)]
type NativeLeaf = std::ffi::CStr;
#[cfg(windows)]
type NativeLeaf = [u16];
mod record;
#[cfg(feature = "local")]
pub(crate) use record::PreparedMaterializationRecord;
#[cfg(unix)]
use record::rename_unix_names_once;
use record::sync_held_descriptor_once;
#[cfg(windows)]
use record::windows_name_packet;
mod work;
pub(crate) use work::*;
mod cwd;
#[cfg(windows)]
pub(crate) use cwd::canonical_directory_wide;
#[cfg(unix)]
use cwd::cwd_metadata_flags;
#[cfg(all(unix, any(test, feature = "native-watch")))]
pub(crate) use cwd::{canonical_directory_path_held, normalize_root_path};
#[cfg(unix)]
mod unix_path;
#[cfg(unix)]
pub(crate) use unix_path::*;
mod open;
pub use open::file_transfer;
pub(crate) use open::*;
mod mutation;
pub(crate) use mutation::*;
mod leaf;
pub(crate) use leaf::require_leaf;
#[cfg(all(target_os = "macos", any(feature = "native-mount", test)))]
mod macos_metadata;
mod metadata;
#[cfg(all(target_os = "macos", any(feature = "native-mount", test)))]
pub(crate) use macos_metadata::*;
mod stat;
pub(crate) use stat::*;
#[cfg(test)]
#[cfg(windows)]
pub(crate) mod stat_probe;
#[cfg(test)]
#[cfg(target_os = "linux")]
pub(crate) use unix_stat::stat_probe;
mod reader;
pub(crate) use reader::*;
mod root;
pub(crate) use root::*;
mod held_files;
#[cfg(all(test, target_os = "linux", acyclic_fs_native_loom))]
pub(crate) use held_files::models as held_record_models;
#[cfg(all(test, target_os = "linux"))]
pub(crate) use held_files::publish_probe;
#[cfg(all(unix, any(test, feature = "native-mount")))]
mod unix_metadata;
#[cfg(all(unix, any(test, feature = "native-mount")))]
pub(crate) use unix_metadata::*;
mod links;
mod unix_stat;
#[cfg(any(test, feature = "native-watch"))]
pub(crate) use unix_stat::root_path_identity;
use unix_stat::*;
#[cfg(windows)]
mod root_path;
#[cfg(windows)]
use root_path::*;
mod identity;
pub(crate) use identity::*;
#[cfg(all(windows, any(feature = "native-mount", test)))]
mod windows_metadata;
#[cfg(all(windows, any(feature = "native-mount", test)))]
pub(crate) use windows_metadata::*;
#[cfg(any(
    windows,
    target_os = "macos",
    test,
    feature = "local",
    feature = "native-watch"
))]
mod sparse;
mod tree;
#[cfg(any(
    windows,
    target_os = "macos",
    test,
    feature = "local",
    feature = "native-watch"
))]
pub(crate) use sparse::*;
#[cfg(any(windows, test, feature = "native-watch"))]
mod ranges;
#[cfg(windows)]
mod yielding;
#[cfg(all(
    any(target_os = "linux", target_os = "macos", windows),
    any(feature = "native-mount", test)
))]
pub use metadata::NativeMetadataError;
#[cfg(any(windows, test, feature = "native-watch"))]
pub(crate) use ranges::*;
pub use reader::HostStatReader;
pub use root::HostRoot;
#[cfg(target_os = "macos")]
pub use sparse::punch_hole;
pub use stat::{HostDataRange, HostListedEntry, HostStat};
#[cfg(windows)]
pub use yielding::{Yielded, YieldingFile};
#[cfg(test)]
#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
#[path = "tests_bounded_record.rs"]
mod bounded_record_tests;
#[cfg(test)]
#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
#[path = "tests_bounded_source.rs"]
mod bounded_source_tests;
#[cfg(test)]
#[cfg(target_os = "macos")]
#[path = "tests_macos_metadata.rs"]
mod macos_metadata_tests;
#[cfg(test)]
#[cfg(unix)]
#[path = "tests_unix.rs"]
mod tests;
#[cfg(test)]
#[cfg(windows)]
#[path = "tests_windows_clone.rs"]
mod windows_clone_tests;
#[cfg(all(test, windows))]
#[path = "tests_windows_directory.rs"]
mod windows_directory_tests;
#[cfg(all(test, windows))]
#[path = "tests_windows_file_opens.rs"]
mod windows_file_open_tests;
#[cfg(test)]
#[cfg(windows)]
#[path = "tests_yielding.rs"]
mod yielding_tests;

#[cfg(test)]
mod test_support;
#[cfg(test)]
use test_support::{test_path, test_root};

#[cfg(windows)]
pub(crate) mod private_file;

#[cfg(test)]
#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
#[path = "tests_flat_parent.rs"]
mod flat_parent_tests;
