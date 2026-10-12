use super::*;
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::native_host::{HostListedEntry, HostRoot, HostStat, HostStatReader};
#[cfg(unix)]
use cap_std::fs::FileTypeExt as _;
#[cfg(unix)]
use cap_std::fs::MetadataExt;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

const MAXIMUM_NATIVE_REQUESTS: usize = 64;

include!("root.rs");
include!("file.rs");
include!("provider.rs");
include!("watch.rs");
include!("metadata.rs");
