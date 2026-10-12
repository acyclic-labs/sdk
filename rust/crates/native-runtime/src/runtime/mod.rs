//! Shared native IO execution.

#[cfg(target_vendor = "apple")]
use crate::apple;
#[cfg(target_os = "linux")]
use crate::linux;
#[cfg(windows)]
use crate::windows;
use crate::{ProcessInput, ProcessTree, obs};
#[cfg(all(unix, not(target_os = "linux")))]
use apple::{read_batch as read_batch_impl, write_all_batch_owned as write_all_batch_impl};
use bytes::Bytes;
#[cfg(target_os = "linux")]
use linux::{read_at as read_at_impl, write_all_at as write_all_at_impl};
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
use std::cell::Cell;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, OnceLock, Weak, mpsc};
use std::task::{Context, Poll, Waker};
#[cfg(windows)]
use windows::{read_at as read_at_impl, write_all_at as write_all_at_impl};

mod health;
pub(crate) use health::*;
mod file;
pub use file::*;
mod scope;
pub(crate) use scope::*;
mod durability;
pub use durability::*;
mod ownership;
pub use ownership::*;
mod namespace;
pub use namespace::*;
mod positional;
pub use positional::*;
mod bounded;
pub use bounded::*;
mod process;
pub use process::*;
mod completion;
pub use completion::*;
mod meter;
pub use meter::*;
mod allocation;
pub use allocation::*;
mod task;
pub use task::*;
mod fence;
pub(crate) use fence::*;
mod retirement;
pub(crate) use retirement::*;
mod job;
pub(crate) use job::*;
mod admission;
pub(crate) use admission::*;
mod wait;
pub(crate) use wait::*;
mod workers;
pub use workers::*;

#[cfg(test)]
pub(crate) mod tests;
