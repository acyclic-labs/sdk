//! The one way this workspace's host code starts a process.
//!
//! On Unix this is `std::process::Command` itself: a child inherits only
//! descriptors without close-on-exec, which std sets on everything it opens.
//!
//! On Windows `std` starts every child with handle inheritance on and no
//! handle list, so each child inherits every inheritable handle in the
//! process: the host's own standard handles, and the pipes of any child
//! being started concurrently. A tool tree that outlives its turn then
//! holds the host's output open, and its reader never sees the end. The
//! Windows [`Command`] keeps std's builder, program resolution, argument
//! quoting and batch-file escaping, and starts the child with exactly its
//! own standard handles in `PROC_THREAD_ATTRIBUTE_HANDLE_LIST`, without
//! changing any handle of the host.
//!
//! A source scan (`sources_tests`) keeps every production spawn in crates
//! that link this one on this type.

#[cfg(unix)]
pub use std::process::{Child, Command, Stdio};

#[cfg(windows)]
pub(crate) use crate::windows::command::start;
#[cfg(windows)]
pub use crate::windows::command::{Child, Command, Stdio};

#[cfg(test)]
mod parity_tests;
#[cfg(test)]
mod sources_tests;
