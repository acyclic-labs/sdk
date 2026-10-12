//! Native file and process primitives.

#[cfg(target_vendor = "apple")]
mod apple;
#[cfg(target_os = "linux")]
mod linux;
pub mod obs;
mod process_tree;
#[cfg(windows)]
mod windows;
pub use process_tree::{CaptureInput, ProcessCaptureFailure, ProcessInput, ProcessTree};
mod runtime;
pub use runtime::*;

#[cfg(unix)]
pub use process_tree::PreparedClosedProcess;
