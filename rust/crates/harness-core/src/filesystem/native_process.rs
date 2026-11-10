//! Native process capture for the live `shell` tool: owned invocation input,
//! the capture engine, and the control that stops a command when its signed
//! scope expires or its turn is cancelled.

use super::{NativeProcessResult, NativeProcessStop, NativeProcessStopKind};
use crate::{Error, Result};

#[doc(hidden)]
pub mod capture;
mod invocation;
#[cfg(any(feature = "filesystem-local", all(test, unix)))]
#[doc(hidden)]
pub mod shell;
pub use capture::NativeProcessObserver;
#[doc(hidden)]
pub use invocation::Invocation;
