//! Query-based terminal capabilities. Keep one detection result for the session.

mod capabilities;
mod decoder;
#[cfg(any(unix, acyclic_harness_tui_loom))]
mod lifecycle;
#[cfg(any(windows, acyclic_harness_tui_loom))]
mod console_owner;
mod probe;
mod restore;
#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
mod session;
#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
mod signals;
#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
mod tty;

pub use capabilities::*;
pub use decoder::{Decoder, encode_key};
pub use probe::{PROBE_LIMIT, PROBE_TIMEOUT, Probe, ProbeIo, ProbeReplies, detect};
pub use restore::{RestoreGuard, ScreenMode};
#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
pub use session::Tty;
#[cfg(all(windows, not(acyclic_harness_tui_loom)))]
mod windows;
#[cfg(all(windows, not(acyclic_harness_tui_loom)))]
pub use windows::Tty;
