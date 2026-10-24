//! Query-based terminal capabilities. Keep one detection result for the session.

mod capabilities;
#[cfg(any(windows, acyclic_harness_tui_loom))]
mod console_owner;
#[cfg(all(any(unix, windows), not(acyclic_harness_tui_loom)))]
mod counters;
mod decoder;
mod frame;
mod region;
#[cfg(any(unix, acyclic_harness_tui_loom))]
mod lifecycle;
mod probe;
mod restore;
#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
mod session;
#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
mod signals;
#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
mod tty;

pub use capabilities::*;
#[cfg(all(any(unix, windows), not(acyclic_harness_tui_loom)))]
pub use counters::{DeviceIo, device_io};
pub use decoder::{Decoder, encode_key};
pub use frame::{FrameEffects, FrameTerminal, Present};
pub use region::RegionScroll;
pub use probe::{PROBE_LIMIT, PROBE_TIMEOUT, Probe, ProbeIo, ProbeReplies, detect};
pub use restore::{RestoreGuard, ScreenMode};
#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
pub use session::{Output, Tty};
#[cfg(all(windows, not(acyclic_harness_tui_loom)))]
mod windows;
#[cfg(all(windows, not(acyclic_harness_tui_loom)))]
pub use windows::{Output, Tty};
