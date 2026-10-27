//! Query-based terminal capabilities. Keep one detection result for the session.

mod capabilities;
#[cfg(any(windows, loom))]
mod console_owner;
#[cfg(all(any(unix, windows), not(loom)))]
mod counters;
mod decoder;
mod frame;
#[cfg(any(unix, loom))]
mod lifecycle;
mod probe;
mod region;
mod restore;
#[cfg(all(unix, not(loom)))]
mod session;
#[cfg(all(unix, not(loom)))]
mod signals;
#[cfg(all(unix, not(loom)))]
mod tty;

pub use capabilities::*;
#[cfg(all(any(unix, windows), not(loom)))]
pub use counters::{DeviceIo, device_io};
pub use decoder::{Decoder, encode_key};
pub use frame::{FrameEffects, FrameTerminal, Present};
pub use probe::{PROBE_LIMIT, PROBE_TIMEOUT, Probe, ProbeIo, ProbeReplies, detect};
pub use region::RegionScroll;
pub use restore::{RestoreGuard, ScreenMode};
#[cfg(all(unix, not(loom)))]
pub use session::{Output, Tty};
#[cfg(all(windows, not(loom)))]
mod windows;
#[cfg(all(windows, not(loom)))]
pub use windows::{Output, Tty};
