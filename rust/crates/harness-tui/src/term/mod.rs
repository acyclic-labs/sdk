//! Query-based terminal capabilities. Keep one detection result for the session.

#[cfg(any(windows, loom))]
mod console_owner;
#[cfg(all(any(unix, windows), not(loom)))]
mod counters;
#[cfg(all(any(unix, windows), not(loom)))]
mod events;
mod frame;
#[cfg(all(any(unix, windows), not(loom)))]
pub use events::input_events;
#[cfg(any(unix, loom))]
mod lifecycle;
mod region;
mod restore;
#[cfg(all(unix, not(loom)))]
mod session;
#[cfg(all(unix, not(loom)))]
mod signals;
#[cfg(all(unix, not(loom)))]
mod tty;

pub use acyclic_harness_tui_terminal::{
    Capabilities, ColourDepth, Decoder, Environment, ImageProtocol, KeyboardProtocol, ModeState,
    Multiplexer, NotificationProtocol, PROBE_LIMIT, PROBE_TIMEOUT, Probe, ProbeIo, ProbeReplies,
    detect, encode_key,
};
#[cfg(all(any(unix, windows), not(loom)))]
pub use counters::{DeviceIo, device_io};
pub use frame::{FrameEffects, FrameTerminal, Present};
pub use region::RegionScroll;
pub use restore::{RestoreGuard, ScreenMode};
#[cfg(all(unix, not(loom)))]
pub use session::{Output, Tty};
#[cfg(all(windows, not(loom)))]
mod windows;
#[cfg(all(windows, not(loom)))]
pub use windows::{Output, Tty};
