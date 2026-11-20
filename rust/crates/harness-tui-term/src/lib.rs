//! The terminal layer: one session over the OS terminal, its frames and its restoration.

#[cfg(any(windows, loom))]
mod console_owner;
#[cfg(all(any(unix, windows), not(loom)))]
mod counters;
#[cfg(all(any(unix, windows), not(loom)))]
mod events;
mod frame;
#[cfg(all(any(unix, windows), not(loom)))]
pub use events::input_events;
mod hyperlink;
#[cfg(any(unix, loom))]
mod lifecycle;
mod region;
mod restore;
#[cfg(any(all(unix, not(loom)), all(windows, not(loom))))]
/// Probe once and retain the same whole-stream decoder for subsequent input.
macro_rules! negotiate {
    () => {
        /// Probe once and retain the same whole-stream decoder for subsequent input.
        pub fn negotiate(&mut self, env: &Environment) -> io::Result<&Capabilities> {
            if self.capabilities.is_none() {
                let (caps, decoder) = detect(self, env)?;
                self.capabilities = Some(caps);
                self.decoder = decoder;
            }
            self.capabilities
                .as_ref()
                .ok_or(io::ErrorKind::NotConnected.into())
        }
    };
}

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
pub use hyperlink::{Hyperlinks, RowLink};
pub use region::RegionScroll;
pub use restore::{RestoreGuard, ScreenMode};
#[cfg(all(unix, not(loom)))]
pub use session::{Output, Tty};
#[cfg(all(windows, not(loom)))]
mod windows;
#[cfg(all(windows, not(loom)))]
pub use windows::{Output, Tty};
