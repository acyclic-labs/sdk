//! Terminal protocol state over a caller-owned transport.
#![cfg(not(target_arch = "wasm32"))]

mod capabilities;
mod decoder;
mod probe;

pub use capabilities::{
    Capabilities, ColourDepth, Environment, ImageProtocol, KeyboardProtocol, ModeState,
    Multiplexer, NotificationProtocol,
};
pub use decoder::{Decoder, encode_key};
pub use probe::{PROBE_LIMIT, PROBE_TIMEOUT, Probe, ProbeIo, ProbeReplies, detect};

/// The two independent terminal mode groups for mouse reporting and encoding.
pub const MOUSE_SELECTIONS: [&[u16]; 2] = [&[9, 1000, 1001, 1002, 1003], &[1005, 1006, 1015, 1016]];
