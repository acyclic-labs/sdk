//! Shared Model Context Protocol framing and native tool serving.

/// Initialized-session protocol revision.
pub const PROTOCOL_VERSION: &str = "2025-11-25";

#[cfg(feature = "stdio")]
mod codec;
#[cfg(feature = "stdio")]
pub use codec::BoundedCodec;
#[cfg(feature = "stdio")]
mod input;
#[cfg(feature = "stdio")]
pub use input::JsonLineReader;
#[cfg(feature = "stdio")]
mod server;
#[cfg(feature = "stdio")]
pub use server::{ServeError, Settled, ToolCall, ToolProvider, ToolServer, Wake};

#[cfg(all(feature = "stdio", loom))]
use loom::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
#[cfg(all(feature = "stdio", not(loom)))]
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
