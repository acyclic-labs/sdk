//! The only library seam for compile-time Cargo metadata.
#![expect(
    clippy::disallowed_macros,
    reason = "explicit Cargo metadata only; user environment discovery is forbidden"
)]

#[cfg(not(target_arch = "wasm32"))]
pub(crate) const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const MCP_INITIALIZATION: &str = concat!(
    "{\"capabilities\":{},\"clientInfo\":{\"name\":\"acyclic-harness\",\"version\":\"",
    env!("CARGO_PKG_VERSION"),
    "\"},\"protocolVersion\":\"2025-11-25\"}",
);
