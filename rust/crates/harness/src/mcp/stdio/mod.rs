//! Bounded newline protocol for an already approved native process exchange.

#[cfg(test)]
use super::PROTOCOL_VERSION;
use super::{
    charge, error,
    rpc::{Envelope, initialization, notification, rpc_result},
};
use crate::{Error, NativeCustody, NativeWork, OperationId, Result, wire::ErrorCode};
use bytes::{Buf as _, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;

mod request;
pub use request::McpStdioRequest;
mod exchange;
pub use exchange::McpStdioExchange;
mod codec;
use codec::*;
#[cfg(test)]
mod tests;
use super::services;

#[cfg(all(feature = "mcp-stdio", not(target_arch = "wasm32")))]
mod native;
#[cfg(all(feature = "mcp-stdio", not(target_arch = "wasm32")))]
pub use native::StdioMcpTransport;
