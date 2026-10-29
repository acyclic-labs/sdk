//! Browser boundary for canonical Stream request validation.

use futures::StreamExt;
use js_sys::{Array, BigInt, Date, Object, Reflect, Uint8Array};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::rc::Rc;
use tsify::{Ts, Tsify};
use wasm_bindgen::{JsCast, prelude::*};

use crate::{
    MAX_COMMAND_BYTES, MemoryStream, StreamError, StreamProvider, StreamProviderExt as _, memory,
    wire, wire_codec,
};

#[cfg(test)]
mod tests;

include!("stream_error_code.rs");

include!("validation.rs");

include!("projection.rs");

mod session_authority;
pub use session_authority::WasmSessionAuthority;

include!("wasm_stream.rs");

include!("follow_state.rs");

include!("wasm_follow.rs");
include!("provider_request.rs");

pub(crate) mod provider_rpc;
mod remote;
mod socket;

mod session_io;
mod session_server;
pub use session_server::{serve_wasm_grpc_session, serve_wasm_session};
include!("wasm_session.rs");
