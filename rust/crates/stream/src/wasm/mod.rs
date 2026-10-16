//! Browser boundary for canonical Stream request validation.

use futures::{
    FutureExt, StreamExt,
    future::{Either, select},
};
use js_sys::{Array, BigInt, Date, Object, Reflect, Uint8Array};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{rc::Rc, sync::Mutex};
use tsify::{Ts, Tsify};
use wasm_bindgen::{JsCast, prelude::*};

use crate::{
    MAX_COMMAND_BYTES, MemoryStream, StreamError, StreamProvider, memory, wire, wire_codec,
};

#[cfg(test)]
mod tests;

include!("stream_error_code.rs");

include!("validation.rs");

include!("projection.rs");

include!("wasm_stream.rs");

include!("follow_state.rs");

include!("wasm_follow.rs");
