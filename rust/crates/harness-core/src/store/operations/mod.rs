//! Atomic operation and message indexes over canonical events.

use crate::{
    Error, store::guarded_boundary as boundary, wire::ErrorCode::Storage, wire_codec::CodecQuota,
};
use crate::{Result, store::HistoryIo};
use acyclic_stream::OwnedBytes;
use acyclic_stream::{ReadRequest, Record, RecordStream, StreamError, StreamPath, StreamProvider};
use std::sync::Arc;

mod codec;
pub(crate) use codec::*;
mod paths;
#[doc(hidden)]
pub use paths::*;
mod sdk;
pub(crate) use sdk::*;
mod chunks;
pub(crate) use chunks::{read_chunks, stage_chunks};

#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::{NativeRecordStream, NativeStreamOutput};
