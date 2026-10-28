//! Atomic operation and message indexes over canonical events.

#[cfg(feature = "filesystem")]
use crate::{
    Error, store::guarded_boundary as boundary, wire::ErrorCode::Storage, wire_codec::CodecQuota,
};
use crate::{Result, store::HistoryIo};
use acyclic_stream::OwnedBytes;
#[cfg(feature = "filesystem")]
use acyclic_stream::{ReadRequest, Record, RecordStream, StreamError, StreamPath, StreamProvider};
#[cfg(feature = "filesystem")]
use std::sync::Arc;

mod codec;
pub(crate) use codec::*;
mod paths;
#[doc(hidden)]
pub use paths::*;
#[cfg(feature = "filesystem")]
mod sdk;
#[cfg(feature = "filesystem")]
pub(crate) use sdk::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_stream::{NativeRecordStream, NativeStreamOutput};
