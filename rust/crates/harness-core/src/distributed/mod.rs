//! Stream-backed conditional commits shared by execution journals.

use crate::store::{guarded_boundary, history::HistoryIo, operations};
use crate::wire_codec::CodecQuota;
use crate::{Error, OperationId, Result, wire};
use acyclic_stream::OwnedBytes;
use acyclic_stream::{
    IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, StreamError, StreamProvider,
};
use std::sync::Arc;

#[cfg(feature = "filesystem")]
mod authority;
mod commit;
mod serializer;

#[cfg(feature = "filesystem")]
#[doc(hidden)]
pub use authority::JournalWrite;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use commit::native_call_result;
pub(crate) use commit::{CommitPhase, add_condition, add_record, commit, commit_request};
pub(crate) use serializer::digest_key;
#[cfg(feature = "filesystem")]
pub(crate) use serializer::execution_path;
