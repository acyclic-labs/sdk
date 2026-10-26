//! Stream-backed conditional commits shared by execution journals.

use crate::store::{guarded_boundary, history::HistoryIo, operations};
use crate::wire_codec::CodecQuota;
use crate::{Error, OperationId, Result, wire};
use acyclic_stream::OwnedBytes;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream, StreamError,
    StreamProvider,
};
use std::sync::Arc;

mod authority;
mod commit;
mod serializer;

#[cfg(feature = "filesystem")]
#[doc(hidden)]
pub use authority::JournalWrite;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use commit::native_call_result;
pub(crate) use commit::{CommitPhase, add_condition, add_record, append, commit, commit_request};
pub(crate) use serializer::{digest_key, execution_path};
