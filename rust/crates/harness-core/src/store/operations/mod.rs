//! Atomic operation and message indexes over canonical events.

use crate::{
    Error, OperationId, Result,
    core::{Authority, AuthorityVerifier, Event, EventPayload},
    store::{HistoryIo, guarded_boundary as boundary},
    wire::ErrorCode::{Invalid, Storage},
    wire_codec::CodecQuota,
};

use acyclic_stream::{
    CommitCondition, CommitRequest, CommittedEnvelope, CommittedMutation, IdempotencyKey,
    OwnedBytes, ReadRequest, Record, RecordStream, StreamBounds, StreamError, StreamPath,
    StreamProvider,
};

use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod codec;
pub(crate) use codec::*;
mod paths;
#[doc(hidden)]
pub use paths::*;
mod publication;
pub(crate) use publication::*;
mod sdk;
pub(crate) use sdk::*;
mod lookup;
pub(crate) use lookup::*;
#[cfg(all(test, feature = "native-execution", not(target_arch = "wasm32")))]
mod native_only_provider_tests;
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_publication_custody_tests;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_stream::{NativeRecordStream, NativeStreamOutput};
