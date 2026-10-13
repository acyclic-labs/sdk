//! Single-lock deterministic reference state machine; no durability or availability claim.

use std::{
    collections::{BTreeMap, VecDeque},
    ops::Bound,
    sync::Arc,
};

use crate::OwnedBytes as Bytes;
use crate::request::{
    append_digest, commit_digest, condition_path, fork_digest, hash_conditions, hash_path,
    hash_records, mutation_path, normalize_commit, request_hasher, validate_append_size,
    validate_commit_shape, validate_fork_size, validate_records,
};
use async_trait::async_trait;
use futures::{StreamExt as _, stream};
#[cfg(test)]
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::{RwLock, watch};

#[cfg(feature = "local")]
mod snapshot;

use crate::{
    AppendOutcome, AppendReceipt, AppendRequest, Child, ChildStream, ChildrenPage,
    ChildrenPageRequest, ChildrenRequest, CommitCondition, CommitConflict, CommitId,
    CommitMutation, CommitOutcome, CommitRequest, CommittedEnvelope, CommittedFork,
    CommittedMutation, ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation,
    IdempotencyOutcome, MAX_ITEMS, MAX_RECORD_BYTES, ReadRequest, Record, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, SystemUnixMillisClock, UnixMillisClock,
};

#[cfg(test)]
mod tests;

#[cfg(not(target_arch = "wasm32"))]
mod prepared;

include!("memory_limits.rs");

include!("memory_stream.rs");
include!("provider.rs");

include!("state.rs");

include!("retained.rs");

include!("path_state.rs");

include!("history.rs");
include!("finite_read.rs");

include!("capacity.rs");

include!("mutation.rs");

include!("history_window.rs");

include!("history_cursor.rs");

include!("work.rs");

include!("commit.rs");

#[cfg(test)]
impl UnixMillisClock for std::sync::atomic::AtomicU64 {
    fn now_unix_millis(&self) -> u64 {
        self.load(std::sync::atomic::Ordering::SeqCst)
    }
}

include!("append.rs");
