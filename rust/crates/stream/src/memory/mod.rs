//! Single-lock deterministic reference state machine; no durability or availability claim.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

#[cfg(any(test, not(target_arch = "wasm32")))]
use crate::MAX_RECORD_BYTES;
use crate::OwnedBytes as Bytes;
#[cfg(not(target_arch = "wasm32"))]
use crate::native::Work;
use crate::preparation::commit_identity;
use crate::query_sync::{RwLock, watch};
#[cfg(test)]
use crate::request::validate_append_size;
use crate::request::{append_digest, commit_digest, condition_path, fork_digest, mutation_path};
#[cfg(not(target_arch = "wasm32"))]
use crate::request::{
    hash_conditions, hash_path, hash_records, normalize_commit, request_hasher,
    validate_commit_shape,
};
use async_trait::async_trait;
use futures::{StreamExt as _, stream};
#[cfg(test)]
use prost::Message;

#[cfg(all(loom, feature = "local"))]
pub(crate) mod parking_model;
// Recovery tests exercise snapshots even without the local storage backend.
#[cfg(any(test, feature = "local"))]
mod snapshot;

use crate::{
    AppendOutcome, AppendReceipt, AppendRecord, AppendRequest, Child, ChildrenPage,
    ChildrenPageRequest, CommitCondition, CommitConflict, CommitId, CommitMutation, CommitOutcome,
    CommitRequest, CommittedEnvelope, CommittedFork, CommittedMutation, ForkReceipt, ForkRequest,
    IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, MAX_ITEMS, ReadRequest, Record,
    RecordStream, StreamError, StreamPath, StreamProvider, SystemUnixMillisClock, UnixMillisClock,
};

#[cfg(all(loom, feature = "local"))]
/// Models checked cursor preparation and publication interests against recovery.
pub mod browser_follow_model;
#[cfg(test)]
mod tests;

#[cfg(not(target_arch = "wasm32"))]
mod prepared;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) use prepared::PreparedCommit;

include!("memory_limits.rs");

include!("memory_stream.rs");
include!("decision.rs");
include!("provider.rs");
include!("follow.rs");
include!("follow_progress.rs");

include!("namespace.rs");
include!("head.rs");
include!("state.rs");

include!("retained.rs");

include!("replay.rs");

include!("commit_index.rs");

include!("path_state.rs");

include!("history.rs");

include!("capacity.rs");

include!("mutation.rs");

include!("history_window.rs");

include!("history_cursor.rs");

include!("work.rs");

include!("commit.rs");
include!("receipt.rs");

#[cfg(test)]
impl UnixMillisClock for std::sync::atomic::AtomicU64 {
    fn now_unix_millis(&self) -> u64 {
        self.load(std::sync::atomic::Ordering::SeqCst)
    }
}

include!("append.rs");
include!("fork.rs");
