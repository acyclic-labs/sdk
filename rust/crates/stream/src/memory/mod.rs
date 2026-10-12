//! Single-lock deterministic reference state machine; no durability or availability claim.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    ops::Bound,
    sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}},
};

use async_trait::async_trait;
use crate::OwnedBytes;
use futures::{StreamExt as _, stream};
use sha2::{Digest, Sha256};
use crate::wire_codec::WireMeter;
use tokio::sync::{RwLock, watch};

#[cfg(test)]
impl crate::UnixMillisClock for AtomicU64 {
    fn now_unix_millis(&self) -> u64 { self.load(Ordering::SeqCst) }
}

#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    ReadAllocation,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::{
    NativeProviderFuture, NativeRecordStream, NativeStreamOutput,
    NativeStreamResult,
};

use crate::{
    AppendOutcome, AppendReceipt, AppendRequest, Child, ChildStream, ChildrenPage,
    ChildrenPageRequest, ChildrenRequest, CommitCondition, CommitConflict, CommitId,
    CommitMutation, CommitOutcome, CommitRequest, CommittedEnvelope, CommittedFork,
    CommittedMutation, ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation,
    IdempotencyOutcome, MAX_COMMAND_BYTES, MAX_ITEMS, MAX_RECORD_BYTES, ReadRequest, Record,
    RecordStream, StreamBounds, StreamError, StreamPath, StreamProvider, SystemUnixMillisClock,
    UnixMillisClock,
};

mod accounting;
mod state;
mod capacity;
mod reads;
mod commit;
mod prepared;
mod publication;
mod provider;
mod history;
mod digest;
mod validation;
mod copying;
#[cfg(feature = "local")]
mod snapshot;
#[cfg(test)]
mod tests;
pub(crate) use accounting::*;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use prepared::NativeMemoryPreparedCommit;
use state::*;
use capacity::*;
use copying::*;
use history::*;
pub(crate) use digest::{append_digest, fork_digest, commit_digest};
use digest::*;
use publication::*;
pub(crate) use validation::{normalize_commit, validate_commit_shape, validate_limit, validate_records, validate_append_size, validate_fork_size};
use validation::*;
/// Explicit fail-closed process-memory ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryLimits {
    /// Maximum created paths, including implicit parents.
    pub paths: usize,
    /// Maximum unique retained path bytes.
    pub path_bytes: usize,
    /// Maximum unique committed records.
    pub records: usize,
    /// Maximum unique retained payload bytes.
    pub payload_bytes: usize,
    /// Maximum immutable commit envelopes.
    pub commits: usize,
    /// Maximum indefinitely retained replay identities. New identities fail
    pub idempotency_results: usize,
}

impl Default for MemoryLimits {
    fn default() -> Self {
        Self {
            paths: 100_000,
            path_bytes: 64 * 1024 * 1024,
            records: 1_000_000,
            payload_bytes: 256 * 1024 * 1024,
            commits: 1_000_000,
            idempotency_results: 1_000_000,
        }
    }
}

/// Deterministic process-local provider.
#[derive(Clone)]
pub struct MemoryStream {
    state: Arc<RwLock<State>>,
    limits: MemoryLimits,
    /// The trusted clock that deadlines are checked against.
    pub(crate) clock: Arc<dyn UnixMillisClock>,
}

impl Default for MemoryStream {
    fn default() -> Self {
        Self::new(MemoryLimits::default())
    }
}

impl MemoryStream {
    pub(crate) async fn next_follow_record(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
    ) -> Result<Option<Record>, StreamError> {
        let state = self.state.read().await;
        let current = state.paths.get(path).ok_or(StreamError::NotFound)?;
        cursor.sync(current.history.as_ref(), current.tail);
        Ok(cursor.next_record())
    }

    /// Constructs one bounded independent provider.
    #[must_use]
    pub fn new(limits: MemoryLimits) -> Self {
        Self::new_with_clock(limits, Arc::new(SystemUnixMillisClock))
    }

    /// Constructs a provider with an injected trusted clock.
    #[must_use]
    pub fn new_with_clock(limits: MemoryLimits, clock: Arc<dyn UnixMillisClock>) -> Self {
        Self { state: Arc::new(RwLock::new(State::default())), limits, clock }
    }

}
