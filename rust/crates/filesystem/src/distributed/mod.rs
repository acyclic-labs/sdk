//! Filesystem storage bindings over the canonical public service providers.

use crate::async_storage::authority_error;
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, GenerationId, Head, OperationId, ProposedCommit,
    Sequence, authority_commit_digest,
};
use crate::kernel::volume_authority_id;
use crate::performance::{WorkBudget, WorkCounters};
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "local")))]
use crate::storage::ObjectId;
use crate::storage::{
    AppendOutcome as FsAppendOutcome, AuthorityStoreError, CreateAuthorityOutcome, FenceOutcome,
    PublicationPermit, PublicationReservation, ReplayLimit, ReservationOutcome,
};
use crate::streams_record::StreamsDurableRecord;
use crate::{
    AsyncAuthorityStore, AuthorityAllocation, AuthorityError, AuthorityGuard, AuthorityWork,
    WorkspaceForkCommit, WorkspaceForkOutcome,
};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

const STREAM_RECORD_LIMIT: u64 = acyclic_stream::MAX_RECORD_BYTES as u64;
const GENESIS_DOMAIN: &[u8] = b"acyclic-fs-stream-genesis-v1\0";
const EPOCH_DOMAIN: &[u8] = b"acyclic-fs-stream-epoch-v1\0";
const LINEAGE_DOMAIN: &[u8] = b"acyclic-fs-stream-lineage-v1\0";
const LINEAGE_TAIL_DOMAIN: &[u8] = b"acyclic-fs-stream-lineage-tail-v1\0";
const OPERATION_LOCATOR_DOMAIN: &[u8] = b"acyclic-fs-stream-operation-locator-v1\0";
const AUTHORITY_MARKER: &[u8] = b"acyclic-fs-authority-v1\0";
const AUTHORITY_RETIRED: &[u8] = b"acyclic-fs-authority-retired-v1\0";
const PUBLICATION_GATE_FREE: &[u8] = b"acyclic-fs-publication-gate-v1\0free";
const PUBLICATION_GATE_ACTIVE: &[u8] = b"acyclic-fs-publication-gate-v1\0active\0";

/// Filesystem authority over native hierarchical Streams.
#[derive(Clone)]
pub struct StreamAuthorityStore<P> {
    provider: Arc<P>,
}

impl<P> StreamAuthorityStore<P> {
    /// Binds one authenticated public Stream provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }

    /// Returns the shared public Stream provider.
    #[must_use]
    pub fn provider(&self) -> Arc<P> {
        Arc::clone(&self.provider)
    }
}

#[derive(Clone, Copy)]
struct AuthoritySnapshot {
    // Validation binds the Stream record tail to head.sequence + 1.
    head: Head,
    epoch_tail: u64,
}

// Private read-only capability: retirement bypass is available only to accounting.
struct AccountingAuthority<'a, P>(&'a StreamAuthorityStore<P>);

use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest, StreamPath};
mod io;
use io::*;
mod native;
use native::*;
mod append;
mod authority;
mod bindings;
mod head_payload;
#[cfg(all(test, acyclic_fs_loom, not(acyclic_loom)))]
compile_error!("head-payload models require RUSTFLAGS='--cfg acyclic_fs_loom --cfg acyclic_loom'");
#[cfg(all(test, acyclic_fs_loom, acyclic_loom))]
mod head_payload_loom_tests;
mod lineage;
mod operations;
mod replay;
use append::seal;
mod records;
mod reservations;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) use records::object_id_from_key;
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "local")))]
pub(crate) use records::object_key;
use records::*;
pub(crate) fn authority_records_path(
    volume: crate::VolumeId,
) -> Result<StreamPath, AuthorityStoreError> {
    stream_path(volume_authority_id(volume), "/records")
}
mod accounting;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod append_retry_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod head_record_tests;
#[cfg(test)]
mod tests;
