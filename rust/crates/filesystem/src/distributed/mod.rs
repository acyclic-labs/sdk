//! Filesystem storage bindings over the canonical public service providers.

use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, GenerationId, Head, OperationId, ProposedCommit,
    Sequence, authority_commit_digest,
};
use crate::kernel::volume_authority_id;
#[cfg(test)]
use crate::performance::OperationFailure;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome as FsAppendOutcome, AuthorityStoreError, CreateAuthorityOutcome, FenceOutcome,
    ObjectId, PublicationPermit, PublicationReservation, ReplayLimit, ReservationOutcome,
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

mod work;
use work::*;
mod accounting;
mod read;
use read::*;
mod publication;
use publication::*;
mod records;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) use records::object_id_from_key;
pub(crate) use records::object_key;
use records::*;
mod reservation;

#[cfg(test)]
mod tests;
