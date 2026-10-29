//! Filesystem storage bindings over the canonical public service providers.

#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::NativeStreamProvider as _;

use crate::async_storage::authority_error;
use crate::cancellation::CancellationToken;
#[cfg(test)]
use crate::foundation::authority_commit_digest;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, GenerationId, Head, OperationId, ProposedCommit,
    Sequence,
};
use crate::kernel::volume_authority_id;
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
use acyclic_stream::StreamProviderExt as _;
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

/// Stream placement accepted by the canonical authority journal.
///
/// Native publication preserves a typed original-owner guard, so its provider
/// must support concrete generic dispatch. Browser publication has no native
/// guard and can retain an event-loop-owned provider behind a trait object.
#[cfg(not(target_arch = "wasm32"))]
pub trait AuthorityProvider: acyclic_stream::NativeStreamProvider {
    /// Concrete dispatch required by the original native publication guard.
    type NativeProvider: acyclic_stream::NativeStreamProvider;
    /// Borrows this same provider without erasing the native guard's type.
    fn native_provider(&self) -> &Self::NativeProvider;
}
#[cfg(not(target_arch = "wasm32"))]
impl<P: acyclic_stream::NativeStreamProvider> AuthorityProvider for P {
    type NativeProvider = P;
    fn native_provider(&self) -> &P {
        self
    }
}

/// Stream placement accepted by the canonical browser authority journal.
#[cfg(target_arch = "wasm32")]
pub trait AuthorityProvider: acyclic_stream::StreamProvider {}
#[cfg(target_arch = "wasm32")]
impl<P: acyclic_stream::StreamProvider + ?Sized> AuthorityProvider for P {}

/// Filesystem authority over the canonical hierarchical Streams provider.
pub struct StreamAuthorityStore<P: ?Sized> {
    provider: Arc<P>,
    placement: Arc<AuthorityPlacement>,
}

impl<P: ?Sized> Clone for StreamAuthorityStore<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            placement: Arc::clone(&self.placement),
        }
    }
}

impl<P: ?Sized> StreamAuthorityStore<P> {
    /// Binds one authenticated public Stream provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self::with_placement(provider, AuthorityPlacement::default())
    }

    /// Binds a provider with authority streams at a caller-chosen placement.
    #[must_use]
    pub fn with_placement(provider: Arc<P>, placement: AuthorityPlacement) -> Self {
        Self {
            provider,
            placement: Arc::new(placement),
        }
    }

    /// Returns where this store places authority streams.
    #[must_use]
    pub fn placement(&self) -> &AuthorityPlacement {
        &self.placement
    }

    /// Returns the shared public Stream provider.
    #[must_use]
    pub fn provider(&self) -> Arc<P> {
        Arc::clone(&self.provider)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct AuthoritySnapshot {
    // Validation binds the Stream record tail to head.sequence + 1.
    head: Head,
    epoch_tail: u64,
    record_commit_id: acyclic_stream::CommitId,
}

// Private read-only capability: retirement bypass is available only to accounting.
struct AccountingAuthority<'a, P: ?Sized>(&'a StreamAuthorityStore<P>);

use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest, StreamPath};
mod io;
use io::*;
mod native;
use native::*;
pub(crate) mod actor;
mod append;
pub use crate::AppendGroupOutcome;
mod authority;
mod bindings;
mod changes;
mod head_payload;
mod policy;
#[cfg(test)]
#[cfg(all(loom, not(loom)))]
compile_error!("head-payload models require RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh");
#[cfg(test)]
#[cfg(all(loom, loom))]
mod head_payload_loom_tests;
mod lineage;
mod operations;
mod replay;
use append::seal;
mod composition;
pub use composition::{FilesystemCommit, FilesystemCommitOutcome};
mod state;
pub use state::{AuthorizedObservation, ObservedRecord, RecordAllocation, RecordReceipt};
mod placement;
pub mod state_record;
#[cfg(test)]
mod state_tests;
pub use placement::AuthorityPlacement;
mod records;
mod reservations;
use records::*;
mod accounting;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod append_retry_tests;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod head_record_tests;
#[cfg(test)]
mod tests;
