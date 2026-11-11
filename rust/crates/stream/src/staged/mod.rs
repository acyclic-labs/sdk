//! Shared staged request ingestion, independent of placement and transport.
//!
//! Staging retains immutable bytes only. Final apply must consume this cursor
//! under the shared publication owner, canonically bind retry identity, and
//! publish every effect together. A staged root is never a Stream position.
use crate::content::tree::{self, Root, Storage};
use crate::{
    CommitCondition, CommitId, CommitMutation, IdempotencyKey, OwnedBytes, StreamError,
    WriteRequest,
};

mod decode;
mod encode;
pub use decode::Reader;

const MAGIC: &[u8] = b"acyclic-stream-staged-v1\0";
// This chooses a chunk representation, never an accepted logical byte ceiling.
const INLINE_BYTES: usize = 4096;

/// Original request family; accepted-output selection does not change identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Family {
    /// One append, including its optional tail condition.
    Append,
    /// One fork, including current-tail selection and exclusions.
    Fork,
    /// Ordered coordinated effects.
    Commit,
}

/// A field's immutable bytes, inline in a page or addressed by content root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Field {
    /// Small bytes retained only for the current decoded token.
    Inline(OwnedBytes),
    /// Arbitrarily large bytes, read through the shared content-tree reader.
    Content(Root),
}

/// One bounded decoded token. Wide counts describe subsequent ordered tokens;
/// they do not allocate result vectors or change Stream authority state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Token {
    /// Begins an effect. All its conditions precede its mutations.
    Effect {
        /// Whether a failed condition skips this effect.
        soft: bool,
        /// Number of following condition tokens.
        conditions: u64,
        /// Number of following mutation descriptors.
        mutations: u64,
    },
    /// Requires existence or absence without sampling either during staging.
    Presence {
        /// Destination whose presence is checked at final apply.
        path: Field,
        /// True for present, false for absent.
        present: bool,
    },
    /// Requires the exact tail, optionally bound to its preceding commit.
    Tail {
        /// Destination checked at final apply.
        path: Field,
        /// Required exclusive tail.
        expected: u64,
        /// Required preceding commit for a Head condition.
        commit_id: Option<CommitId>,
    },
    /// Ordered append records follow, including each record's header tokens.
    Append {
        /// Append destination.
        path: Field,
        /// Number of subsequent record descriptors.
        records: u64,
    },
    /// Exclusions then optional appended records follow this fork descriptor.
    Fork {
        /// Source whose cut is selected at final apply.
        source: Field,
        /// Fork destination.
        destination: Field,
        /// Explicit source cut, or the current tail at final apply.
        at_tail: Option<u64>,
        /// Number of subsequent exclusion tokens.
        exclusions: u64,
        /// Number of subsequent appended records.
        records: u64,
    },
    /// A segment-prefix exclusion, preserving the caller's fork semantics.
    Exclusion(Field),
    /// One value, then ordered key and attribute tokens. Values remain ordered
    /// across all physical page and large-content boundaries.
    Record {
        /// Original value or its content root.
        value: Field,
        /// Number of subsequent key tokens.
        keys: u64,
        /// Number of subsequent attribute tokens.
        attributes: u64,
    },
    /// One canonical indexed key.
    Key(Field),
    /// One canonical named attribute.
    Attribute {
        /// Canonical attribute name.
        name: Field,
        /// Opaque attribute value.
        value: Field,
    },
}

/// A complete sealed submission bound to one commit id and original retry key.
/// Private fields prevent rebinding; storage adapters must additionally enforce
/// account/owner authority and retain pages until final apply or retirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedWrite {
    commit_id: CommitId,
    key: IdempotencyKey,
    family: Family,
    body: Root,
    accepted: bool,
}

impl StagedWrite {
    /// Permanently bound commit identity. It carries no assigned position.
    pub const fn commit_id(&self) -> CommitId {
        self.commit_id
    }
    /// Original caller-owned retry identity.
    pub fn idempotency_key(&self) -> &IdempotencyKey {
        &self.key
    }
    /// Original request family.
    pub const fn family(&self) -> Family {
        self.family
    }
    /// Immutable request bytes, not the final canonical retry digest.
    pub const fn body(&self) -> Root {
        self.body
    }
    /// Whether the caller selected the accepted-envelope projection.
    pub const fn accepted(&self) -> bool {
        self.accepted
    }
}

/// Stages any request count or record size through the one shared page writer.
/// Returns a sealed root only after every field has been stored. Failure or
/// cancellation returns no submission and makes no Stream state visible.
pub async fn stage(
    storage: &impl Storage,
    commit_id: CommitId,
    request: &WriteRequest,
) -> Result<StagedWrite, StreamError> {
    encode::stage(storage, commit_id, request).await
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
