//! Owner-bound preparation of an immutable Summary fork projection.
//!
//! This reuses canonical checkpoint/history verification. It performs no model
//! operation, workspace allocation, seed publication or child admission.

use crate::{
    Error, Result,
    context::{CanonicalContextCheckpoint, Context},
    conversation::{ContentResidencyVerifier, FileRef, Limits},
    core::{Authority, Reducer},
    executor::{ExecutionJournal, load_canonical_checkpoint_through},
    runtime::RuntimeScope,
    store::{HistoryCursor, HistoryReadLimits, HistoryReader},
};
use acyclic_stream::StreamProvider;
use serde::{Deserialize, Serialize};

/// Immutable checkpoint and bounded canonical work selected before fork admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryForkSelection {
    /// Exact checkpoint published by the original parent execution journal.
    pub checkpoint: FileRef,
    /// Finite model projection and content bounds, narrowed by receiving scope.
    pub limits: Limits,
    /// Shared event/encoded-byte allowance for checkpoint proof and canonical tail.
    pub history_limits: HistoryReadLimits,
}

impl SummaryForkSelection {
    /// Checks declared bounds before any history or content provider read.
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.limits.validate_file(&self.checkpoint)?;
        if self.checkpoint.descriptor().media_type() != "application/json"
            || self.checkpoint.descriptor().byte_length() > self.limits.render_bytes
            || self.history_limits.maximum_events == 0
            || self.history_limits.maximum_bytes == 0
        {
            return Err(Error::Invalid("summary fork selection is invalid".into()));
        }
        Ok(())
    }
}

/// Verified original-parent projection before child-owned materialization.
/// Private fields prevent callers constructing an unchecked capture. This is
/// not a published seed or permission to activate a child model operation.
#[derive(Clone, Debug)]
pub struct PreparedSummaryFork {
    parent: Authority,
    parent_revision: u64,
    through_sequence: u64,
    selection: SummaryForkSelection,
    checkpoint: CanonicalContextCheckpoint,
    context: Context,
}

impl PreparedSummaryFork {
    /// Exact parent aggregate whose original journal verified this projection.
    #[must_use]
    pub const fn parent(&self) -> &Authority {
        &self.parent
    }

    /// Immutable parent event boundary; subsequent appends are excluded.
    #[must_use]
    pub const fn parent_revision(&self) -> u64 {
        self.parent_revision
    }

    /// Full logical parent history inherited, independently of model projection.
    #[must_use]
    pub const fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    /// Immutable declared checkpoint and work bounds.
    #[must_use]
    pub const fn selection(&self) -> &SummaryForkSelection {
        &self.selection
    }

    /// Original verified checkpoint envelope. Its compaction proof identifies
    /// the summary output; other admitted references retain their identities.
    #[must_use]
    pub const fn checkpoint(&self) -> &CanonicalContextCheckpoint {
        &self.checkpoint
    }

    /// Exact retained messages and metadata plus the complete bounded tail.
    /// The historical current-input marker is cleared; child input is separate.
    #[must_use]
    pub const fn context(&self) -> &Context {
        &self.context
    }
}

/// Explicit read-only preparation through the existing checkpoint/history path.
/// The original parent reducer, journal, cursor and receiving scope must already
/// be supplied by the owner. No new scope, capability, summary or model effect
/// is manufactured. Later child capture/publication must bind this exact parent
/// event/logical cut and selection, then use its original admitted child reader.
pub async fn prepare_summary_fork_context<P: StreamProvider>(
    parent: &Reducer,
    selection: SummaryForkSelection,
    journal: &dyn ExecutionJournal,
    scope: &RuntimeScope,
    history: &HistoryReader<P>,
    cursor: &HistoryCursor,
    resolver: &dyn ContentResidencyVerifier,
) -> Result<PreparedSummaryFork> {
    selection.validate()?;
    if parent.authority() != &cursor.authority || parent.revision() != cursor.through_revision {
        return Err(Error::Conflict(
            "summary fork requires the exact pinned parent revision".into(),
        ));
    }
    let conversation = parent
        .conversation()
        .filter(|conversation| conversation.agent.is_some())
        .ok_or_else(|| Error::Invalid("summary fork parent conversation is unbound".into()))?;
    let through_sequence = conversation
        .messages()
        .last()
        .map_or(0, |message| message.sequence);
    let (checkpoint, context) = load_canonical_checkpoint_through(
        journal,
        &selection.checkpoint,
        selection.limits,
        scope,
        history,
        cursor,
        through_sequence,
        selection.history_limits,
        resolver,
    )
    .await?;
    Ok(PreparedSummaryFork {
        parent: parent.authority().clone(),
        parent_revision: parent.revision(),
        through_sequence,
        selection,
        checkpoint,
        context,
    })
}
