//! Owner-bound preparation of an immutable Summary fork projection.
//!
//! This reuses canonical checkpoint/history verification. It performs no model
//! operation, workspace allocation, seed publication or child admission.

use crate::{
    Error, Result,
    context::{CanonicalContextCheckpoint, Context},
    conversation::{ContentResidencyVerifier, FileRef, Limits, VolumeRef},
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct SummaryForkSelection {
    /// Exact checkpoint published by the original parent execution journal.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub checkpoint: FileRef,
    /// Finite model projection and content bounds, narrowed by receiving scope.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmNativeLimitsWire"))]
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

/// Exact child-owned Summary projection bound by the original fork seed.
/// Payload copies preserve bytes and descriptors; other references retain their original immutable identities.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct SummaryForkCapture {
    /// Original immutable checkpoint and bounded-work selection.
    pub selection: SummaryForkSelection,
    /// Canonical whole Context in the child-owned inherited namespace.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub context: FileRef,
    /// Owner-controlled copies of private model payloads at their pinned generation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire[]"))]
    pub payloads: Vec<FileRef>,
    /// Original noncopied references required by the captured Context.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire[]"))]
    pub references: Vec<FileRef>,
}

impl SummaryForkCapture {
    /// Validates the exact typed capture before provider residency checks.
    pub fn validate(&self, child_private: &VolumeRef) -> Result<()> {
        self.selection.validate()?;
        if self
            .payloads
            .len()
            .checked_add(2)
            .is_none_or(|count| count as u64 > super::MAX_FORK_RESOURCES as u64)
            || self.references.len() as u64 > super::MAX_FORK_REFERENCES as u64
        {
            return Err(Error::Invalid(
                "summary fork capture exceeds protocol counts".into(),
            ));
        }
        self.selection.limits.validate_file(&self.context)?;
        if self.context.volume() != child_private
            || self.context.path() != ".system/inherited-conversation/summary-context.json"
            || self.context.descriptor().media_type() != "application/json"
            || self.context.descriptor().byte_length() > self.selection.limits.render_bytes
        {
            return Err(Error::Invalid(
                "summary fork context is not the selected child capture".into(),
            ));
        }
        let mut paths = std::collections::BTreeSet::new();
        for file in &self.payloads {
            self.selection.limits.validate_file(file)?;
            if file.volume() != child_private
                || !file
                    .path()
                    .starts_with(".system/inherited-conversation/summary-payloads/")
                || !paths.insert(file.path())
            {
                return Err(Error::Invalid(
                    "summary fork payload is not a unique child copy".into(),
                ));
            }
        }
        let mut refs = std::collections::BTreeSet::new();
        for file in &self.references {
            self.selection.limits.validate_file(file)?;
            if !refs.insert(crate::contract::canonical_json_digest(file)?) {
                return Err(Error::Invalid(
                    "summary fork reference appears twice".into(),
                ));
            }
            if crate::conversation::is_internal_path(file.path())
                && !crate::conversation::is_inherited_context_path(file.path())
            {
                return Err(Error::Unauthorized(
                    "summary fork cannot delegate private execution storage".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn inherited_files(&self) -> impl Iterator<Item = &FileRef> {
        std::iter::once(&self.context).chain(&self.payloads)
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
