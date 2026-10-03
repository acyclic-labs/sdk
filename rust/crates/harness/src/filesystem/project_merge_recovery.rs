//! Durable, restart-safe records for provider joins followed by Stream
//! publication.

use crate::{
    Error, OperationId, Result,
    conversation::{ConversationMessage, VolumeRef},
    core::Authority,
    executor::{ExecutionEvent, ExecutionJournal, load_json, stage_json},
    merge::ProjectMergeReceipt,
};
use serde::{Deserialize, Serialize};

/// Immutable inputs retained before a provider join is dispatched.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectMergeIntent {
    /// Stable Harness operation identity.
    pub operation_id: OperationId,
    /// Direct child conversation being promoted.
    pub child: Authority,
    /// Child project volume selected by the inspected plan.
    pub source_project: VolumeRef,
    /// Exact child generation selected by the inspected plan.
    pub source_generation: crate::resources::GenerationRef,
    /// Parent project volume receiving the join.
    pub target_project: VolumeRef,
    /// Exact target generation required by the join CAS.
    pub expected_target_generation: crate::resources::GenerationRef,
    /// Merge notice that will be published with the receipt.
    pub notice: ConversationMessage,
}

impl ProjectMergeIntent {
    /// Validates the ref-only recovery envelope before it is retained.
    pub fn validate(&self) -> Result<()> {
        if self.operation_id.into_bytes().iter().all(|byte| *byte == 0)
            || self.child.kind != crate::core::AggregateKind::Conversation
            || self.source_project.class() != crate::conversation::VolumeClass::Project
            || self.target_project.class() != crate::conversation::VolumeClass::Project
            || self.source_project == self.target_project
        {
            return Err(Error::Invalid(
                "project merge intent is inconsistent".into(),
            ));
        }
        self.child.stream_path()?;
        self.source_project.validate()?;
        self.target_project.validate()?;
        self.source_generation.validate()?;
        self.expected_target_generation.validate()?;
        self.notice.validate()?;
        if self.notice.kind != crate::conversation::MessageKind::Merge {
            return Err(Error::Invalid(
                "project merge intent notice is not a merge".into(),
            ));
        }
        Ok(())
    }
}

/// Durable provider/publication state retained in the private execution
/// journal. `receipt` is present only after the provider join was observed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectMergeRecoveryEntry {
    /// Immutable pre-dispatch inputs.
    pub intent: ProjectMergeIntent,
    /// Provider witness and result, once the join is durable.
    #[serde(default)]
    pub receipt: Option<ProjectMergeReceipt>,
}

impl ProjectMergeRecoveryEntry {
    /// Validates the entry and ensures the receipt cannot change its intent.
    pub fn validate(&self) -> Result<()> {
        self.intent.validate()?;
        if let Some(receipt) = &self.receipt {
            receipt.validate_shape()?;
            if receipt.operation_id != self.intent.operation_id
                || receipt.child != self.intent.child
                || receipt.source_project != self.intent.source_project
                || receipt.source_generation != self.intent.source_generation
                || receipt.target_project != self.intent.target_project
                || receipt.expected_target_generation != self.intent.expected_target_generation
                || receipt.notice != self.intent.notice
            {
                return Err(Error::Conflict(
                    "project merge receipt does not match retained intent".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Journal adapter for the provider-join then conversation-publication gap.
///
/// It uses the existing durable execution journal only as storage: the
/// `BatchPublicationStarted` event retains the private JSON record before a
/// provider effect, a second started event records the provider receipt, and
/// `BatchPublicationCompleted` closes the publication after Stream succeeds.
/// Reopening therefore returns the exact receipt to retry without replanning.
pub struct ProjectMergeRecovery<'a> {
    journal: &'a dyn ExecutionJournal,
    operation_id: OperationId,
}

impl<'a> ProjectMergeRecovery<'a> {
    /// Binds one recovery ledger to a stable operation identity.
    pub fn new(journal: &'a dyn ExecutionJournal, operation_id: OperationId) -> Self {
        Self {
            journal,
            operation_id,
        }
    }

    /// Retains the exact plan inputs before dispatching the provider join.
    pub async fn prepare(&self, intent: ProjectMergeIntent) -> Result<()> {
        if intent.operation_id != self.operation_id {
            return Err(Error::Conflict(
                "project merge operation identity changed".into(),
            ));
        }
        let entry = ProjectMergeRecoveryEntry {
            intent,
            receipt: None,
        };
        entry.validate()?;
        self.append_entry("project-merge:prepared", 0, &entry).await
    }

    /// Retains the exact provider receipt before attempting Stream
    /// publication. A restart can now retry publication without reapplying a
    /// join or consulting mutable workspace heads.
    pub async fn record_applied(&self, receipt: ProjectMergeReceipt) -> Result<()> {
        let mut entry = self
            .pending_entry()
            .await?
            .ok_or_else(|| Error::Conflict("project merge intent is missing".into()))?;
        if entry.intent.operation_id != receipt.operation_id {
            return Err(Error::Conflict(
                "project merge receipt operation changed".into(),
            ));
        }
        entry.receipt = Some(receipt);
        entry.validate()?;
        self.append_entry("project-merge:applied", 1, &entry).await
    }

    /// Returns the retained applied receipt after a restart, if Stream
    /// publication remains incomplete.
    pub async fn pending_receipt(&self) -> Result<Option<ProjectMergeReceipt>> {
        Ok(self.pending_entry().await?.and_then(|entry| entry.receipt))
    }

    /// Reopens the exact retained intent and optional provider receipt. A
    /// missing receipt means the provider effect was not durably observed and
    /// must remain subject to the caller's normal operation-identity recovery
    /// policy.
    pub async fn reopen(&self) -> Result<Option<ProjectMergeRecoveryEntry>> {
        self.pending_entry().await
    }

    /// Marks the conversation publication complete after the Stream append
    /// has been durably acknowledged.
    pub async fn complete(&self) -> Result<()> {
        let records = self.journal.replay(self.operation_id).await?;
        let Some((_, reference)) = latest_started(&records) else {
            return Err(Error::Conflict(
                "project merge recovery record is missing".into(),
            ));
        };
        let entry: ProjectMergeRecoveryEntry = load_json(self.journal, reference).await?;
        entry.validate()?;
        if entry.receipt.is_none() {
            return Err(Error::Conflict(
                "project provider result is not retained".into(),
            ));
        }
        let bytes = crate::contract::canonical_json_bytes(&entry)?;
        let digest = *blake3::hash(&bytes).as_bytes();
        self.journal
            .append(
                self.operation_id,
                "project-merge:completed".into(),
                ExecutionEvent::BatchPublicationCompleted {
                    step: 2,
                    publication_digest: digest,
                },
            )
            .await
    }

    async fn pending_entry(&self) -> Result<Option<ProjectMergeRecoveryEntry>> {
        let records = self.journal.replay(self.operation_id).await?;
        let Some((_, reference)) = latest_started(&records) else {
            return Ok(None);
        };
        let entry: ProjectMergeRecoveryEntry = load_json(self.journal, reference).await?;
        entry.validate()?;
        let bytes = crate::contract::canonical_json_bytes(&entry)?;
        let digest = *blake3::hash(&bytes).as_bytes();
        if records.iter().any(|record| {
            matches!(
                record.event,
                ExecutionEvent::BatchPublicationCompleted {
                    step: 2,
                    publication_digest,
                } if publication_digest == digest
            )
        }) {
            return Ok(None);
        }
        Ok(Some(entry))
    }

    async fn append_entry(
        &self,
        key: &str,
        step: u32,
        entry: &ProjectMergeRecoveryEntry,
    ) -> Result<()> {
        let reference = stage_json(self.journal, self.operation_id, key, entry).await?;
        self.journal
            .append(
                self.operation_id,
                key.into(),
                ExecutionEvent::BatchPublicationStarted {
                    step,
                    publication: reference,
                },
            )
            .await
    }
}

fn latest_started<'a>(
    records: &'a [crate::executor::ExecutionRecord],
) -> Option<(u32, &'a crate::conversation::FileRef)> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            ExecutionEvent::BatchPublicationStarted { step, publication } => {
                Some((*step, publication))
            }
            _ => None,
        })
        .max_by_key(|(step, _)| *step)
}
