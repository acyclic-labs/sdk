//! Durable, restart-safe records for provider joins followed by Stream
//! publication.

use crate::{
    Error, OperationId, Result,
    conversation::{ConversationMessage, VolumeRef},
    core::Authority,
    executor::{ExecutionEvent, ExecutionJournal, load_json, stage_json},
    merge::ProjectMergeReceipt,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
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
    /// Exact authority head used by the provider's publication fingerprint.
    pub expected_target_head: acyclic_fs::Head,
    /// Exact provider resolution digest retained before dispatch.
    pub resolutions_digest: [u8; 32],
    /// Authenticated approval scope retained with the operation claim.
    pub approval_scope_id: String,
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
            || self.source_project.provider() != self.target_project.provider()
            || self.source_project.owner() != self.target_project.owner()
            || self.source_generation.as_resource().provider() != self.source_project.provider()
            || self.expected_target_generation.as_resource().provider()
                != self.target_project.provider()
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
        if self.resolutions_digest == [0; 32] {
            return Err(Error::Invalid(
                "project merge intent resolution digest is empty".into(),
            ));
        }
        if self.approval_scope_id.is_empty() {
            return Err(Error::Invalid(
                "project merge approval scope is empty".into(),
            ));
        }
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
                || receipt.provider_operation_id
                    != super::project_join_key(self.intent.operation_id)?
                        .into_bytes()
                        .to_vec()
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
        let records = self.journal.replay(self.operation_id).await?;
        if let Some((_, reference)) = latest_started(&records) {
            let existing: ProjectMergeRecoveryEntry = load_json(self.journal, reference).await?;
            existing.validate()?;
            if existing.intent == intent {
                return Ok(());
            }
            return Err(Error::Conflict(
                "project merge recovery claim changed".into(),
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
    pub async fn record_applied<A, O>(
        &self,
        receipt: ProjectMergeReceipt,
        verifier: &super::FilesystemProjectMergeVerifier<A, O>,
    ) -> Result<()>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        use crate::merge::ProjectMergeVerifier;

        let records = self.journal.replay(self.operation_id).await?;
        let Some((_, reference)) = latest_started(&records) else {
            return Err(Error::Conflict("project merge intent is missing".into()));
        };
        let mut entry: ProjectMergeRecoveryEntry = load_json(self.journal, reference).await?;
        entry.validate()?;
        if entry.intent.operation_id != receipt.operation_id {
            return Err(Error::Conflict(
                "project merge receipt operation changed".into(),
            ));
        }
        if let Some(existing) = &entry.receipt {
            if existing != &receipt {
                return Err(Error::Conflict(
                    "project merge provider receipt changed".into(),
                ));
            }
            // A completed publication retains its applied receipt. This makes
            // a lost reply after completion safely idempotent instead of
            // treating the durable result as a missing pending operation.
            return Ok(());
        }
        // Check the receipt against the immutable intent before invoking the
        // provider verifier, whose successful verification may retain
        // generations. Invalid or mis-scoped records therefore produce no
        // provider-side mutation.
        let candidate = ProjectMergeRecoveryEntry {
            intent: entry.intent.clone(),
            receipt: Some(receipt.clone()),
        };
        candidate.validate()?;
        verifier.verify(&receipt).await?;
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
        if records.iter().any(|record| {
            matches!(
                &record.event,
                ExecutionEvent::BatchPublicationCompleted {
                    step: 2,
                    publication_digest,
                } if *publication_digest == digest
            )
        }) {
            return Ok(());
        }
        if !self
            .journal
            .append_if_tail(
                self.operation_id,
                records.len() as u64,
                "project-merge:completed".into(),
                ExecutionEvent::BatchPublicationCompleted {
                    step: 2,
                    publication_digest: digest,
                },
            )
            .await?
        {
            return Err(Error::Indeterminate(self.operation_id));
        }
        Ok(())
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
                &record.event,
                ExecutionEvent::BatchPublicationCompleted {
                    step: 2,
                    publication_digest,
                } if *publication_digest == digest
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
        let records = self.journal.replay(self.operation_id).await?;
        if let Some((existing_step, reference)) = latest_started(&records)
            && existing_step == step
        {
            let existing: ProjectMergeRecoveryEntry = load_json(self.journal, reference).await?;
            existing.validate()?;
            if existing == *entry {
                return Ok(());
            }
            return Err(Error::Conflict(
                "project merge recovery claim changed".into(),
            ));
        }
        let reference = stage_json(self.journal, self.operation_id, key, entry).await?;
        if !self
            .journal
            .append_if_tail(
                self.operation_id,
                records.len() as u64,
                key.into(),
                ExecutionEvent::BatchPublicationStarted {
                    step,
                    publication: reference,
                },
            )
            .await?
        {
            return Err(Error::Indeterminate(self.operation_id));
        }
        Ok(())
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
