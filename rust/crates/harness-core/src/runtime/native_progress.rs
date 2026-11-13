//! Original native content measurements, separate from tool output activity.
use super::*;
use serde::{Deserialize, Serialize};

/// Original source evidence selected by the native capture owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCaptureEvidence {
    /// An original authored DATA receipt and its actual serial predecessor.
    Authored {
        /// Original capture operation.
        operation: acyclic_fs::OperationId,
        /// Original workspace.
        workspace: acyclic_fs::WorkspaceId,
        /// Actual predecessor of that receipt.
        before: acyclic_fs::GenerationId,
        /// Actual accepted generation.
        after: acyclic_fs::GenerationId,
    },
    /// Both exact source-backed cuts retained by the original DATA receipt.
    Lazy {
        /// Original sealed receipt, not a later head.
        publication: acyclic_fs::NativePublicationSnapshot,
        /// Complete original logical predecessor.
        before: acyclic_fs::LazySnapshotRef,
        /// Complete original logical result.
        after: acyclic_fs::LazySnapshotRef,
    },
    /// The physical owner is still running after a background start.
    Pending {
        /// Original call's capture operation, not yet completed.
        operation: acyclic_fs::OperationId,
    },
    /// Original content evidence cannot establish a complete comparison.
    Unavailable {
        /// Original capture operation whose evidence was requested.
        operation: acyclic_fs::OperationId,
    },
}

/// Version-one native progress receipt on the host-owned output volume.
/// `None` is an unavailable measurement, never a positive progress fact.
#[derive(Clone, Debug, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct NativeCaptureProgress {
    /// Must be one.
    pub version: u32,
    /// Original execution, including its continuation segment.
    pub execution: OperationId,
    /// Original native effect identity.
    pub call: OperationId,
    /// Original accepted evidence or its explicit absence.
    #[heap_layout(as = serde_json::Value)]
    pub evidence: NativeCaptureEvidence,
    /// Known byte change, known unchanged content, or unavailable comparison.
    pub changed: Option<bool>,
}

impl crate::filesystem::PreparationRecord for NativeCaptureProgress {}

impl NativeCaptureProgress {
    pub(crate) fn name(&self) -> &'static str {
        if matches!(self.evidence, NativeCaptureEvidence::Pending { .. }) {
            "capture-pending.json"
        } else {
            "capture-progress.json"
        }
    }

    pub(crate) fn validate(&self, execution: OperationId, receipt: &FileRef) -> Result<()> {
        if self.version != 1
            || self.execution != execution
            || receipt.path() != format!("tools/{execution}/{}/{}", self.call, self.name())
        {
            return Err(Error::Invalid(
                "native capture receipt identity differs".into(),
            ));
        }
        self.validate_evidence()
    }

    fn validate_evidence(&self) -> Result<()> {
        if matches!(
            self.evidence,
            NativeCaptureEvidence::Pending { .. } | NativeCaptureEvidence::Unavailable { .. }
        ) && self.changed.is_some()
        {
            return Err(Error::Invalid(
                "unavailable native content cannot establish progress".into(),
            ));
        }
        if let NativeCaptureEvidence::Lazy {
            publication,
            before,
            after,
        } = &self.evidence
        {
            if publication.operation().is_none()
                || publication.workspace_id() != before.workspace_id
                || before.workspace_id != after.workspace_id
                || publication.generation() != after.authored_generation
            {
                return Err(Error::Invalid("native capture logical cuts differ".into()));
            }
        }
        Ok(())
    }
}

impl ToolContext {
    /// Disabled native progress observation performs no output or query I/O.
    pub fn native_progress_enabled(&self) -> bool {
        self.task
            .journal
            .as_ref()
            .and_then(|(journal, _)| journal.run_admission())
            .is_some_and(|source| source.value.no_progress_turns.is_some())
    }

    /// Replays the original terminal measurement before a capture can consume
    /// later filesystem changes under this call's identity.
    pub async fn recover_native_progress(&self) -> Result<bool> {
        if !self.native_progress_enabled() {
            return Ok(false);
        }
        let Some((file, bytes)) = self.read_output_file_once("capture-progress.json").await? else {
            return Ok(false);
        };
        let record: NativeCaptureProgress =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        record.validate(self.turn_id(), &file)?;
        if record.call != self.operation_id() {
            return Err(Error::Invalid(
                "native capture belongs to another call".into(),
            ));
        }
        self.publish_native_progress(&file).await?;
        Ok(true)
    }

    /// The physical owner supplies an original SDK comparison, before its tool
    /// terminal. Unknown or pending evidence must use `None`, not progress.
    pub async fn record_native_progress(
        &self,
        evidence: NativeCaptureEvidence,
        changed: Option<bool>,
    ) -> Result<()> {
        if !self.native_progress_enabled() {
            return Ok(());
        }
        let record = NativeCaptureProgress {
            version: 1,
            execution: self.turn_id(),
            call: self.operation_id(),
            evidence,
            changed,
        };
        record.validate_evidence()?;
        let bytes = crate::contract::canonical_json_bytes(&record)?;
        let file = self
            .stage_output_file_once(record.name(), &bytes, "application/json", record.name())
            .await?;
        record.validate(self.turn_id(), &file)?;
        self.publish_native_progress(&file).await
    }

    async fn publish_native_progress(&self, file: &FileRef) -> Result<()> {
        if let Some((journal, _)) = &self.task.journal {
            journal
                .published_progress(
                    self.turn_id(),
                    self.operation_id(),
                    crate::executor::ProgressPublication::NativeCapture(file),
                    None,
                )?
                .await?;
        }
        Ok(())
    }
}
