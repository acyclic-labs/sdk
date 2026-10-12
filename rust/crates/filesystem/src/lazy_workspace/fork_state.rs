use super::*;

/// Readiness of an original fork allocation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyForkPhase {
    /// Source captured before child creation.
    Prepared,
    /// Original child creation authenticated.
    Committed,
}

/// Immutable source cut retained across creation retries.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LazyForkOrigin {
    /// Original creation identity.
    pub operation_id: crate::OperationId,
    /// Original complete source cut.
    pub snapshot: LazySnapshotRef,
    /// Source decode configuration.
    pub config: VolumeConfig,
    /// Whether child creation completed.
    pub phase: LazyForkPhase,
}

/// Latest selected import; historical results use immutable operation evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedSelectedImport {
    /// Original publication identity.
    pub operation: crate::OperationId,
    /// Committed cut.
    pub snapshot: LazySnapshotRef,
    /// Original destination cut.
    pub destination: LazySnapshotRef,
    /// Original source cut.
    pub source: LazySnapshotRef,
    /// Selected input digest.
    pub input: crate::Digest,
    /// Destination decode configuration.
    pub config: VolumeConfig,
    /// Source decode configuration.
    pub source_config: VolumeConfig,
}

impl LazyWorkspaceState {
    pub(super) fn new(
        workspace_id: WorkspaceId,
        source: SourceReference,
        overlay: LazyOverlayId,
        shadows: LazyShadowId,
    ) -> Self {
        Self {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id,
            source,
            overlay,
            shadows,
            parent_workspace_id: None,
            fork_origin: None,
            completed_selected_import: None,
            pending_remove: None,
        }
    }

    /// Validates an original root allocation without rebinding its source.
    pub fn validate_root_binding(
        &self,
        workspace_id: WorkspaceId,
        original_source: SourceReference,
    ) -> Result<(), LazyWorkspaceError> {
        if self.schema_version != LAZY_STATE_SCHEMA
            || self.workspace_id != workspace_id
            || self.parent_workspace_id.is_some()
            || self.fork_origin.is_some()
            || self.source.identity != original_source.identity
            || self.source.epoch < original_source.epoch
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(())
    }

    pub(super) fn require_ready_fork(&self) -> Result<(), LazyWorkspaceError> {
        if self
            .fork_origin
            .is_some_and(|origin| origin.phase == LazyForkPhase::Prepared)
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(())
    }
}
