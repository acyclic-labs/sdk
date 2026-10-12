use super::*;
/// Actual SDK publication identity and immutable logical view.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationSnapshot {
    pub(crate) operation: Option<crate::OperationId>,
    pub(crate) generation: crate::GenerationId,
    pub(crate) context: NativeSnapshotContext,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LazyPublicationCut {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) config: VolumeConfig,
    pub(crate) source: SourceReference,
    pub(crate) overlay: LazyOverlayId,
    pub(crate) shadows: LazyShadowId,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum NativeSnapshotContext {
    Authored {
        workspace_id: WorkspaceId,
        config: VolumeConfig,
    },
    Lazy(LazyPublicationCut),
    Selected {
        cut: LazyPublicationCut,
        input: crate::Digest,
    },
}
impl NativeSnapshotContext {
    pub(crate) const fn workspace_id(self) -> WorkspaceId {
        match self {
            Self::Authored { workspace_id, .. } => workspace_id,
            Self::Lazy(cut) | Self::Selected { cut, .. } => cut.workspace_id,
        }
    }
    pub(crate) const fn config(self) -> VolumeConfig {
        match self {
            Self::Authored { config, .. } => config,
            Self::Lazy(cut) | Self::Selected { cut, .. } => cut.config,
        }
    }
}
impl NativePublicationSnapshot {
    /// Actual DATA operation, absent when no publication occurred.
    pub const fn operation(&self) -> Option<crate::OperationId> {
        self.operation
    }
    /// Authenticated immutable generation.
    pub const fn generation(&self) -> crate::GenerationId {
        self.generation
    }
    /// Workspace represented by this view.
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.context.workspace_id()
    }
    /// Exact decode configuration.
    pub const fn volume_config(&self) -> VolumeConfig {
        self.context.config()
    }
    /// Whether the view includes unresolved source metadata.
    pub const fn is_lazy(&self) -> bool {
        !matches!(self.context, NativeSnapshotContext::Authored { .. })
    }
    /// Projects the immutable lazy cut using the same accounting context.
    pub fn lazy_snapshot(
        self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazySnapshotRef>, crate::FsError> {
        ctx.check()?;
        let cut = match self.context {
            NativeSnapshotContext::Authored { .. } => return Ok(None),
            NativeSnapshotContext::Lazy(cut) | NativeSnapshotContext::Selected { cut, .. } => cut,
        };
        ctx.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            bytes_copied: std::mem::size_of::<LazySnapshotRef>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(Some(LazySnapshotRef::new(
            cut.workspace_id,
            self.generation,
            cut.source,
            cut.overlay,
            cut.shadows,
        )))
    }
    pub(crate) const fn unchanged(
        context: NativeSnapshotContext,
        generation: crate::GenerationId,
    ) -> Self {
        Self {
            operation: None,
            context,
            generation,
        }
    }
    pub(crate) const fn published(
        context: NativeSnapshotContext,
        operation: crate::OperationId,
        generation: crate::GenerationId,
    ) -> Self {
        Self {
            operation: Some(operation),
            context,
            generation,
        }
    }
}
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Captures a publication view without rebinding or modifying companion state.
    pub async fn publication_snapshot(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<NativePublicationSnapshot, LazyWorkspaceError> {
        let cut = self.publication_cut(None, ctx).await?;
        let generation = self.head(ctx).await?;
        ctx.check()?;
        if self.source.reference() != cut.source {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(NativePublicationSnapshot::unchanged(
            NativeSnapshotContext::Lazy(cut),
            generation.id(),
        ))
    }
    pub(crate) async fn publication_cut(
        &self,
        operation: Option<crate::OperationId>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazyPublicationCut, LazyWorkspaceError> {
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id(), ctx)
            .await?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        state.require_ready_fork()?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != self.workspace.id()
            || state.source != self.source.reference()
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        match (operation, state.pending_remove.as_ref()) {
            (Some(op), Some(pending))
                if state.overlay == pending.prepared_overlay
                    && matches!(pending.kind,PendingLazyRemoveKind::Mounted { idempotency_key } if idempotency_key.operation_id() == op) =>
                {}
            (None, None) => {}
            _ => return Err(LazyWorkspaceError::OriginalSnapshotUnavailable),
        }
        retire_node(
            self.store
                .load_lazy_overlay(state.overlay, ctx)
                .await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?,
            ctx,
        )?;
        retire_node(
            self.store
                .load_lazy_shadow(state.shadows, ctx)
                .await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?,
            ctx,
        )?;
        ctx.charge(WorkCounters {
            bytes_copied: std::mem::size_of::<LazyPublicationCut>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        if self.source.reference() != state.source {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(LazyPublicationCut {
            workspace_id: state.workspace_id,
            config: self.workspace.volume.config,
            source: state.source,
            overlay: state.overlay,
            shadows: state.shadows,
        })
    }
}
