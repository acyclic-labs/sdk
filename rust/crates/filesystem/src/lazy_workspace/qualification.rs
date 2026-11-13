use super::*;
/// Actual SDK publication identity and immutable logical view.
///
/// This value identifies a view; it does not pin its authored objects, source
/// epoch, overlay, shadows or publication proof. Persisting it alone does not
/// make that view available after collection or source replacement.
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
    pub(crate) prior_overlay: LazyOverlayId,
    pub(crate) prior_shadows: LazyShadowId,
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
    /// Authenticated immutable authored generation.
    ///
    /// For a source-backed view this is only one component of the cut. Use
    /// [`Self::lazy_snapshot`] to identify its source epoch, overlay and identity
    /// shadows. Equal authored generations do not imply equal
    /// source-backed views or an empty complete diff.
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
    /// Whether the view identifies source-backed state alongside its authored
    /// generation. This does not certify complete source alias coverage.
    pub const fn is_lazy(&self) -> bool {
        !matches!(self.context, NativeSnapshotContext::Authored { .. })
    }
    /// Projects the immutable lazy cut using the same accounting context.
    ///
    /// The projection does not retain storage or carry selected-publication
    /// input evidence. Preserve the original publication evidence when admitting
    /// a retained view; this projection cannot replace that proof.
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
            bytes_hashed: LazySnapshotRef::hash_bytes(self.generation),
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
}
impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    /// Recover both logical cuts of an original native publication. The DATA
    /// receipt owns the immutable generation and its exact predecessor; its sealed
    /// context retains the prior overlay and identity shadows after cleanup.
    /// Missing or collected evidence is refused, never replaced by the head.
    pub async fn original_publication_cuts(
        &self,
        operation: crate::OperationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazySnapshotRef, LazySnapshotRef)>, LazyWorkspaceError> {
        let Some((publication, prior, digest)) =
            self.load_original_publication(operation, None, ctx).await?
        else {
            return Ok(None);
        };
        let cut = match publication.context {
            NativeSnapshotContext::Lazy(cut) | NativeSnapshotContext::Selected { cut, .. } => cut,
            NativeSnapshotContext::Authored { .. } => {
                return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
            }
        };
        let after = publication
            .lazy_snapshot(ctx)?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        let parent = self
            .workspace
            .volume
            .fs
            .workspace_generation_at_position(&self.workspace.volume, prior, digest, ctx)
            .await?;
        ctx.charge(WorkCounters {
            bytes_hashed: LazySnapshotRef::hash_bytes(parent),
            ..WorkCounters::UNCHARGED
        })?;
        let before = LazySnapshotRef::new(
            cut.workspace_id,
            parent,
            cut.source,
            cut.prior_overlay,
            cut.prior_shadows,
        );
        self.validate_snapshot(before, ctx).await?;
        self.validate_snapshot(after, ctx).await?;
        Ok(Some((before, after)))
    }

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
        Ok(NativePublicationSnapshot {
            context: NativeSnapshotContext::Lazy(cut),
            operation: None,
            generation: generation.id(),
        })
    }
    /// Checks exact pending state, or a ready successor against original DATA.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn publication_companion_settled(
        &self,
        operation: crate::OperationId,
        context: NativeSnapshotContext,
        generation: Option<crate::GenerationId>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let cut = match context {
            NativeSnapshotContext::Lazy(cut) | NativeSnapshotContext::Selected { cut, .. } => cut,
            NativeSnapshotContext::Authored { .. } => {
                return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
            }
        };
        let (state, source) = self
            .recorded_binding(|| LazyWorkspaceError::OriginalSnapshotUnavailable, ctx)
            .await?;
        if cut.workspace_id != self.workspace.id()
            || cut.config != self.workspace.volume.config
            || state.workspace_id != cut.workspace_id
            || state.source.identity != cut.source.identity
            || source.epoch < cut.source.epoch
            || cut.source.identity != source.identity
            || state.overlay != cut.overlay
            || state.shadows != cut.shadows
        {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        match state.pending_remove {
            None => {
                let generation =
                    generation.ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
                if self
                    .operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()), ctx)
                    .await?
                    != Some(generation)
                {
                    return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
                }
                Ok(true)
            }
            Some(pending)
                if state.source == cut.source
                    && pending.prepared_overlay == cut.overlay
                    && cut.prior_overlay == pending.prior_overlay
                    && cut.prior_shadows == pending.prior_shadows
                    && matches!(pending.kind, PendingLazyRemoveKind::Mounted { idempotency_key }
                    if idempotency_key.operation_id() == operation) =>
            {
                Ok(false)
            }
            _ => Err(LazyWorkspaceError::Concurrent),
        }
    }
    pub(crate) async fn publication_cut(
        &self,
        operation: Option<crate::OperationId>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazyPublicationCut, LazyWorkspaceError> {
        let (mut state, source) = self
            .recorded_binding(|| LazyWorkspaceError::OriginalSnapshotUnavailable, ctx)
            .await?;
        if operation.is_some() && state.source != source {
            return Err(LazyWorkspaceError::StaleSource);
        }
        state.source = source;
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
            prior_overlay: state
                .pending_remove
                .as_ref()
                .map_or(state.overlay, |pending| pending.prior_overlay),
            prior_shadows: state
                .pending_remove
                .as_ref()
                .map_or(state.shadows, |pending| pending.prior_shadows),
        })
    }
}
