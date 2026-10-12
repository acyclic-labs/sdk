use super::*;

/// Native durable operation-window coordinator bound to one filesystem's
#[napi]
pub struct NativeOperationWindowCoordinator {
    pub(in crate::bindings) inner: OperationWindowCoordinator<LocalOperationWindowStore>,
}

/// One overlapping filesystem-tool lease.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeOperationWindowLease {
    pub workspace_id: Buffer,
    pub lease_id: Buffer,
    pub pinned_parent: Buffer,
    pub expires_at_millis: BigInt,
}

/// Stable, compact operation-window phase projection.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeOperationWindowPhase {
    pub kind: String,
    pub ticket: Option<Buffer>,
    pub pinned_parent: Option<Buffer>,
    pub pending_parent: Option<Buffer>,
    pub active_lease_count: Option<u32>,
}

/// Result of closing one operation-window lease.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeOperationWindowClose {
    pub kind: String,
    pub remaining: Option<u32>,
    pub ticket: Option<Buffer>,
    pub pinned_parent: Option<Buffer>,
    pub pending_parent: Option<Buffer>,
}

/// Bounds for final-close or crash-recovery reconciliation.
#[napi(object)]
#[derive(Clone, Copy)]
pub struct NativeOperationReconcileOptions {
    /// Maximum immutable generations examined for ancestry.
    pub maximum_generations: NapiU32,
    /// Maximum semantic changes admitted by reconciliation.
    pub maximum_changes: NapiU32,
    /// Maximum exact conflicts returned.
    pub maximum_conflicts: NapiU32,
}

#[napi]
impl NativeOperationWindowCoordinator {
    /// Opens one overlapping tool lease, pinning the first observed parent.
    #[napi]
    pub async fn begin(
        &self,
        workspace_id: Buffer,
        parent: Buffer,
        owner: String,
        now_millis: BigInt,
        expires_at_millis: BigInt,
        lease_id: Option<Buffer>,
    ) -> Result<NativeOperationWindowLease> {
        self.inner
            .begin_with_lease_id(
                WorkspaceId::from_bytes(fixed_16(&workspace_id)?),
                generation_id(&parent)?,
                owner,
                bigint_u64(&now_millis, "nowMillis")?,
                bigint_u64(&expires_at_millis, "expiresAtMillis")?,
                match lease_id {
                    Some(value) => OperationLeaseId::from_bytes(fixed_16(&value)?),
                    None => OperationLeaseId::new(),
                },
            )
            .await
            .map(|lease| native_operation_window_lease(&lease))
            .map_err(napi_error)
    }

    /// Extends the exact live lease; its previous publication permit is fenced.
    #[napi]
    pub async fn renew(
        &self,
        lease: NativeOperationWindowLease,
        now_millis: BigInt,
        expires_at_millis: BigInt,
    ) -> Result<NativeOperationWindowLease> {
        self.inner
            .renew(
                &operation_window_lease(&lease)?,
                bigint_u64(&now_millis, "nowMillis")?,
                bigint_u64(&expires_at_millis, "expiresAtMillis")?,
            )
            .await
            .map(|lease| native_operation_window_lease(&lease))
            .map_err(napi_error)
    }

    /// Coalesces a newer parent generation without changing an active mount.
    #[napi]
    pub async fn observe_parent(&self, workspace_id: Buffer, parent: Buffer) -> Result<bool> {
        self.inner
            .observe_parent(
                WorkspaceId::from_bytes(fixed_16(&workspace_id)?),
                generation_id(&parent)?,
            )
            .await
            .map_err(napi_error)
    }

    /// Closes one lease and returns reconciliation ownership to the last closer.
    #[napi]
    pub async fn finish(
        &self,
        lease: NativeOperationWindowLease,
        now_millis: BigInt,
    ) -> Result<NativeOperationWindowClose> {
        self.inner
            .finish(
                &operation_window_lease(&lease)?,
                bigint_u64(&now_millis, "nowMillis")?,
            )
            .await
            .map(native_operation_window_close)
            .map_err(napi_error)
    }

    /// Closes one lease and performs the final deferred workspace rebase.
    #[napi]
    pub async fn finish_workspace(
        &self,
        workspace: &NativeWorkspace,
        lease: NativeOperationWindowLease,
        now_millis: BigInt,
        options: NativeOperationReconcileOptions,
    ) -> Result<NativeWorkspaceOperationFinish> {
        self.inner
            .finish_workspace(
                &workspace.inner,
                &operation_window_lease(&lease)?,
                bigint_u64(&now_millis, "nowMillis")?,
                operation_reconcile_limits(options),
            )
            .await
            .map(native_workspace_operation_finish)
            .map_err(napi_error)
    }

    /// Claims and completes reconciliation left by an expired lease or crash.
    #[napi]
    pub async fn recover_workspace(
        &self,
        workspace: &NativeWorkspace,
        now_millis: BigInt,
        options: NativeOperationReconcileOptions,
    ) -> Result<Option<NativeWorkspaceRebaseResult>> {
        self.inner
            .recover_workspace(
                &workspace.inner,
                bigint_u64(&now_millis, "nowMillis")?,
                operation_reconcile_limits(options),
            )
            .await
            .map(|outcome| outcome.map(native_workspace_rebase_result))
            .map_err(napi_error)
    }

    /// Returns the current durable operation-window phase.
    #[napi]
    pub async fn inspect(&self, workspace_id: Buffer) -> Result<NativeOperationWindowPhase> {
        self.inner
            .inspect(WorkspaceId::from_bytes(fixed_16(&workspace_id)?))
            .await
            .map(|snapshot| native_operation_window_phase(snapshot.phase))
            .map_err(napi_error)
    }
}

pub(in crate::bindings) fn native_operation_window_lease(
    lease: &OperationWindowLease,
) -> NativeOperationWindowLease {
    NativeOperationWindowLease {
        workspace_id: Buffer::from(lease.workspace_id.into_bytes().to_vec()),
        lease_id: Buffer::from(lease.lease_id.into_bytes().to_vec()),
        pinned_parent: Buffer::from(lease.pinned_parent.digest().into_bytes().to_vec()),
        expires_at_millis: bigint(lease.expires_at_millis),
    }
}

pub(in crate::bindings) fn operation_window_lease(
    lease: &NativeOperationWindowLease,
) -> Result<OperationWindowLease> {
    Ok(OperationWindowLease {
        workspace_id: WorkspaceId::from_bytes(fixed_16(&lease.workspace_id)?),
        lease_id: OperationLeaseId::from_bytes(fixed_16(&lease.lease_id)?),
        pinned_parent: generation_id(&lease.pinned_parent)?,
        expires_at_millis: bigint_u64(&lease.expires_at_millis, "expiresAtMillis")?,
    })
}

pub(in crate::bindings) fn native_operation_window_phase(
    phase: OperationWindowPhase,
) -> NativeOperationWindowPhase {
    match phase {
        OperationWindowPhase::Idle => NativeOperationWindowPhase {
            kind: "idle".to_owned(),
            ticket: None,
            pinned_parent: None,
            pending_parent: None,
            active_lease_count: None,
        },
        OperationWindowPhase::Active {
            pinned_parent,
            leases,
            pending_parent,
        } => NativeOperationWindowPhase {
            kind: "active".to_owned(),
            ticket: None,
            pinned_parent: Some(native_generation_buffer(pinned_parent)),
            pending_parent: pending_parent.map(native_generation_buffer),
            active_lease_count: Some(u32::try_from(leases.len()).unwrap_or(u32::MAX)),
        },
        OperationWindowPhase::Reconciling {
            ticket,
            pinned_parent,
            pending_parent,
            ..
        } => NativeOperationWindowPhase {
            kind: "reconciling".to_owned(),
            ticket: Some(Buffer::from(ticket.into_bytes().to_vec())),
            pinned_parent: Some(native_generation_buffer(pinned_parent)),
            pending_parent: pending_parent.map(native_generation_buffer),
            active_lease_count: None,
        },
    }
}

pub(in crate::bindings) fn native_operation_window_close(
    close: OperationWindowFinish,
) -> NativeOperationWindowClose {
    match close {
        OperationWindowFinish::StillActive { remaining } => NativeOperationWindowClose {
            kind: "still-active".to_owned(),
            remaining: Some(remaining),
            ticket: None,
            pinned_parent: None,
            pending_parent: None,
        },
        OperationWindowFinish::AlreadyClosed => NativeOperationWindowClose {
            kind: "already-closed".to_owned(),
            remaining: None,
            ticket: None,
            pinned_parent: None,
            pending_parent: None,
        },
        OperationWindowFinish::Reconcile(reconcile) => NativeOperationWindowClose {
            kind: "reconcile".to_owned(),
            remaining: None,
            ticket: Some(Buffer::from(reconcile.ticket.into_bytes().to_vec())),
            pinned_parent: Some(native_generation_buffer(reconcile.pinned_parent)),
            pending_parent: reconcile.pending_parent.map(native_generation_buffer),
        },
    }
}
