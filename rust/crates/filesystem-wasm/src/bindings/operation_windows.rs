//! Browser ABI for the existing operation-window coordinator.

use super::*;
use acyclic_fs::{
    OperationLeaseId, OperationReconcileLimits, OperationWindowCoordinator, OperationWindowFinish,
    OperationWindowLease, OperationWindowPhase, WorkspaceOperationFinish,
};

/// Durable leases sharing the filesystem's exact `IndexedDB` authority.
#[wasm_bindgen]
pub struct BrowserOperationWindowCoordinator {
    inner: OperationWindowCoordinator<IndexedDbAuthorityStore>,
    owner: BrowserEngine,
}

#[derive(Deserialize, Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserOperationWindowLease {
    #[tsify(type = "Uint8Array")]
    workspace_id: serde_bytes::ByteBuf,
    #[tsify(type = "Uint8Array")]
    lease_id: serde_bytes::ByteBuf,
    #[tsify(type = "Uint8Array")]
    pinned_parent: serde_bytes::ByteBuf,
    expires_at_millis: u64,
}

impl BrowserOperationWindowLease {
    pub(super) fn decode(&self) -> Result<OperationWindowLease, JsValue> {
        Ok(OperationWindowLease {
            workspace_id: WorkspaceId::from_bytes(fixed_16(&self.workspace_id)?),
            lease_id: OperationLeaseId::from_bytes(fixed_16(&self.lease_id)?),
            pinned_parent: acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                &self.pinned_parent,
                "pinned parent",
            )?)),
            expires_at_millis: self.expires_at_millis,
        })
    }
}

pub(super) fn browser_publication_permit(
    lease: Option<JsValue>,
) -> Result<acyclic_fs::PublicationPermit, JsValue> {
    match lease {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            let lease: BrowserOperationWindowLease =
                serde_wasm_bindgen::from_value(value).map_err(js_error)?;
            Ok(lease.decode()?.publication_permit())
        }
        _ => Ok(acyclic_fs::PublicationPermit::Unrestricted),
    }
}

impl From<OperationWindowLease> for BrowserOperationWindowLease {
    fn from(lease: OperationWindowLease) -> Self {
        Self {
            workspace_id: lease.workspace_id.into_bytes().to_vec().into(),
            lease_id: lease.lease_id.into_bytes().to_vec().into(),
            pinned_parent: lease.pinned_parent.digest().into_bytes().to_vec().into(),
            expires_at_millis: lease.expires_at_millis,
        }
    }
}

#[derive(Serialize, Tsify)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum BrowserOperationWindowPhase {
    Idle,
    Active {
        #[tsify(type = "Uint8Array")]
        pinned_parent: serde_bytes::ByteBuf,
        #[tsify(type = "Uint8Array | undefined")]
        pending_parent: Option<serde_bytes::ByteBuf>,
        active_lease_count: usize,
    },
    Reconciling {
        #[tsify(type = "Uint8Array")]
        ticket: serde_bytes::ByteBuf,
        #[tsify(type = "Uint8Array")]
        pinned_parent: serde_bytes::ByteBuf,
        #[tsify(type = "Uint8Array | undefined")]
        pending_parent: Option<serde_bytes::ByteBuf>,
    },
}

#[derive(Serialize, Tsify)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum BrowserOperationWindowClose {
    StillActive {
        remaining: u32,
    },
    AlreadyClosed,
    Reconcile {
        #[tsify(type = "Uint8Array")]
        ticket: serde_bytes::ByteBuf,
        #[tsify(type = "Uint8Array")]
        pinned_parent: serde_bytes::ByteBuf,
        #[tsify(type = "Uint8Array | undefined")]
        pending_parent: Option<serde_bytes::ByteBuf>,
    },
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum BrowserWorkspaceOperationClose {
    StillActive {
        remaining: u32,
    },
    AlreadyClosed,
    Reconciled {
        rebase: BrowserWorkspaceRebaseResult,
    },
}

#[derive(Deserialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserOperationReconcileOptions {
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
}

impl From<BrowserOperationReconcileOptions> for OperationReconcileLimits {
    fn from(value: BrowserOperationReconcileOptions) -> Self {
        Self {
            maximum_generations: value.maximum_generations,
            maximum_changes: value.maximum_changes,
            maximum_conflicts: value.maximum_conflicts,
        }
    }
}

fn generation_bytes(value: acyclic_fs::GenerationId) -> serde_bytes::ByteBuf {
    value.digest().into_bytes().to_vec().into()
}

fn generation(value: &[u8]) -> Result<acyclic_fs::GenerationId, JsValue> {
    Ok(acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
        value,
        "generation",
    )?)))
}

#[wasm_bindgen]
impl BrowserFs {
    /// Opens a coordinator on the exact persistent authority; construction acquires no lease.
    #[wasm_bindgen(js_name = operationWindows)]
    pub fn operation_windows(&self) -> Result<BrowserOperationWindowCoordinator, JsValue> {
        let inner = match self.engine.as_ref().ok_or_else(closed_error)? {
            BrowserEngine::IndexedDb(fs) => fs.operation_windows(),
            BrowserEngine::IndexedDbOpfs(fs) => fs.operation_windows(),
            BrowserEngine::Memory(_) => {
                return Err(js_error(
                    "operation windows require persistent browser authority",
                ));
            }
        };
        Ok(BrowserOperationWindowCoordinator {
            inner,
            owner: self.engine.as_ref().ok_or_else(closed_error)?.clone(),
        })
    }
}

#[wasm_bindgen]
impl BrowserOperationWindowCoordinator {
    /// Opens an overlapping lease through the shared Rust transition.
    pub async fn begin(
        &self,
        workspace_id: &[u8],
        parent: &[u8],
        owner: String,
        now_millis: u64,
        expires_at_millis: u64,
        lease_id: Option<Vec<u8>>,
    ) -> Result<Ts<BrowserOperationWindowLease>, JsValue> {
        self.inner
            .begin_with_lease_id(
                WorkspaceId::from_bytes(fixed_16(workspace_id)?),
                generation(parent)?,
                owner,
                now_millis,
                expires_at_millis,
                match lease_id {
                    Some(value) => OperationLeaseId::from_bytes(fixed_16(&value)?),
                    None => OperationLeaseId::new(),
                },
            )
            .await
            .map(BrowserOperationWindowLease::from)
            .map_err(js_error)
            .and_then(ts)
    }

    /// Extends the exact live lease; its previous publication permit is fenced.
    pub async fn renew(
        &self,
        lease: Ts<BrowserOperationWindowLease>,
        now_millis: u64,
        expires_at_millis: u64,
    ) -> Result<Ts<BrowserOperationWindowLease>, JsValue> {
        self.inner
            .renew(
                &lease.to_rust().map_err(js_error)?.decode()?,
                now_millis,
                expires_at_millis,
            )
            .await
            .map(BrowserOperationWindowLease::from)
            .map_err(js_error)
            .and_then(ts)
    }

    /// Coalesces an authenticated parent advance.
    #[wasm_bindgen(js_name = observeParent)]
    pub async fn observe_parent(
        &self,
        workspace_id: &[u8],
        parent: &[u8],
    ) -> Result<bool, JsValue> {
        self.inner
            .observe_parent(
                WorkspaceId::from_bytes(fixed_16(workspace_id)?),
                generation(parent)?,
            )
            .await
            .map_err(js_error)
    }

    /// Closes a lease; the final closer owns reconciliation.
    pub async fn finish(
        &self,
        lease: Ts<BrowserOperationWindowLease>,
        now_millis: u64,
    ) -> Result<Ts<BrowserOperationWindowClose>, JsValue> {
        ts(
            match self
                .inner
                .finish(&lease.to_rust().map_err(js_error)?.decode()?, now_millis)
                .await
                .map_err(js_error)?
            {
                OperationWindowFinish::AlreadyClosed => BrowserOperationWindowClose::AlreadyClosed,
                OperationWindowFinish::StillActive { remaining } => {
                    BrowserOperationWindowClose::StillActive { remaining }
                }
                OperationWindowFinish::Reconcile(value) => BrowserOperationWindowClose::Reconcile {
                    ticket: value.ticket.into_bytes().to_vec().into(),
                    pinned_parent: generation_bytes(value.pinned_parent),
                    pending_parent: value.pending_parent.map(generation_bytes),
                },
            },
        )
    }

    /// Inspects the current durable phase without acquiring authority.
    pub async fn inspect(
        &self,
        workspace_id: &[u8],
    ) -> Result<Ts<BrowserOperationWindowPhase>, JsValue> {
        ts(
            match self
                .inner
                .inspect(WorkspaceId::from_bytes(fixed_16(workspace_id)?))
                .await
                .map_err(js_error)?
                .phase
            {
                OperationWindowPhase::Idle => BrowserOperationWindowPhase::Idle,
                OperationWindowPhase::Active {
                    pinned_parent,
                    pending_parent,
                    leases,
                } => BrowserOperationWindowPhase::Active {
                    pinned_parent: generation_bytes(pinned_parent),
                    pending_parent: pending_parent.map(generation_bytes),
                    active_lease_count: leases.len(),
                },
                OperationWindowPhase::Reconciling {
                    ticket,
                    pinned_parent,
                    pending_parent,
                    ..
                } => BrowserOperationWindowPhase::Reconciling {
                    ticket: ticket.into_bytes().to_vec().into(),
                    pinned_parent: generation_bytes(pinned_parent),
                    pending_parent: pending_parent.map(generation_bytes),
                },
            },
        )
    }

    /// Closes and completes deferred workspace reconciliation through the existing coordinator.
    #[wasm_bindgen(js_name = finishWorkspace)]
    pub async fn finish_workspace(
        &self,
        workspace: &BrowserWorkspace,
        lease: Ts<BrowserOperationWindowLease>,
        now_millis: u64,
        options: Ts<BrowserOperationReconcileOptions>,
    ) -> Result<Ts<BrowserWorkspaceOperationClose>, JsValue> {
        self.require_workspace(workspace)?;
        let lease = lease.to_rust().map_err(js_error)?.decode()?;
        let limits = options.to_rust().map_err(js_error)?.into();
        macro_rules! finish {
            ($value:expr) => {
                match self
                    .inner
                    .finish_workspace($value, &lease, now_millis, limits)
                    .await
                    .map_err(js_error)?
                {
                    WorkspaceOperationFinish::StillActive { remaining } => {
                        BrowserWorkspaceOperationClose::StillActive { remaining }
                    }
                    WorkspaceOperationFinish::AlreadyClosed => {
                        BrowserWorkspaceOperationClose::AlreadyClosed
                    }
                    WorkspaceOperationFinish::Reconciled(value) => {
                        BrowserWorkspaceOperationClose::Reconciled {
                            rebase: browser_workspace_rebase_result(value),
                        }
                    }
                }
            };
        }
        ts(match &workspace.engine {
            BrowserWorkspaceEngine::IndexedDb(value) => finish!(value),
            BrowserWorkspaceEngine::IndexedDbOpfs(value) => finish!(value),
            BrowserWorkspaceEngine::Memory(_) => {
                return Err(js_error(
                    "persistent operation window requires a persistent workspace",
                ));
            }
        })
    }

    /// Recovers expired leases and interrupted reconciliation without rerunning external effects.
    #[wasm_bindgen(js_name = recoverWorkspace)]
    pub async fn recover_workspace(
        &self,
        workspace: &BrowserWorkspace,
        now_millis: u64,
        options: Ts<BrowserOperationReconcileOptions>,
    ) -> Result<Option<Ts<BrowserWorkspaceRebaseResult>>, JsValue> {
        self.require_workspace(workspace)?;
        let limits = options.to_rust().map_err(js_error)?.into();
        Ok(match &workspace.engine {
            BrowserWorkspaceEngine::IndexedDb(value) => self
                .inner
                .recover_workspace(value, now_millis, limits)
                .await
                .map_err(js_error)?
                .map(browser_workspace_rebase_result)
                .map(ts)
                .transpose()?,
            BrowserWorkspaceEngine::IndexedDbOpfs(value) => self
                .inner
                .recover_workspace(value, now_millis, limits)
                .await
                .map_err(js_error)?
                .map(browser_workspace_rebase_result)
                .map(ts)
                .transpose()?,
            BrowserWorkspaceEngine::Memory(_) => {
                return Err(js_error(
                    "persistent operation window requires a persistent workspace",
                ));
            }
        })
    }
}

impl BrowserOperationWindowCoordinator {
    fn require_workspace(&self, workspace: &BrowserWorkspace) -> Result<(), JsValue> {
        let owned = match (&self.owner, &workspace.engine) {
            (BrowserEngine::IndexedDb(fs), BrowserWorkspaceEngine::IndexedDb(value)) => {
                fs.owns_workspace(value)
            }
            (BrowserEngine::IndexedDbOpfs(fs), BrowserWorkspaceEngine::IndexedDbOpfs(value)) => {
                fs.owns_workspace(value)
            }
            _ => false,
        };
        if owned {
            Ok(())
        } else {
            Err(js_error("workspace belongs to another browser filesystem"))
        }
    }
}
