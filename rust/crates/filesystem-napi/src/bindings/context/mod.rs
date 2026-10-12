use super::*;

/// Native recursive-workspace graph backed by the shared core-state namespace.
#[napi]
pub struct NativeWorkspaceGraph {
    pub(in crate::bindings) inner: WorkspaceGraph<LocalCoreStateStore>,
}

/// Durable, recursively forkable multi-root workspace-context registry.
#[napi]
pub struct NativeWorkspaceContextRegistry {
    pub(in crate::bindings) inner: WorkspaceContextRegistry<LocalCoreStateStore>,
}

/// One durable workspace-lineage record.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeWorkspaceLineageRecord {
    pub version: u32,
    pub revision: BigInt,
    pub workspace_id: Buffer,
    pub workspace_name: String,
    pub parent_workspace_id: Option<Buffer>,
    pub parent_workspace_name: Option<String>,
    pub fork_generation: Buffer,
    pub initial_generation: Buffer,
}

#[napi]
impl NativeWorkspaceContextRegistry {
    /// Opens context state in the shared private namespace.
    #[napi(factory)]
    pub fn open(state_root: String) -> Self {
        Self {
            inner: WorkspaceContextRegistry::new(LocalCoreStateStore::new(state_root)),
        }
    }

    /// Registers a root context without enumerating its physical roots.
    #[napi]
    pub async fn register_root(&self, context_id: Buffer, roots_wire: Buffer) -> Result<Buffer> {
        let roots = workspace_context_wire::decode_roots(&roots_wire).map_err(napi_wire_error)?;
        let context = self
            .inner
            .register_root(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                roots,
            )
            .await
            .map_err(napi_error)?;
        workspace_context_wire::encode_context(&context)
            .map(Buffer::from)
            .map_err(napi_wire_error)
    }

    /// Registers a child whose roots point at its exact direct parent.
    #[napi]
    pub async fn register_child(
        &self,
        context_id: Buffer,
        parent_context_id: Buffer,
        roots_wire: Buffer,
    ) -> Result<Buffer> {
        let roots = workspace_context_wire::decode_roots(&roots_wire).map_err(napi_wire_error)?;
        let context = self
            .inner
            .register_child(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                WorkspaceContextId::from_bytes(fixed_16(&parent_context_id)?),
                roots,
            )
            .await
            .map_err(napi_error)?;
        workspace_context_wire::encode_context(&context)
            .map(Buffer::from)
            .map_err(napi_wire_error)
    }

    /// Adopts one parent-authorized root without enumerating its contents.
    #[napi]
    pub async fn adopt_root(&self, context_id: Buffer, root_wire: Buffer) -> Result<Buffer> {
        let root = workspace_context_wire::decode_root(&root_wire).map_err(napi_wire_error)?;
        let context = self
            .inner
            .adopt_root(WorkspaceContextId::from_bytes(fixed_16(&context_id)?), root)
            .await
            .map_err(napi_error)?;
        workspace_context_wire::encode_context(&context)
            .map(Buffer::from)
            .map_err(napi_wire_error)
    }

    /// Releases one root after callers have settled its filesystem changes.
    #[napi]
    pub async fn remove_root(&self, context_id: Buffer, root_id: Buffer) -> Result<Buffer> {
        let context = self
            .inner
            .remove_root(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                WorkspaceRootId::from_bytes(fixed_16(&root_id)?),
            )
            .await
            .map_err(napi_error)?;
        workspace_context_wire::encode_context(&context)
            .map(Buffer::from)
            .map_err(napi_wire_error)
    }

    /// Resolves one durable context as stable JSON.
    #[napi]
    pub async fn resolve(&self, context_id: Buffer) -> Result<Buffer> {
        let context = self
            .inner
            .resolve(WorkspaceContextId::from_bytes(fixed_16(&context_id)?))
            .await
            .map_err(napi_error)?;
        workspace_context_wire::encode_context(&context)
            .map(Buffer::from)
            .map_err(napi_wire_error)
    }

    /// Freezes or resumes a retained context.
    #[napi]
    pub async fn set_active(&self, context_id: Buffer, active: bool) -> Result<Buffer> {
        let context = self
            .inner
            .set_active(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                active,
            )
            .await
            .map_err(napi_error)?;
        workspace_context_wire::encode_context(&context)
            .map(Buffer::from)
            .map_err(napi_wire_error)
    }

    /// Advances one root binding after a compatibility branch switch.
    #[napi]
    pub async fn set_workspace(
        &self,
        context_id: Buffer,
        root_id: Buffer,
        workspace_id: Buffer,
        workspace_name: String,
        parent_workspace_id: Option<Buffer>,
    ) -> Result<Buffer> {
        let parent = parent_workspace_id
            .as_ref()
            .map(|value| fixed_16(value).map(WorkspaceId::from_bytes))
            .transpose()?;
        let context = self
            .inner
            .set_workspace(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                WorkspaceRootId::from_bytes(fixed_16(&root_id)?),
                WorkspaceId::from_bytes(fixed_16(&workspace_id)?),
                workspace_name,
                parent,
            )
            .await
            .map_err(napi_error)?;
        workspace_context_wire::encode_context(&context)
            .map(Buffer::from)
            .map_err(napi_wire_error)
    }

    /// Recursively tombstones a direct-child context subtree.
    #[napi]
    pub async fn discard_subtree(
        &self,
        parent_context_id: Buffer,
        child_context_id: Buffer,
        maximum: NapiU32,
    ) -> Result<Buffer> {
        let maximum = maximum.into_inner();

        let discarded = self
            .inner
            .discard_subtree(
                WorkspaceContextId::from_bytes(fixed_16(&parent_context_id)?),
                WorkspaceContextId::from_bytes(fixed_16(&child_context_id)?),
                maximum,
            )
            .await
            .map_err(napi_error)?;
        workspace_context_wire::encode_discard(&discarded)
            .map(Buffer::from)
            .map_err(napi_wire_error)
    }
}

#[napi]
impl NativeWorkspaceGraph {
    /// Opens the recursive-workspace graph in the shared private namespace.
    #[napi(factory)]
    pub fn open(state_root: String) -> Self {
        Self {
            inner: WorkspaceGraph::new(LocalCoreStateStore::new(state_root)),
        }
    }

    /// Registers a live SDK workspace as a publication root.
    #[napi]
    pub async fn register_root(
        &self,
        workspace: &NativeWorkspace,
    ) -> Result<NativeWorkspaceLineageRecord> {
        self.inner
            .register_root(&workspace.inner)
            .await
            .map(native_workspace_lineage_record)
            .map_err(napi_error)
    }

    /// Forks a live child workspace and records its exact direct parent.
    #[napi]
    pub async fn fork(
        &self,
        parent: &NativeWorkspace,
        destination: String,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeWorkspace> {
        self.inner
            .fork(
                &parent.inner,
                destination,
                native_idempotency_key(idempotency_key)?,
            )
            .await
            .map(|inner| NativeWorkspace { inner })
            .map_err(napi_error)
    }

    /// Verifies and returns the child's exact direct-parent registration.
    #[napi]
    pub async fn authorize_join(
        &self,
        child_workspace_id: Buffer,
        parent_workspace_id: Buffer,
    ) -> Result<NativeWorkspaceLineageRecord> {
        self.inner
            .authorize_join(
                WorkspaceId::from_bytes(fixed_16(&child_workspace_id)?),
                WorkspaceId::from_bytes(fixed_16(&parent_workspace_id)?),
            )
            .await
            .map(native_workspace_lineage_record)
            .map_err(napi_error)
    }

    /// Returns the bounded direct-parent chain, nearest parent first.
    #[napi]
    pub async fn ancestors(
        &self,
        workspace_id: Buffer,
        maximum: NapiU32,
    ) -> Result<Vec<NativeWorkspaceLineageRecord>> {
        let maximum = maximum.into_inner();

        self.inner
            .ancestors(WorkspaceId::from_bytes(fixed_16(&workspace_id)?), maximum)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(native_workspace_lineage_record)
                    .collect()
            })
            .map_err(napi_error)
    }
}

pub(in crate::bindings) fn native_workspace_lineage_record(
    record: WorkspaceLineageRecord,
) -> NativeWorkspaceLineageRecord {
    NativeWorkspaceLineageRecord {
        version: record.version,
        revision: bigint(record.revision),
        workspace_id: Buffer::from(record.workspace_id.into_bytes().to_vec()),
        workspace_name: record.workspace_name,
        parent_workspace_id: record
            .parent_workspace_id
            .map(|id| Buffer::from(id.into_bytes().to_vec())),
        parent_workspace_name: record.parent_workspace_name,
        fork_generation: Buffer::from(record.fork_generation.digest().into_bytes().to_vec()),
        initial_generation: Buffer::from(record.initial_generation.digest().into_bytes().to_vec()),
    }
}
