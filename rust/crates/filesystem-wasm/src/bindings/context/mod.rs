use super::*;

/// Browser-safe recursive multi-root context registry.
#[wasm_bindgen]
pub struct BrowserWorkspaceContextRegistry {
    pub(in crate::bindings) inner: WorkspaceContextRegistry<MemoryWorkspaceContextStore>,
}

impl Default for BrowserWorkspaceContextRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl BrowserWorkspaceContextRegistry {
    /// Creates an in-process registry. Host persistence can be injected by
    #[wasm_bindgen(constructor)]
    pub fn new() -> BrowserWorkspaceContextRegistry {
        Self {
            inner: WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new()),
        }
    }

    /// Registers one root context from the shared serde contract.
    #[wasm_bindgen(js_name = registerRoot)]
    pub async fn register_root(
        &self,
        context_id: Vec<u8>,
        roots_wire: Vec<u8>,
    ) -> Result<Vec<u8>, JsValue> {
        let roots = workspace_context_wire::decode_roots(&roots_wire).map_err(js_error)?;
        let context = self
            .inner
            .register_root(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                roots,
            )
            .await
            .map_err(js_error)?;
        workspace_context_wire::encode_context(&context).map_err(js_error)
    }

    /// Registers an exact direct child from the shared serde contract.
    #[wasm_bindgen(js_name = registerChild)]
    pub async fn register_child(
        &self,
        context_id: Vec<u8>,
        parent_context_id: Vec<u8>,
        roots_wire: Vec<u8>,
    ) -> Result<Vec<u8>, JsValue> {
        let roots = workspace_context_wire::decode_roots(&roots_wire).map_err(js_error)?;
        let context = self
            .inner
            .register_child(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                WorkspaceContextId::from_bytes(fixed_16(&parent_context_id)?),
                roots,
            )
            .await
            .map_err(js_error)?;
        workspace_context_wire::encode_context(&context).map_err(js_error)
    }

    /// Adopts one parent-authorized root without enumerating its contents.
    #[wasm_bindgen(js_name = adoptRoot)]
    pub async fn adopt_root(
        &self,
        context_id: Vec<u8>,
        root_wire: Vec<u8>,
    ) -> Result<Vec<u8>, JsValue> {
        let root = workspace_context_wire::decode_root(&root_wire).map_err(js_error)?;
        let context = self
            .inner
            .adopt_root(WorkspaceContextId::from_bytes(fixed_16(&context_id)?), root)
            .await
            .map_err(js_error)?;
        workspace_context_wire::encode_context(&context).map_err(js_error)
    }

    /// Releases one root after callers have settled its filesystem changes.
    #[wasm_bindgen(js_name = removeRoot)]
    pub async fn remove_root(
        &self,
        context_id: Vec<u8>,
        root_id: Vec<u8>,
    ) -> Result<Vec<u8>, JsValue> {
        let context = self
            .inner
            .remove_root(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                WorkspaceRootId::from_bytes(fixed_16(&root_id)?),
            )
            .await
            .map_err(js_error)?;
        workspace_context_wire::encode_context(&context).map_err(js_error)
    }

    /// Resolves one context as stable JSON.
    #[wasm_bindgen(js_name = resolve)]
    pub async fn resolve(&self, context_id: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let context = self
            .inner
            .resolve(WorkspaceContextId::from_bytes(fixed_16(&context_id)?))
            .await
            .map_err(js_error)?;
        workspace_context_wire::encode_context(&context).map_err(js_error)
    }

    /// Freezes or resumes one durable context.
    #[wasm_bindgen(js_name = setActive)]
    pub async fn set_active(&self, context_id: Vec<u8>, active: bool) -> Result<Vec<u8>, JsValue> {
        let context = self
            .inner
            .set_active(
                WorkspaceContextId::from_bytes(fixed_16(&context_id)?),
                active,
            )
            .await
            .map_err(js_error)?;
        workspace_context_wire::encode_context(&context).map_err(js_error)
    }

    /// Advances one root binding after a compatibility branch switch.
    #[wasm_bindgen(js_name = setWorkspace)]
    pub async fn set_workspace(
        &self,
        context_id: Vec<u8>,
        root_id: Vec<u8>,
        workspace_id: Vec<u8>,
        workspace_name: String,
        parent_workspace_id: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, JsValue> {
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
            .map_err(js_error)?;
        workspace_context_wire::encode_context(&context).map_err(js_error)
    }

    /// Recursively tombstones a direct-child subtree.
    #[wasm_bindgen(js_name = discardSubtree)]
    pub async fn discard_subtree(
        &self,
        parent_context_id: Vec<u8>,
        child_context_id: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let maximum = wasm_u32(&maximum)?;

        let discarded = self
            .inner
            .discard_subtree(
                WorkspaceContextId::from_bytes(fixed_16(&parent_context_id)?),
                WorkspaceContextId::from_bytes(fixed_16(&child_context_id)?),
                maximum,
            )
            .await
            .map_err(js_error)?;
        workspace_context_wire::encode_discard(&discarded).map_err(js_error)
    }
}
