use super::*;

/// Browser-safe Git-shaped compatibility history over the canonical Rust state machine.
#[wasm_bindgen]
pub struct BrowserGitCompatRepository {
    pub(in crate::bindings) inner: GitCompatRepository<MemoryGitCompatStore>,
}

#[wasm_bindgen]
impl BrowserGitCompatRepository {
    /// Creates process-local compatibility state for one SDK workspace.
    #[wasm_bindgen(constructor)]
    pub fn new(workspace_id: &[u8]) -> Result<BrowserGitCompatRepository, JsValue> {
        Ok(Self {
            inner: GitCompatRepository::new(
                WorkspaceId::from_bytes(fixed_16(workspace_id)?),
                MemoryGitCompatStore::default(),
            ),
        })
    }

    /// Parses and executes a Git-shaped argv command using the Rust source of truth.
    #[wasm_bindgen(js_name = executeArgvJson)]
    pub async fn execute_argv_json(
        &self,
        argv: Vec<String>,
        workspace_generation: Vec<u8>,
        default_author: String,
        now_seconds: i64,
    ) -> Result<String, JsValue> {
        let generation = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &workspace_generation,
            "workspace generation",
        )?));
        let output = self
            .inner
            .execute_argv(
                &argv,
                acyclic_fs::GitTreeRef::exact(self.inner.workspace_id(), generation),
                &default_author,
                now_seconds,
            )
            .await
            .map_err(js_error)?;
        serde_json::to_string(&output).map_err(js_error)
    }

    /// Executes one typed command encoded with the public serde contract.
    #[wasm_bindgen(js_name = executeJson)]
    pub async fn execute_json(
        &self,
        command_json: String,
        workspace_generation: Vec<u8>,
    ) -> Result<String, JsValue> {
        let command: GitCommand = serde_json::from_str(&command_json).map_err(js_error)?;
        let generation = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &workspace_generation,
            "workspace generation",
        )?));
        let output = self
            .inner
            .execute(
                command,
                acyclic_fs::GitTreeRef::exact(self.inner.workspace_id(), generation),
            )
            .await
            .map_err(js_error)?;
        serde_json::to_string(&output).map_err(js_error)
    }

    /// Executes a natural JavaScript Git command through the Rust
    #[wasm_bindgen(js_name = executePublicJson)]
    pub async fn execute_public_json(
        &self,
        command_json: String,
        workspace_generation: Vec<u8>,
    ) -> Result<String, JsValue> {
        let command = parse_git_public_command(&command_json).map_err(js_error)?;
        let generation = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &workspace_generation,
            "workspace generation",
        )?));
        let output = self
            .inner
            .execute(
                command,
                acyclic_fs::GitTreeRef::exact(self.inner.workspace_id(), generation),
            )
            .await
            .map_err(js_error)?;
        serde_json::to_string(&output).map_err(js_error)
    }

    /// Validates and canonicalizes a Rust Git output before JS projection.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "WASM owns JavaScript strings at the ABI boundary"
    )]
    #[wasm_bindgen(js_name = canonicalizeOutputJson)]
    pub fn canonicalize_output_json(&self, value_json: String) -> Result<String, JsValue> {
        canonicalize_git_output_json(&value_json).map_err(js_error)
    }

    /// Validates and canonicalizes a durable pending transition before JS
    #[allow(
        clippy::needless_pass_by_value,
        reason = "WASM owns JavaScript strings at the ABI boundary"
    )]
    #[wasm_bindgen(js_name = canonicalizePendingTransitionJson)]
    pub fn canonicalize_pending_transition_json(
        &self,
        value_json: String,
    ) -> Result<String, JsValue> {
        canonicalize_git_pending_transition_json(&value_json).map_err(js_error)
    }
}
