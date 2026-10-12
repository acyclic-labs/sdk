use super::*;

#[wasm_bindgen]
impl WasmReducer {
    /// Issues a host grant, optionally bound to an acting agent.
    #[wasm_bindgen(js_name = issueScope)]
    pub fn issue_scope(
        &self,
        id: String,
        capabilities: JsValue,
        agent: Option<String>,
    ) -> Result<JsValue, JsValue> {
        let capabilities: Vec<String> = from_js(capabilities)?;
        let agent = agent
            .map(|agent| AgentId::parse(&agent))
            .transpose()
            .map_err(js_error)?;
        to_js(
            &self
                .issuer
                .issue(
                    &id,
                    &Capabilities::new(capabilities),
                    agent,
                    None,
                    &mut None,
                )
                .map_err(js_error)?,
        )
    }

    /// Resolves policy layers and issues their effective grant for an optional agent.
    #[wasm_bindgen(js_name = issueScopeWithPolicies)]
    pub fn issue_scope_with_policies(
        &self,
        id: String,
        layers: JsValue,
        agent: Option<String>,
    ) -> Result<JsValue, JsValue> {
        let layers: Vec<PolicyLayer> = from_js(layers)?;
        let agent = agent
            .map(|agent| AgentId::parse(&agent))
            .transpose()
            .map_err(js_error)?;
        to_js(
            &self
                .issuer
                .issue(
                    &id,
                    &crate::resolve_policy_layers(&layers).map_err(js_error)?,
                    agent,
                    None,
                    &mut None,
                )
                .map_err(js_error)?,
        )
    }

    /// Attenuates a scope without permitting capability expansion.
    pub fn attenuate(
        &self,
        parent: JsValue,
        id: String,
        capabilities: JsValue,
    ) -> Result<JsValue, JsValue> {
        let parent: Scope = from_js(parent)?;
        let capabilities: Vec<String> = from_js(capabilities)?;
        let scope = self
            .issuer
            .attenuate(&parent, id, Capabilities::new(capabilities))
            .map_err(js_error)?;
        to_js(&scope)
    }

    /// Delegates one pinned private file from its signed owner to a reader.
    #[wasm_bindgen(js_name = delegatePrivateFileRead)]
    pub fn delegate_private_file_read(
        &self,
        owner_scope: JsValue,
        reader: String,
        id: String,
        file: JsValue,
    ) -> Result<JsValue, JsValue> {
        let owner_scope: Scope = from_js(owner_scope)?;
        let reader = AgentId::parse(&reader).map_err(js_error)?;
        let file: FileRef = from_js(file)?;
        to_js(
            &self
                .issuer
                .delegate_private_file_read(&owner_scope, reader, id, &file)
                .map_err(js_error)?,
        )
    }

    /// Delegates a segment-bounded private directory to a reader without a fork.
    #[wasm_bindgen(js_name = delegatePrivateDirectoryRead)]
    pub fn delegate_private_directory_read(
        &self,
        owner_scope: JsValue,
        reader: String,
        id: String,
        volume: JsValue,
        prefix: String,
    ) -> Result<JsValue, JsValue> {
        let owner_scope: Scope = from_js(owner_scope)?;
        let reader = AgentId::parse(&reader).map_err(js_error)?;
        let volume: VolumeRef = from_js(volume)?;
        to_js(
            &self
                .issuer
                .delegate_private_directory_read(&owner_scope, reader, id, &volume, &prefix)
                .map_err(js_error)?,
        )
    }

    /// Authenticates the signed scope before provider discovery or routing.
    #[wasm_bindgen(js_name = verifyScope)]
    pub fn verify_scope(&self, scope: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        self.issuer
            .verifier(&mut None)
            .map_err(js_error)?
            .verify(&scope, &mut None)
            .map_err(js_error)
    }
}
