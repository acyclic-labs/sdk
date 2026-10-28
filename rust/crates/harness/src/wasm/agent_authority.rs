use super::*;

fn optional_agent(agent: Option<String>) -> Result<Option<AgentId>, JsValue> {
    agent
        .map(|agent| AgentId::parse(&agent))
        .transpose()
        .map_err(js_error)
}

impl WasmAgentAuthority {
    fn verifier(&self) -> Result<crate::core::AuthorityVerifier, JsValue> {
        self.issuer.verifier(&mut None).map_err(js_error)
    }
}

#[wasm_bindgen]
impl WasmAgentAuthority {
    /// Owns the host-managed signing authority used by the agent runtime.
    #[wasm_bindgen(constructor)]
    pub fn new(
        authority: JsValue,
        issuer_id: String,
        issuer_key: Vec<u8>,
    ) -> Result<Self, JsValue> {
        let authority: Authority = from_js(authority)?;
        let issuer = AuthorityRoot::new(issuer_id, key_bytes(issuer_key)?, authority);
        issuer.verifier(&mut None).map_err(js_error)?;
        Ok(Self { issuer })
    }

    #[wasm_bindgen(js_name = protocolIdentity)]
    pub fn protocol_identity(&self) -> Result<JsValue, JsValue> {
        let identity = protocol_identity();
        to_js(&crate::ProtocolIdentity {
            version: identity.version,
            descriptor_digest: identity.descriptor_digest,
        })
    }

    /// Issues a host grant, optionally bound to an acting agent.
    #[wasm_bindgen(js_name = issueScope)]
    pub fn issue_scope(
        &self,
        id: String,
        capabilities: JsValue,
        agent: Option<String>,
    ) -> Result<JsValue, JsValue> {
        let capabilities = Capabilities::new(from_js::<Vec<String>>(capabilities)?);
        let scope = self
            .issuer
            .bootstrap_scope(&id, &capabilities, optional_agent(agent)?, &mut None)
            .map_err(js_error)?;
        to_js(&scope)
    }

    /// Resolves policy layers and issues their effective grant.
    #[wasm_bindgen(js_name = issueScopeWithPolicies)]
    pub fn issue_scope_with_policies(
        &self,
        id: String,
        layers: JsValue,
        agent: Option<String>,
    ) -> Result<JsValue, JsValue> {
        let layers: Vec<PolicyLayer> = from_js(layers)?;
        let capabilities = crate::resolve_policy_layers(&layers).map_err(js_error)?;
        let scope = self
            .issuer
            .bootstrap_scope(&id, &capabilities, optional_agent(agent)?, &mut None)
            .map_err(js_error)?;
        to_js(&scope)
    }

    /// Attenuates a scope without permitting capability expansion.
    pub fn attenuate(
        &self,
        parent: JsValue,
        id: String,
        capabilities: JsValue,
    ) -> Result<JsValue, JsValue> {
        let parent: Scope = from_js(parent)?;
        let capabilities = Capabilities::new(from_js::<Vec<String>>(capabilities)?);
        to_js(
            &self
                .issuer
                .attenuate(&parent, id, &capabilities)
                .map_err(js_error)?,
        )
    }

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
        let scope = self
            .issuer
            .delegate_private_file_read(&owner_scope, reader, id, &file)
            .map_err(js_error)?;
        to_js(&scope)
    }

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
        let scope = self
            .issuer
            .delegate_private_directory_read(&owner_scope, reader, id, &volume, &prefix)
            .map_err(js_error)?;
        to_js(&scope)
    }

    /// Authenticates the signed scope before provider discovery or routing.
    #[wasm_bindgen(js_name = verifyScope)]
    pub fn verify_scope(&self, scope: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        self.verifier()?.verify(&scope, &mut None).map_err(js_error)
    }

    #[wasm_bindgen(js_name = volumeCapability)]
    pub fn volume_capability(&self, volume: JsValue, operation: String) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        let operation = match operation.as_str() {
            "read" => VolumeOperation::Read,
            "write" => VolumeOperation::Write,
            _ => {
                return Err(js_error(crate::Error::Invalid(
                    "volume operation is invalid".into(),
                )));
            }
        };
        volume.capability(operation, &mut None).map_err(js_error)
    }

    #[wasm_bindgen(js_name = fileReadCapability)]
    pub fn file_read_capability(&self, file: JsValue) -> Result<String, JsValue> {
        let file: FileRef = from_js(file)?;
        file.read_capability(&mut None).map_err(js_error)
    }

    #[wasm_bindgen(js_name = directoryReadCapability)]
    pub fn directory_read_capability(
        &self,
        volume: JsValue,
        prefix: String,
    ) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        volume
            .directory_read_capability(&prefix, &mut None)
            .map_err(js_error)
    }

    /// Authenticates an owner-issued read grant for this exact immutable file.
    #[wasm_bindgen(js_name = verifyContentRead)]
    pub fn verify_content_read(&self, scope: JsValue, file: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let file: FileRef = from_js(file)?;
        ContentGrant::verify_read(&self.verifier()?, &scope, &file).map_err(js_error)?;
        Ok(())
    }

    /// Authenticates access beneath an owner-granted private directory.
    #[wasm_bindgen(js_name = verifyPrivateDirectoryRead)]
    pub fn verify_private_directory_read(
        &self,
        scope: JsValue,
        volume: JsValue,
        granted_prefix: String,
        path: String,
    ) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let volume: VolumeRef = from_js(volume)?;
        ContentGrant::verify_directory_read(
            &self.verifier()?,
            &scope,
            &volume,
            &granted_prefix,
            &mut None,
        )
        .and_then(|grant| grant.require_directory_path(&volume, &path, None))
        .map_err(js_error)
    }

    /// Authenticates a writer against the original private-volume owner.
    #[wasm_bindgen(js_name = verifyContentWrite)]
    pub fn verify_content_write(&self, scope: JsValue, volume: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let volume: VolumeRef = from_js(volume)?;
        ContentGrant::verify(
            &self.verifier()?,
            &scope,
            &volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )
        .map_err(js_error)?;
        Ok(())
    }

    /// Stable physical namespace shared by Filesystem and Objects adapters.
    #[wasm_bindgen(js_name = volumeStorageName)]
    pub fn volume_storage_name(&self, volume: JsValue) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        volume.storage_name(&mut None).map_err(js_error)
    }

    #[wasm_bindgen(js_name = validateFileUnderLimits)]
    pub fn validate_file_under_limits(
        &self,
        file: JsValue,
        limits: JsValue,
    ) -> Result<(), JsValue> {
        let file: FileRef = from_js(file)?;
        let limits: Limits = from_js(limits)?;
        limits.validate_file(&file, &mut None).map_err(js_error)
    }

    /// Composes a captured immutable prefix after owner authorization.
    #[wasm_bindgen(js_name = prepareInheritedModelRequest)]
    pub async fn prepare_inherited_model_request(
        &self,
        scope: JsValue,
        request: JsValue,
        prefix: JsValue,
        files: JsValue,
        limits: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let limits: Limits = from_js(limits)?;
        limits.validate(&mut None).map_err(js_error)?;
        let files = WasmProjectionResolver::from_js(files)?;
        let total = files
            .files
            .values()
            .try_fold(0_u64, |sum, bytes| sum.checked_add(bytes.len() as u64));
        let maximum_files = limits
            .context_messages
            .saturating_mul(limits.attachments.saturating_add(2));
        if total.is_none() || files.files.len() > maximum_files {
            return Err(JsValue::from_str("captured prefix files exceed limits"));
        }
        let reader = WasmPrefixReader {
            files,
            verifier: self.verifier()?,
            scope: from_js(scope)?,
        };
        reader
            .verifier
            .verify(&reader.scope, &mut None)
            .map_err(js_error)?;
        let prefix: FileRef = from_js(prefix)?;
        let input: WasmModelRequestInput = from_js(request)?;
        let prepared = crate::model::PreparedModelRequest::inherit(
            input.into(),
            &prefix,
            &reader,
            limits,
            None,
        )
        .await
        .map_err(js_error)?;
        Ok(prepared.bytes().to_vec())
    }
}
