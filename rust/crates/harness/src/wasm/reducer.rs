use super::*;

#[derive(Serialize)]
struct ConversationPage<'a> {
    agent: Option<AgentId>,
    event_revision: u64,
    total_messages: u64,
    messages: &'a [ConversationMessage],
    next_sequence: Option<u64>,
}

#[derive(Deserialize)]
struct SchemaDefinition {
    name: String,
    version: u32,
    schema: serde_json::Value,
    implementation_digest: [u8; 32],
    fork_policy: ExtensionForkPolicy,
}

pub(super) fn schema_registry(value: JsValue) -> Result<SchemaRegistry, JsValue> {
    let definitions: Vec<SchemaDefinition> = from_js(value)?;
    let mut registry = SchemaRegistry::new();
    for definition in definitions {
        registry
            .register(
                definition.name,
                definition.version,
                definition.schema,
                definition.implementation_digest,
                definition.fork_policy,
                [],
                None,
            )
            .map_err(js_error)?;
    }
    Ok(registry)
}

fn optional_agent(agent: Option<String>) -> Result<Option<AgentId>, JsValue> {
    agent
        .map(|agent| AgentId::parse(&agent))
        .transpose()
        .map_err(js_error)
}

impl WasmReducer {
    fn verifier(&self) -> Result<crate::core::AuthorityVerifier, JsValue> {
        self.issuer.verifier(&mut None).map_err(js_error)
    }
}

#[wasm_bindgen]
impl WasmReducer {
    /// Creates an empty reducer with explicit host-managed authority.
    #[wasm_bindgen(constructor)]
    pub fn new(
        authority: JsValue,
        issuer_id: String,
        issuer_key: Vec<u8>,
        schemas: JsValue,
    ) -> Result<Self, JsValue> {
        let authority: Authority = from_js(authority)?;
        let issuer = AuthorityIssuer::new(issuer_id, key_bytes(issuer_key)?, authority.clone());
        Ok(Self {
            reducer: Reducer::new(
                authority,
                issuer.verifier(&mut None).map_err(js_error)?,
                schema_registry(schemas)?,
            ),
            issuer,
        })
    }

    /// Restores a snapshot under explicit host-managed authority.
    pub fn restore(
        snapshot: JsValue,
        issuer_id: String,
        issuer_key: Vec<u8>,
        schemas: JsValue,
    ) -> Result<Self, JsValue> {
        let snapshot: Snapshot = from_js(snapshot)?;
        let issuer =
            AuthorityIssuer::new(issuer_id, key_bytes(issuer_key)?, snapshot.authority.clone());
        let reducer = Reducer::restore(
            snapshot,
            issuer.verifier(&mut None).map_err(js_error)?,
            schema_registry(schemas)?,
            &mut None,
        )
        .map_err(js_error)?;
        Ok(Self { reducer, issuer })
    }

    pub fn apply(&mut self, command: JsValue) -> Result<JsValue, JsValue> {
        let command: Command = from_js(command)?;
        let result: ApplyResult = self.reducer.apply(command).map_err(js_error)?;
        to_js(&result)
    }

    /// Applies an authority-bound canonical Protobuf command.
    #[wasm_bindgen(js_name = applyWire)]
    pub fn apply_wire(&mut self, command: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let (authority, command) = decode_command(&command).map_err(js_error)?;
        if &authority != self.reducer.authority() {
            return Err(JsValue::from_str("command authority does not match reducer"));
        }
        let result = self.reducer.apply(command).map_err(js_error)?;
        encode_apply_result(&authority, &result).map_err(js_error)
    }

    #[wasm_bindgen(js_name = protocolIdentity)]
    pub fn protocol_identity(&self) -> Result<JsValue, JsValue> {
        let identity = protocol_identity();
        to_js(&crate::ProtocolIdentity {
            version: identity.version,
            descriptor_digest: identity.descriptor_digest,
        })
    }

    pub fn head(&self) -> Result<JsValue, JsValue> {
        to_js(&(self.reducer.authority(), self.reducer.revision()))
    }

    pub fn snapshot(&self) -> Result<JsValue, JsValue> {
        to_js(&self.reducer.snapshot(usize::MAX, &mut None).map_err(js_error)?.0)
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
            .issue(&id, &capabilities, optional_agent(agent)?, None, &mut None)
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
            .issue(&id, &capabilities, optional_agent(agent)?, None, &mut None)
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
        to_js(&self.issuer.attenuate(&parent, id, capabilities).map_err(js_error)?)
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
            _ => return Err(js_error(crate::Error::Invalid("volume operation is invalid".into()))),
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
        volume.directory_read_capability(&prefix, &mut None).map_err(js_error)
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
    pub fn validate_file_under_limits(&self, file: JsValue, limits: JsValue) -> Result<(), JsValue> {
        let file: FileRef = from_js(file)?;
        let limits: Limits = from_js(limits)?;
        limits.validate_file(&file, &mut None).map_err(js_error)
    }

    /// Selects fork history against this reducer before provider preparation.
    #[wasm_bindgen(js_name = prepareForkRequest, unchecked_return_type = "ForkRequest")]
    pub fn prepare_fork_request(
        &self,
        #[wasm_bindgen(unchecked_param_type = "unknown")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ForkHistoryPolicy | null")] policy: JsValue,
    ) -> Result<JsValue, JsValue> {
        let request: ForkRequest = from_js(request)?;
        let policy: crate::fork::ForkHistoryPolicy = if policy.is_null() || policy.is_undefined() {
            crate::fork::ForkHistoryPolicy::default()
        } else {
            from_js(policy)?
        };
        let request = request
            .with_history_policy(&self.reducer, policy, &mut None)
            .map_err(js_error)?;
        to_js_admitted(&request)
    }

    /// Returns a bounded conversation page with exact revisions and cursors.
    #[wasm_bindgen(js_name = conversationPage)]
    pub fn conversation_page(&self, after_sequence: u64, limit: u32) -> Result<JsValue, JsValue> {
        if limit == 0 || limit as usize > crate::conversation::MAX_CONVERSATION_PAGE_MESSAGES {
            return Err(JsValue::from_str("conversation page limit is invalid"));
        }
        let conversation = self
            .reducer
            .conversation()
            .ok_or_else(|| JsValue::from_str("aggregate is not a conversation"))?;
        let total_messages = conversation.logical_revision();
        let messages = conversation
            .page(after_sequence, total_messages, limit as usize)
            .map_err(js_error)?;
        let end = messages.last().map_or(after_sequence, |message| message.sequence);
        to_js_admitted(&ConversationPage {
            agent: conversation.agent,
            event_revision: self.reducer.revision(),
            total_messages,
            messages,
            next_sequence: (end < total_messages).then_some(end),
        })
    }

    #[wasm_bindgen(js_name = contextSelectionForOperation)]
    pub fn context_selection_for_operation(&self, operation: JsValue) -> Result<JsValue, JsValue> {
        let operation = from_js(operation)?;
        to_js_admitted(&self.reducer.context_selection_for_operation(operation))
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
        reader.verifier.verify(&reader.scope, &mut None).map_err(js_error)?;
        let prefix: FileRef = from_js(prefix)?;
        let input: WasmModelRequestInput = from_js(request)?;
        let prepared =
            crate::model::PreparedModelRequest::inherit(input.into(), &prefix, &reader, limits, None)
                .await
                .map_err(js_error)?;
        Ok(prepared.bytes().to_vec())
    }
}
