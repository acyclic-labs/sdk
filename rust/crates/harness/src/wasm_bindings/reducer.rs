use super::*;


#[derive(Serialize)]
pub(super) struct ConversationPage<'a> {
    agent: Option<AgentId>,
    event_revision: u64,
    total_messages: u64,
    messages: &'a [ConversationMessage],
    next_sequence: Option<u64>,
}

pub(super) fn conversation_page_data(
    reducer: &Reducer,
    after_sequence: u64,
    limit: u32,
) -> Result<ConversationPage<'_>, JsValue> {
    if limit == 0 || limit as usize > crate::conversation::MAX_CONVERSATION_PAGE_MESSAGES {
        return Err(JsValue::from_str("conversation page limit is invalid"));
    }
    let conversation = reducer
        .conversation()
        .ok_or_else(|| JsValue::from_str("aggregate is not a conversation"))?;
    let total_messages = conversation.logical_revision();
    let messages = conversation
        .page(after_sequence, total_messages, limit as usize)
        .map_err(js_error)?;
    let end = messages
        .last()
        .map_or(after_sequence, |message| message.sequence);
    Ok(ConversationPage {
        agent: conversation.agent,
        event_revision: reducer.revision(),
        total_messages,
        messages,
        next_sequence: (end < total_messages).then_some(end),
    })
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
        let key = key_bytes(issuer_key)?;
        let issuer = AuthorityIssuer::new(issuer_id, key, authority.clone());
        Ok(Self {
            reducer: Reducer::new(authority, issuer.verifier(&mut None).expect("verifier copy"), schema_registry(schemas)?),
            issuer,
        })
    }

    /// Issues a root scope from this host's explicit authority object.
    #[wasm_bindgen(js_name = issueScope)]
    pub fn issue_scope(&self, id: String, capabilities: JsValue) -> Result<JsValue, JsValue> {
        let capabilities: Vec<String> = from_js(capabilities)?;
        to_js(&self.issuer.issue(&id, &Capabilities::new(capabilities), None, None, &mut None).expect("scope proof"))
    }

    /// Issues a root scope signed for one acting agent.
    #[wasm_bindgen(js_name = issueScopeForAgent)]
    pub fn issue_scope_for_agent(
        &self,
        agent: String,
        id: String,
        capabilities: JsValue,
    ) -> Result<JsValue, JsValue> {
        let agent = AgentId::parse(&agent).map_err(js_error)?;
        let capabilities: Vec<String> = from_js(capabilities)?;
        to_js(
            &self
                .issuer.issue(&id, &Capabilities::new(capabilities), Some(agent), None, &mut None).expect("scope proof"),
        )
    }

    /// Resolves named policy layers and issues only their effective grant.
    #[wasm_bindgen(js_name = issueScopeWithPolicies)]
    pub fn issue_scope_with_policies(
        &self,
        id: String,
        layers: JsValue,
    ) -> Result<JsValue, JsValue> {
        let layers: Vec<PolicyLayer> = from_js(layers)?;
        to_js(
            &self
                .issuer.issue(&id, &crate::resolve_policy_layers(&layers).map_err(js_error)?, None, None, &mut None)
                .map_err(js_error)?,
        )
    }

    /// Resolves hierarchy policy and binds its result to one acting agent.
    #[wasm_bindgen(js_name = issueScopeWithPoliciesForAgent)]
    pub fn issue_scope_with_policies_for_agent(
        &self,
        agent: String,
        id: String,
        layers: JsValue,
    ) -> Result<JsValue, JsValue> {
        let agent = AgentId::parse(&agent).map_err(js_error)?;
        let layers: Vec<PolicyLayer> = from_js(layers)?;
        to_js(
            &self
                .issuer.issue(&id, &crate::resolve_policy_layers(&layers).map_err(js_error)?, Some(agent), None, &mut None)
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

    /// Computes the canonical Rust capability for one validated volume operation.
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

    /// Computes the exact-version read capability without embedding it in a ref.
    #[wasm_bindgen(js_name = fileReadCapability)]
    pub fn file_read_capability(&self, file: JsValue) -> Result<String, JsValue> {
        let file: FileRef = from_js(file)?;
        file.read_capability().map_err(js_error)
    }

    /// Computes the canonical capability for one private directory boundary.
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
        ContentGrant::verify_read(&self.issuer.verifier(&mut None).expect("verifier copy"), &scope, &file).map_err(js_error)?;
        Ok(())
    }

    /// Composes the native immutable prefix after authenticating every exact reference against this owner's signed scope.
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
        if total.is_none()
            || files.files.len()
                > limits
                    .context_messages
                    .saturating_mul(limits.attachments.saturating_add(2))
        {
            return Err(JsValue::from_str("captured prefix files exceed limits"));
        }
        let reader = WasmPrefixReader {
            files,
            verifier: self.issuer.verifier(&mut None).expect("verifier copy"),
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

    /// Authenticates lazy directory and named-path access against the owner's signed, segment-bounded private-volume read grant.
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
        let grant = ContentGrant::verify_directory_read(
            &self.issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            &volume,
            &granted_prefix, &mut None,
        )
        .map_err(js_error)?;
        grant
            .require_directory_path(&volume, &path, None)
            .map_err(js_error)
    }

    /// Authenticates the signed scope before provider discovery or routing.
    #[wasm_bindgen(js_name = verifyScope)]
    pub fn verify_scope(&self, scope: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        self.issuer.verifier(&mut None).expect("verifier copy").verify(&scope, &mut None).map_err(js_error)
    }

    /// Authenticates a writer against the original private-volume owner.
    #[wasm_bindgen(js_name = verifyContentWrite)]
    pub fn verify_content_write(&self, scope: JsValue, volume: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let volume: VolumeRef = from_js(volume)?;
        ContentGrant::verify(
            &self.issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            &volume,
            VolumeOperation::Write, u64::MAX, &mut None,
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

    /// Applies configured runtime bounds to one exact file reference.
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

    /// Selects logical fork history against this reducer before provider preparation.
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
        to_js_admitted(
            &request
                .with_history_policy(&self.reducer, policy, &mut None)
                .map_err(js_error)?,
        )
    }

    /// Returns the authoritative conversation projection, never a parallel JS reducer.
    #[wasm_bindgen(js_name = conversationJson)]
    pub fn conversation_json(&self) -> Result<String, JsValue> {
        let conversation = self
            .reducer
            .conversation()
            .ok_or_else(|| JsValue::from_str("aggregate is not a conversation"))?;
        let bytes = crate::contract::canonical_json_bytes(conversation).map_err(js_error)?;
        String::from_utf8(bytes).map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Returns an immutable, bounded page of canonical conversation records. `after_sequence` is an exclusive cursor in the message sequence, not the potentially larger aggregate event revision.
    #[wasm_bindgen(js_name = conversationPageJson)]
    pub fn conversation_page_json(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> Result<String, JsValue> {
        let page = conversation_page_data(&self.reducer, after_sequence, limit)?;
        let bytes = crate::contract::canonical_json_bytes(&page).map_err(js_error)?;
        String::from_utf8(bytes).map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Returns the same bounded page with Rust-owned JS integer projection: revisions and cursors stay `BigInt`, validated file lengths become Number.
    #[wasm_bindgen(js_name = conversationPage)]
    pub fn conversation_page(&self, after_sequence: u64, limit: u32) -> Result<JsValue, JsValue> {
        let page = conversation_page_data(&self.reducer, after_sequence, limit)?;
        to_js_admitted(&page)
    }

    /// Applies a deterministic command.
    pub fn apply(&mut self, command: JsValue) -> Result<JsValue, JsValue> {
        let command: Command = from_js(command)?;
        let result: ApplyResult = self.reducer.apply(command).map_err(js_error)?;
        to_js(&result)
    }

    /// Applies one canonical Protobuf command, excluding fresh migrations that require executable content admission, and returns a response.
    #[wasm_bindgen(js_name = applyWire)]
    pub fn apply_wire(&mut self, command: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let (authority, command) = decode_command(&command).map_err(js_error)?;
        if &authority != self.reducer.authority() {
            return Err(JsValue::from_str(
                "command authority does not match reducer",
            ));
        }
        let result = self.reducer.apply(command).map_err(js_error)?;
        encode_apply_result(&authority, &result).map_err(js_error)
    }

    /// Returns the exact wire identity used by this compiled core.
    #[wasm_bindgen(js_name = protocolIdentity)]
    pub fn protocol_identity(&self) -> Result<JsValue, JsValue> {
        let identity = protocol_identity();
        to_js(&crate::ProtocolIdentity {
            version: identity.version,
            descriptor_digest: identity.descriptor_digest,
        })
    }

    /// Returns only authority and revision; command admission needs no snapshot.
    pub fn head(&self) -> Result<JsValue, JsValue> {
        to_js(&(self.reducer.authority(), self.reducer.revision()))
    }

    /// Reads one admitted selection through the reducer's retained identity index.
    #[wasm_bindgen(js_name = contextSelectionForOperation)]
    pub fn context_selection_for_operation(&self, operation: JsValue) -> Result<JsValue, JsValue> {
        let operation = from_js(operation)?;
        to_js_admitted(&self.reducer.context_selection_for_operation(operation))
    }

    /// Returns a versioned issuer-authenticated restoration snapshot.
    pub fn snapshot(&self) -> Result<JsValue, JsValue> {
        to_js(&self.reducer.snapshot(usize::MAX, &mut None).map(|(snapshot, _)| snapshot).map_err(js_error)?)
    }

    /// Restores a snapshot under explicit host-managed authority.
    pub fn restore(
        snapshot: JsValue,
        issuer_id: String,
        issuer_key: Vec<u8>,
        schemas: JsValue,
    ) -> Result<Self, JsValue> {
        let snapshot: Snapshot = from_js(snapshot)?;
        let issuer = AuthorityIssuer::new(
            issuer_id,
            key_bytes(issuer_key)?,
            snapshot.authority.clone(),
        );
        let reducer = Reducer::restore(snapshot, issuer.verifier(&mut None).expect("verifier copy"), schema_registry(schemas)?, &mut None)
            .map_err(js_error)?;
        Ok(Self { reducer, issuer })
    }
}

#[derive(serde::Deserialize)]
pub(super) struct SchemaDefinition {
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
                definition.fork_policy, [], None,
            )
            .map_err(js_error)?;
    }
    Ok(registry)
}

/// Decodes one canonical event payload using the native event union.
#[wasm_bindgen(js_name = decodeEventPayload)]
pub fn decode_event_payload(
    event_type: String,
    canonical_payload_json: Vec<u8>,
) -> Result<JsValue, JsValue> {
    let payload = decode_payload_wire(&event_type, &canonical_payload_json).map_err(js_error)?;
    to_js(&payload)
}

/// Decodes and validates a canonical Protobuf apply response using Rust-owned event, authority, scope, digest, and payload rules.
#[wasm_bindgen(js_name = decodeApplyResponse)]
pub fn decode_apply_response(bytes: Vec<u8>) -> Result<JsValue, JsValue> {
    let response = crate::wire::ApplyResponse::decode(bytes.as_slice())
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let envelope = response
        .event
        .ok_or_else(|| JsValue::from_str("apply response event is missing"))?;
    let (_, event) =
        crate::wire_codec::decode_event(&envelope.encode_to_vec(), &mut None).map_err(js_error)?.0;
    let result = match crate::wire::ApplyState::try_from(response.state)
        .map_err(|_| JsValue::from_str("apply response state is invalid"))?
    {
        crate::wire::ApplyState::Applied => ApplyResult::Applied { event },
        crate::wire::ApplyState::Replayed => ApplyResult::Replayed { event },
        crate::wire::ApplyState::Unspecified => {
            return Err(JsValue::from_str("apply response state is unspecified"));
        }
    };
    to_js(&result)
}

/// Decodes a generated aggregate kind using the native enum mapping.
#[wasm_bindgen(js_name = decodeAggregateKind)]
pub fn decode_aggregate_kind_wasm(value: i32) -> Result<JsValue, JsValue> {
    let kind =
        crate::wire_codec::decode_aggregate_kind(value, crate::Error::Invalid).map_err(js_error)?;
    to_js(&kind)
}
