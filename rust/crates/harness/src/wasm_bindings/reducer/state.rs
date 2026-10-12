use super::*;

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
            reducer: Reducer::new(
                authority,
                issuer.verifier(&mut None).map_err(js_error)?,
                schema_registry(schemas)?,
            ),
            issuer,
        })
    }

    /// Applies a deterministic command.
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

    /// Returns a versioned issuer-authenticated restoration snapshot.
    pub fn snapshot(&self) -> Result<JsValue, JsValue> {
        to_js(
            &self
                .reducer
                .snapshot(usize::MAX, &mut None)
                .map_err(js_error)?
                .0,
        )
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
        let reducer = Reducer::restore(
            snapshot,
            issuer.verifier(&mut None).map_err(js_error)?,
            schema_registry(schemas)?,
            &mut None,
        )
        .map_err(js_error)?;
        Ok(Self { reducer, issuer })
    }
}

#[derive(serde::Deserialize)]
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
