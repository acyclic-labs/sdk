//! Thin JavaScript host for the exact native reducer.

use crate::wire_codec::{decode_command, encode_apply_result, protocol_identity};
use crate::{
    Capabilities, PolicyLayer,
    core::{
        ApplyResult, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry, Scope, Snapshot,
    },
};
use wasm_bindgen::prelude::*;

/// Opaque synchronous reducer hosted in WebAssembly.
#[wasm_bindgen]
pub struct WasmReducer {
    reducer: Reducer,
    issuer: AuthorityIssuer,
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
            reducer: Reducer::new(authority, issuer.verifier(), schema_registry(schemas)?),
            issuer,
        })
    }

    /// Issues a root scope from this host's explicit authority object.
    #[wasm_bindgen(js_name = issueScope)]
    pub fn issue_scope(&self, id: String, capabilities: JsValue) -> Result<JsValue, JsValue> {
        let capabilities: Vec<String> = from_js(capabilities)?;
        to_js(&self.issuer.root(id, Capabilities::new(capabilities)))
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
                .issuer
                .root_with_policies(id, &layers)
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

    /// Applies one command through the same deterministic Rust reducer as native hosts.
    pub fn apply(&mut self, command: JsValue) -> Result<JsValue, JsValue> {
        let command: Command = from_js(command)?;
        let result: ApplyResult = self.reducer.apply(command).map_err(js_error)?;
        to_js(&result)
    }

    /// Applies one canonical Protobuf command and returns a Protobuf response.
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

    /// Returns a versioned integrity-checked restoration snapshot.
    pub fn snapshot(&self) -> Result<JsValue, JsValue> {
        to_js(&self.reducer.snapshot().map_err(js_error)?)
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
        let reducer = Reducer::restore(snapshot, issuer.verifier(), schema_registry(schemas)?)
            .map_err(js_error)?;
        Ok(Self { reducer, issuer })
    }
}

#[derive(serde::Deserialize)]
struct SchemaDefinition {
    name: String,
    version: u32,
    schema: serde_json::Value,
}

fn schema_registry(value: JsValue) -> Result<SchemaRegistry, JsValue> {
    let definitions: Vec<SchemaDefinition> = from_js(value)?;
    let mut registry = SchemaRegistry::new();
    for definition in definitions {
        registry
            .register(definition.name, definition.version, definition.schema)
            .map_err(js_error)?;
    }
    Ok(registry)
}

fn key_bytes(value: Vec<u8>) -> Result<[u8; 32], JsValue> {
    value
        .try_into()
        .map_err(|_| JsValue::from_str("authority key must contain exactly 32 bytes"))
}

fn from_js<T: serde::de::DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(|error| JsValue::from_str(&error.to_string()))
}

fn to_js<T: serde::Serialize>(value: &T) -> Result<JsValue, JsValue> {
    let serializer =
        serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true);
    value
        .serialize(&serializer)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

fn js_error(error: crate::Error) -> JsValue {
    JsValue::from_str(&error.to_string())
}
