#[cfg(test)]
mod alloy_tests;
mod cancel;
mod stream;
#[cfg(test)]
mod tests;

use crate::{
    AlloySpec, AlloyTurn, BrowserTransport, Credentials, ModelFeatures, ProviderSpec, Registry,
    ResolvedAlloy, ResolvedModel,
};
use acyclic_harness::{
    conversation::Limits,
    model::{Model, ModelDispatch, ModelProvider, PreparedModelRequest},
};
use js_sys::{Function, Reflect, Uint8Array};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, rc::Rc, sync::Arc};
use wasm_bindgen::prelude::*;

fn error(error: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&error.to_string()).into()
}

fn from_js<T: serde::de::DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(error)
}

fn to_js(value: &impl Serialize) -> Result<JsValue, JsValue> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(error)
}

#[wasm_bindgen(inline_js = r#"
export function literalObjects(value) {
  if (value instanceof Map) {
    return Object.fromEntries((function* () {
      for (const entry of value) { entry[1] = literalObjects(entry[1]); yield entry; }
    })());
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = literalObjects(value[i]);
  } else if (value !== null && typeof value === 'object') {
    for (const key of Object.keys(value)) value[key] = literalObjects(value[key]);
  }
  return value;
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = literalObjects)]
    fn literal_objects(value: JsValue) -> JsValue;
}

/// Arbitrary model JSON retains integers and literal map keys across the JS boundary.
fn to_js_json(value: &impl Serialize) -> Result<JsValue, JsValue> {
    value
        .serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_missing_as_null(true)
                .serialize_large_number_types_as_bigints(true),
        )
        .map(literal_objects)
        .map_err(error)
}

#[wasm_bindgen(typescript_custom_section)]
const TYPES: &str = r#"
export type ProviderFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
"#;

/// Shared adapters using explicit browser credentials and an injectable fetch.
#[wasm_bindgen]
pub struct WasmProviderRegistry {
    registry: Registry,
    limits: Limits,
}

#[wasm_bindgen]
impl WasmProviderRegistry {
    #[wasm_bindgen(constructor)]
    pub fn new(
        #[wasm_bindgen(unchecked_param_type = "Readonly<Record<string, string>>")] keys: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
        #[wasm_bindgen(unchecked_optional_param_type = "ProviderFetch")] fetch: Option<Function>,
        #[wasm_bindgen(
            unchecked_optional_param_type = "(file: WasmFileRefWire, maximumBytes: number) => Promise<Uint8Array>"
        )]
        reader: Option<Function>,
    ) -> Result<WasmProviderRegistry, JsValue> {
        let keys: BTreeMap<String, String> = from_js(keys)?;
        let limits: Limits = from_js(limits)?;
        limits.validate(&mut None).map_err(error)?;
        let transport = match fetch {
            Some(fetch) => BrowserTransport::new(fetch),
            None => BrowserTransport::global().map_err(error)?,
        };
        let mut registry =
            Registry::with_transport(Credentials::explicit(keys), Arc::new(transport));
        if let Some(reader) = reader {
            registry = registry.with_content(
                Arc::new(acyclic_harness::BrowserContentReader(reader)),
                limits.render_bytes,
            );
        }
        Ok(Self { registry, limits })
    }

    /// Registers endpoint dialect and exact local or remote model capacities.
    pub fn register(
        &mut self,
        provider: String,
        #[wasm_bindgen(unchecked_param_type = "ProviderSpec")] config: JsValue,
    ) -> Result<(), JsValue> {
        self.registry
            .register(provider, from_js::<ProviderSpec>(config)?)
            .map_err(error)
    }

    pub fn resolve(
        &self,
        selection: &str,
        #[wasm_bindgen(unchecked_optional_param_type = "unknown")] options: Option<JsValue>,
    ) -> Result<WasmModelProvider, JsValue> {
        let options = match options {
            None => serde_json::json!({}),
            Some(options) => acyclic_harness::deserialize_json_value(
                serde_wasm_bindgen::Deserializer::from(options),
            )
            .map_err(error)?,
        };
        Ok(WasmModelProvider {
            provider: Arc::new(
                self.registry
                    .resolve_with_options(selection, options)
                    .map_err(error)?,
            ),
            limits: self.limits,
        })
    }

    /// Resolves the same declarative policy and capabilities as native execution.
    #[wasm_bindgen(js_name = resolveAlloy)]
    pub fn resolve_alloy(
        &self,
        #[wasm_bindgen(unchecked_param_type = "AlloySpec")] spec: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ModelFeatures")] required: JsValue,
    ) -> Result<WasmModelAlloy, JsValue> {
        Ok(WasmModelAlloy {
            alloy: self
                .registry
                .resolve_alloy(
                    from_js::<AlloySpec>(spec)?,
                    from_js::<ModelFeatures>(required)?,
                )
                .map_err(error)?,
            limits: self.limits,
        })
    }
}

/// Immutable alloy policy; selection uses the durable turn key, never a local counter.
#[wasm_bindgen]
pub struct WasmModelAlloy {
    alloy: ResolvedAlloy,
    limits: Limits,
}

#[wasm_bindgen]
impl WasmModelAlloy {
    #[wasm_bindgen(getter, unchecked_return_type = "PublicModelCapacity")]
    pub fn capacity(&self) -> Result<JsValue, JsValue> {
        let capacity = self.alloy.capacity();
        to_js(
            &serde_json::json!({"contextTokens":capacity.context_tokens,"outputTokens":capacity.output_tokens}),
        )
    }

    /// Reuse all three fields on resume and on tool follow-ups.
    pub fn select(
        &self,
        operation_id: &str,
        ordinal: u64,
        role: &str,
    ) -> Result<WasmModelProvider, JsValue> {
        let operation_id = acyclic_harness::OperationId::parse(operation_id).map_err(error)?;
        Ok(WasmModelProvider {
            provider: self
                .alloy
                .select(AlloyTurn {
                    operation_id,
                    ordinal,
                    role,
                })
                .clone(),
            limits: self.limits,
        })
    }
}

/// Immutable selection implementing the public TypeScript model provider contract.
#[wasm_bindgen]
pub struct WasmModelProvider {
    provider: Arc<ResolvedModel>,
    limits: Limits,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Dispatch {
    operation_id: acyclic_harness::OperationId,
    step: u32,
}

#[wasm_bindgen]
impl WasmModelProvider {
    #[wasm_bindgen(getter, unchecked_return_type = "PublicModel")]
    pub fn model(&self) -> Result<JsValue, JsValue> {
        to_js_json(self.provider.model())
    }

    #[wasm_bindgen(js_name = contextCapacity, unchecked_return_type = "PublicModelCapacity")]
    pub fn context_capacity(
        &self,
        #[wasm_bindgen(unchecked_param_type = "PublicModel")] model: JsValue,
    ) -> Result<JsValue, JsValue> {
        let capacity = self
            .provider
            .context_capacity(&from_js::<Model>(model)?)
            .map_err(error)?;
        to_js(&serde_json::json!({
            "contextTokens": capacity.context_tokens, "outputTokens": capacity.output_tokens,
        }))
    }

    /// Canonical admission, verified file reads and projection are request-scoped.
    pub async fn prepare(
        &self,
        #[wasm_bindgen(unchecked_param_type = "PublicProviderRequest")] request: JsValue,
    ) -> Result<WasmPreparedModelRequest, JsValue> {
        let admitted = self.decode(&request)?;
        let signal = Reflect::get(&request, &"signal".into())?;
        let prepared = cancel::run(
            async {
                crate::prepared::PreparedInput::new(&self.provider, admitted)
                    .await
                    .map_err(error)
            },
            signal.clone(),
        )
        .await?;
        Ok(WasmPreparedModelRequest {
            provider: self.provider.clone(),
            prepared: Rc::new(prepared),
            signal,
        })
    }

    /// Streaming APIs cannot recover an interrupted attempt without durable provider storage.
    #[wasm_bindgen(unchecked_return_type = "Promise<readonly PublicModelEvent[] | undefined>")]
    pub fn reconcile(&self, _attempt: JsValue) -> js_sys::Promise {
        js_sys::Promise::resolve(&JsValue::UNDEFINED)
    }
}

impl WasmModelProvider {
    fn decode(&self, request: &JsValue) -> Result<PreparedModelRequest, JsValue> {
        let model: Model = from_js(Reflect::get(request, &"model".into())?)?;
        self.provider.validate(&model).map_err(error)?;
        let bytes = Reflect::get(request, &"serializedInput".into())?
            .dyn_into::<Uint8Array>()
            .map_err(|_| error("serializedInput must be a Uint8Array"))?
            .to_vec();
        let prepared = PreparedModelRequest::decode(&bytes, self.limits, None).map_err(error)?;
        self.provider
            .validate(&prepared.request().model)
            .map_err(error)?;
        Ok(prepared)
    }
}

/// Immutable preparation owned by one request; no registry/global cache.
#[wasm_bindgen]
pub struct WasmPreparedModelRequest {
    provider: Arc<ResolvedModel>,
    prepared: Rc<crate::prepared::PreparedInput>,
    signal: JsValue,
}

#[wasm_bindgen]
impl WasmPreparedModelRequest {
    #[wasm_bindgen(js_name = countTokens, unchecked_return_type = "PublicModelTokenCount")]
    pub async fn count_tokens(&self) -> Result<JsValue, JsValue> {
        let count = cancel::run(
            async { self.prepared.count(&self.provider).await.map_err(error) },
            self.signal.clone(),
        )
        .await?;
        let result = js_sys::Object::new();
        Reflect::set(
            &result,
            &"requestDigest".into(),
            &Uint8Array::from(count.request_digest.as_slice()),
        )?;
        Reflect::set(
            &result,
            &"inputTokens".into(),
            &js_sys::BigInt::from(count.input_tokens),
        )?;
        Reflect::set(&result, &"fixedTokens".into(), &count.fixed_tokens.into())?;
        Reflect::set(
            &result,
            &"messageTokens".into(),
            &to_js(&count.message_tokens)?,
        )?;
        Ok(result.into())
    }

    #[wasm_bindgen(unchecked_return_type = "AsyncIterable<PublicModelEvent>")]
    pub fn generate(
        &self,
        #[wasm_bindgen(unchecked_param_type = "PublicModelGeneration")] dispatch: JsValue,
    ) -> Result<JsValue, JsValue> {
        let dispatch: Dispatch = from_js(dispatch)?;
        let dispatch = ModelDispatch {
            operation_id: dispatch.operation_id,
            step: dispatch.step,
            request_digest: self.prepared.request.request_digest(),
        };
        Ok(stream::iterator(
            self.provider.clone(),
            self.prepared.clone(),
            dispatch,
            self.signal.clone(),
        ))
    }
}
