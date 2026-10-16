mod cancel;
mod content;
mod stream;
#[cfg(test)]
mod tests;

use crate::{BrowserTransport, Credentials, ProviderSpec, Registry, ResolvedModel};
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

/// Arbitrary model JSON retains every 64-bit integer across the JS boundary.
fn to_js_json(value: &impl Serialize) -> Result<JsValue, JsValue> {
    value
        .serialize(
            &serde_wasm_bindgen::Serializer::json_compatible()
                .serialize_large_number_types_as_bigints(true),
        )
        .map_err(error)
}

#[wasm_bindgen(typescript_custom_section)]
const TYPES: &str = r#"
export type ProviderModel = Readonly<Omit<Model, 'options'> & { options: unknown }>;
export interface ProviderRequest {
  readonly model: ProviderModel;
  readonly serializedInput: Uint8Array;
  readonly signal?: AbortSignal;
}
export interface ProviderDispatch { readonly operationId: string; readonly step: number }
export interface ProviderCapacity { readonly contextTokens: number; readonly outputTokens: number }
export interface ProviderTokenCount {
  readonly requestDigest: Uint8Array;
  readonly inputTokens: bigint;
  readonly fixedTokens: number;
  readonly messageTokens: readonly number[];
}
type PublicProviderEvent<Event> =
  Event extends Readonly<{kind: 'tool_call'}>
    ? Readonly<Omit<Event, 'call_id' | 'arguments'> & {callId: string; arguments: unknown}>
    : Event extends Readonly<{kind: 'completed'}>
      ? Readonly<Omit<Event, 'metadata'> & {metadata: unknown}> : Readonly<Event>;
export type ProviderEvent = PublicProviderEvent<ModelEvent>;
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
        #[wasm_bindgen(unchecked_optional_param_type = "typeof globalThis.fetch")] fetch: Option<
            Function,
        >,
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
            registry =
                registry.with_content(Arc::new(content::Reader(reader)), limits.render_bytes);
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
            Some(options) => from_js(options)?,
        };
        Ok(WasmModelProvider {
            provider: Rc::new(
                self.registry
                    .resolve_with_options(selection, options)
                    .map_err(error)?,
            ),
            limits: self.limits,
        })
    }
}

/// Immutable selection implementing the public TypeScript model provider contract.
#[wasm_bindgen]
pub struct WasmModelProvider {
    provider: Rc<ResolvedModel>,
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
    #[wasm_bindgen(getter, unchecked_return_type = "ProviderModel")]
    pub fn model(&self) -> Result<JsValue, JsValue> {
        to_js_json(self.provider.model())
    }

    #[wasm_bindgen(js_name = contextCapacity, unchecked_return_type = "ProviderCapacity")]
    pub fn context_capacity(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ProviderModel")] model: JsValue,
    ) -> Result<JsValue, JsValue> {
        let capacity = self
            .provider
            .context_capacity(&from_js::<Model>(model)?)
            .map_err(error)?;
        to_js(&serde_json::json!({
            "contextTokens": capacity.context_tokens, "outputTokens": capacity.output_tokens,
        }))
    }

    #[wasm_bindgen(js_name = countTokens, unchecked_return_type = "Promise<ProviderTokenCount>")]
    pub async fn count_tokens(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ProviderRequest")] request: JsValue,
    ) -> Result<JsValue, JsValue> {
        let prepared = self.prepare(&request)?;
        let signal = Reflect::get(&request, &"signal".into())?;
        let count = cancel::run(
            async { self.provider.count_tokens(&prepared).await.map_err(error) },
            signal,
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

    #[wasm_bindgen(unchecked_return_type = "AsyncIterable<ProviderEvent>")]
    pub fn generate(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ProviderRequest")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ProviderDispatch")] dispatch: JsValue,
    ) -> Result<JsValue, JsValue> {
        let prepared = self.prepare(&request)?;
        let dispatch: Dispatch = from_js(dispatch)?;
        let dispatch = ModelDispatch {
            operation_id: dispatch.operation_id,
            step: dispatch.step,
            request_digest: prepared.request_digest(),
        };
        let signal = Reflect::get(&request, &"signal".into())?;
        Ok(stream::iterator(
            self.provider.clone(),
            prepared,
            dispatch,
            signal,
        ))
    }

    /// Streaming APIs cannot recover an interrupted attempt without durable provider storage.
    #[wasm_bindgen(unchecked_return_type = "Promise<readonly ProviderEvent[] | undefined>")]
    pub fn reconcile(&self, _attempt: JsValue) -> js_sys::Promise {
        js_sys::Promise::resolve(&JsValue::UNDEFINED)
    }
}

impl WasmModelProvider {
    fn prepare(&self, request: &JsValue) -> Result<PreparedModelRequest, JsValue> {
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
