//! Browser network callbacks feed the portable Rust HTTP protocol owner.

use super::super::{from_js, js_error, to_js};
use crate::{
    Error, OperationId, Result,
    mcp::{
        McpToolTransport,
        http::{HttpMcpTransport, McpHttpProvider, McpHttpRequest, McpHttpResponse},
    },
};
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use js_sys::{Function, Promise, Reflect, Uint8Array};
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc};
use tsify::Tsify;
use wasm_bindgen::{JsCast as _, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
struct McpBrowserResponseHead {
    status: u16,
    #[tsify(type = "Record<string,string>")]
    headers: BTreeMap<String, String>,
}

#[wasm_bindgen(typescript_custom_section)]
const PROVIDER: &'static str = r#"
/** A host-owned bounded HTTP exchange; cancellation must synchronously abort I/O. */
export interface McpBrowserExchange {
    readonly response: Promise<McpBrowserResponseHead>;
    readonly read: () => Promise<Uint8Array | null>;
    readonly cancel: () => void;
}
/** Platform I/O only. Do not retry requests or follow redirects. */
export type McpBrowserHttpProvider = (operation: string, request: McpHttpRequest) => McpBrowserExchange;
"#;

struct BrowserProvider(Function);
struct ExchangeBody {
    receiver: JsValue,
    read: Function,
    cancel: Function,
    operation: OperationId,
    maximum_bytes: u32,
    received: u64,
}

impl Drop for ExchangeBody {
    fn drop(&mut self) {
        let _ = self.cancel.call0(&self.receiver);
    }
}

impl McpHttpProvider for BrowserProvider {
    fn exchange<'a>(
        &'a self,
        operation: OperationId,
        request: McpHttpRequest,
    ) -> BoxProviderFuture<'a, Result<McpHttpResponse>> {
        Box::pin(async move {
            let query = to_js(&request)
                .map_err(|_| Error::Invalid("MCP browser request serialization failed".into()))?;
            let receiver = self
                .0
                .call2(
                    &JsValue::NULL,
                    &JsValue::from_str(&operation.to_string()),
                    &query,
                )
                .map_err(|_| Error::Indeterminate(operation))?;
            let function = |name: &str| {
                Reflect::get(&receiver, &JsValue::from_str(name))
                    .map_err(|_| Error::Indeterminate(operation))?
                    .dyn_into::<Function>()
                    .map_err(|_| Error::Invalid(format!("MCP browser exchange needs {name}")))
            };
            let cancel = function("cancel")?;
            let read = match function("read") {
                Ok(read) => read,
                Err(error) => {
                    let _ = cancel.call0(&receiver);
                    return Err(error);
                }
            };
            let body = ExchangeBody {
                receiver,
                read,
                cancel,
                operation,
                maximum_bytes: request.maximum_response_bytes,
                received: 0,
            };
            let response = Reflect::get(&body.receiver, &JsValue::from_str("response"))
                .map_err(|_| Error::Indeterminate(operation))?;
            let head = JsFuture::from(Promise::resolve(&response))
                .await
                .map_err(|_| Error::Indeterminate(operation))?;
            let head: McpBrowserResponseHead = from_js(head)
                .map_err(|_| Error::Invalid("MCP browser response head invalid".into()))?;
            let stream = futures::stream::try_unfold(body, |mut body| async move {
                let chunk = body
                    .read
                    .call0(&body.receiver)
                    .map_err(|_| Error::Indeterminate(body.operation))?;
                let chunk = JsFuture::from(Promise::resolve(&chunk))
                    .await
                    .map_err(|_| Error::Indeterminate(body.operation))?;
                if chunk.is_null() {
                    return Ok(None);
                }
                let bytes: Uint8Array = chunk
                    .dyn_into()
                    .map_err(|_| Error::Invalid("MCP browser body needs Uint8Array".into()))?;
                body.received = body.received.saturating_add(u64::from(bytes.length()));
                if body.received > u64::from(body.maximum_bytes) {
                    return Err(Error::Indeterminate(body.operation));
                }
                Ok(Some((Bytes::from(bytes.to_vec()), body)))
            });
            Ok(McpHttpResponse {
                status: head.status,
                headers: head.headers,
                body: Box::pin(stream),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpHttpResponse>>> {
        Box::pin(async { Ok(None) })
    }
}

/// Low-level explicitly bound transport for a host's admitted tool executor.
/// The existing Harness tool/task journal owns admission and result retention.
#[wasm_bindgen]
pub struct WasmMcpHttpTransport(HttpMcpTransport);

#[wasm_bindgen]
impl WasmMcpHttpTransport {
    /// Binds an initialized session and bounded platform I/O without effects.
    #[wasm_bindgen(constructor)]
    pub fn new(
        #[wasm_bindgen(unchecked_param_type = "McpBrowserHttpProvider")] provider: Function,
        endpoint: String,
        session: Option<String>,
        maximum_bytes: u32,
        timeout_ms: u32,
    ) -> std::result::Result<Self, JsValue> {
        HttpMcpTransport::new(
            Arc::new(BrowserProvider(provider)),
            endpoint,
            session,
            maximum_bytes,
            timeout_ms,
        )
        .map(Self)
        .map_err(js_error)
    }
    /// Calls an already admitted operation. Canonical JSON bytes preserve
    /// full-width remote numbers and avoid a second JavaScript result engine.
    #[wasm_bindgen(js_name=callJson)]
    pub async fn call_json(
        &self,
        operation: String,
        name: String,
        #[wasm_bindgen(unchecked_param_type = "WasmToolJsonValue")] arguments: JsValue,
    ) -> std::result::Result<Vec<u8>, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        let result = self
            .0
            .call(operation, &name, from_js(arguments)?)
            .await
            .map_err(js_error)?;
        serde_json::to_vec(&result).map_err(|error| JsValue::from_str(&error.to_string()))
    }
    /// Queries the transport's explicit receipt capability without network I/O.
    /// This HTTP provider has none; the owning journal retains known results.
    #[wasm_bindgen(js_name=reconcileJson)]
    pub async fn reconcile_json(
        &self,
        operation: String,
    ) -> std::result::Result<Option<Vec<u8>>, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        self.0
            .reconcile(operation)
            .await
            .map_err(js_error)?
            .map(|result| {
                serde_json::to_vec(&result).map_err(|error| JsValue::from_str(&error.to_string()))
            })
            .transpose()
    }
}
