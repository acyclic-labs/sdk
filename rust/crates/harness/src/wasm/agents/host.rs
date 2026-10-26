//! Trusted JavaScript model and tool callbacks behind the stock executor's
//! provider and tool contracts.

use super::{Error, Result, from_js, to_js};
use crate::{
    context::{ModelContextCapacity, ModelTokenCount},
    conversation::{ContentResidencyVerifier, verified_content_bytes},
    model::{
        FileProjectionPolicy, Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest,
    },
    tool::{ToolExecutor, ToolInvocation, ToolProjection, ToolResult},
};
use js_sys::{Function, Promise};
use serde_json::Value;
use std::sync::Arc;
use wasm_bindgen::{JsCast as _, prelude::*};
use wasm_bindgen_futures::JsFuture;

/// Trusted host effects are explicitly composed; rejection is an uncertain
/// provider result, so the existing runner retains its reconciliation journal.
pub(super) struct HostTool {
    pub(super) execute: Function,
    pub(super) reconcile: Function,
    pub(super) project: Function,
}
impl HostTool {
    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        callback: &Function,
        invocation: ToolInvocation,
    ) -> Result<T> {
        let input = to_js(&invocation)
            .map_err(|_| Error::Invalid("tool invocation encoding failed".into()))?;
        let result = callback
            .call1(&JsValue::UNDEFINED, &input)
            .map_err(|_| Error::Storage("host tool result is uncertain".into()))?;
        let result = JsFuture::from(Promise::resolve(&result))
            .await
            .map_err(|_| Error::Storage("host tool result is uncertain".into()))?;
        // The callback already ran; a malformed receipt cannot establish that
        // execution was rejected. Retain the original attempt for reconciliation.
        from_js(result).map_err(|_| Error::Storage("host tool result is uncertain".into()))
    }
}
impl ToolExecutor for HostTool {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.call(&self.execute, invocation))
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(self.call(&self.reconcile, invocation))
    }
}
impl ToolProjection for HostTool {
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        _work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        let invocation = to_js(invocation)
            .map_err(|_| Error::Invalid("tool projection invocation encoding failed".into()))?;
        let result = to_js(result)
            .map_err(|_| Error::Invalid("tool projection result encoding failed".into()))?;
        let value = self
            .project
            .call2(&JsValue::UNDEFINED, &invocation, &result)
            .map_err(|_| Error::Invalid("tool projection failed".into()))?;
        from_js(value)
            .map(|value| (value, None))
            .map_err(|_| Error::Invalid("tool projection is invalid".into()))
    }
}

/// A trusted JavaScript model. Its `generate` receives the canonical request
/// bytes, then the verified bytes of each file the request projects in full, in
/// request order, read through the turn's authenticated reader.
#[derive(Clone)]
pub(super) struct HostModel {
    pub(super) generate: Function,
    pub(super) reconcile: Function,
    pub(super) capacity: Option<Function>,
    pub(super) count_tokens: Option<Function>,
    pub(super) content: Option<(Arc<dyn ContentResidencyVerifier>, u64)>,
}

/// An `AbortController` whose signal a host callback receives, aborted when
/// the Rust side drops the call: the one cancellation of a JS host call.
pub(super) struct Cancel {
    controller: JsValue,
    abort: Function,
    pub(super) signal: JsValue,
}

impl Cancel {
    pub(super) fn new() -> std::result::Result<Self, &'static str> {
        let constructor =
            js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("AbortController"))
                .ok()
                .and_then(|value| value.dyn_into::<Function>().ok())
                .ok_or("cancellation provider is unavailable")?;
        let controller = js_sys::Reflect::construct(&constructor, &js_sys::Array::new())
            .map_err(|_| "cancellation provider failed")?;
        let abort = js_sys::Reflect::get(&controller, &JsValue::from_str("abort"))
            .ok()
            .and_then(|value| value.dyn_into::<Function>().ok())
            .ok_or("cancellation provider is invalid")?;
        let signal = js_sys::Reflect::get(&controller, &JsValue::from_str("signal"))
            .map_err(|_| "cancellation signal is unavailable")?;
        Ok(Self {
            controller,
            abort,
            signal,
        })
    }
}

impl Drop for Cancel {
    fn drop(&mut self) {
        let _ = self.abort.call0(&self.controller);
    }
}

struct HostModelIterator {
    iterator: JsValue,
    cancel: Cancel,
}
impl Drop for HostModelIterator {
    fn drop(&mut self) {
        let _ = self.cancel.abort.call0(&self.cancel.controller);
        if let Ok(value) = js_sys::Reflect::get(&self.iterator, &JsValue::from_str("return"))
            && let Ok(close) = value.dyn_into::<Function>()
        {
            let _ = close.call0(&self.iterator);
        }
    }
}

impl HostModel {
    /// Verified bytes of every non-reference file part, in request order.
    async fn files(&self, request: &PreparedModelRequest) -> Result<js_sys::Array> {
        let files = js_sys::Array::new();
        for message in &request.request().messages {
            for (file, policy) in message.content.file_parts() {
                if matches!(policy, FileProjectionPolicy::Reference) {
                    continue;
                }
                let (reader, maximum) = self.content.as_ref().ok_or_else(|| {
                    Error::Unsupported("model file byte resolution is unavailable".into())
                })?;
                let (bytes, _hold) =
                    verified_content_bytes(reader.as_ref(), file, *maximum, None, None).await?;
                files.push(&js_sys::Uint8Array::from(bytes.as_slice()));
            }
        }
        Ok(files)
    }

    async fn start(
        &self,
        request: &PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> Result<HostModelIterator> {
        let files = self.files(request).await?;
        let cancel =
            Cancel::new().map_err(|error| Error::Unsupported(format!("browser model {error}")))?;
        let signal = cancel.signal.clone();
        let mut attempt = HostModelIterator {
            iterator: JsValue::UNDEFINED,
            cancel,
        };
        let arguments = js_sys::Array::new();
        arguments.push(&js_sys::Uint8Array::from(request.bytes()));
        arguments.push(
            &to_js(&dispatch)
                .map_err(|_| Error::Invalid("model dispatch encoding failed".into()))?,
        );
        arguments.push(&signal);
        arguments.push(&files);
        attempt.iterator = self
            .generate
            .apply(&JsValue::UNDEFINED, &arguments)
            .map_err(|_| Error::Storage("host model dispatch is uncertain".into()))?;
        Ok(attempt)
    }
}

impl ModelProvider for HostModel {
    fn bind_content(
        &self,
        reader: Arc<dyn ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        Some(Arc::new(Self {
            content: Some((reader, maximum_bytes)),
            ..self.clone()
        }))
    }

    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        let callback = self.capacity.as_ref().ok_or_else(|| {
            Error::Unsupported("selected model context capacity is unavailable".into())
        })?;
        let model = to_js(model)
            .map_err(|_| Error::Invalid("model capacity selection encoding failed".into()))?;
        let value = callback
            .call1(&JsValue::UNDEFINED, &model)
            .map_err(|_| Error::Invalid("model capacity callback failed".into()))?;
        let capacity: ModelContextCapacity = from_js(value)
            .map_err(|_| Error::Invalid("model capacity callback returned invalid data".into()))?;
        capacity.validate()?;
        Ok(capacity)
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            let callback = self.count_tokens.as_ref().ok_or_else(|| {
                Error::Unsupported("selected model token counting is unavailable".into())
            })?;
            let digest = to_js(&request.request_digest())
                .map_err(|_| Error::Invalid("model counting digest encoding failed".into()))?;
            let value = callback
                .call2(
                    &JsValue::UNDEFINED,
                    &js_sys::Uint8Array::from(request.bytes()),
                    &digest,
                )
                .map_err(|_| Error::Invalid("model token counting callback failed".into()))?;
            let value = JsFuture::from(Promise::resolve(&value))
                .await
                .map_err(|_| Error::Storage("model token counting callback failed".into()))?;
            let count: ModelTokenCount = from_js(value).map_err(|_| {
                Error::Invalid("model counting callback returned invalid data".into())
            })?;
            count.validate(request, &mut None)?;
            Ok(count)
        })
    }

    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        // Pull exactly one event per poll. The stock executor owns admission,
        // event limits, cancellation, request identity and durable consumption.
        Box::pin(futures::stream::try_unfold(
            None,
            move |iterator: Option<HostModelIterator>| async move {
                let iterator = match iterator {
                    Some(iterator) => iterator,
                    None => self.start(request, dispatch).await?,
                };
                let next = js_sys::Reflect::get(&iterator.iterator, &JsValue::from_str("next"))
                    .ok()
                    .and_then(|value| value.dyn_into::<Function>().ok())
                    .ok_or_else(|| {
                        Error::Invalid("host model must return an async iterator".into())
                    })?;
                let pending = next
                    .call0(&iterator.iterator)
                    .map_err(|_| Error::Storage("host model stream is uncertain".into()))?;
                let item = JsFuture::from(Promise::resolve(&pending))
                    .await
                    .map_err(|_| Error::Storage("host model stream is uncertain".into()))?;
                let done = js_sys::Reflect::get(&item, &JsValue::from_str("done"))
                    .map_err(|_| Error::Invalid("host model iterator result is invalid".into()))?;
                if done.as_bool() == Some(true) {
                    return Ok(None);
                }
                if done.as_bool() != Some(false) {
                    return Err(Error::Invalid(
                        "host model iterator completion flag is invalid".into(),
                    ));
                }
                let value = js_sys::Reflect::get(&item, &JsValue::from_str("value"))
                    .map_err(|_| Error::Invalid("host model event is missing".into()))?;
                let event = from_js(value)
                    .map_err(|_| Error::Invalid("host model event is invalid".into()))?;
                Ok(Some((event, Some(iterator))))
            },
        ))
    }

    fn reconcile<'a>(
        &'a self,
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            let attempt = to_js(&dispatch)
                .map_err(|_| Error::Invalid("model attempt encoding failed".into()))?;
            super::super::set_js_field(
                &attempt,
                "observed",
                &to_js(&observed)
                    .map_err(|_| Error::Invalid("model attempt encoding failed".into()))?,
            )
            .map_err(|_| Error::Invalid("model attempt encoding failed".into()))?;
            let pending = self
                .reconcile
                .call1(&JsValue::UNDEFINED, &attempt)
                .map_err(|_| Error::Storage("host model reconciliation is uncertain".into()))?;
            let result = JsFuture::from(Promise::resolve(&pending))
                .await
                .map_err(|_| Error::Storage("host model reconciliation is uncertain".into()))?;
            from_js(result)
                .map_err(|_| Error::Invalid("host model reconciliation is invalid".into()))
        })
    }
}

#[cfg(test)]
#[cfg(target_arch = "wasm32")]
#[path = "accounting_tests.rs"]
mod accounting_tests;
