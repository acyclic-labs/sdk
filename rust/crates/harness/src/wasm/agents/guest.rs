//! The agent guest artifact's canonical entry: the same [`super::AgentHandler`]
//! the in-process browser host runs, behind the byte facade of
//! [`acyclic_actors::guest::wire`]. Its host (an `AgentHost::hosting` runtime,
//! or another scheduler over `acyclic_actors::local::Store`) delivers each
//! record through `onRecord`, and alone owns sealing, exact retry and
//! settlement of the transition it returns.

use super::{
    WasmAgentAuthority, WasmAgentProviders, WasmAgentRegistry, WasmBrowserAgentOptions,
    WasmMachinesWorkspace, from_js, js_error,
};
use crate::Error;
use crate::wasm::cancel::{Cancel, Listening};
use acyclic_actors::guest::{
    CancellationToken, GUEST_ABI_VERSION, GuestError, GuestFuture, Handler as _, wire,
};
use js_sys::{Function, Promise, Uint8Array};
use wasm_bindgen::{JsCast as _, prelude::*};
use wasm_bindgen_futures::JsFuture;

/// One agent handler package, loaded in a guest.
#[wasm_bindgen]
pub struct AgentGuest {
    manifest: Vec<u8>,
    providers: WasmAgentProviders,
    options: WasmBrowserAgentOptions,
    owner: WasmAgentAuthority,
    registry: WasmAgentRegistry,
}

fn guest_error(error: GuestError) -> JsValue {
    js_error(Error::Invalid(error.to_string()))
}

#[wasm_bindgen]
impl AgentGuest {
    /// Opens over injected storage and the original platform-admitted owner.
    /// Request options cannot replace its caller or grant.
    pub fn open(
        providers: &WasmAgentProviders,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
        owner: &WasmAgentAuthority,
        registry: &WasmAgentRegistry,
    ) -> Result<AgentGuest, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        let handler =
            providers
                .providers
                .handler(&options, owner, registry, &owner.admission()?)?;
        Ok(Self {
            manifest: wire::encode_manifest(handler.manifest()),
            providers: WasmAgentProviders {
                providers: providers.providers.clone(),
            },
            options,
            owner: owner.clone(),
            registry: registry.clone(),
        })
    }

    /// The frozen guest ABI version, checked before any delivery.
    #[wasm_bindgen(js_name = abiVersion)]
    #[must_use]
    pub fn abi_version() -> u32 {
        GUEST_ABI_VERSION
    }

    /// Canonical manifest bytes: code digest, checkpoint codec,
    /// subscriptions, bindings and limits (handler timeout, memory and
    /// checkpoint bytes).
    #[must_use]
    pub fn manifest(&self) -> Vec<u8> {
        self.manifest.clone()
    }

    /// Runs one canonical delivery (with the actor's committed checkpoint)
    /// and returns its canonical transition. Nothing is published here; a
    /// repeated delivery may run again. Aborting `signal` (the host gave up,
    /// as at its handler timeout) drops the turn, cancelling its model call
    /// as a dropped in-process turn does.
    #[wasm_bindgen(js_name = onRecord)]
    pub async fn on_record(
        &self,
        delivery: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "AbortSignal | undefined")] signal: JsValue,
        #[wasm_bindgen(unchecked_param_type = "AbortSignal | undefined")] interrupt: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let (delivery, checkpoint) = wire::decode_delivery(&delivery).map_err(guest_error)?;
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let _interrupt = Listening::abort(&interrupt, move || token.cancel())?;
        // Admission, executor selection and the turn share this attempt's lifetime.
        // No admitted Machine is written back into the namespace registry.
        let (turn, handle) = futures::future::abortable(async {
            let mut registry = self.registry.clone();
            if let Some(source) = &self.registry.workspace_source {
                let workspace = WasmMachinesWorkspace::admit(
                    &self.owner,
                    &self.providers,
                    source,
                    delivery.request.get(),
                )
                .await?;
                registry.bind_machines_workspace(&workspace);
            }
            let handler = self.providers.providers.handler(
                &self.options,
                &self.owner,
                &registry,
                &self.owner.admission()?,
            )?;
            handler
                .on_record(delivery, checkpoint.as_ref(), cancellation)
                .await
                .map_err(guest_error)
        });
        let _listening = Listening::abort(&signal, move || handle.abort())?;
        let transition = turn
            .await
            .map_err(|_| guest_error(GuestError::Handler("delivery was aborted".into())))??;
        wire::encode_transition(&transition).map_err(guest_error)
    }
}

fn rejected(error: JsValue) -> GuestError {
    GuestError::Handler(format!("agent guest failed: {error:?}"))
}

/// The host's call of a guest's `onRecord`, for [`wire::Remote`]. Dropping
/// the call (the runtime's handler timeout) aborts the guest's turn.
pub(super) fn call(
    on_record: Function,
) -> impl Fn(Vec<u8>, CancellationToken) -> GuestFuture<'static, Vec<u8>> {
    move |delivery, cancellation| {
        let started = Cancel::new()
            .map_err(|error| GuestError::Handler(error.into()))
            .and_then(|cancel| {
                let interrupt = Cancel::new().map_err(|error| GuestError::Handler(error.into()))?;
                let promise = on_record
                    .call3(
                        &JsValue::UNDEFINED,
                        &Uint8Array::from(delivery.as_slice()),
                        &cancel.signal,
                        &interrupt.signal,
                    )
                    .map_err(rejected)?;
                Ok((cancel, interrupt, promise))
            });
        Box::pin(async move {
            let (_cancel, interrupt, promise) = started?;
            let reply = JsFuture::from(Promise::resolve(&promise));
            let reply = core::pin::pin!(reply);
            let stopped = cancellation.cancelled();
            let stopped = core::pin::pin!(stopped);
            let value = match futures::future::select(reply, stopped).await {
                futures::future::Either::Left((value, _)) => value,
                futures::future::Either::Right(((), reply)) => {
                    drop(interrupt);
                    reply.await
                }
            }
            .map_err(rejected)?;
            let view: Uint8Array = value.dyn_into().map_err(rejected)?;
            acyclic_stream::javascript_bytes::snapshot(&view, |_| Ok(()))
                .map(|(bytes, ())| bytes)
                .map_err(rejected)
        })
    }
}
