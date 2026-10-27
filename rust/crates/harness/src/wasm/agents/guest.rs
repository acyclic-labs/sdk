//! The agent guest artifact's canonical entry: the same [`super::AgentHandler`]
//! the in-process browser host runs, behind the byte facade of
//! [`acyclic_actors::guest::wire`]. Its host (an `AgentHost::hosting` runtime,
//! or another scheduler over `acyclic_actors::local::Store`) delivers each
//! record through `onRecord`, and alone owns sealing, exact retry and
//! settlement of the transition it returns.

use super::host::Cancel;
use super::{
    BrowserHandler, WasmAgentProviders, WasmAgentRegistry, WasmBrowserAgentOptions, WasmReducer,
    from_js, js_error,
};
use crate::{
    Error,
    core::{Authority, Scope},
    filesystem::TurnAdmission,
};
use acyclic_actors::guest::{GUEST_ABI_VERSION, GuestError, GuestFuture, Handler as _, wire};
use js_sys::{Function, Promise, Uint8Array};
use serde::Deserialize;
use tsify::Tsify;
use wasm_bindgen::{JsCast as _, prelude::*};
use wasm_bindgen_futures::JsFuture;

/// The admission a guest's turns act under, given by whoever opens it.
#[derive(Deserialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WasmGuestAdmission {
    /// The opener holds `owner`'s custody and is the root of the tree it
    /// hosts: each agent's scope is a root grant.
    Root,
    /// The opener was admitted with `scope`, issued for `audience`: each
    /// agent's scope is delegated from it and grants no more.
    Admitted {
        /// The audience `scope` was issued for.
        audience: Authority,
        /// The opener's verified scope.
        scope: Scope,
    },
}

impl From<WasmGuestAdmission> for TurnAdmission {
    fn from(admission: WasmGuestAdmission) -> Self {
        match admission {
            WasmGuestAdmission::Root => Self::LocalRoot,
            WasmGuestAdmission::Admitted { audience, scope } => Self::Admitted { audience, scope },
        }
    }
}

/// One agent handler package, loaded in a guest.
#[wasm_bindgen]
pub struct AgentGuest {
    handler: BrowserHandler,
}

fn guest_error(error: GuestError) -> JsValue {
    js_error(Error::Invalid(error.to_string()))
}

#[wasm_bindgen]
impl AgentGuest {
    /// Opens the guest over the storage its host injects: the guest owns
    /// none. `options` carries the provider identity and limits, `owner` the
    /// agents' signing custody, `registry` the turns' model and tools, and
    /// `admission` the authority its turns act under: an admitted opener's
    /// own scope, or the root only for the opener that is the tree's root.
    /// There is no default.
    pub fn open(
        providers: &WasmAgentProviders,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
        owner: &WasmReducer,
        registry: &WasmAgentRegistry,
        #[wasm_bindgen(unchecked_param_type = "WasmGuestAdmission")] admission: JsValue,
    ) -> Result<AgentGuest, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        let admission: WasmGuestAdmission = from_js(admission)?;
        Ok(Self {
            handler: providers
                .providers
                .handler(&options, owner, registry, &admission.into())?,
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
        wire::encode_manifest(self.handler.manifest())
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
    ) -> Result<Vec<u8>, JsValue> {
        let (delivery, checkpoint) = wire::decode_delivery(&delivery).map_err(guest_error)?;
        let (turn, handle) =
            futures::future::abortable(self.handler.on_record(delivery, checkpoint.as_ref()));
        let _listening = Listening::abort(&signal, move || handle.abort())?;
        let transition = turn
            .await
            .map_err(|_| guest_error(GuestError::Handler("delivery was aborted".into())))?
            .map_err(guest_error)?;
        wire::encode_transition(&transition).map_err(guest_error)
    }
}

/// An `abort` listener on a host's signal, removed when dropped.
struct Listening {
    signal: JsValue,
    listener: Closure<dyn FnMut()>,
}

impl Listening {
    fn abort(signal: &JsValue, abort: impl FnMut() + 'static) -> Result<Option<Self>, JsValue> {
        if signal.is_undefined() {
            return Ok(None);
        }
        if js_sys::Reflect::get(signal, &JsValue::from_str("aborted"))?.is_truthy() {
            return Err(guest_error(GuestError::Handler(
                "delivery was aborted".into(),
            )));
        }
        let listening = Self {
            signal: signal.clone(),
            listener: Closure::new(abort),
        };
        listening.method("addEventListener")?;
        Ok(Some(listening))
    }

    fn method(&self, name: &str) -> Result<(), JsValue> {
        js_sys::Reflect::get(&self.signal, &JsValue::from_str(name))?
            .dyn_into::<Function>()?
            .call2(
                &self.signal,
                &JsValue::from_str("abort"),
                self.listener.as_ref(),
            )
            .map(drop)
    }
}

impl Drop for Listening {
    fn drop(&mut self) {
        let _ = self.method("removeEventListener");
    }
}

fn rejected(error: JsValue) -> GuestError {
    GuestError::Handler(format!("agent guest failed: {error:?}"))
}

/// The host's call of a guest's `onRecord`, for [`wire::Remote`]. Dropping
/// the call (the runtime's handler timeout) aborts the guest's turn.
pub(super) fn call(on_record: Function) -> impl Fn(Vec<u8>) -> GuestFuture<'static, Vec<u8>> {
    move |delivery| {
        let started = Cancel::new()
            .map_err(|error| GuestError::Handler(error.into()))
            .and_then(|cancel| {
                let promise = on_record
                    .call2(
                        &JsValue::UNDEFINED,
                        &Uint8Array::from(delivery.as_slice()),
                        &cancel.signal,
                    )
                    .map_err(rejected)?;
                Ok((cancel, promise))
            });
        Box::pin(async move {
            let (_cancel, promise) = started?;
            let value = JsFuture::from(Promise::resolve(&promise))
                .await
                .map_err(rejected)?;
            let view: Uint8Array = value.dyn_into().map_err(rejected)?;
            super::super::bytes::snapshot(&view, |_| Ok(()))
                .map(|(bytes, ())| bytes)
                .map_err(rejected)
        })
    }
}
