//! The agent guest artifact's canonical entry: the same [`super::AgentHandler`]
//! the in-process browser host runs, behind the byte facade of
//! [`acyclic_actors::guest::wire`]. Its host (an `AgentHost::hosting` runtime,
//! or another scheduler over `acyclic_actors::local::Store`) delivers each
//! record through `onRecord`, and alone owns sealing, exact retry and
//! settlement of the transition it returns.

use super::{
    BrowserHandler, Providers, WasmAgentRegistry, WasmBrowserAgentOptions, WasmReducer, from_js,
    js_error,
};
use crate::Error;
use acyclic_actors::guest::{GUEST_ABI_VERSION, GuestError, GuestFuture, Handler as _, wire};
use js_sys::{Function, Promise, Uint8Array};
use wasm_bindgen::{JsCast as _, prelude::*};
use wasm_bindgen_futures::JsFuture;

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
    /// Opens the guest over the providers `options` names; `owner` holds the
    /// agents' signing custody and `registry` the turns' model and tools.
    pub async fn open(
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
        owner: &WasmReducer,
        registry: &WasmAgentRegistry,
    ) -> Result<AgentGuest, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        let providers = Providers::open(&options).await?;
        Ok(Self {
            handler: providers.handler(&options, owner, registry)?,
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
    /// repeated delivery may run again.
    #[wasm_bindgen(js_name = onRecord)]
    pub async fn on_record(&self, delivery: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let (delivery, checkpoint) = wire::decode_delivery(&delivery).map_err(guest_error)?;
        let transition = self
            .handler
            .on_record(delivery, checkpoint.as_ref())
            .await
            .map_err(guest_error)?;
        wire::encode_transition(&transition).map_err(guest_error)
    }
}

fn rejected(error: JsValue) -> GuestError {
    GuestError::Handler(format!("agent guest failed: {error:?}"))
}

/// The host's call of a guest's `onRecord`, for [`wire::Remote`].
pub(super) fn call(on_record: Function) -> impl Fn(Vec<u8>) -> GuestFuture<'static, Vec<u8>> {
    move |delivery| {
        let promise = on_record.call1(&JsValue::UNDEFINED, &Uint8Array::from(delivery.as_slice()));
        Box::pin(async move {
            let value = JsFuture::from(Promise::resolve(&promise.map_err(rejected)?))
                .await
                .map_err(rejected)?;
            let view: Uint8Array = value.dyn_into().map_err(rejected)?;
            super::super::bytes::snapshot(&view, |_| Ok(()))
                .map(|(bytes, ())| bytes)
                .map_err(rejected)
        })
    }
}
