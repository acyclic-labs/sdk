//! Thin event-owned Actors factory; no harness-agent or executor composition.
use acyclic_actors::{
    addressing::ActorReference,
    guest::wire,
    hosting::Host,
    local::{RuntimeLimits, Store, SystemClock},
};
use acyclic_stream::{StreamPath, StreamProvider, WasmStream};
use js_sys::Function;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// One platform-admitted actor running an arbitrary strict-wire Handler.
/// Storage, artifact and actor identity are supplied by the owning platform.
#[wasm_bindgen]
pub struct WasmActorHost {
    host: Host<dyn StreamProvider>,
}

fn failed(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!(
        "STEP actor-host failed: {error}; recovery: reopen the same actor/root/provider and retry its retained event"
    ))
}

#[wasm_bindgen]
impl WasmActorHost {
    /// Pure factory: no provider reads, creation, turn or background runtime.
    /// The owner must authenticate/admit the actor before opening this binding.
    /// `onRecord` implements the canonical Actors guest wire, not AgentGuest.
    pub fn open(
        stream: &WasmStream,
        root: &str,
        actor: &[u8],
        manifest: &[u8],
        #[wasm_bindgen(
            unchecked_param_type = "(delivery: Uint8Array, signal: AbortSignal, interrupt: AbortSignal) => Promise<Uint8Array>"
        )]
        on_record: Function,
        active: u32,
        work: u32,
    ) -> Result<WasmActorHost, JsValue> {
        let actor = ActorReference::decode(actor).map_err(failed)?;
        let root = StreamPath::new(root).map_err(failed)?;
        let manifest = wire::decode_manifest(manifest).map_err(failed)?;
        let limits = RuntimeLimits::new(active as usize, work as usize).map_err(failed)?;
        let handler = wire::Remote::new(manifest, super::guest::call(on_record));
        Ok(Self {
            host: Host::new(
                Store::new(stream.provider(), root),
                actor,
                Arc::new(handler),
                Arc::new(SystemClock),
                limits,
            ),
        })
    }

    /// Commit a durable pause fence without waiting for the running guest.
    /// Resume does not execute input until the platform routes a wake.
    pub async fn pause(&self, paused: bool) -> Result<(), JsValue> {
        self.host.pause(paused).await.map_err(failed)
    }

    /// The original state/subscription routing paths, without delivering a turn.
    #[wasm_bindgen(js_name = wakePaths)]
    pub async fn wake_paths(&self) -> Result<Vec<String>, JsValue> {
        Ok(self
            .host
            .wake_paths()
            .await
            .map_err(failed)?
            .into_iter()
            .map(|path| path.to_string())
            .collect())
    }

    /// Complete one platform event. Persist the returned native alarm before
    /// returning; retain recovery wake custody if the call fails or is aborted.
    pub async fn wake(&self) -> Result<Option<u64>, JsValue> {
        self.host.wake().await.map_err(failed)
    }
}
