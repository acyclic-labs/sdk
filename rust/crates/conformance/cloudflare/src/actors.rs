//! Platform controls around the unchanged public Actors verifier and package.
use acyclic_actors::{
    addressing::ActorReference,
    conformance::{Backend, Package},
    guest::{CancellationToken, Handler, wire},
    local::{Clock, Error, Store, SystemClock},
};
use acyclic_stream::{
    BoxProviderFuture, JournalStream, JournalStreamLimits, ReadRequest, StreamError, StreamPath,
    StreamProvider, WasmStream,
    testing::{Counted, RequestClass, RequestLedger},
};
use futures::TryStreamExt;
use js_sys::{Function, Promise, Uint8Array};
use std::sync::Arc;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

fn failed(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!(
        "STEP hosted-actors failed: {error}; recovery: bash scripts/qualify-cloudflare.sh"
    ))
}

/// The suite and every remote owner dispatch through this ONE counted journal.
#[wasm_bindgen]
pub struct ActorsDomain {
    store: Store<Counted<JournalStream>>,
    clock: SystemClock,
    pause: Function,
    restart: Function,
}

#[wasm_bindgen]
impl ActorsDomain {
    pub async fn open(
        journal: JsValue,
        pause: Function,
        restart: Function,
    ) -> Result<Self, JsValue> {
        let provider = JournalStream::open(journal, JournalStreamLimits::default())
            .await
            .map_err(failed)?;
        Ok(Self {
            store: Store::new(
                Arc::new(Counted::new(provider)),
                StreamPath::new("actors/state").map_err(failed)?,
            ),
            clock: SystemClock,
            pause,
            restart,
        })
    }

    /// Only canonical byte RPC crosses a DO/module boundary, never this object.
    pub fn provider(&self) -> WasmStream {
        WasmStream::from_provider(self.store.provider().clone())
    }

    /// Incremental discovery uses the ORIGINAL Store root activation records.
    /// The platform persists the returned cursor after admitting every address.
    pub async fn created(&self, from: u64) -> Result<JsValue, JsValue> {
        let records = match self
            .store
            .provider()
            .read(ReadRequest {
                path: self.store.root().clone(),
                from,
                limit: 256,
            })
            .await
        {
            Ok(records) => records.try_collect::<Vec<_>>().await.map_err(failed)?,
            Err(StreamError::NotFound) => Vec::new(),
            Err(error) => return Err(failed(error)),
        };
        let next = records.last().map_or(from, |record| record.sequence + 1);
        let full = records.len() == 256;
        let actors = js_sys::Array::new();
        for record in records {
            if let Ok(actor) = ActorReference::decode(&record.value) {
                actors.push(&Uint8Array::from(actor.encode().as_slice()));
            }
        }
        let result = js_sys::Object::new();
        js_sys::Reflect::set(
            &result,
            &"next".into(),
            &JsValue::from(js_sys::BigInt::from(next)),
        )?;
        js_sys::Reflect::set(&result, &"actors".into(), &actors)?;
        js_sys::Reflect::set(&result, &"full".into(), &full.into())?;
        Ok(result.into())
    }

    pub async fn verify(&self) -> Result<(), JsValue> {
        acyclic_actors::conformance::verify(self)
            .await
            .map_err(failed)
    }

    pub async fn bench(&self) -> Result<JsValue, JsValue> {
        let sample = acyclic_actors::conformance::bench(self)
            .await
            .map_err(failed)?;
        let classes = [
            RequestClass::Data,
            RequestClass::Metadata,
            RequestClass::List,
            RequestClass::Head,
            RequestClass::Lease,
            RequestClass::Token,
        ]
        .map(|class| sample.requests.class(class));
        serde_wasm_bindgen::to_value(&serde_json::json!({"deliveries": sample.deliveries, "elapsed_ms": sample.elapsed.as_millis(), "requests": classes})).map_err(failed)
    }
}

async fn control(result: Result<JsValue, JsValue>) -> Result<(), Error> {
    JsFuture::from(Promise::resolve(&result.map_err(|error| {
        Error::Handler(acyclic_actors::guest::GuestError::Handler(format!(
            "platform control: {error:?}"
        )))
    })?))
    .await
    .map_err(|error| {
        Error::Handler(acyclic_actors::guest::GuestError::Handler(format!(
            "platform control: {error:?}"
        )))
    })?;
    Ok(())
}

impl Backend for ActorsDomain {
    type Provider = Counted<JournalStream>;
    fn requests(&self) -> RequestLedger {
        self.store.provider().ledger()
    }
    fn store(&self) -> &Store<Self::Provider> {
        &self.store
    }
    fn clock(&self) -> &dyn Clock {
        &self.clock
    }
    fn pause<'a>(
        &'a self,
        actor: &'a ActorReference,
        paused: bool,
    ) -> BoxProviderFuture<'a, Result<(), Error>> {
        Box::pin(control(self.pause.call2(
            &JsValue::UNDEFINED,
            &Uint8Array::from(actor.encode().as_slice()),
            &paused.into(),
        )))
    }
    fn restart(&self) -> BoxProviderFuture<'_, Result<(), Error>> {
        Box::pin(control(self.restart.call0(&JsValue::UNDEFINED)))
    }
}

/// The existing Package's exact manifest/delivery/transition codec over bytes.
#[wasm_bindgen]
pub struct ActorsPackage(Package);

#[wasm_bindgen]
impl ActorsPackage {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<Self, JsValue> {
        Ok(Self(Package::new().map_err(failed)?))
    }
    pub fn manifest(&self) -> Vec<u8> {
        wire::encode_manifest(self.0.manifest())
    }
    #[wasm_bindgen(js_name = onRecord)]
    pub async fn on_record(
        &self,
        bytes: &[u8],
        signal: JsValue,
        interrupt: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let (delivery, checkpoint) = wire::decode_delivery(bytes).map_err(failed)?;
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let _interrupt = Listening::new(interrupt, move || token.cancel())?;
        let (turn, handle) = futures::future::abortable(self.0.on_record(
            delivery,
            checkpoint.as_ref(),
            cancellation,
        ));
        let _abort = Listening::new(signal, move || handle.abort())?;
        let transition = turn
            .await
            .map_err(|_| failed("guest delivery aborted"))?
            .map_err(failed)?;
        wire::encode_transition(&transition).map_err(failed)
    }
}

struct Listening {
    signal: JsValue,
    listener: Closure<dyn FnMut()>,
}
impl Listening {
    fn new(signal: JsValue, mut action: impl FnMut() + 'static) -> Result<Self, JsValue> {
        if js_sys::Reflect::get(&signal, &"aborted".into())?.is_truthy() {
            action();
        }
        let listener = Closure::new(action);
        let add: Function =
            js_sys::Reflect::get(&signal, &"addEventListener".into())?.dyn_into()?;
        add.call2(&signal, &"abort".into(), listener.as_ref())?;
        Ok(Self { signal, listener })
    }
}
impl Drop for Listening {
    fn drop(&mut self) {
        if let Ok(remove) = js_sys::Reflect::get(&self.signal, &"removeEventListener".into())
            .and_then(|value| value.dyn_into::<Function>())
        {
            let _ = remove.call2(&self.signal, &"abort".into(), self.listener.as_ref());
        }
    }
}
