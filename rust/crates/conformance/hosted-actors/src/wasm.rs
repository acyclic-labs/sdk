//! Platform controls around the unchanged public Actors verifier and package.
use acyclic_actors::{
    addressing::ActorReference,
    conformance::{Backend, Package},
    guest::{CancellationToken, Handler, wire},
    local::{Clock, Error, Store, SystemClock},
};
use acyclic_stream::{
    BoxProviderFuture, InjectedStream, ReadRequest, StreamError, StreamPath, StreamProvider,
    WasmStream,
    testing::{Counted, RequestClass, RequestLedger},
};
use futures::TryStreamExt;
use js_sys::{Function, Promise, Uint8Array};
use std::sync::Arc;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

fn failed(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("hosted Actors conformance failed: {error}"))
}

/// The suite and every remote owner dispatch through this ONE admitted provider.
#[wasm_bindgen]
pub struct ActorsDomain {
    store: Store<Counted<InjectedStream>>,
    clock: SystemClock,
    pause: Function,
    restart: Function,
    complete: Function,
}

#[wasm_bindgen]
impl ActorsDomain {
    pub async fn open(
        port: JsValue,
        pause: Function,
        restart: Function,
        complete: Function,
    ) -> Result<Self, JsValue> {
        let provider = InjectedStream::new(port).map_err(failed)?;
        Ok(Self {
            store: Store::new(
                Arc::new(Counted::new(provider)),
                StreamPath::new("actors/state").map_err(failed)?,
            ),
            clock: SystemClock,
            pause,
            restart,
            complete,
        })
    }

    /// Only canonical byte RPC crosses an owner/module boundary, never this object.
    pub fn provider(&self) -> WasmStream {
        WasmStream::from_provider(self.store.provider().clone())
    }

    /// Public scheduling paths whose committed changes require discovery.
    #[wasm_bindgen(js_name = schedulingPaths)]
    pub fn scheduling_paths(&self) -> Result<js_sys::Array, JsValue> {
        let paths = js_sys::Array::new();
        for shard in 0..self.store.scheduling_shards() {
            paths.push(
                &self
                    .store
                    .scheduling_path(shard)
                    .map_err(failed)?
                    .as_str()
                    .into(),
            );
        }
        Ok(paths)
    }

    /// One bounded page across the Store's stable activation shards.
    /// Persist all returned cursors only after admitting every address.
    pub async fn created(&self, mut from: Vec<u64>) -> Result<JsValue, JsValue> {
        let shards = usize::from(self.store.scheduling_shards());
        if from.is_empty() {
            from.resize(shards, 0);
        }
        if from.len() != shards {
            return Err(failed("discovery cursor shape"));
        }
        let actors = js_sys::Array::new();
        let mut full = false;
        for (shard, cursor) in from.iter_mut().enumerate() {
            let records = match self
                .store
                .provider()
                .read(ReadRequest {
                    key: None,
                    path: self.store.scheduling_path(shard as u16).map_err(failed)?,
                    at_least: Default::default(),
                    from: *cursor,
                    limit: 256,
                })
                .await
            {
                Ok(records) => records.try_collect::<Vec<_>>().await.map_err(failed)?,
                Err(StreamError::NotFound) => Vec::new(),
                Err(error) => return Err(failed(error)),
            };
            *cursor = records.last().map_or(*cursor, |record| record.sequence + 1);
            full |= records.len() == 256;
            for record in records {
                let actor = ActorReference::decode(&record.value).map_err(failed)?;
                actors.push(&Uint8Array::from(actor.encode().as_slice()));
            }
        }
        let next = js_sys::Array::new();
        for cursor in from {
            next.push(&JsValue::from(js_sys::BigInt::from(cursor)));
        }
        let result = js_sys::Object::new();
        js_sys::Reflect::set(&result, &"next".into(), &next)?;
        js_sys::Reflect::set(&result, &"actors".into(), &actors)?;
        js_sys::Reflect::set(&result, &"full".into(), &full.into())?;
        Ok(result.into())
    }
    /// Qualification of paged activation discovery and authority alarm admission.
    #[wasm_bindgen(js_name = verifyDiscovery)]
    pub async fn verify_discovery(&self) -> Result<(), JsValue> {
        use acyclic_actors::local::{Creation, PreparedPublish};
        use acyclic_stream::IdempotencyKey;
        use std::collections::BTreeSet;
        let mut expected = BTreeSet::new();
        let mut selected = [0; 2];
        for index in 0_u64.. {
            let actor = ActorReference::new(b"discovery", b"pages", index.to_string().as_bytes())
                .map_err(failed)?;
            let shard = self.store.scheduling_shard(&actor);
            if shard > 1 || selected[usize::from(shard)] >= if shard == 0 { 257 } else { 1 } {
                continue;
            }
            let mut publication = PreparedPublish::foreign(
                IdempotencyKey::new(index.to_le_bytes().to_vec()).map_err(failed)?,
            );
            publication
                .join(
                    self.store
                        .create(None, &actor, Creation::default())
                        .map_err(failed)?,
                )
                .map_err(failed)?;
            publication
                .join(self.store.activation(&actor).map_err(failed)?)
                .map_err(failed)?;
            self.store.submit(publication).await.map_err(failed)?;
            expected.insert(actor.encode());
            selected[usize::from(shard)] += 1;
            if selected == [257, 1] {
                break;
            }
        }
        let mut cursor = Vec::new();
        let mut found = BTreeSet::new();
        let mut pages = 0;
        loop {
            let page = self.created(cursor).await?;
            let actors = js_sys::Array::from(&js_sys::Reflect::get(&page, &"actors".into())?);
            for actor in actors {
                found.insert(Uint8Array::new(&actor).to_vec());
            }
            cursor =
                js_sys::BigUint64Array::new(&js_sys::Reflect::get(&page, &"next".into())?).to_vec();
            pages += 1;
            if !js_sys::Reflect::get(&page, &"full".into())?
                .as_bool()
                .ok_or_else(|| failed("discovery page flag"))?
            {
                break;
            }
        }
        if found != expected || pages != 2 {
            return Err(failed("activation discovery lost a shard or page"));
        }
        acyclic_actors::conformance::early_alarm(
            self.store.provider().clone(),
            StreamPath::new("discovery-alarm").map_err(failed)?,
        )
        .await
        .map_err(failed)
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
    type Provider = Counted<InjectedStream>;
    fn requests(&self) -> RequestLedger {
        self.store.provider().ledger()
    }
    fn store(&self) -> &Store<Self::Provider> {
        &self.store
    }
    fn clock(&self) -> &dyn Clock {
        &self.clock
    }
    fn complete<'a>(
        &'a self,
        actor: &'a ActorReference,
    ) -> BoxProviderFuture<'a, Result<(), Error>> {
        Box::pin(control(self.complete.call1(
            &JsValue::UNDEFINED,
            &Uint8Array::from(actor.encode().as_slice()),
        )))
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
    #[wasm_bindgen(js_name = acceptsInterrupt)]
    pub async fn accepts_interrupt(&self, query: &[u8], signal: JsValue) -> Result<bool, JsValue> {
        let (query, handle) = futures::future::abortable(wire::accepts_interrupt(&self.0, query));
        let _abort = Listening::new(signal, move || handle.abort())?;
        query
            .await
            .map_err(|_| failed("guest interrupt aborted"))?
            .map_err(failed)
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
