use acyclic_actors::guest::Handler;
use acyclic_agent_host_conformance::{Backend, Result, Storage};
use acyclic_stream::{BoxProviderFuture, JournalStream, JournalStreamLimits, StreamProvider};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// Provision the same lazy spawn fixture for the real event-owned host.
#[wasm_bindgen(js_name = spawnAdoptionFixture)]
pub async fn spawn_adoption_fixture(port: JsValue) -> std::result::Result<String, JsValue> {
    let provider = acyclic_stream::WasmStream::connect_provider(port)?.provider();
    let within = acyclic_actors::addressing::ActorReference::new(b"browser", b"agents", b"host")
        .map_err(|error| JsValue::from_str(&format!("STEP adoption-fixture identity failed: {error}; recovery: bash scripts/qualify-cloudflare.sh")))?;
    acyclic_agent_host_conformance::fixtures::spawned(provider, within)
        .await
        .map(|path| path.to_string())
        .map_err(|error| JsValue::from_str(&format!("STEP adoption-fixture spawn failed: {error}; recovery: bash scripts/qualify-cloudflare.sh")))
}

/// Produce a real canonical append through the supplied SQLite ledger port.
#[wasm_bindgen(js_name = appendWakeRecord)]
pub async fn append_wake_record(port: JsValue, path: String) -> std::result::Result<(), JsValue> {
    let provider = acyclic_stream::WasmStream::connect_provider(port)?.provider();
    let result = async {
        provider
            .append(acyclic_stream::AppendRequest {
                path: acyclic_stream::StreamPath::new(path)?,
                records: vec![acyclic_stream::AppendRecord::from(b"wake".as_slice())],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        Ok::<_, acyclic_stream::StreamError>(())
    }
    .await;
    result.map_err(|error| {
        JsValue::from_str(&format!(
            "STEP wake-record append failed: {error}; recovery: bash scripts/qualify-cloudflare.sh"
        ))
    })
}

struct Sqlite(js_sys::Function);
struct Domain(JsValue);
struct Rpc(js_sys::Function);
struct Port(JsValue);

impl Storage for Port {
    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>> {
        Box::pin(async move {
            Ok(acyclic_stream::WasmStream::connect_provider(self.0.clone())
                .map_err(|error| format!("provider port failed: {error:?}"))?
                .provider())
        })
    }
}

impl Backend for Rpc {
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>> {
        Box::pin(async move {
            let port = self
                .0
                .call0(&JsValue::UNDEFINED)
                .map_err(|error| format!("RPC domain creation failed: {error:?}"))?;
            Ok(Box::new(Port(port)) as Box<dyn Storage>)
        })
    }
    fn handler(&self, handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
        acyclic_agent_host_conformance::remote(handler)
    }
}

impl Storage for Domain {
    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>> {
        Box::pin(async move {
            Ok(
                Arc::new(JournalStream::open(self.0.clone(), JournalStreamLimits::default()).await?)
                    as Arc<dyn StreamProvider>,
            )
        })
    }
}

impl Backend for Sqlite {
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>> {
        Box::pin(async move {
            let journal = self
                .0
                .call0(&JsValue::UNDEFINED)
                .map_err(|error| format!("SQLite domain creation failed: {error:?}"))?;
            Ok(Box::new(Domain(journal)) as Box<dyn Storage>)
        })
    }

    fn handler(&self, handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
        acyclic_agent_host_conformance::remote(handler)
    }
}

/// Runs both suites without selecting or replacing vectors. `fresh` returns a
/// new SDK SqliteJournal on the calling Durable Object's real SQLite storage.
#[wasm_bindgen(js_name = qualifyCloudflare)]
pub async fn qualify(fresh: js_sys::Function) -> std::result::Result<(), JsValue> {
    verify(&Sqlite(fresh)).await
}

/// The same suites over actual cross-DO canonical RPC, including pull cursors.
#[wasm_bindgen(js_name = qualifyCloudflareRpc)]
pub async fn qualify_rpc(fresh: js_sys::Function) -> std::result::Result<(), JsValue> {
    verify(&Rpc(fresh)).await
}

async fn verify(backend: &dyn Backend) -> std::result::Result<(), JsValue> {
    let result: Result = async {
        let storage = backend.fresh().await?;
        let stream = storage.open().await?;
        acyclic_stream::conformance::verify(stream.as_ref()).await?;
        drop(stream);
        acyclic_agent_host_conformance::verify(backend).await
    }
    .await;
    result.map_err(|error| JsValue::from_str(&format!("STEP cloudflare-conformance failed: {error}; recovery: bash scripts/qualify-cloudflare.sh")))
}
