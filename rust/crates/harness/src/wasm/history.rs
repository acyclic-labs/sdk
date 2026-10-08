//! Browser bindings over the existing Stream aggregate and archival reader.
//! This module forwards admission and publication to the native owner classes.

use super::{from_js, js_error, key_bytes, schema_registry, to_js};
use crate::{
    core::{Authority, AuthorityIssuer, Command, Snapshot},
    store::{HistoryCursor, HistoryReadLimits, HistoryReader, StreamAggregate},
};
use acyclic_stream::{BrowserStream, BrowserStreamLimits, MemoryLimits, StreamClient};
use serde::Deserialize;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserHistoryOptions {
    database: String,
    maximum_commands: u32,
    maximum_journal_bytes: u64,
    authority: Authority,
    issuer_id: String,
    issuer_key: Vec<u8>,
    #[serde(default)]
    memory: Option<BrowserMemoryLimits>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserMemoryLimits {
    paths: u32,
    path_bytes: u32,
    records: u32,
    payload_bytes: u32,
    commits: u32,
    idempotency_results: u32,
}

impl From<BrowserMemoryLimits> for MemoryLimits {
    fn from(limits: BrowserMemoryLimits) -> Self {
        Self {
            paths: limits.paths as usize,
            path_bytes: limits.path_bytes as usize,
            records: limits.records as usize,
            payload_bytes: limits.payload_bytes as usize,
            commits: limits.commits as usize,
            idempotency_results: limits.idempotency_results as usize,
        }
    }
}

impl BrowserHistoryOptions {
    async fn connect(self) -> Result<(StreamClient<BrowserStream>, AuthorityIssuer), JsValue> {
        self.authority.stream_path().map_err(js_error)?;
        let issuer =
            AuthorityIssuer::new(self.issuer_id, key_bytes(self.issuer_key)?, self.authority);
        let limits = BrowserStreamLimits {
            commands: u64::from(self.maximum_commands),
            journal_bytes: self.maximum_journal_bytes,
            memory: self.memory.map(MemoryLimits::from).unwrap_or_default(),
        };
        let provider = BrowserStream::open(&self.database, limits)
            .await
            .map_err(|error| js_error(error.into()))?;
        Ok((StreamClient::new(Arc::new(provider)), issuer))
    }
}

/// An explicit archival reader bound to one owning browser Stream and issuer.
#[wasm_bindgen]
pub struct WasmBrowserHistoryReader {
    inner: HistoryReader<BrowserStream>,
}

#[wasm_bindgen]
impl WasmBrowserHistoryReader {
    /// Opens the provider and reader without constructing a reducer projection.
    #[wasm_bindgen(js_name = openBrowser)]
    pub async fn open_browser(options: JsValue) -> Result<WasmBrowserHistoryReader, JsValue> {
        let options: BrowserHistoryOptions = from_js(options)?;
        let (client, issuer) = options.connect().await?;
        let verifier = issuer.verifier();
        let inner =
            HistoryReader::new(&client, verifier.audience(), verifier.clone()).map_err(js_error)?;
        Ok(Self { inner })
    }

    /// Captures a committed traversal boundary; subsequent appends are excluded.
    pub async fn pin(&self, after_revision: u64) -> Result<JsValue, JsValue> {
        to_js(&self.inner.pin(after_revision).await.map_err(js_error)?)
    }

    /// Runs the same bounded page and attestation checks as the native reader.
    #[wasm_bindgen(js_name = readPage)]
    pub async fn read_page(&self, cursor: JsValue, limits: JsValue) -> Result<JsValue, JsValue> {
        let cursor: HistoryCursor = from_js(cursor)?;
        let limits: HistoryReadLimits = from_js(limits)?;
        to_js(
            &self
                .inner
                .read_page(&cursor, limits)
                .await
                .map_err(js_error)?,
        )
    }

    /// Resolves the existing atomically published operation location and event.
    #[wasm_bindgen(js_name = operationEvent)]
    pub async fn operation_event(&self, operation: JsValue) -> Result<JsValue, JsValue> {
        let operation = from_js(operation)?;
        to_js(
            &self
                .inner
                .operation_event(operation)
                .await
                .map_err(js_error)?,
        )
    }
}

/// Owner-bound publication through the existing aggregate, retry index and CAS.
#[wasm_bindgen]
pub struct WasmBrowserAggregate {
    inner: StreamAggregate<BrowserStream>,
}

#[wasm_bindgen]
impl WasmBrowserAggregate {
    /// Opens canonical history, optionally from an authenticated checkpoint.
    /// Content/fork/merge adapters remain explicit; unsupported effects fail closed.
    #[wasm_bindgen(js_name = openBrowser)]
    pub async fn open_browser(
        options: JsValue,
        schemas: JsValue,
        snapshot: Option<JsValue>,
    ) -> Result<WasmBrowserAggregate, JsValue> {
        let options: BrowserHistoryOptions = from_js(options)?;
        let schemas = schema_registry(schemas)?;
        let snapshot: Option<Snapshot> = snapshot.map(from_js).transpose()?;
        let (client, issuer) = options.connect().await?;
        let verifier = issuer.verifier();
        let authority = verifier.audience().clone();
        let inner = if let Some(snapshot) = snapshot {
            StreamAggregate::open_from_snapshot(&client, authority, verifier, schemas, snapshot)
                .await
        } else {
            StreamAggregate::open(&client, authority, verifier, schemas).await
        }
        .map_err(js_error)?;
        Ok(Self { inner })
    }

    /// Publishes the event and archived-operation location in one existing commit.
    pub async fn execute(&mut self, command: JsValue) -> Result<JsValue, JsValue> {
        let command: Command = from_js(command)?;
        to_js(&self.inner.execute(command).await.map_err(js_error)?)
    }

    /// Reconciles only the exact admitted command; no replacement dispatch is authored.
    pub async fn reconcile(&mut self, command: JsValue) -> Result<JsValue, JsValue> {
        let command: Command = from_js(command)?;
        to_js(&self.inner.reconcile(&command).await.map_err(js_error)?)
    }

    /// Advances the projection through at most one caller-bounded page.
    #[wasm_bindgen(js_name = refreshThrough)]
    pub async fn refresh_through(
        &mut self,
        revision: u64,
        maximum_events: u32,
    ) -> Result<bool, JsValue> {
        self.inner
            .refresh_through(revision, maximum_events)
            .await
            .map_err(js_error)
    }

    /// Reads the command head without serializing retained history.
    pub fn head(&self) -> Result<JsValue, JsValue> {
        let reducer = self.inner.reducer();
        to_js(&(reducer.authority(), reducer.revision()))
    }

    /// Captures the existing authenticated reducer projection.
    pub fn snapshot(&self) -> Result<JsValue, JsValue> {
        to_js(&self.inner.reducer().snapshot().map_err(js_error)?)
    }

    /// Binds an archival reader to this exact provider and owner verifier.
    #[wasm_bindgen(js_name = historyReader)]
    pub fn history_reader(&self) -> Result<WasmBrowserHistoryReader, JsValue> {
        Ok(WasmBrowserHistoryReader {
            inner: self.inner.history_reader().map_err(js_error)?,
        })
    }
}
