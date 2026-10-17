//! JavaScript rendering over the native swarm fold and platform session.
use super::{
    json::{from_js, to_js},
    query::{self, QueryFileDelta, QueryFileInfo, WasmTaskVolume},
};
use crate::{
    client::{
        ClientViewHandle, ClientViewLimits, InterestDocument, InterestTier, SwarmChange,
        SwarmClient, SwarmClientLimits,
    },
    task_volume::{Cursor, FileKey, Query, TaskVolume},
};
use acyclic_stream::{StreamPath, WasmStream};
use futures::FutureExt as _;
use js_sys::{Function, Promise};
use serde::Serialize;
use std::cell::RefCell;
use tsify::Tsify;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

fn error(value: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&value.to_string())
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
enum SwarmDelivery<'a> {
    Retired {
        agent: &'a str,
    },
    Agent {
        agent: &'a str,
    },
    Layout,
    Snapshot {
        agent: &'a str,
        cursor: &'a Cursor,
    },
    Data {
        agent: &'a str,
        overview: bool,
        #[tsify(type = "QueryFileDelta | null")]
        file: Option<QueryFileDelta<'a>>,
    },
    Watermark {
        agent: &'a str,
        overview: bool,
        accepted: u64,
        durable: u64,
    },
}
fn delivery(change: &SwarmChange) -> Result<JsValue, JsValue> {
    to_js(&match change {
        SwarmChange::Retired { agent } => SwarmDelivery::Retired {
            agent: agent.as_str(),
        },
        SwarmChange::Agent { agent } => SwarmDelivery::Agent {
            agent: agent.as_str(),
        },
        SwarmChange::Layout => SwarmDelivery::Layout,
        SwarmChange::Snapshot { agent, cursor } => SwarmDelivery::Snapshot {
            agent: agent.as_str(),
            cursor,
        },
        SwarmChange::Data {
            agent,
            overview,
            file,
            ..
        } => SwarmDelivery::Data {
            agent: agent.as_str(),
            overview: *overview,
            file: file.as_ref().map(QueryFileDelta::from),
        },
        SwarmChange::Watermark {
            agent,
            overview,
            watermark,
        } => SwarmDelivery::Watermark {
            agent: agent.as_str(),
            overview: *overview,
            accepted: watermark.accepted,
            durable: watermark.durable,
        },
    })
}

#[derive(Serialize, Tsify)]
struct SwarmStatusValue<'a> {
    #[serde(serialize_with = "query::bytes")]
    #[tsify(type = "Uint8Array")]
    key: &'a [u8],
    #[serde(serialize_with = "query::bytes")]
    #[tsify(type = "Uint8Array")]
    value: &'a [u8],
    attention: bool,
}
#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
struct SwarmStatusInfo<'a> {
    #[tsify(type = "bigint | null")]
    next: Option<u64>,
    accepted: u64,
    durable: u64,
    values: Vec<SwarmStatusValue<'a>>,
}

/// The exact read-only native views, readable while the follow awaits I/O.
#[wasm_bindgen]
pub struct WasmClientViews {
    pub(super) views: ClientViewHandle,
}
impl From<ClientViewHandle> for WasmClientViews {
    fn from(views: ClientViewHandle) -> Self {
        Self { views }
    }
}
#[wasm_bindgen]
impl WasmClientViews {
    /// Discovered agents, including those outside the selected content tier.
    #[wasm_bindgen(unchecked_return_type = "string[]")]
    pub fn agents(&self) -> Result<JsValue, JsValue> {
        self.views
            .read(|views| to_js(&views.agents().map(StreamPath::as_str).collect::<Vec<_>>()))
            .map_err(error)?
    }
    /// Current rendering demand derived from the followed layout files.
    #[wasm_bindgen(unchecked_return_type = "InterestDocument")]
    pub fn layout(&self) -> Result<JsValue, JsValue> {
        self.views
            .read(|views| {
                to_js(&InterestDocument {
                    in_view: views.layout().visible().cloned().collect(),
                    prefetch: views
                        .layout()
                        .full_fidelity()
                        .filter(|agent| views.layout().tier(agent) == Some(InterestTier::Prefetch))
                        .cloned()
                        .collect(),
                })
            })
            .map_err(error)?
    }
    /// Latest producer status and accepted/durable watermarks for one agent.
    #[wasm_bindgen(unchecked_return_type = "SwarmStatusInfo | null")]
    pub fn status(&self, agent: &str) -> Result<JsValue, JsValue> {
        let agent = StreamPath::new(agent).map_err(error)?;
        self.views
            .read(|views| {
                let Some(status) = views.status(&agent) else {
                    return Ok(JsValue::NULL);
                };
                to_js(&SwarmStatusInfo {
                    next: status.cursor().map(|cursor| cursor.next),
                    accepted: status.watermarks().accepted,
                    durable: status.watermarks().durable,
                    values: status
                        .values()
                        .map(|value| SwarmStatusValue {
                            key: &value.key,
                            value: &value.value,
                            attention: value.attention,
                        })
                        .collect(),
                })
            })
            .map_err(error)?
    }
    /// Complete snapshot/follow cut of the selected agent files.
    #[wasm_bindgen(unchecked_return_type = "Cursor | null")]
    pub fn cursor(&self, agent: &str) -> Result<JsValue, JsValue> {
        let agent = StreamPath::new(agent).map_err(error)?;
        self.views
            .read(|views| to_js(&views.cursor(&agent)))
            .map_err(error)?
    }
    /// Logical file metadata for the requested selected agent.
    #[wasm_bindgen(unchecked_return_type = "QueryFileInfo[]")]
    pub fn files(&self, agent: &str) -> Result<JsValue, JsValue> {
        let agent = StreamPath::new(agent).map_err(error)?;
        self.views
            .read(|views| {
                to_js(
                    &views
                        .files(&agent)
                        .map(QueryFileInfo::from)
                        .collect::<Vec<_>>(),
                )
            })
            .map_err(error)?
    }
    /// Model choices from the same followed per-agent files.
    #[wasm_bindgen(unchecked_return_type = "ModelQueryView[]")]
    pub fn models(&self) -> Result<JsValue, JsValue> {
        self.views
            .read(|views| to_js(&views.models().collect::<Vec<_>>()))
            .map_err(error)?
    }
    /// Goals and request progress from the same followed files and cursors.
    #[wasm_bindgen(unchecked_return_type = "GoalQueryView[]")]
    pub fn goals(&self) -> Result<JsValue, JsValue> {
        self.views
            .read(|views| to_js(&views.goals().collect::<Vec<_>>()))
            .map_err(error)?
    }
    /// Cached plan rows of one followed agent; redraw never reparses Markdown.
    #[wasm_bindgen(unchecked_return_type = "string[]")]
    pub fn plans(&self, agent: &str) -> Result<JsValue, JsValue> {
        let agent = StreamPath::new(agent).map_err(error)?;
        self.views
            .read(|views| to_js(&views.plans(&agent).collect::<Vec<_>>()))
            .map_err(error)?
    }

    /// Ordered immutable segments, without joining accumulated text.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array[] | null")]
    pub fn segments(
        &self,
        agent: &str,
        #[wasm_bindgen(unchecked_param_type = "FileKey")] key: JsValue,
    ) -> Result<JsValue, JsValue> {
        let agent = StreamPath::new(agent).map_err(error)?;
        let key: FileKey = from_js(key)?;
        self.views
            .read(|views| query::segments(views.file(&agent, &key)))
            .map_err(error)
    }
}

/// One disposable subscription cache; demand and all task state remain files.
#[wasm_bindgen]
pub struct WasmSwarmClient {
    client: RefCell<Option<SwarmClient>>,
    views: ClientViewHandle,
}
#[wasm_bindgen]
impl WasmSwarmClient {
    #[wasm_bindgen(constructor)]
    pub fn new(
        root: &str,
        #[wasm_bindgen(unchecked_param_type = "SwarmClientLimits")] limits: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ClientViewLimits")] view_limits: JsValue,
    ) -> Result<WasmSwarmClient, JsValue> {
        let limits: SwarmClientLimits = from_js(limits)?;
        let view_limits: ClientViewLimits = from_js(view_limits)?;
        let client = SwarmClient::new(StreamPath::new(root).map_err(error)?, limits, view_limits)
            .map_err(error)?;
        let views = client.views();
        Ok(Self {
            client: RefCell::new(Some(client)),
            views,
        })
    }
    /// Reads the same live fold used by the native client.
    #[wasm_bindgen]
    pub fn views(&self) -> WasmClientViews {
        WasmClientViews::from(self.views.clone())
    }

    /// Folds config queries and agent follows on the caller's shared pool. A
    /// cancelled or failed cache is replaced by a fresh query subscription.
    #[wasm_bindgen]
    pub async fn follow(
        &self,
        config: &WasmTaskVolume,
        parent: &WasmTaskVolume,
        stream: &WasmStream,
        #[wasm_bindgen(unchecked_param_type = "(change: SwarmDelivery) => void | Promise<void>")]
        changed: Function,
        cancelled: Promise,
    ) -> Result<(), JsValue> {
        let session = stream
            .session_client()
            .ok_or_else(|| error("stream has no session"))?;
        let client = self
            .client
            .try_borrow_mut()
            .map_err(error)?
            .take()
            .ok_or_else(|| error("swarm cache already followed; create a fresh cache"))?;
        let provider = stream.provider();
        let followed = client
            .follow(
                config.volume.as_ref(),
                parent.volume.as_ref(),
                &session,
                move |agent, maximum| {
                    let provider = provider.clone();
                    async move {
                        let path = StreamPath::new(format!("{agent}/files"))
                            .map_err(crate::task_volume::Error::from)?;
                        let volume = TaskVolume::open_existing(provider, path).await?;
                        volume.heads(&Query::Root, None, maximum).await
                    }
                },
                async |change| {
                    let value = delivery(&change)
                        .map_err(|failure| crate::Error::Storage(format!("{failure:?}")))?;
                    let result = changed
                        .call1(&JsValue::UNDEFINED, &value)
                        .map_err(|failure| crate::Error::Storage(format!("{failure:?}")))?;
                    JsFuture::from(Promise::resolve(&result))
                        .await
                        .map_err(|failure| crate::Error::Storage(format!("{failure:?}")))?;
                    Ok(())
                },
            )
            .fuse();
        let stopped = JsFuture::from(cancelled).fuse();
        futures::pin_mut!(followed, stopped);
        futures::select! { outcome = followed => outcome.map_err(error), _ = stopped => Ok(()) }
    }
}
