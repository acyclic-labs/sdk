//! Platform volume I/O and a shared Rust query fold for JavaScript renderers.

use super::json::{from_js, to_js};
use crate::{
    client::{FileViews, PublishError, QuerySubscription, QueryViewLimits, query_error},
    task_volume::{
        Change, Channel, Cursor, FileKey, FileValue, Generation, HeadFile, Query, TaskVolume, Write,
    },
};
use acyclic_stream::{StreamPath, StreamProvider, WasmStream};
use futures::FutureExt as _;
use js_sys::{Array, Function, Promise, Uint8Array};
use serde::{Serialize, Serializer};
use std::{cell::RefCell, rc::Rc, sync::Arc};
use tsify::Tsify;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

fn error(value: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&value.to_string())
}
fn busy() -> JsValue {
    error("query is already following or reconnecting")
}

/// A logical volume on the caller's existing platform Streams provider.
#[wasm_bindgen]
pub struct WasmTaskVolume {
    pub(super) volume: Arc<TaskVolume<dyn StreamProvider>>,
    pub(super) connections: Option<acyclic_stream::session::Connections>,
    pub(super) session: Option<acyclic_stream::session::Client>,
}

#[wasm_bindgen]
impl WasmTaskVolume {
    /// Opens published files without creating a task or changing its state.
    #[wasm_bindgen]
    pub async fn open(stream: &WasmStream, path: String) -> Result<WasmTaskVolume, JsValue> {
        let path = StreamPath::new(path).map_err(error)?;
        let volume = TaskVolume::open_existing(stream.provider(), path)
            .await
            .map_err(error)?;
        Ok(Self {
            connections: stream.connections(),
            session: stream.session_client(),
            volume: Arc::new(volume),
        })
    }

    /// Explicit initialization for a local runtime or a newly admitted volume.
    #[wasm_bindgen]
    pub async fn create(stream: &WasmStream, path: String) -> Result<WasmTaskVolume, JsValue> {
        let path = StreamPath::new(path).map_err(error)?;
        let volume = TaskVolume::open(stream.provider(), path)
            .await
            .map_err(error)?;
        Ok(Self {
            connections: stream.connections(),
            session: stream.session_client(),
            volume: Arc::new(volume),
        })
    }

    /// Current fence; a write must supply this exact generation.
    #[wasm_bindgen(unchecked_return_type = "Generation")]
    pub async fn head(&self) -> Result<JsValue, JsValue> {
        to_js(&self.volume.head().await.map_err(error)?)
    }

    /// One file write per publication, using the provider's authorization policy.
    #[wasm_bindgen(unchecked_return_type = "Generation")]
    pub async fn write(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Generation")] expected: JsValue,
        #[wasm_bindgen(unchecked_param_type = "FileKey")] key: JsValue,
        bytes: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "Channel | null")] channel: JsValue,
    ) -> Result<JsValue, JsValue> {
        let expected: Generation = from_js(expected)?;
        let mut write = Write::new(from_js(key)?, bytes);
        write.channel = from_js(channel)?;
        to_js(&self.volume.append(expected, &write).await.map_err(error)?)
    }
}

#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
pub(super) struct QueryFileInfo<'a> {
    key: &'a FileKey,
    length: u64,
    #[tsify(type = "bigint | null")]
    through_sequence: Option<u64>,
    segments: usize,
}

pub(super) fn bytes<S: Serializer>(value: &&[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bytes(value)
}

#[derive(Serialize, Tsify)]
pub(super) struct QueryFileDelta<'a> {
    key: &'a FileKey,
    #[tsify(type = "Channel | null")]
    channel: Option<Channel>,
    #[serde(serialize_with = "bytes")]
    #[tsify(type = "Uint8Array")]
    bytes: &'a [u8],
}

impl<'a> From<&'a HeadFile> for QueryFileInfo<'a> {
    fn from(file: &'a HeadFile) -> Self {
        Self {
            key: &file.key,
            length: file.length,
            through_sequence: file.through_sequence,
            segments: file.segments.len(),
        }
    }
}
impl<'a> From<&'a FileValue> for QueryFileDelta<'a> {
    fn from(file: &'a FileValue) -> Self {
        Self {
            key: &file.key,
            channel: file.channel,
            bytes: &file.bytes,
        }
    }
}
pub(super) fn segments(file: Option<&HeadFile>) -> JsValue {
    let Some(file) = file else {
        return JsValue::NULL;
    };
    let segments = Array::new();
    for bytes in &file.segments {
        segments.push(&Uint8Array::from(bytes.as_ref()));
    }
    segments.into()
}

#[derive(Serialize, Tsify)]
struct QueryDelivery<'a> {
    cursor: &'a Cursor,
    #[tsify(type = "QueryFileDelta | null")]
    file: Option<QueryFileDelta<'a>>,
}

fn delivery(change: &Change) -> Result<JsValue, JsValue> {
    to_js(&QueryDelivery {
        cursor: &change.cursor,
        file: change.file.as_ref().map(QueryFileDelta::from),
    })
}

/// Read-only renderer handle; it remains readable while source I/O is pending.
#[wasm_bindgen]
pub struct WasmQueryViews {
    views: Rc<RefCell<FileViews>>,
}

#[wasm_bindgen]
impl WasmQueryViews {
    #[wasm_bindgen(unchecked_return_type = "Cursor")]
    pub fn cursor(&self) -> Result<JsValue, JsValue> {
        to_js(self.views.try_borrow().map_err(error)?.cursor())
    }

    /// Metadata only; growing text stays in shared immutable segments in Rust.
    #[wasm_bindgen(unchecked_return_type = "QueryFileInfo[]")]
    pub fn files(&self) -> Result<JsValue, JsValue> {
        let views = self.views.try_borrow().map_err(error)?;
        let files: Vec<_> = views.files().map(QueryFileInfo::from).collect();
        to_js(&files)
    }

    /// Copies only the requested logical file across the WASM ABI, by segment.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array[] | null")]
    pub fn segments(
        &self,
        #[wasm_bindgen(unchecked_param_type = "FileKey")] key: JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = from_js(key)?;
        let views = self.views.try_borrow().map_err(error)?;
        Ok(segments(views.file(&key)))
    }
}

/// One Rust-owned snapshot plus follow; JavaScript supplies only output I/O.
#[wasm_bindgen]
pub struct WasmClientQuery {
    views: Rc<RefCell<FileViews>>,
    subscription: RefCell<Option<QuerySubscription>>,
    limits: QueryViewLimits,
}

#[wasm_bindgen]
impl WasmClientQuery {
    #[wasm_bindgen]
    pub async fn open(
        volume: &WasmTaskVolume,
        #[wasm_bindgen(unchecked_param_type = "Query")] query: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Cursor | null")] cursor: JsValue,
        #[wasm_bindgen(unchecked_param_type = "QueryViewLimits")] limits: JsValue,
    ) -> Result<WasmClientQuery, JsValue> {
        let query: Query = from_js(query)?;
        let limits: QueryViewLimits = from_js(limits)?;
        let (subscription, snapshot) = QuerySubscription::open(
            volume.volume.as_ref(),
            query,
            from_js(cursor)?,
            limits.segments,
        )
        .await
        .map_err(error)?;
        let views = FileViews::snapshot(snapshot, limits).map_err(error)?;
        Ok(Self {
            views: Rc::new(RefCell::new(views)),
            subscription: RefCell::new(Some(subscription)),
            limits,
        })
    }

    #[wasm_bindgen]
    pub fn views(&self) -> WasmQueryViews {
        WasmQueryViews {
            views: self.views.clone(),
        }
    }

    /// Cancel only an idle source wait, never an in-flight publication. A rejected
    /// output retains its frame for a subsequent follow; the cursor never skips it.
    /// No RefCell borrow is held across JavaScript calls or asynchronous I/O.
    #[wasm_bindgen]
    pub async fn follow(
        &self,
        #[wasm_bindgen(unchecked_param_type = "(delivery: QueryDelivery) => void | Promise<void>")]
        publish: Function,
        #[wasm_bindgen(unchecked_param_type = "() => void")] changed: Function,
        cancelled: Promise,
    ) -> Result<(), JsValue> {
        let mut subscription = self
            .subscription
            .try_borrow_mut()
            .map_err(error)?
            .take()
            .ok_or_else(busy)?;
        let cancelled = JsFuture::from(cancelled).fuse();
        futures::pin_mut!(cancelled);
        let result = async {
            loop {
                futures::select! {
                    ready = subscription.wait_next().fuse() => ready.map_err(error)?,
                    _ = cancelled => return Ok(()),
                }
                subscription
                    .publish_next(async |change| {
                        let apply = self
                            .views
                            .try_borrow()
                            .map_err(|e| {
                                PublishError::Failed(crate::Error::Conflict(e.to_string()))
                            })?
                            .prepare(change)
                            .map_err(query_error)
                            .map_err(PublishError::Rejected)?;
                        let value = delivery(change).map_err(|e| {
                            PublishError::Failed(crate::Error::Invalid(format!("{e:?}")))
                        })?;
                        let output = publish.call1(&JsValue::UNDEFINED, &value).map_err(|e| {
                            PublishError::Failed(crate::Error::Storage(format!("{e:?}")))
                        })?;
                        JsFuture::from(Promise::resolve(&output))
                            .await
                            .map_err(|e| {
                                PublishError::Failed(crate::Error::Storage(format!("{e:?}")))
                            })?;
                        let mut views = self.views.try_borrow_mut().map_err(|e| {
                            PublishError::Failed(crate::Error::Conflict(e.to_string()))
                        })?;
                        apply(&mut views)
                            .map_err(query_error)
                            .map_err(PublishError::Failed)
                    })
                    .await
                    .map_err(error)?;
                changed.call0(&JsValue::UNDEFINED)?;
            }
        }
        .await;
        *self.subscription.try_borrow_mut().map_err(error)? = Some(subscription);
        result
    }

    /// Resumes the retained cut or atomically replaces it when reset is required.
    #[wasm_bindgen]
    pub async fn reconnect(&self, volume: &WasmTaskVolume) -> Result<bool, JsValue> {
        let mut subscription = self
            .subscription
            .try_borrow_mut()
            .map_err(error)?
            .take()
            .ok_or_else(busy)?;
        let result = subscription
            .reconnect(volume.volume.as_ref(), |snapshot| {
                let replacement =
                    FileViews::snapshot(snapshot, self.limits).map_err(query_error)?;
                *self
                    .views
                    .try_borrow_mut()
                    .map_err(|e| crate::Error::Conflict(e.to_string()))? = replacement;
                Ok(())
            })
            .await
            .map_err(error);
        *self.subscription.try_borrow_mut().map_err(error)? = Some(subscription);
        result
    }
}
