//! Browser access to the same selected-stream owner used by native clients.

use acyclic_harness_client::{DemandPolicy, StreamView, ViewDemand, WireSession, WireSessionError};
use acyclic_stream::{StreamPath, WasmStream, wire};
use js_sys::{Array, JsString, Uint8Array};
use prost::Message as _;
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, task::Waker};
use wasm_bindgen::prelude::*;

struct ReadState {
    waker: Option<Waker>,
    cancelled: bool,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ReadKey {
    Stream(StreamPath),
    Resource(String),
}

pub(super) fn resource(value: JsString) -> Result<String, JsValue> {
    let ReadKey::Resource(resource) = ReadKey::parse(value)? else {
        return Err(error("CLIENT_INVALID_RESOURCE"));
    };
    Ok(resource)
}

impl ReadKey {
    fn parse(value: JsString) -> Result<Self, JsValue> {
        if value.length() as usize > acyclic_stream::MAX_PATH_BYTES + 4 {
            return Err(error("CLIENT_PATH_CAPACITY"));
        }
        let text = value
            .as_string()
            .ok_or_else(|| error("CLIENT_INVALID_PATH"))?;
        if text.starts_with("fs:/") {
            acyclic_stream::wire_codec::session::validate_resource(&text).map_err(stream_error)?;
            Ok(Self::Resource(text))
        } else {
            StreamPath::new(text)
                .map(Self::Stream)
                .map_err(stream_error)
        }
    }

    fn view<'a>(&self, session: &'a WireSession) -> Option<&'a StreamView> {
        match self {
            Self::Stream(path) => session.view(path),
            Self::Resource(resource) => session.resource_view(resource),
        }
    }
}

type Readers = Rc<RefCell<BTreeMap<ReadKey, ReadState>>>;

fn error(message: impl std::fmt::Display) -> JsValue {
    let message = message.to_string();
    coded_error(&message, &message)
}

fn coded_error(code: &str, message: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&message.to_string());
    error.set_name(code);
    error.into()
}

fn stream_error(cause: acyclic_stream::StreamError) -> JsValue {
    coded_error(cause.code(), cause)
}

fn session_error(cause: WireSessionError) -> JsValue {
    coded_error(cause.code(), cause)
}

fn command_bytes(view: &Uint8Array, capacity_code: &str) -> Result<Vec<u8>, JsValue> {
    let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(view, |count| {
        if count > acyclic_stream::MAX_COMMAND_BYTES as u64 {
            Err(error(capacity_code))
        } else {
            Ok(())
        }
    })?;
    Ok(bytes)
}

fn path(value: JsString) -> Result<StreamPath, JsValue> {
    // Stream paths are ASCII, so UTF-16 length is a valid pre-copy cap.
    if value.length() as usize > acyclic_stream::MAX_PATH_BYTES {
        return Err(error("CLIENT_PATH_CAPACITY"));
    }
    let value = value
        .as_string()
        .ok_or_else(|| error("CLIENT_INVALID_PATH"))?;
    StreamPath::new(value).map_err(stream_error)
}

fn paths(input: &Array, maximum: usize) -> Result<Vec<StreamPath>, JsValue> {
    let length = input.length();
    if length as usize > maximum {
        return Err(error("CLIENT_DEMAND_CAPACITY"));
    }
    (0..length)
        .map(|index| input.get(index))
        .map(|value| {
            if !value.is_string() {
                return Err(error("CLIENT_INVALID_PATH"));
            }
            path(JsString::from(value))
        })
        .collect()
}

struct ReadGuard {
    readers: Readers,
    key: ReadKey,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.readers.borrow_mut().remove(&self.key);
    }
}

/// Bounded logical interests over the caller's existing canonical Streams pool.
/// No JavaScript queue, reconnect loop, cursor or per-agent transport is added.
#[wasm_bindgen]
pub struct WasmWireSession {
    session: Rc<RefCell<WireSession>>,
    policy: DemandPolicy,
    readers: Readers,
}

#[wasm_bindgen]
impl WasmWireSession {
    /// Share the platform's existing pool; refuse providers with no session I/O.
    #[wasm_bindgen(constructor)]
    pub fn new(stream: &WasmStream, selected: usize, expanded: usize) -> Result<Self, JsValue> {
        let policy = DemandPolicy::new(selected, expanded)
            .map_err(|cause| session_error(WireSessionError::Demand(cause)))?;
        let client = stream
            .session_client()
            .ok_or_else(|| error("CLIENT_NO_SESSION"))?;
        Ok(Self {
            session: Rc::new(RefCell::new(WireSession::new(client, policy))),
            policy,
            readers: Rc::new(RefCell::new(BTreeMap::new())),
        })
    }

    /// Atomically replace explicit demand, dropping deselected pending/open I/O.
    /// Full paths name conversation streams, expanded paths name tree nodes.
    #[wasm_bindgen(js_name = setView)]
    pub fn set_view(&self, visible: Array, focused: Array, expanded: Array) -> Result<(), JsValue> {
        let demand = ViewDemand::new(
            self.policy,
            paths(&visible, self.policy.selected_capacity())?,
            paths(&focused, self.policy.selected_capacity())?,
            paths(&expanded, self.policy.expanded_capacity())?,
        )
        .map_err(|cause| session_error(WireSessionError::Demand(cause)))?;
        self.session
            .borrow_mut()
            .set_view(demand)
            .map_err(session_error)?;
        {
            let session = self.session.borrow();
            for (key, state) in self.readers.borrow_mut().iter_mut() {
                if let ReadKey::Stream(path) = key {
                    state.cancelled |= !session.demand().contains_full(path);
                }
            }
        }
        self.wake_readers();
        Ok(())
    }

    /// Register the credential on the shared pool after checking its byte bound.
    #[wasm_bindgen(js_name = registerCredential)]
    pub fn register_credential(&self, slot: u32, credential: Uint8Array) -> Result<(), JsValue> {
        if slot as usize >= acyclic_stream::session::Limits::default().channels {
            return Err(error("CLIENT_CREDENTIAL_CAPACITY"));
        }
        let credential = command_bytes(&credential, "CLIENT_CREDENTIAL_CAPACITY")?;
        self.session
            .borrow()
            .register_credential(slot, credential.into())
            .map_err(session_error)
    }

    /// Admit the canonical encoded subscription; network I/O starts in `next`.
    #[wasm_bindgen(js_name = openStream)]
    pub fn open_stream(&self, resource: JsString, subscription: Uint8Array) -> Result<(), JsValue> {
        let subscription = command_bytes(&subscription, "CLIENT_SUBSCRIPTION_CAPACITY")?;
        let path = path(resource)?;
        let subscription = wire::SessionSubscription::decode(subscription.as_slice())
            .map_err(|cause| coded_error("invalid_subscription", cause))?;
        self.session
            .borrow_mut()
            .begin_stream(path, subscription)
            .map_err(session_error)
    }

    /// Select filtered filesystem/tool output on the same pool and view budget.
    #[wasm_bindgen(js_name = openResource)]
    pub fn open_resource(&self, subscription: Uint8Array) -> Result<(), JsValue> {
        let bytes = command_bytes(&subscription, "CLIENT_SUBSCRIPTION_CAPACITY")?;
        let subscription = wire::SessionSubscription::decode(bytes.as_slice())
            .map_err(|cause| coded_error("invalid_subscription", cause))?;
        self.session
            .borrow_mut()
            .begin_resource(subscription)
            .map_err(session_error)
    }

    /// Release output custody and wake any pending read of this generation.
    #[wasm_bindgen(js_name = releaseResource)]
    pub fn release_resource(&self, resource: JsString) -> Result<bool, JsValue> {
        let resource = self::resource(resource)?;
        let released = self
            .session
            .borrow_mut()
            .release_resource(&resource)
            .map_err(session_error)?;
        if let Some(state) = self
            .readers
            .borrow_mut()
            .get_mut(&ReadKey::Resource(resource))
        {
            state.cancelled = true;
        }
        self.wake_readers();
        Ok(released)
    }

    /// Consume one bounded canonical frame. Only one pending read per stream;
    /// demand changes remain callable while this future awaits the network.
    pub async fn next(&self, resource: JsString) -> Result<Option<Uint8Array>, JsValue> {
        let key = ReadKey::parse(resource)?;
        let requested = match &key {
            ReadKey::Stream(path) => self.session.borrow().demand().contains_full(path),
            ReadKey::Resource(resource) => self.session.borrow().resource_view(resource).is_some(),
        };
        if !requested {
            return Err(error("CLIENT_NOT_REQUESTED"));
        }
        {
            let mut readers = self.readers.borrow_mut();
            if readers.contains_key(&key) {
                return Err(error("CLIENT_READ_BUSY"));
            }
            if readers.len() >= self.policy.selected_capacity() {
                return Err(error("CLIENT_READ_CAPACITY"));
            }
            readers.insert(
                key.clone(),
                ReadState {
                    waker: None,
                    cancelled: false,
                },
            );
        }
        let guard = ReadGuard {
            readers: self.readers.clone(),
            key,
        };
        let frame = futures::future::poll_fn(|context| {
            if let Some(state) = self.readers.borrow_mut().get_mut(&guard.key) {
                // Sticky until this read settles: deselect/reselect must not let
                // an older pending read consume a replacement subscription.
                if state.cancelled {
                    return std::task::Poll::Ready(Err(
                        acyclic_harness_client::WireSessionError::NotRequested,
                    ));
                }
                state.waker = Some(context.waker().clone());
            }
            let mut session = self.session.borrow_mut();
            match &guard.key {
                ReadKey::Stream(path) => session.poll_stream(path, context),
                ReadKey::Resource(resource) => session.poll_resource(resource, context),
            }
        })
        .await
        .map_err(session_error)?;
        Ok(frame.map(|frame| Uint8Array::from(frame.encode_to_vec().as_slice())))
    }

    /// Retire every interest and wake pending readers to observe the refusal.
    pub fn close(&self) -> Result<(), JsValue> {
        self.session.borrow_mut().clear().map_err(session_error)?;
        for state in self.readers.borrow_mut().values_mut() {
            state.cancelled = true;
        }
        self.wake_readers();
        Ok(())
    }

    /// Includes bounded pending admissions as well as established follows.
    #[wasm_bindgen(js_name = followedAgents)]
    pub fn followed_agents(&self) -> usize {
        self.session.borrow().followed_agents()
    }

    /// Counts conversation, status and filesystem interests together.
    #[wasm_bindgen(js_name = followedInterests)]
    pub fn followed_interests(&self) -> usize {
        self.session.borrow().followed_interests()
    }

    /// Revision of the shared Rust fold. An unchanged view needs no JS rebuild.
    #[wasm_bindgen(js_name = viewRevision)]
    pub fn view_revision(&self, resource: JsString) -> Result<u64, JsValue> {
        let key = ReadKey::parse(resource)?;
        key.view(&self.session.borrow())
            .map(|view| view.revision())
            .ok_or_else(|| error("CLIENT_VIEW_NOT_OPEN"))
    }

    /// Earliest displayed sequence; moves when the bounded live tail evicts.
    #[wasm_bindgen(js_name = firstSequence)]
    pub fn first_sequence(&self, resource: JsString) -> Result<u64, JsValue> {
        let key = ReadKey::parse(resource)?;
        key.view(&self.session.borrow())
            .map(|view| view.first_sequence())
            .ok_or_else(|| error("CLIENT_VIEW_NOT_OPEN"))
    }

    /// Number of records in the current bounded display tail.
    #[wasm_bindgen(js_name = recordCount)]
    pub fn record_count(&self, resource: JsString) -> Result<usize, JsValue> {
        let key = ReadKey::parse(resource)?;
        key.view(&self.session.borrow())
            .map(|view| view.records().len())
            .ok_or_else(|| error("CLIENT_VIEW_NOT_OPEN"))
    }

    /// Copy one canonical record across the host boundary, never the whole view.
    pub fn record(&self, resource: JsString, index: usize) -> Result<Option<Uint8Array>, JsValue> {
        let key = ReadKey::parse(resource)?;
        let session = self.session.borrow();
        let view = key
            .view(&session)
            .ok_or_else(|| error("CLIENT_VIEW_NOT_OPEN"))?;
        Ok(view
            .record(index)
            .map(|record| Uint8Array::from(record.encode_to_vec().as_slice())))
    }
}

impl WasmWireSession {
    fn wake_readers(&self) {
        // Release the RefCell borrow before invoking an executor's wake hook.
        let wakers: Vec<_> = self
            .readers
            .borrow_mut()
            .values_mut()
            .filter_map(|state| state.waker.take())
            .collect();
        for waker in wakers {
            waker.wake();
        }
    }
}
