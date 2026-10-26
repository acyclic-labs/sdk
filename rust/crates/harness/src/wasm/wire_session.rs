//! Browser access to the same selected-stream owner used by native clients.

use acyclic_harness_client::{DemandPolicy, ViewDemand, WireSession, WireSessionError};
use acyclic_stream::{StreamPath, WasmStream, wire};
use js_sys::{Array, JsString, Uint8Array};
use prost::Message as _;
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, task::Waker};
use wasm_bindgen::prelude::*;

struct ReadState {
    waker: Option<Waker>,
    cancelled: bool,
}

type Readers = Rc<RefCell<BTreeMap<StreamPath, ReadState>>>;

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
    let (bytes, ()) = super::bytes::snapshot(view, |count| {
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
    path: StreamPath,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.readers.borrow_mut().remove(&self.path);
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
            for (path, state) in self.readers.borrow_mut().iter_mut() {
                state.cancelled |= !session.demand().contains_full(path);
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

    /// Consume one bounded canonical frame. Only one pending read per stream;
    /// demand changes remain callable while this future awaits the network.
    pub async fn next(&self, resource: JsString) -> Result<Option<Uint8Array>, JsValue> {
        let path = path(resource)?;
        if !self.session.borrow().demand().contains_full(&path) {
            return Err(error("CLIENT_NOT_REQUESTED"));
        }
        {
            let mut readers = self.readers.borrow_mut();
            if readers.contains_key(&path) {
                return Err(error("CLIENT_READ_BUSY"));
            }
            if readers.len() >= self.policy.selected_capacity() {
                return Err(error("CLIENT_READ_CAPACITY"));
            }
            readers.insert(
                path.clone(),
                ReadState {
                    waker: None,
                    cancelled: false,
                },
            );
        }
        let guard = ReadGuard {
            readers: self.readers.clone(),
            path,
        };
        let frame = futures::future::poll_fn(|context| {
            if let Some(state) = self.readers.borrow_mut().get_mut(&guard.path) {
                // Sticky until this read settles: deselect/reselect must not let
                // an older pending read consume a replacement subscription.
                if state.cancelled {
                    return std::task::Poll::Ready(Err(
                        acyclic_harness_client::WireSessionError::NotRequested,
                    ));
                }
                state.waker = Some(context.waker().clone());
            }
            self.session.borrow_mut().poll_stream(&guard.path, context)
        })
        .await
        .map_err(session_error)?;
        Ok(frame.map(|frame| Uint8Array::from(frame.encode_to_vec().as_slice())))
    }

    /// Retire every interest and wake pending readers to observe the refusal.
    pub fn close(&self) -> Result<(), JsValue> {
        self.session
            .borrow_mut()
            .set_view(ViewDemand::default())
            .map_err(session_error)?;
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

    /// Revision of the shared Rust fold. An unchanged view needs no JS rebuild.
    #[wasm_bindgen(js_name = viewRevision)]
    pub fn view_revision(&self, resource: JsString) -> Result<u64, JsValue> {
        let path = path(resource)?;
        self.session
            .borrow()
            .view(&path)
            .map(|view| view.revision())
            .ok_or_else(|| error("CLIENT_VIEW_NOT_OPEN"))
    }

    /// Earliest displayed sequence; moves when the bounded live tail evicts.
    #[wasm_bindgen(js_name = firstSequence)]
    pub fn first_sequence(&self, resource: JsString) -> Result<u64, JsValue> {
        let path = path(resource)?;
        self.session
            .borrow()
            .view(&path)
            .map(|view| view.first_sequence())
            .ok_or_else(|| error("CLIENT_VIEW_NOT_OPEN"))
    }

    /// Number of records in the current bounded display tail.
    #[wasm_bindgen(js_name = recordCount)]
    pub fn record_count(&self, resource: JsString) -> Result<usize, JsValue> {
        let path = path(resource)?;
        self.session
            .borrow()
            .view(&path)
            .map(|view| view.records().len())
            .ok_or_else(|| error("CLIENT_VIEW_NOT_OPEN"))
    }

    /// Copy one canonical record across the host boundary, never the whole view.
    pub fn record(&self, resource: JsString, index: usize) -> Result<Option<Uint8Array>, JsValue> {
        let path = path(resource)?;
        let session = self.session.borrow();
        let view = session
            .view(&path)
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
