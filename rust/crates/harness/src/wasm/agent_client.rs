//! Thin browser ownership of the same AgentClient as the native agent adapter.

use super::json::to_js_client as to_js;
use crate::{
    agent::AgentPath,
    client::{AgentClient, AgentClientError, AgentSelection},
};
use acyclic_stream::WasmStream;
use js_sys::JsString;
use prost::Message as _;
use std::{
    cell::{Cell, RefCell},
    task::{Poll, Waker},
};
use wasm_bindgen::prelude::*;

// The generic exact-integer serializer used by this client preserves even
// descriptor lengths as BigInt; the older admitted-file projection normalizes
// those lengths to Number. Reuse its shape without changing this wire value.
#[wasm_bindgen(typescript_custom_section)]
const AGENT_FILE_REFERENCE: &str = r#"
export type WasmAgentFileRef = Omit<WasmFileRefWire, 'descriptor'> & {
    descriptor: Omit<WasmFileDescriptorWire, 'byte_length'> & {byte_length: bigint};
};
/** Indeterminate admission retains the exact canonical Streams request. */
export interface WasmUnknownAgentError extends Error {
    readonly name: 'agent_unknown';
    readonly request: Uint8Array;
}
"#;

#[wasm_bindgen(inline_js = "
export function unknownAgentError(message, borrowedRequest) {
    const error = new Error(message);
    error.name = 'agent_unknown';
    // The WASM slice expires when this call returns. Custody must own its bytes.
    Object.defineProperty(error, 'request', {
        value: new Uint8Array(borrowedRequest), enumerable: true
    });
    return error;
}
")]
extern "C" {
    #[wasm_bindgen(js_name = unknownAgentError)]
    fn unknown_agent_error(message: &str, request: &[u8]) -> JsValue;
}

/// Bounded borrowed-view metadata; sequences and counters keep exact integers
/// across the WASM boundary. No conversation payload is serialized here.
#[derive(serde::Serialize, tsify::Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
struct AgentWindow {
    first_sequence: u64,
    next_sequence: u64,
    record_count: usize,
    retained_bytes: usize,
    revision: u64,
}

fn error(code: &str, message: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&message.to_string());
    error.set_name(code);
    error.into()
}

pub(super) fn client_error(cause: AgentClientError) -> JsValue {
    match cause {
        AgentClientError::Record(cause) => actor_error(cause),
        cause => error(cause.code(), cause),
    }
}

/// One boundary adapter for the browser host's create/send/approval controls.
/// An uncertain commit is never flattened into a string or an acknowledgement.
pub(crate) fn actor_error(cause: crate::agent::Error) -> JsValue {
    if let Some(request) = cause.unknown_request() {
        let bytes = acyclic_stream::wire_codec::commit_to_wire(request).encode_to_vec();
        return unknown_agent_error(&cause.to_string(), &bytes);
    }
    error(cause.code(), cause)
}

pub(super) fn agent_path(agent: JsString) -> Result<AgentPath, JsValue> {
    if agent.length() as usize > acyclic_stream::MAX_PATH_BYTES {
        return Err(error("invalid_agent_path", "agent path exceeds byte bound"));
    }
    AgentPath::try_from(
        agent
            .as_string()
            .ok_or_else(|| error("invalid_agent_path", "agent path is not a string"))?,
    )
    .map_err(actor_error)
}

struct Reading<'a> {
    busy: &'a Cell<bool>,
    waker: &'a RefCell<Option<Waker>>,
}

impl Drop for Reading<'_> {
    fn drop(&mut self) {
        let waker = self.waker.borrow_mut().take();
        self.busy.set(false);
        drop(waker);
    }
}

/// Selected agents' bounded conversation/status fold. The platform supplies
/// its existing authenticated pool; this object creates no transport or queue.
#[wasm_bindgen]
pub struct WasmAgentClient {
    client: RefCell<Option<AgentClient>>,
    reading: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

#[wasm_bindgen]
impl WasmAgentClient {
    /// Tool-output follows share this observer's bounded fold and next().
    #[wasm_bindgen(js_name = openResource)]
    pub fn open_resource(&self, encoded: &js_sys::Uint8Array) -> Result<(), JsValue> {
        let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(encoded, |length| {
            if length > acyclic_stream::MAX_COMMAND_BYTES as u64 {
                Err(error(
                    "client_subscription_capacity",
                    "subscription exceeds byte bound",
                ))
            } else {
                Ok(())
            }
        })?;
        let subscription = acyclic_stream::wire::SessionSubscription::decode(bytes.as_slice())
            .map_err(|cause| error("invalid_subscription", cause))?;
        self.client
            .borrow_mut()
            .as_mut()
            .ok_or_else(|| error("client_closed", "agent client is closed"))?
            .open_resource(subscription)
            .map_err(client_error)?;
        self.wake_reader();
        Ok(())
    }

    /// Release retained output and let a pending combined read see the change.
    #[wasm_bindgen(js_name = releaseResource)]
    pub fn release_resource(&self, resource: JsString) -> Result<bool, JsValue> {
        let resource = super::wire_session::resource(resource)?;
        let released = self
            .client
            .borrow_mut()
            .as_mut()
            .ok_or_else(|| error("client_closed", "agent client is closed"))?
            .release_resource(&resource)
            .map_err(client_error)?;
        self.wake_reader();
        Ok(released)
    }

    /// Metadata only; no full output history crosses the WASM boundary.
    #[wasm_bindgen(js_name = resourceWindow, unchecked_return_type = "AgentWindow")]
    pub fn resource_window(&self, resource: JsString) -> Result<JsValue, JsValue> {
        let resource = super::wire_session::resource(resource)?;
        self.with_client(|client| {
            let view = client.resource_view(&resource).map_err(client_error)?;
            to_js(&AgentWindow {
                first_sequence: view.first_sequence(),
                next_sequence: view.next_sequence(),
                record_count: view.records().len(),
                retained_bytes: view.retained_bytes(),
                revision: view.revision(),
            })
        })?
    }

    /// Copy one displayed canonical record, rather than joining tool output.
    #[wasm_bindgen(js_name = resourceRecord)]
    pub fn resource_record(
        &self,
        resource: JsString,
        index: usize,
    ) -> Result<Option<js_sys::Uint8Array>, JsValue> {
        let resource = super::wire_session::resource(resource)?;
        self.with_client(|client| {
            let view = client.resource_view(&resource).map_err(client_error)?;
            Ok(view
                .record(index)
                .map(|record| js_sys::Uint8Array::from(record.encode_to_vec().as_slice())))
        })?
    }

    /// Attach host-published controls to this observer's exact pool and slot.
    pub fn controls(
        &self,
        encoded_binding: &js_sys::Uint8Array,
    ) -> Result<super::agent_command::WasmAgentControls, JsValue> {
        let binding = super::agent_command::binding(encoded_binding)?;
        self.with_client(|client| {
            super::agent_command::WasmAgentControls::from_controls(client.controls(binding))
        })
    }

    /// Explicit cursors select a bounded page or retained live edge. The slot
    /// must already be authenticated on the supplied stream's session pool.
    #[wasm_bindgen(constructor)]
    pub fn new(
        stream: &WasmStream,
        agent: JsString,
        conversation_from: u64,
        status_from: u64,
        credential: u32,
    ) -> Result<Self, JsValue> {
        let agent = agent_path(agent)?;
        let pool = stream
            .session_client()
            .ok_or_else(|| error("client_no_session", "provider has no authenticated session"))?;
        Self::from_session(pool, agent, conversation_from, status_from, credential)
    }

    /// Replace focus (first row) and visible agents in the shared Rust core.
    /// Lengths are admitted before copying any array. Full-width initial
    /// cursors stay BigInt; overlapping interests retain their actual cursors.
    /// A pending next() wakes to observe the replacement, with no second loop.
    #[wasm_bindgen(js_name = setView)]
    pub fn set_view(
        &self,
        agents: js_sys::Array,
        conversation_from: js_sys::BigUint64Array,
        status_from: js_sys::BigUint64Array,
    ) -> Result<(), JsValue> {
        let selections = selections(agents, conversation_from, status_from)?;
        let mut selections = selections.into_iter();
        let focused = selections
            .next()
            .ok_or_else(|| error("selected_capacity", "view is empty"))?;
        {
            let mut client = self.client.borrow_mut();
            client
                .as_mut()
                .ok_or_else(|| error("client_closed", "agent client is closed"))?
                .set_view(focused, selections)
                .map_err(client_error)?;
        }
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// One viewport transaction: omitted tool-output panes are cancelled and
    /// evicted together with off-screen agents. Pass an empty resource array
    /// when no output pane is visible. Encoded values are SessionSubscription.
    #[wasm_bindgen(js_name = setViewport)]
    pub fn set_viewport(
        &self,
        agents: js_sys::Array,
        conversation_from: js_sys::BigUint64Array,
        status_from: js_sys::BigUint64Array,
        resources: js_sys::Array,
    ) -> Result<(), JsValue> {
        let selections = selections(agents, conversation_from, status_from)?;
        if resources.length() as usize
            > acyclic_harness_client::DemandPolicy::default().selected_capacity()
        {
            return Err(error("selected_capacity", "too many output panes"));
        }
        let mut subscriptions = Vec::with_capacity(resources.length() as usize);
        for value in resources.iter() {
            let encoded = value
                .dyn_into::<js_sys::Uint8Array>()
                .map_err(|_| error("invalid_subscription", "expected encoded subscription"))?;
            let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(&encoded, |length| {
                if length > acyclic_stream::MAX_COMMAND_BYTES as u64 {
                    Err(error(
                        "client_subscription_capacity",
                        "subscription exceeds byte bound",
                    ))
                } else {
                    Ok(())
                }
            })?;
            subscriptions.push(
                acyclic_stream::wire::SessionSubscription::decode(bytes.as_slice())
                    .map_err(|cause| error("invalid_subscription", cause))?,
            );
        }
        let mut selections = selections.into_iter();
        let focused = selections
            .next()
            .ok_or_else(|| error("selected_capacity", "view is empty"))?;
        self.client
            .borrow_mut()
            .as_mut()
            .ok_or_else(|| error("client_closed", "agent client is closed"))?
            .set_viewport(focused, selections, subscriptions)
            .map_err(client_error)?;
        self.wake_reader();
        Ok(())
    }

    /// Selected identities only, bounded by the same Rust interest policy.
    #[wasm_bindgen(js_name = selectedAgents, unchecked_return_type = "string[]")]
    pub fn selected_agents(&self) -> Result<JsValue, JsValue> {
        self.with_client(|client| to_js(&client.selected_agents().collect::<Vec<_>>()))?
    }

    /// Copy one displayed turn, never the complete selected history.
    #[wasm_bindgen(js_name = agentRecord, unchecked_return_type = "AgentEvent | null")]
    pub fn agent_record(&self, agent: JsString, index: usize) -> Result<JsValue, JsValue> {
        let agent = agent_path(agent)?;
        let record = self
            .with_client(|client| client.agent_entry(&agent, index))?
            .map_err(client_error)?;
        to_js(&record)
    }

    /// Read one actual lifecycle state directly from the shared Rust fold.
    #[wasm_bindgen(js_name = agentStatus, unchecked_return_type = "StatusRecord | null")]
    pub fn agent_status(&self, agent: JsString) -> Result<JsValue, JsValue> {
        let agent = agent_path(agent)?;
        let status = self
            .with_client(|client| client.agent_status(&agent))?
            .map_err(client_error)?;
        to_js(&status)
    }

    /// Window metadata for one selected conversation, without copying its
    /// records. Unknown agents are typed refusals rather than revision zero.
    #[wasm_bindgen(js_name = agentWindow, unchecked_return_type = "AgentWindow")]
    pub fn agent_window(&self, agent: JsString) -> Result<JsValue, JsValue> {
        let agent = agent_path(agent)?;
        let window = self.with_client(|client| {
            let view = client
                .agent_conversation(&agent)
                .ok_or_else(|| error("not_requested", "agent was not selected"))?;
            to_js(&AgentWindow {
                first_sequence: view.first_sequence(),
                next_sequence: view.next_sequence(),
                record_count: view.records().len(),
                retained_bytes: view.retained_bytes(),
                revision: client.session().revision(),
            })
        })??;
        Ok(window)
    }

    /// One transition decoded and folded by the exact native Rust core. There
    /// can be one pending read; a slow browser never creates a second queue.
    #[wasm_bindgen(unchecked_return_type = "AgentEvent")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        if self.reading.replace(true) {
            return Err(error(
                "client_read_busy",
                "an agent read is already pending",
            ));
        }
        let _reading = Reading {
            busy: &self.reading,
            waker: &self.waker,
        };
        let event = futures::future::poll_fn(|context| {
            let mut client = self.client.borrow_mut();
            let Some(client) = client.as_mut() else {
                return Poll::Ready(Err(error("client_closed", "agent client is closed")));
            };
            *self.waker.borrow_mut() = Some(context.waker().clone());
            client
                .poll_next(context)
                .map(|result| result.map_err(client_error))
        })
        .await?;
        // Exact 64-bit sequences cross as BigInt through the shared codec.
        to_js(&event)
    }

    /// Release follows and their retained views. Wake a pending read after all
    /// interior borrows are released; it reports closure, never a fake status.
    pub fn close(&self) {
        let client = self.client.borrow_mut().take();
        let waker = self.waker.borrow_mut().take();
        drop(client);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Read the shared fold's revision without copying conversation history.
    #[wasm_bindgen(js_name = viewRevision)]
    pub fn view_revision(&self) -> Result<u64, JsValue> {
        self.with_client(|client| client.session().revision())
    }

    /// The live-tail eviction edge, distinct from its recovery cursor.
    #[wasm_bindgen(js_name = firstSequence)]
    pub fn first_sequence(&self) -> Result<u64, JsValue> {
        self.with_client(|client| client.conversation().first_sequence())
    }

    /// Retained displayed turns, capped by the shared Rust StreamView policy.
    #[wasm_bindgen(js_name = recordCount)]
    pub fn record_count(&self) -> Result<usize, JsValue> {
        self.with_client(|client| client.conversation().records().len())
    }

    /// Decode and copy one visible turn through the same native reader.
    /// An index beyond the retained window returns null.
    #[wasm_bindgen(unchecked_return_type = "AgentEvent | null")]
    pub fn record(&self, index: usize) -> Result<JsValue, JsValue> {
        let entry = self
            .with_client(|client| client.conversation_entry(index))?
            .map_err(client_error)?;
        to_js(&entry)
    }

    /// Read the latest status from the Rust fold, or null until one is actually
    /// delivered. The browser does not maintain a second status map.
    #[wasm_bindgen(js_name = currentStatus, unchecked_return_type = "StatusRecord | null")]
    pub fn current_status(&self) -> Result<JsValue, JsValue> {
        let status = self
            .with_client(AgentClient::current_status)?
            .map_err(client_error)?;
        to_js(&status)
    }
}

impl WasmAgentClient {
    fn wake_reader(&self) {
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Browser hosts return the same client over their one authenticated pool.
    /// The host retains credential custody; no second WasmStream or transport
    /// is needed to observe agents created by that host.
    pub(crate) fn from_session(
        pool: acyclic_stream::session::Client,
        agent: AgentPath,
        conversation_from: u64,
        status_from: u64,
        credential: u32,
    ) -> Result<Self, JsValue> {
        let client = AgentClient::new(pool, agent, conversation_from, status_from, credential)
            .map_err(client_error)?;
        Ok(Self::from_client(client))
    }

    pub(super) fn from_client(client: AgentClient) -> Self {
        Self {
            client: RefCell::new(Some(client)),
            reading: Cell::new(false),
            waker: RefCell::new(None),
        }
    }

    fn with_client<T>(&self, read: impl FnOnce(&AgentClient) -> T) -> Result<T, JsValue> {
        let client = self.client.borrow();
        let client = client
            .as_ref()
            .ok_or_else(|| error("client_closed", "agent client is closed"))?;
        Ok(read(client))
    }
}

fn selections(
    agents: js_sys::Array,
    conversation_from: js_sys::BigUint64Array,
    status_from: js_sys::BigUint64Array,
) -> Result<Vec<AgentSelection>, JsValue> {
    let length = agents.length();
    if length == 0
        || length as usize > AgentClient::selected_capacity()
        || conversation_from.length() != length
        || status_from.length() != length
    {
        return Err(error(
            "selected_capacity",
            "view requires 1..=8 agents and matching cursor arrays",
        ));
    }
    let mut selections = Vec::with_capacity(length as usize);
    for index in 0..length {
        let agent = agents
            .get(index)
            .dyn_into::<JsString>()
            .map_err(|_| error("invalid_agent_path", "agent path is not a string"))?;
        selections.push(AgentSelection {
            agent: agent_path(agent)?,
            conversation_from: conversation_from.get_index(index),
            status_from: status_from.get_index(index),
        });
    }
    Ok(selections)
}
