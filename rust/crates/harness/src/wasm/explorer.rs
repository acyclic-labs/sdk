//! Browser access to the one native reader: the agent tree and the open
//! agent's history over a [`WasmStream`] provider, rendered from committed
//! records. Persistence is the reader's snapshot, which the page stores in
//! IndexedDB under the same byte budget.

use super::json::to_js_client;
use acyclic_harness_client::{
    Explorer, ReaderPolicy,
    explorer::{HistoryRow, Pos, Row},
    reader::{Connectivity, Driver, ReadIssue},
};
use acyclic_stream::{StreamPath, StreamProvider, WasmStream};
use js_sys::{Function, Uint8Array};
use serde::Serialize;
use std::{cell::RefCell, rc::Rc, task::Waker};
use wasm_bindgen::prelude::*;

struct State {
    explorer: Explorer,
    driver: Driver<dyn StreamProvider>,
    waker: Option<Waker>,
    changed: Function,
    origin: f64,
    closed: bool,
}

impl State {
    fn now(&self) -> u64 {
        // Whole milliseconds since the explorer opened.
        (js_sys::Date::now() - self.origin).max(0.0) as u64
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// `value` with `bytes` set as a `Uint8Array` field, or `null`.
fn with_bytes(value: &JsValue, field: &str, bytes: Option<&[u8]>) -> Result<(), JsValue> {
    let bytes = bytes.map_or(JsValue::NULL, |bytes| Uint8Array::from(bytes).into());
    js_sys::Reflect::set(value, &JsValue::from_str(field), &bytes).map(drop)
}

fn error(cause: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&cause.to_string()).into()
}

fn agent(path: &str) -> Result<acyclic_harness_client::AgentPath, JsValue> {
    acyclic_harness_client::AgentPath::try_from(path.to_owned()).map_err(error)
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum RowView<'a> {
    #[serde(rename_all = "camelCase")]
    Agent {
        path: &'a str,
        depth: usize,
        status: Option<&'a acyclic_harness_client::StatusRecord>,
        conversation: Option<u64>,
        expanded: bool,
    },
    Loading {
        depth: usize,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Health<'a> {
    connectivity: &'static str,
    retry_at: Option<u64>,
    cache_bytes: usize,
    requests: usize,
    follows: usize,
    wake_in: Option<u64>,
    issues: Vec<Issue<'a>>,
}

#[derive(Serialize)]
struct Issue<'a> {
    stream: &'a str,
    kind: &'static str,
}

fn depth(pos: &Pos) -> usize {
    let parent: &StreamPath = match pos {
        Pos::Row { parent, .. } | Pos::Gap { parent, .. } => parent,
    };
    parent.as_str().matches("/children").count()
}

/// The agent tree and history explorer over one browser stream session.
#[wasm_bindgen]
pub struct WasmExplorer {
    state: Rc<RefCell<State>>,
}

#[wasm_bindgen]
impl WasmExplorer {
    /// Opens an explorer whose reads and follows share `stream`'s session.
    /// `changed` is called whenever work completes (records, replies,
    /// failures) or the wake deadline moves; the page then renders the next
    /// frame and re-arms its one timer. `memory` is the device memory budget in bytes
    /// (0 keeps the default).
    ///
    /// # Errors
    /// Rejects an invalid reader policy.
    #[wasm_bindgen(constructor)]
    pub fn new(stream: &WasmStream, changed: Function, memory: f64) -> Result<Self, JsValue> {
        // Whole bytes; the budget saturates at u64::MAX.
        let policy = if memory > 0.0 {
            ReaderPolicy::for_device_memory(memory as u64)
        } else {
            ReaderPolicy::default()
        };
        let state = Rc::new(RefCell::new(State {
            explorer: Explorer::new(policy).map_err(error)?,
            driver: Driver::new(stream.provider()),
            waker: None,
            changed,
            origin: js_sys::Date::now(),
            closed: false,
        }));
        let task = Rc::downgrade(&state);
        wasm_bindgen_futures::spawn_local(std::future::poll_fn(move |cx| {
            let Some(state) = task.upgrade() else {
                return std::task::Poll::Ready(());
            };
            let changed = {
                let mut state = state.borrow_mut();
                if state.closed {
                    return std::task::Poll::Ready(());
                }
                let now = state.now();
                let progress = |reader: &acyclic_harness_client::Reader| {
                    (reader.generation(), reader.next_deadline())
                };
                let before = progress(state.explorer.reader_ref());
                let State {
                    explorer, driver, ..
                } = &mut *state;
                driver.step(explorer.reader(), now, cx);
                state.waker = Some(cx.waker().clone());
                (progress(state.explorer.reader_ref()) != before).then(|| state.changed.clone())
            };
            // Outside the borrow: the page may render a frame synchronously.
            if let Some(changed) = changed {
                drop(changed.call0(&JsValue::NULL));
            }
            std::task::Poll::Pending
        }));
        Ok(Self { state })
    }

    /// Plans one frame and returns the visible tree rows.
    ///
    /// # Errors
    /// Fails only if a row cannot be converted for JavaScript.
    pub fn frame(&self) -> Result<js_sys::Array, JsValue> {
        let mut state = self.state.borrow_mut();
        let now = state.now();
        let rows = js_sys::Array::new();
        for (pos, row) in state.explorer.frame(now) {
            let (view, latest) = match row {
                Row::Agent {
                    path,
                    status,
                    conversation,
                    latest,
                    expanded,
                } => (
                    RowView::Agent {
                        path: path.as_str(),
                        depth: depth(pos),
                        status: status.as_ref(),
                        conversation: *conversation,
                        expanded: *expanded,
                    },
                    Some(latest.as_deref()),
                ),
                Row::Loading { depth } => (RowView::Loading { depth: *depth }, None),
            };
            let view = to_js_client(&view)?;
            if let Some(latest) = latest {
                with_bytes(&view, "latest", latest)?;
            }
            rows.push(&view);
        }
        state.wake();
        Ok(rows)
    }

    /// The open agent's history window, rendered from the cache.
    ///
    /// # Errors
    /// Fails only if a row cannot be converted for JavaScript.
    pub fn history(&self) -> Result<js_sys::Array, JsValue> {
        let state = self.state.borrow();
        let rows = js_sys::Array::new();
        for HistoryRow { sequence, value } in state.explorer.history() {
            let row = js_sys::Object::new();
            js_sys::Reflect::set(&row, &"sequence".into(), &js_sys::BigInt::from(sequence))?;
            with_bytes(&row, "value", value.as_deref())?;
            rows.push(&row);
        }
        Ok(rows)
    }

    /// Connectivity, residency and the delay of the one timer the page arms.
    ///
    /// # Errors
    /// Fails only if the value cannot be converted for JavaScript.
    pub fn health(&self) -> Result<JsValue, JsValue> {
        let state = self.state.borrow();
        let now = state.now();
        let reader = state.explorer.reader_ref();
        let residency = reader.residency();
        let (connectivity, retry_at) = match reader.connectivity() {
            Connectivity::Online => ("online", None),
            Connectivity::Degraded { retry_at } => ("degraded", Some(retry_at)),
            Connectivity::Offline { retry_at, .. } => ("offline", Some(retry_at)),
        };
        to_js_client(&Health {
            connectivity,
            retry_at,
            cache_bytes: residency.cache_bytes,
            requests: residency.requests,
            follows: residency.follows,
            wake_in: reader
                .next_deadline()
                .map(|deadline| deadline.saturating_sub(now)),
            issues: reader
                .issues()
                .map(|(stream, issue)| Issue {
                    stream: stream.as_str(),
                    kind: match issue {
                        ReadIssue::AccessDenied => "accessDenied",
                        ReadIssue::RetentionLost => "retentionLost",
                        ReadIssue::HierarchyChanged => "hierarchyChanged",
                    },
                })
                .collect(),
        })
    }

    /// Rows shown and rows kept warm on each side.
    #[wasm_bindgen(js_name = setViewport)]
    pub fn set_viewport(&self, height: u32, overscan: u32, frame_ms: u32) {
        self.state.borrow_mut().explorer.set_viewport(
            height as usize,
            overscan as usize,
            u64::from(frame_ms),
        );
    }

    /// Scrolls the tree by `rows`; the sign is the direction.
    pub fn scroll(&self, rows: i32) {
        self.state.borrow_mut().explorer.scroll(i64::from(rows));
    }

    /// Pointer intent over an agent row, or none.
    ///
    /// # Errors
    /// Rejects an invalid agent path.
    pub fn hover(&self, path: Option<String>) -> Result<(), JsValue> {
        let agent = path.as_deref().map(agent).transpose()?;
        self.state.borrow_mut().explorer.hover(agent);
        Ok(())
    }

    /// Keyboard focus on an agent row, or none.
    ///
    /// # Errors
    /// Rejects an invalid agent path.
    pub fn focus(&self, path: Option<String>) -> Result<(), JsValue> {
        let agent = path.as_deref().map(agent).transpose()?;
        self.state.borrow_mut().explorer.focus(agent);
        Ok(())
    }

    /// Shows an agent's children.
    ///
    /// # Errors
    /// Rejects an invalid agent path.
    pub fn expand(&self, path: &str) -> Result<(), JsValue> {
        self.state.borrow_mut().explorer.expand(&agent(path)?);
        Ok(())
    }

    /// Hides an agent's children.
    ///
    /// # Errors
    /// Rejects an invalid agent path.
    pub fn collapse(&self, path: &str) -> Result<(), JsValue> {
        self.state.borrow_mut().explorer.collapse(&agent(path)?);
        Ok(())
    }

    /// Opens an agent's history at its end, `rows` tall.
    ///
    /// # Errors
    /// Rejects an invalid agent path.
    pub fn open(&self, path: &str, rows: u32) -> Result<(), JsValue> {
        self.state
            .borrow_mut()
            .explorer
            .open(agent(path)?, rows as usize);
        Ok(())
    }

    /// Closes the history view.
    pub fn close(&self) {
        self.state.borrow_mut().explorer.close();
    }

    /// Moves the history window to `sequence`.
    pub fn seek(&self, sequence: u64) {
        self.state.borrow_mut().explorer.seek(sequence);
    }

    /// Moves the history window to the live end.
    #[wasm_bindgen(js_name = jumpEnd)]
    pub fn jump_end(&self) {
        self.state.borrow_mut().explorer.jump_end();
    }

    /// The browser's online state changed.
    #[wasm_bindgen(js_name = networkChanged)]
    pub fn network_changed(&self, online: bool) {
        let mut state = self.state.borrow_mut();
        let now = state.now();
        state.explorer.reader().network_changed(online, now);
        state.wake();
    }

    /// The cache as bytes for IndexedDB.
    pub fn snapshot(&self) -> Uint8Array {
        Uint8Array::from(&self.state.borrow().explorer.reader_ref().snapshot()[..])
    }

    /// Restores a snapshot written by [`Self::snapshot`].
    ///
    /// # Errors
    /// Rejects a snapshot that does not decode; the cache is then unchanged.
    pub fn restore(&self, bytes: &[u8]) -> Result<(), JsValue> {
        self.state
            .borrow_mut()
            .explorer
            .reader()
            .restore(bytes)
            .map_err(error)
    }
}

impl Drop for WasmExplorer {
    /// Ends the driver task, which drops every follow and read.
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.try_borrow_mut() {
            state.closed = true;
            state.wake();
        }
    }
}
