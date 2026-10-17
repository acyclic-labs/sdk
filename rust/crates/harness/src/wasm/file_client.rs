//! Browser handles for the Rust file client; platform recovery owns draining.
use super::{
    file_worker::{Command, State, start},
    json::{from_js, to_js},
    query::WasmTaskVolume,
};
use crate::client::mailbox;
use crate::client::waiting_calls::WaitingCalls;
use crate::client::{FileClient, FileClientError, FileClientLimits};
use std::rc::Rc;
use wasm_bindgen::prelude::*;

pub(super) fn error(value: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&value.to_string())
}
pub(super) fn admission_error(value: FileClientError) -> JsValue {
    to_js(&value).unwrap_or_else(|error| error)
}

/// One ordered Rust owner of private outbox and remote task publication.
#[wasm_bindgen]
pub struct WasmFileClient {
    commands: Rc<mailbox::Sender<Command>>,
    state: Rc<State>,
    waiting: Rc<WaitingCalls>,
}

#[wasm_bindgen]
impl WasmFileClient {
    /// Restores intent, then drains remote targets after enqueue and push recovery.
    #[wasm_bindgen]
    pub async fn open(
        outbox: &WasmTaskVolume,
        target: &WasmTaskVolume,
        #[wasm_bindgen(unchecked_param_type = "FileClientLimits")] limits: JsValue,
    ) -> Result<WasmFileClient, JsValue> {
        let limits = from_js::<FileClientLimits>(limits)?;
        let inner = FileClient::open(outbox.volume.clone(), target.volume.clone(), limits)
            .await
            .map_err(admission_error)?;
        let (commands, state) = start(inner, outbox, target).await?;
        Ok(Self {
            commands: Rc::new(commands),
            state,
            waiting: WaitingCalls::new(limits),
        })
    }

    /// Length admission happens in Rust before this JavaScript handle is copied.
    #[wasm_bindgen(unchecked_return_type = "Promise<void>")]
    pub fn enqueue(
        &self,
        #[wasm_bindgen(unchecked_param_type = "FileKey")] key: JsValue,
        bytes: js_sys::Uint8Array,
    ) -> js_sys::Promise {
        let key = match from_js(key) {
            Ok(key) => key,
            Err(failure) => return js_sys::Promise::reject(&failure),
        };
        if let Err(failure) = crate::client::validate_request_key(&key) {
            return js_sys::Promise::reject(&admission_error(failure));
        }
        // Copy synchronously after admission. No future or command retains JS storage.
        let (bytes, reservation) = match super::bytes::snapshot(&bytes, |length| {
            self.waiting.reserve(length).map_err(admission_error)
        }) {
            Ok(snapshot) => snapshot,
            Err(failure) => return js_sys::Promise::reject(&failure),
        };
        let commands = self.commands.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            let _reservation = reservation;
            let (reply, result) = mailbox::channel();
            commands
                .send(Command::Enqueue { key, bytes, reply })
                .await
                .map_err(|()| error("client owner stopped"))?;
            result.await.map_err(|()| error("client owner stopped"))??;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Attempts publication now. Indeterminate requests remain durably pending.
    #[wasm_bindgen]
    pub async fn flush(&self) -> Result<usize, JsValue> {
        let _reservation = self.waiting.reserve(0).map_err(admission_error)?;
        let (reply, result) = mailbox::channel();
        self.commands
            .send(Command::Flush(reply))
            .await
            .map_err(|()| error("client owner stopped"))?;
        result.await.map_err(|()| error("client owner stopped"))?
    }

    /// Waits for automatic remote draining, without transport or retry code in JS.
    #[wasm_bindgen(js_name = whenIdle)]
    pub async fn when_idle(&self) -> Result<(), JsValue> {
        let reservation = self.waiting.reserve(0).map_err(admission_error)?;
        let (reply, result) = mailbox::channel();
        self.commands
            .send(Command::Idle(reply))
            .await
            .map_err(|()| error("client owner stopped"))?;
        // The owner bounds registered idle waiters separately. Leave capacity for
        // the flush that makes this wait complete, even at a one-command limit.
        drop(reservation);
        result.await.map_err(|()| error("client owner stopped"))?
    }

    /// Current pending count derived from the owned Rust client.
    #[wasm_bindgen]
    pub fn pending(&self) -> usize {
        self.state.pending.get()
    }

    /// Current acknowledged outbox cursor.
    #[wasm_bindgen(unchecked_return_type = "Cursor")]
    pub fn cursor(&self) -> Result<JsValue, JsValue> {
        to_js(&*self.state.cursor.borrow())
    }

    /// Most recent automatic publication error, including a retired rejection.
    #[wasm_bindgen(js_name = lastError)]
    pub fn last_error(&self) -> JsValue {
        self.state.error.borrow().clone()
    }

    /// Stops waiting and leaves all unacknowledged intent in its volume.
    #[wasm_bindgen]
    pub fn close(&self) {
        self.commands.close();
    }
}
