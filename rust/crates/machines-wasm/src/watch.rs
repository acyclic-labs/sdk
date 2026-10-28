//! The browser watch owns the same pull cursor as Stream follows.
use acyclic_machines::{
    OperationStream,
    core::{CursorStatus, PullCursor, PullError},
};
use futures::StreamExt;
use wasm_bindgen::prelude::*;

/// A live operation watch with one observation of demand per `next` call.
#[wasm_bindgen]
pub struct OperationWatch {
    state: PullCursor<acyclic_machines::OperationObservation, acyclic_machines::ProviderError>,
}

impl OperationWatch {
    pub(crate) fn new(stream: OperationStream) -> Self {
        Self {
            state: PullCursor::new(stream.boxed_local()),
        }
    }
}

#[wasm_bindgen]
impl OperationWatch {
    /// Pulls one correlated observation; null means the cursor has ended.
    #[wasm_bindgen(unchecked_return_type = "OperationOut | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        match self.state.next().await.map_err(|error| match error {
            PullError::Source(error) => crate::public::provider_error(error),
            PullError::Busy => crate::js_error(
                "busy",
                "watch next already pending; recovery: await it before pulling again",
            ),
            PullError::Poisoned => crate::js_error(
                "unavailable",
                "watch ownership failed; recovery: reopen from the original operation identity",
            ),
        })? {
            Some(value) => crate::public::to_js(&crate::public::operation_out(value)),
            None => Ok(JsValue::NULL),
        }
    }

    /// Open, completed, closed, or failed; source failure also rejects `next`.
    #[wasm_bindgen(getter)]
    pub fn status(&self) -> String {
        match self.state.status() {
            CursorStatus::Open => "open",
            CursorStatus::Completed => "completed",
            CursorStatus::Closed => "closed",
            CursorStatus::Failed => "failed",
        }
        .into()
    }

    /// Cancels the observation stream and wakes any pending `next`.
    pub fn close(&self) {
        self.state.close();
    }
}
