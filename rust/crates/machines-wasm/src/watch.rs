//! Pull-only projection of the original operation observation stream.

use std::cell::Cell;

use acyclic_machines::{
    MachinesProvider, OperationId, OperationObservation, OperationPhase, ProviderError,
};
use acyclic_stream::pull::PullCursor;
use futures::StreamExt;
use wasm_bindgen::prelude::*;

use crate::public;

/// One live operation watch. Closing stops observation, never the remote operation.
#[wasm_bindgen]
pub struct WasmOperationWatch {
    cursor: PullCursor<OperationObservation, ProviderError>,
    operation: OperationId,
    closed: Cell<bool>,
}

pub(crate) async fn open(
    provider: &impl MachinesProvider,
    operation: JsValue,
) -> Result<WasmOperationWatch, JsValue> {
    let operation = public::operation(public::from_js(operation)?)?;
    let stream = provider
        .watch_operation(operation)
        .await
        .map_err(public::provider_error)?;
    Ok(WasmOperationWatch {
        cursor: PullCursor::new(
            stream.boxed_local(),
            ProviderError::OperationIndeterminate(operation),
        ),
        operation,
        closed: Cell::new(false),
    })
}

#[wasm_bindgen]
impl WasmOperationWatch {
    /// Pulls one observation. A terminal observation does not wait for remote EOF.
    #[wasm_bindgen(unchecked_return_type = "OperationOut | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        if self.closed.get() {
            return Ok(JsValue::NULL);
        }
        let observation = self.cursor.next().await;
        match observation {
            Ok(Some(value)) if value.id == self.operation => {
                if value.phase != OperationPhase::Pending {
                    self.close();
                }
                public::to_js(&public::operation_out(value))
            }
            Ok(None) if self.closed.get() => Ok(JsValue::NULL),
            result => {
                self.close();
                Err(public::provider_error(match result {
                    Err(error) => error,
                    _ => ProviderError::OperationIndeterminate(self.operation),
                }))
            }
        }
    }

    /// Wakes a pending pull and releases the original observation source.
    pub fn close(&self) {
        self.closed.set(true);
        self.cursor.close();
    }
}

impl Drop for WasmOperationWatch {
    fn drop(&mut self) {
        self.close();
    }
}
