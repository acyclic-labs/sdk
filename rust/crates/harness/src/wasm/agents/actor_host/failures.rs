//! Thin projection of the original Actors decoder and Stream publication.
use super::{WasmActorHost, failed};
use acyclic_actors::local::{FailureDisposition, FailureEntry, FailureResolution};
use acyclic_stream::{IdempotencyKey, Record, StreamPath, wire, wire_codec};
use prost::Message as _;
use wasm_bindgen::prelude::*;

/// An explicit terminal choice; no automatic retry or deletion policy.
#[wasm_bindgen]
pub enum ActorFailureResolution {
    /// Deliver the retained original input under a new sequence.
    Replay,
    /// Retire the failure without delivering the input.
    Purge,
}

fn record(bytes: &[u8], path: &StreamPath) -> Result<Record, JsValue> {
    let record = wire::Record::decode(bytes).map_err(failed)?;
    // Reads/follows elide their selected path; child-tail projections spell it.
    let followed = record.origin.is_empty().then_some(path);
    wire_codec::record(record, followed).map_err(failed)
}

/// One selected failure tail decoded by Rust's sole versioned implementation.
/// Pages and follows remain ordinary Streams operations; this holds no cursor,
/// provider, background work or private copy of the retained input body.
#[wasm_bindgen]
pub struct WasmActorFailure {
    entry: FailureEntry,
}

#[wasm_bindgen]
impl WasmActorFailure {
    /// `path` is the selected child, and `record` is its projected `status`
    /// tail encoded with the canonical Streams Record protobuf.
    pub fn decode(path: &str, record_bytes: &[u8]) -> Result<Self, JsValue> {
        let path = StreamPath::new(path).map_err(failed)?;
        let status = StreamPath::new(format!("{path}/status")).map_err(failed)?;
        Ok(Self {
            entry: FailureEntry::decode(&path, &record(record_bytes, &status)?).map_err(failed)?,
        })
    }

    /// Absent for an unresolved failure; otherwise the committed terminal.
    pub fn resolution(&self) -> Option<ActorFailureResolution> {
        match &self.entry {
            FailureEntry::Failed(_) => None,
            FailureEntry::Resolved(FailureResolution::Replay) => {
                Some(ActorFailureResolution::Replay)
            }
            FailureEntry::Resolved(FailureResolution::Purge) => Some(ActorFailureResolution::Purge),
        }
    }

    /// Fetch sequence zero only when replay or display needs the original body.
    #[wasm_bindgen(js_name = inputPath)]
    pub fn input_path(&self) -> Result<Option<String>, JsValue> {
        match &self.entry {
            FailureEntry::Failed(input) => input
                .input_path()
                .map(|path| Some(path.to_string()))
                .map_err(failed),
            FailureEntry::Resolved(_) => Ok(None),
        }
    }

    /// The original typed guest cause, using its existing serde representation.
    /// Terminals carry no second copy of the failed input or its cause.
    pub fn cause(&self) -> Result<JsValue, JsValue> {
        match &self.entry {
            FailureEntry::Failed(input) => super::super::to_js(input.cause()),
            FailureEntry::Resolved(_) => Ok(JsValue::NULL),
        }
    }
}

#[wasm_bindgen]
impl WasmActorHost {
    /// Page this ordinary subtree with `tail_stream = "status"`. Follow the
    /// returned tail's origin for further disposition updates.
    #[wasm_bindgen(js_name = failuresPath)]
    pub fn failures_path(&self) -> Result<String, JsValue> {
        self.host
            .store()
            .failures_path(self.host.actor())
            .map(|path| path.to_string())
            .map_err(failed)
    }

    /// Return a canonical Streams CommitRequest, without submitting it. Send
    /// through the original authenticated Streams commit owner, retaining these
    /// SAME bytes/key through Unknown; a new key would be a new intent.
    /// Replay requires the captured sequence-zero Record; purge accepts none.
    #[wasm_bindgen(js_name = prepareDisposition)]
    pub fn prepare_disposition(
        &self,
        failure: &WasmActorFailure,
        resolution: ActorFailureResolution,
        captured: Option<Vec<u8>>,
        key: Vec<u8>,
    ) -> Result<Vec<u8>, JsValue> {
        let FailureEntry::Failed(input) = &failure.entry else {
            return Err(failed("failure already has a terminal disposition"));
        };
        if matches!(resolution, ActorFailureResolution::Replay) != captured.is_some() {
            return Err(failed("only replay requires the original captured input"));
        }
        let captured = captured
            .as_deref()
            .map(|bytes| record(bytes, &input.input_path().map_err(failed)?))
            .transpose()?;
        let disposition = match (resolution, captured.as_ref()) {
            (ActorFailureResolution::Replay, Some(captured)) => {
                FailureDisposition::Replay(captured)
            }
            (ActorFailureResolution::Purge, None) => FailureDisposition::Purge,
            _ => return Err(failed("only replay requires the original captured input")),
        };
        let request = self
            .host
            .prepare_disposition(
                input,
                disposition,
                IdempotencyKey::new(key).map_err(failed)?,
            )
            .and_then(|prepared| prepared.into_request())
            .map_err(failed)?;
        Ok(wire_codec::commit_to_wire(&request).encode_to_vec())
    }
}
