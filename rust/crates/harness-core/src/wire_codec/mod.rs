//! Canonical typed wire conversions.
#[cfg(test)]
use crate::contract::canonical_json_bytes;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
use crate::core::ApplyResult;
use crate::{
    AgentId, Capabilities, Error, IdempotencyKey, NativeCustody, OperationId, Result,
    core::{
        Action, AggregateKind, Authority, Command, Event, EventPayload, EventReference,
        RecordedScope, Scope,
    },
    store::{guarded_boundary as boundary, history::HistoryIo},
    wire,
};
use wire::ErrorCode::{Invalid, Storage};
const EVENT_WIRE_VERSION: &str = "2";
const PAYLOAD_BOUNDS: crate::executor::json::JsonBounds = crate::executor::json::JsonBounds {
    maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
    maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
    maximum_depth: 256,
};
mod admission;
pub(crate) use admission::Quota as CodecQuota;
mod event;
pub(crate) use event::{decode_event, encode_event};
pub use event::{decode_event_payload, encode_event_envelope};
mod command;
pub use command::decode_command_envelope;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
#[doc(hidden)]
pub use command::{decode_command, encode_apply_result};
mod protocol;
#[doc(hidden)]
pub use protocol::{COMPILED_PROTOCOL, protocol_identity, validate_protocol};
mod errors;
pub use errors::encode_error;
mod authority;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
#[doc(hidden)]
pub use authority::decode_aggregate_kind;
pub use authority::encode_authority;
pub(crate) use authority::{decode_authority, decode_scope};
use authority::{decode_reference, encode_reference, parse_scope_agent};
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod admission_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod command_tests;
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod event_decode_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod protocol_tests;
#[cfg(test)]
mod tests;
