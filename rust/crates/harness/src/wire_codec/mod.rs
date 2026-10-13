//! Canonical typed wire conversions shared by native and WASM consumers.

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
use crate::core::ApplyResult;
use crate::core::RecordedScope;
use crate::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    contract::canonical_json_bytes,
    core::{Action, AggregateKind, Authority, Command, Event, EventPayload, EventReference, Scope},
    wire,
};
mod admission;
pub(crate) use admission::Quota as CodecQuota;
#[cfg(all(test, feature = "filesystem"))]
mod admission_tests;

const EVENT_WIRE_VERSION: &str = "2";

mod event;
pub(crate) use event::{decode_event, encode_event};
pub use event::{decode_event_payload, encode_event_envelope};
mod command;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) use command::decode_command;
pub use command::decode_command_envelope;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) use command::encode_apply_result;
mod protocol;
pub(crate) use protocol::{protocol_identity, validate_protocol};
mod errors;
pub use errors::encode_error;
mod authority;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) use authority::decode_aggregate_kind;
pub use authority::encode_authority;
pub(crate) use authority::{decode_authority, decode_scope};
use authority::{decode_reference, encode_reference};
#[cfg(test)]
mod tests;

fn boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}
