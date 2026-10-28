//! Canonical protocol, authority and error wire conversions.
#[cfg(feature = "filesystem")]
use crate::NativeCustody;
use crate::{
    AgentId, Capabilities, Error, Result,
    core::{AggregateKind, Authority, Scope},
    store::{guarded_boundary as boundary, history::HistoryIo},
    wire,
};
use wire::ErrorCode::{Invalid, Storage};
const PROTOCOL_VERSION: &str = "2";
mod admission;
pub(crate) use admission::Quota as CodecQuota;
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
#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod admission_tests;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod protocol_tests;
