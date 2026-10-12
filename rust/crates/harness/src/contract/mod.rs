//! Common harness identities and operation outcomes.

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

/// Maximum UTF-8 byte length for names crossing a component boundary.
pub const COMPONENT_LABEL_MAX_BYTES: usize = crate::conversation::MAX_PORTABLE_COUNT;

/// Exact labels and separators rejected by every component registry.
pub const COMPONENT_LABEL_FORBIDDEN_EXACT: [&str; 2] = [".", ".."];
/// Separators rejected by every component registry.
pub const COMPONENT_LABEL_FORBIDDEN_SEPARATORS: [char; 2] = ['/', '\\'];

/// Capability names the substrate checks; scopes grant them as plain strings.
pub(crate) mod capability {
    use std::fmt::Display;

    pub(crate) const LIFECYCLE_MANAGE: &str = "lifecycle:manage";
    pub(crate) const EVENT_APPEND: &str = "event:append";
    pub(crate) const EXTENSION_MIGRATE: &str = "extension:migrate";
    pub(crate) const EXTENSION_ACTIVATE: &str = "extension:activate";
    pub(crate) const EXTENSION_CONFIGURE: &str = "extension:configure";
    pub(crate) const EFFECT_RUN: &str = "effect:run";
    pub(crate) const EFFECT_PLAN: &str = "effect:plan";
    pub(crate) const FORK_PUBLISH: &str = "fork:publish";
    pub(crate) const PROJECT_MERGE: &str = "project:merge";
    pub(crate) const CONVERSATION_BIND: &str = "conversation:bind";
    pub(crate) const CONVERSATION_APPEND: &str = "conversation:append";
    pub(crate) const CONVERSATION_SELECT_CONTEXT: &str = "conversation:select_context";
    pub(crate) const INTERACTION_OPEN: &str = "interaction:open";
    pub(crate) const INTERACTION_RESOLVE: &str = "interaction:resolve";
    pub(crate) const INTERACTION_ROUTE: &str = "interaction:route";
    pub(crate) const OPERATION_OBSERVE: &str = "operation:observe";
    pub(crate) const OPERATION_CANCEL: &str = "operation:cancel";
    pub(crate) const OPERATION_DECLARE: &str = "operation:declare";
    pub(crate) const MAIL_SEND: &str = "mail:send";
    pub(crate) const MAIL_READ: &str = "mail:read";
    pub(crate) const TIMER_WAIT: &str = "timer:wait";
    pub(crate) const MODEL_GENERATE: &str = "model:generate";
    pub(crate) const CONTEXT_BUILD: &str = "context:build";

    pub(crate) fn effect_provider(provider: impl Display) -> String {
        format!("effect:provider:{provider}")
    }

    pub(crate) fn tool_call(tool: impl Display) -> String {
        format!("tool:call:{tool}")
    }

    pub(crate) fn interaction_respond(id: impl Display) -> String {
        format!("interaction:respond:{id}")
    }
}

/// Successor of a durable revision or sequence, failing instead of wrapping.
pub(crate) fn next_revision(revision: u64) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("revision exhausted".into()))
}

mod identity;
pub(crate) use identity::validate_component_label;
pub use identity::*;
mod policy;
pub use policy::*;
mod error;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use error::native_work_error_code;
pub use error::*;
mod encoding;
pub(crate) use encoding::*;
mod schema;
pub(crate) use schema::*;
mod formatting;
pub(crate) use formatting::preparation_format;
#[cfg(test)]
mod tests;

mod json;
pub(crate) use json::{
    admit_json, admit_json_value_shape, json_owned_value_shape_bounded,
    json_typed_value_decode_owned_bytes, json_value_virtual_owned_bytes,
    lexical_json_diagnostic_bytes, owned_logical_bytes,
};
mod event_json;
pub(crate) use event_json::{
    audited_serde_diagnostic_bytes, event_payload_json_admission, json_decode_owned_bytes,
};
pub use event_json::{json_typed_diagnostic_owned_bytes, preparation_serde_diagnostic_label_bytes};
mod snapshot_json;
pub use json::json_from_value_bounded;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use json::validate_json_byte_bound_native_trusted;
pub use json::{
    JsonBounds, JsonCodecCharge, JsonValueSchema, JsonValueShape, canonical_json_bytes_bounded,
    json_bytes_bounded, json_from_slice_bounded, json_owned_value_conversion_additional_bytes,
    json_value_bounded, json_value_decode_owned_bytes, sdk_json_diagnostic_bytes,
};
pub(crate) use snapshot_json::snapshot_json_admission;
