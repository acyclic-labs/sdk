//! Closed Snapshot-schema quota for the original authenticated checkpoint path.

use super::{JsonBounds, JsonCodecCharge};
use crate::{Error, Result, core::Snapshot};

pub(super) const SNAPSHOT_DIAGNOSTICS: &str = concat!(
    "Snapshot Projection ConversationStateWire ArchivedConversationPrefix ",
    "format_version authority revision events projection state_digest attestation ",
    "lifecycle active_extensions active_extension_revision extension_states ",
    "configured_extensions active_configurations effects indexed_terminal_effects ",
    "effects_archived forks published_merges conversation latest_context_checkpoint ",
    "interactions bindings capabilities issuer intent_digest scope causal_parent ",
    "messages archived_prefix through_sequence history_digest ",
    "archived conversation prefix is unbound conversation order or identity is invalid ",
    "message identity is nil message sequence must start at one ",
    "extension key must be namespaced tool linkage does not match message kind ",
    "file or volume label is invalid checkpoint snapshot is invalid ",
);

/// Same-pass lexical and selected Raw-Value admission, then ONE ownership quota.
pub(crate) fn snapshot_json_admission<F: FnMut(JsonCodecCharge) -> Result<()> + ?Sized>(
    bytes: &[u8], bounds: JsonBounds, charge: &mut F,
) -> Result<(u64, u64)> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let value_shape = super::json::admit_json_value_shape(
        bytes, bounds, super::json::JsonValueSchema::Snapshot, &mut |event| charge(event),
    )?;
    let admission = super::event_json::json_decode_owned_bytes(value_shape, true, None)?;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes: admission.0, value_shape })?;
    Ok((admission.0, admission.2))
}

