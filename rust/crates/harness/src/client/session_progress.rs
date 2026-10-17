//! Operation-bound lifecycle needed by legacy replay settlement, never rendering.
use crate::{Result, core::EventPayload, wire};

pub(super) fn authority_key(authority: &wire::Authority) -> String {
    let kind = wire::AggregateKind::try_from(authority.kind)
        .map_or("AGGREGATE_KIND_UNSPECIFIED", |kind| kind.as_str_name());
    format!("{kind}:{}", authority.id)
}
pub(super) fn payload(event: &wire::EventEnvelope) -> Result<EventPayload> {
    crate::wire_codec::decode_event_payload(
        &event.event_type,
        &event.canonical_payload_json,
        &mut None,
    )
    .map(|(payload, _)| payload)
}
