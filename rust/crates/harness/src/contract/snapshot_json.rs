//! Ownership admission for authenticated snapshot decoding.
use super::{JsonBounds, JsonCodecCharge};
pub(super) const SNAPSHOT_DIAGNOSTIC_BYTES: u64 = 784;
pub(crate) fn snapshot_json_admission<F: FnMut(JsonCodecCharge) -> crate::Result<()> + ?Sized>(bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> crate::Result<(u64, u64)> {
    let value_shape = super::json::admit_json_value_shape(bytes, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, super::json::JsonValueSchema::Snapshot, &mut |event| charge(event))?;
    let (maximum_owned_bytes, _, conversation) = super::event_json::json_decode_owned_bytes(value_shape, true, None)?;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?; Ok((maximum_owned_bytes, conversation))
}
