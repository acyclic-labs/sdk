//! Checked ownership admission for history event and snapshot decoding.
use super::{JsonBounds, JsonCodecCharge, JsonValueShape, Result};
use crate::{
    conversation::{Attachment, ConversationMessage, FileRef},
    core::{ExtensionConfiguration, ExtensionDependency},
    fork::{CapturedResource, ForkOmission, ForkSeed, ReferenceGrant, SharedGrant},
    merge::ProjectMergeReceipt,
};
use serde::__private229::de::Content;
use serde_json::Value;
use std::mem::size_of;
use uuid::Uuid;
const EVENT_LABEL_BYTES: u64 = 3994;
const COMMON_LABEL_BYTES: u64 = 317;
/// Static diagnostic allowance for the SDK's preparation records.
pub const fn preparation_serde_diagnostic_label_bytes() -> u64 {
    9862
}
/// Owned diagnostic quota for an admitted typed value.
pub fn json_typed_diagnostic_owned_bytes(
    shape: JsonValueShape,
    labels: u64,
    code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    json_typed_diagnostic_source_owned_bytes(shape.input_bytes(), labels, code)
}
pub(crate) fn json_typed_diagnostic_source_owned_bytes(
    source_bytes: u64,
    labels: u64,
    code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    multiply_with_code(diagnostic_bytes(source_bytes, labels, code)?, 2, code)
}
pub(crate) fn audited_serde_diagnostic_bytes(input: u64, labels: u64) -> Result<u64> {
    diagnostic_bytes(input, labels, None)
}
fn diagnostic_bytes(input: u64, labels: u64, code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    let fixed = sum_with_code(
        &[labels, COMMON_LABEL_BYTES, 2 * u64::from(usize::BITS)],
        code,
    )?;
    sum_with_code(
        &[
            multiply_with_code(input, 12, code)?,
            multiply_with_code(fixed, 2, code)?,
        ],
        code,
    )
}
fn multiply_with_code(
    value: u64,
    factor: u64,
    code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    value
        .checked_mul(factor)
        .ok_or_else(|| super::boundary("event JSON admission overflow", code))
}
fn sum_with_code(values: &[u64], code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    values.iter().try_fold(0u64, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| super::boundary("event JSON admission overflow", code))
    })
}
pub(crate) fn json_decode_owned_bytes(
    shape: JsonValueShape,
    snapshot: bool,
    code: Option<crate::wire::ErrorCode>,
) -> Result<(u64, u64, u64)> {
    let mul = |value, factor| multiply_with_code(value, factor, code);
    let sum = |values: &[u64]| sum_with_code(values, code);
    let input = shape.input_bytes();
    let items = shape.lexical_items();
    let slots = mul(items, 10)?;
    let content_slot = 4 * size_of::<(Content<'static>, Content<'static>)>()
        .max(size_of::<Content<'static>>()) as u64;
    let typed_slot = if snapshot {
        mul(crate::core::Snapshot::LOGICAL_SCHEMA_SLOT_BYTES, 3)?
    } else {
        [
            size_of::<ExtensionDependency>(),
            size_of::<ExtensionConfiguration>(),
            size_of::<CapturedResource>(),
            size_of::<ForkOmission>(),
            size_of::<SharedGrant>(),
            size_of::<ReferenceGrant>(),
            size_of::<Attachment>(),
            size_of::<(String, FileRef)>(),
        ]
        .into_iter()
        .fold(0, usize::max) as u64
    };
    // Event collection elements larger than a Value are the object-shaped
    // records audited above. Digest bytes, UUIDs, strings and enum labels do
    // not each allocate a CapturedResource/ReferenceGrant slot. Bound those
    // larger slots by the original object/array openings, retaining the
    // existing growth factor. Content buffering and scalar collections still
    // use every lexical item. Snapshots keep their separate aggregate bound.
    let typed_items = if snapshot || shape.unresolved_raw() {
        items
    } else {
        shape.lexical_containers().unwrap_or(items)
    };
    let value_slot = size_of::<Value>().max(size_of::<(String, Value)>()) as u64;
    let virtual_bytes =
        super::json_value_virtual_owned_bytes(shape).map_err(|error| match (code, error) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            (Some(code), crate::Error::NativeBoundary { reason, .. }) => {
                crate::Error::NativeBoundary { code, reason }
            }
            (_, error) => error,
        })?;
    let boxed_slot = [
        size_of::<ForkSeed>(),
        size_of::<ProjectMergeReceipt>(),
        size_of::<ConversationMessage>(),
        size_of::<FileRef>(),
    ]
    .into_iter()
    .fold(0, usize::max) as u64;
    let mut diagnostic = diagnostic_bytes(input, EVENT_LABEL_BYTES, code)?;
    let mut conversation = 0;
    if snapshot {
        diagnostic = sum(&[diagnostic, mul(SNAPSHOT_DIAGNOSTIC_BYTES + 4, 2)?])?;
        let uuid_text = 2 * size_of::<Uuid>() as u64 + 4;
        let width = size_of::<[u8; 32]>() as u64
            + 10 * size_of::<usize>() as u64
            + 2 * size_of::<(Uuid, usize)>() as u64
            + 17 * (10 * value_slot + 2 * uuid_text + 4);
        conversation = sum(&[
            mul(items, width)?,
            mul(input, 7)?,
            mul(uuid_text.max(8), 2)?,
        ])?;
    }
    let maximum = sum(&[
        mul(slots, content_slot + value_slot)?,
        mul(mul(typed_items, 10)?, typed_slot)?,
        virtual_bytes,
        mul(if snapshot { items } else { 1 }, boxed_slot)?,
        mul(input, 6)?,
        mul(input.max(8), 2)?,
        diagnostic,
        conversation,
    ])?;
    Ok((maximum, diagnostic, conversation))
}

pub(super) const SNAPSHOT_DIAGNOSTIC_BYTES: u64 = 784;
pub(crate) fn snapshot_json_admission<F: FnMut(JsonCodecCharge) -> crate::Result<()> + ?Sized>(
    bytes: &[u8],
    bounds: JsonBounds,
    charge: &mut F,
) -> crate::Result<(u64, u64)> {
    let value_shape = super::admit_json_value_shape(
        bytes,
        JsonBounds {
            maximum_depth: bounds.maximum_depth.min(256),
            ..bounds
        },
        super::JsonValueSchema::Snapshot,
        &mut |event| charge(event),
    )?;
    let (maximum_owned_bytes, _, conversation) = json_decode_owned_bytes(value_shape, true, None)?;
    charge(JsonCodecCharge::Admission {
        maximum_owned_bytes,
        value_shape,
    })?;
    Ok((maximum_owned_bytes, conversation))
}
