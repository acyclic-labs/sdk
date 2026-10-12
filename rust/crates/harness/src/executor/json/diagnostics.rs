//! Checked ownership admission for history event and snapshot decoding.
use super::{Error, JsonBounds, JsonCodecCharge, JsonValueSchema, JsonValueShape, Result};
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
// Preserve the existing closed-schema static diagnostic allowances.
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
    multiply_with_code(
        diagnostic_bytes(shape.input_bytes(), labels, code)?,
        2,
        code,
    )
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
fn ownership_overflow(code: Option<crate::wire::ErrorCode>) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(code) = code {
        return Error::NativeBoundary {
            code,
            reason: "event JSON admission overflow",
        };
    }
    Error::Invalid("event JSON admission overflow".into())
}
pub(super) fn multiply_with_code(
    value: u64,
    factor: u64,
    code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    value
        .checked_mul(factor)
        .ok_or_else(|| ownership_overflow(code))
}
pub(super) fn sum_with_code(values: &[u64], code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    values.iter().try_fold(0u64, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| ownership_overflow(code))
    })
}
pub(crate) fn event_payload_json_admission<F: FnMut(JsonCodecCharge) -> Result<()> + ?Sized>(
    bytes: &[u8],
    bounds: JsonBounds,
    charge: &mut F,
) -> Result<u64> {
    let bounds = JsonBounds {
        maximum_bytes: bounds
            .maximum_bytes
            .min(acyclic_stream::MAX_RECORD_BYTES as u64),
        maximum_depth: bounds.maximum_depth.min(256),
        ..bounds
    };
    let value_shape = super::admit_json_value_shape(
        bytes,
        bounds,
        JsonValueSchema::EventPayload,
        &mut |event| charge(event),
    )?;
    let maximum_owned_bytes = json_decode_owned_bytes(value_shape, false, None)?.0;
    charge(JsonCodecCharge::Admission {
        maximum_owned_bytes,
        value_shape,
    })?;
    Ok(maximum_owned_bytes)
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
    let content = mul(
        mul(
            slots,
            size_of::<(Content<'static>, Content<'static>)>().max(size_of::<Content<'static>>())
                as u64,
        )?,
        4,
    )?;
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
        .max()
        .unwrap() as u64
    };
    let value_slot = size_of::<Value>().max(size_of::<(String, Value)>()) as u64;
    let virtual_bytes =
        super::json_value_virtual_owned_bytes(shape).map_err(|error| match (code, error) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            (Some(code), Error::NativeBoundary { reason, .. }) => {
                Error::NativeBoundary { code, reason }
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
    .max()
    .unwrap() as u64;
    let mut diagnostic = diagnostic_bytes(input, EVENT_LABEL_BYTES, code)?;
    let mut conversation = 0;
    if snapshot {
        diagnostic = sum(&[diagnostic, mul(SNAPSHOT_DIAGNOSTIC_BYTES + 4, 2)?])?;
        let cache = sum(&[
            mul(items, size_of::<[u8; 32]>() as u64)?,
            mul(slots, size_of::<usize>() as u64)?,
            mul(items, 2 * size_of::<(Uuid, usize)>() as u64)?,
        ])?;
        let uuid_text = 2 * size_of::<Uuid>() as u64 + 4;
        let canonical_items = mul(items, 17)?;
        conversation = sum(&[
            cache,
            mul(mul(canonical_items, 10)?, value_slot)?,
            input,
            mul(canonical_items, uuid_text)?,
            mul(input, 6)?,
            mul(canonical_items, uuid_text + 4)?,
            mul(uuid_text.max(8), 2)?,
        ])?;
    }
    let maximum = sum(&[
        content,
        mul(slots, typed_slot)?,
        sum(&[mul(slots, value_slot)?, virtual_bytes])?,
        mul(if snapshot { items } else { 1 }, boxed_slot)?,
        mul(input, 6)?,
        mul(input.max(8), 2)?,
        diagnostic,
        conversation,
    ])?;
    Ok((maximum, diagnostic, conversation))
}

// Snapshot admission.

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
