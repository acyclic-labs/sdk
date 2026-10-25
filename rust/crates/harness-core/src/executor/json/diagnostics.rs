//! Checked ownership admission for history event and snapshot decoding.
use super::{JsonValueShape, Result};
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
    json_typed_diagnostic_source_owned_bytes(shape.diagnostic_source_bytes(), labels, code)
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
/// Diagnostic bytes of a history event or checkpoint decode.
///
/// The decoder stops at its first error, which quotes at most one string or
/// key of the document beside the static labels.
pub(crate) fn history_diagnostic_bytes(
    shape: JsonValueShape,
    code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    sum_with_code(
        &[
            diagnostic_bytes(shape.diagnostic_source_bytes(), EVENT_LABEL_BYTES, code)?,
            multiply_with_code(SNAPSHOT_DIAGNOSTIC_BYTES + 4, 2, code)?,
        ],
        code,
    )
}

const SNAPSHOT_DIAGNOSTIC_BYTES: u64 = 784;
