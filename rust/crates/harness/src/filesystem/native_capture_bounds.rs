//! Shared admission arithmetic for bounded native capture.

pub(crate) const fn capture_allowances_valid(
    timeout_ms: u32,
    control_timeout_ms: u32,
    cancellation_poll_ms: u32,
    maximum_output_bytes: u32,
    maximum_result_bytes: u32,
) -> bool {
    // JSON byte arrays use at most four characters per byte. Fixed fields and
    // the bounded stop diagnostic fit within the additional metadata allowance.
    timeout_ms > 0
        && control_timeout_ms > 0
        && control_timeout_ms <= timeout_ms
        && cancellation_poll_ms > 0
        && cancellation_poll_ms <= timeout_ms
        && maximum_output_bytes > 0
        && maximum_result_bytes > 0
        && (maximum_output_bytes as u64) * 4 + 2048 <= maximum_result_bytes as u64
}
