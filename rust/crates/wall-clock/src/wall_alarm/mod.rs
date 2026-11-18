//! Wall-clock readings and native change-responsive timer waits.

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "the wall-clock timer and its wake handles are platform OS calls"
)]
#[path = "apple.rs"]
pub mod platform;

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "the wall-clock timer and its wake handles are platform OS calls"
)]
#[path = "windows.rs"]
pub mod platform;

/// Reads milliseconds since the Unix epoch, saturating at the representable bounds.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}
