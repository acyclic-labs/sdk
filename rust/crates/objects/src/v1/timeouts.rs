#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
use super::{Error, wire};
use std::time::Duration;

/// Default whole-request deadline, retained for metadata when transfers are extended.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Largest caller-selected deadline for an entire object or multipart-part transfer.
pub const MAX_TRANSFER_TIMEOUT: Duration = Duration::from_secs(1800);

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(super) fn validate(timeout: Duration) -> Result<Duration, Error> {
    if timeout.is_zero() || timeout > MAX_TRANSFER_TIMEOUT {
        return Err(wire::ErrorCode::InvalidArgument.into());
    }
    Ok(timeout)
}
