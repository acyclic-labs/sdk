//! Bounded history IO shared by harness preparation and storage.

pub(crate) mod history;

#[doc(hidden)]
pub mod operations;

pub use history::*;

use crate::Error;

/// Default finite cold input allowance.
#[must_use]
pub const fn default_projection_read_limits() -> HistoryReadLimits {
    HistoryReadLimits {
        maximum_events: 64,
        maximum_bytes: 32 * acyclic_stream::MAX_COMMAND_BYTES as u64,
    }
}

pub(crate) fn guarded_boundary(
    work: &Option<&mut dyn history::HistoryIo>,
    kind: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_ref()
        .is_some_and(|work| work.original_native_work().is_some())
    {
        return Error::NativeBoundary { code: kind, reason };
    }
    let _ = work;
    match kind {
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}

/// Provider required by an execution journal on this platform.
/// Native filesystem work must retain its original owner through publication.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub trait JournalProvider: acyclic_stream::NativeStreamProvider {}
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl<P: acyclic_stream::NativeStreamProvider + ?Sized> JournalProvider for P {}
/// Portable execution journals require no native funding capability.
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub trait JournalProvider: acyclic_stream::StreamProvider {}
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
impl<P: acyclic_stream::StreamProvider + ?Sized> JournalProvider for P {}
