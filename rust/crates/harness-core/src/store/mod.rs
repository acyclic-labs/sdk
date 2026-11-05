//! Bounded history IO shared by harness preparation and storage.

pub(crate) mod history;

#[doc(hidden)]
pub mod operations;

pub use history::*;

use crate::Error;

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
    crate::Error::from_code(kind, reason)
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
