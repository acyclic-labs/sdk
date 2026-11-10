use super::*;

pub(crate) fn original_native_decode_mode(_observer: &dyn HistoryIo) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    return _observer.original_native_work().is_some();
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    false
}
