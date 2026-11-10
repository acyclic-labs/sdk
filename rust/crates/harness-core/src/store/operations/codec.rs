use super::*;

pub(crate) fn original_native_decode_mode(_observer: &dyn HistoryIo) -> bool {
    #[cfg(not(target_arch = "wasm32"))]
    return _observer.original_native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    false
}
