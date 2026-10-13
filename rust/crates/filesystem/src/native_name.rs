use crate::demand::DemandError;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn original_name_error(error: crate::NativeTreeMaterializationError) -> DemandError {
    match error {
        crate::NativeTreeMaterializationError::Work(error) => DemandError::Work(error),
        crate::NativeTreeMaterializationError::Cancelled => DemandError::Cancelled,
        crate::NativeTreeMaterializationError::OriginalWorkPoisoned => {
            DemandError::OriginalWorkPoisoned
        }
        crate::NativeTreeMaterializationError::AllocationFailed(error) => {
            DemandError::Allocation(error)
        }
        crate::NativeTreeMaterializationError::Io(error) => DemandError::Io(error),
        _ => DemandError::InvalidRequest,
    }
}

#[cfg(any(test, feature = "native-watch"))]
mod encoding;
#[cfg(any(test, feature = "native-mount"))]
pub(crate) use encoding::host_name_bytes_with_native_work;
#[cfg(all(windows, any(test, feature = "native-watch")))]
pub(crate) use encoding::host_wide_name_bytes_with_source_work;
#[cfg(any(test, feature = "native-watch"))]
pub(crate) use encoding::{host_name_bytes, host_name_bytes_with_source_work};
