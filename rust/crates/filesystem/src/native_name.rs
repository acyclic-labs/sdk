#[cfg(any(test, feature = "native-watch"))]
mod encoding;
#[cfg(any(test, feature = "native-mount"))]
pub(crate) use encoding::host_name_bytes_with_native_work;
#[cfg(all(windows, any(test, feature = "native-watch")))]
pub(crate) use encoding::host_wide_name_bytes_with_source_work;
#[cfg(any(test, feature = "native-watch"))]
pub(crate) use encoding::{host_name_bytes, host_name_bytes_with_source_work};
