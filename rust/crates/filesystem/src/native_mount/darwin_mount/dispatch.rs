use super::*;

pub(super) fn context(address: usize) -> Result<&'static DarwinMountContext, i32> {
    if address == 0 {
        return Err(libc::ESTALE);
    }
    // SAFETY: nonzero addresses come only from the bridge, which passes back the
    // `Arc::into_raw` context that `DriverSessionResources` keeps alive until
    // the loop has exited.
    Ok(unsafe { &*(address as *const DarwinMountContext) })
}

/// Runs one native callback on the context at `address`. Every change it
/// makes is this mount's own, which the client learns of from the reply,
/// and it completes before any barrier that began after it (see
/// [`DarwinMountContext::revalidate`]). A panic fails it with `EIO`.
pub(super) fn callback<T>(
    address: usize,
    span: &tracing::span::EnteredSpan,
    operation: impl FnOnce(&'static DarwinMountContext) -> Result<T, i32>,
) -> Result<T, i32> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = context(address)?;
        let _callback = context.callbacks.enter();
        let _origin = context.origin.enter();
        operation(context)
    }))
    .unwrap_or(Err(libc::EIO))
    .inspect_err(|error| {
        span.record("errno", error);
    })
}

pub(super) fn ffi_status(
    address: usize,
    span: &tracing::span::EnteredSpan,
    operation: impl FnOnce(&'static DarwinMountContext) -> Result<c_int, i32>,
) -> c_int {
    callback(address, span, operation).unwrap_or_else(|error| -error)
}

pub(super) fn ffi_offset(
    address: usize,
    span: &tracing::span::EnteredSpan,
    operation: impl FnOnce(&'static DarwinMountContext) -> Result<i64, i32>,
) -> i64 {
    callback(address, span, operation).unwrap_or_else(|error| -i64::from(error))
}

