use super::*;

pub(super) fn context(address: usize) -> Result<&'static DarwinMountContext, i32> {
    if address == 0 {
        return Err(libc::ESTALE);
    }
    // SAFETY: the bridge borrows the context retained by the live SDK session.
    Ok(unsafe { &*(address as *const DarwinMountContext) })
}

pub(super) fn ffi<T: From<i32>>(
    address: usize,
    span: &tracing::span::EnteredSpan,
    authored_write: bool,
    operation: impl FnOnce(
        &DarwinMountContext,
        &crate::NativeMaterializationWork,
    ) -> Result<T, NativeMountError>,
) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = context(address)?;
        let _callback = context.callbacks.enter();
        let _origin = context.origin.enter();
        context.run(authored_write, |work| operation(context, work))
    }))
    .unwrap_or(Err(libc::EIO))
    .unwrap_or_else(|error| {
        span.record("errno", error);
        T::from(-error)
    })
}
