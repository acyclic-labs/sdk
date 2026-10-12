use super::*;

#[cfg(test)]
pub(super) fn fuse_fixture_work() -> crate::NativeMaterializationWork {
    crate::NativeMaterializationWork::from_meter(Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
        crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
    ))))
}

#[cfg(test)]
pub(super) fn fuse_fixture_errno(error: FuseRequestError) -> i32 {
    match error {
        FuseRequestError::Errno(error) => error,
        FuseRequestError::Native(error) => native_callback_errno(&error),
    }
}
