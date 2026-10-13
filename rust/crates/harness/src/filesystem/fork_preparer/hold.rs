//! Scoped storage reserved on the caller's SDK ledger.
use super::*;

/// Storage released with the value it protects.
pub struct DecodeReservation<'a> {
    pub(super) io: &'a PreparationIo,
    pub(super) bytes: u64,
}
impl DecodeReservation<'_> {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn substitute(&mut self, bytes: u64) -> Result<()> {
        self.io.preflight(acyclic_fs::WorkCounters::default())?;
        let released = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| self.io.invalid("decoded storage exceeds admission"))?;
        self.io.release(released)?;
        self.bytes = bytes;
        Ok(())
    }
    pub(crate) fn into_retained(mut self) {
        self.bytes = 0;
    }
}
impl Drop for DecodeReservation<'_> {
    fn drop(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.original_native_work().is_some() {
            drop(
                self.io
                    .take_native_custody(self.bytes)
                    .expect("admitted reservation"),
            );
            return;
        }
        self.io.release(self.bytes).expect("admitted reservation");
    }
}
pub(crate) fn retain_decoded<T>(decoded: (T, DecodeReservation<'_>)) -> T {
    decoded.1.into_retained();
    decoded.0
}
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_decoded<T>(
    decoded: (T, DecodeReservation<'_>),
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let allocation = decoded.1.io.take_native_custody(decoded.1.bytes)?;
    decoded.1.into_retained();
    Ok((decoded.0, allocation))
}
