use crate::*;

/// Reserved native operations, borrowed or owned by an accepted request.
pub struct NativeOperationReservation<M: std::ops::Deref<Target: NativeReadMeter>> {
    pub(crate) meter: M,
    pub(crate) kind: NativeOperationKind,
    pub(crate) remaining: u64,
}

impl<M: std::ops::Deref<Target: NativeReadMeter>> NativeOperationReservation<M> {
    /// Reserves the possible operation sequence before admission.
    pub fn new(meter: M, kind: NativeOperationKind, count: u64) -> io::Result<Self> {
        meter.reserve_operations(kind, count)?;
        Ok(Self { meter, kind, remaining: count })
    }

    /// Runs an admitted operation even when recording reports a late error.
    pub fn attempt<T>(&mut self, operation: impl FnOnce() -> T) -> (T, io::Result<()>) {
        self.remaining = self.remaining.checked_sub(1)
            .expect("a physical native operation owns an admitted request credit");
        let recorded = self.meter.record_reserved_operation(self.kind);
        (operation(), recorded)
    }

}

impl<M: std::ops::Deref<Target: NativeReadMeter>> Drop for NativeOperationReservation<M> {
    fn drop(&mut self) {
        self.meter.cancel_reserved_operations(self.kind, self.remaining);
    }
}

/// Reserved physical bytes, borrowed or owned by an accepted request.
pub struct NativeByteReservation<M: std::ops::Deref<Target: NativeReadMeter>> {
    pub(crate) meter: M,
    pub(crate) kind: NativeOperationKind,
    pub(crate) maximum: Option<u64>,
}

impl<M: std::ops::Deref<Target: NativeReadMeter>> NativeByteReservation<M> {
    /// Reserves the maximum physical transfer before admission.
    pub fn new(meter: M, kind: NativeOperationKind, maximum: usize) -> io::Result<Self> {
        let maximum = maximum as u64;
        meter.reserve_bytes(kind, maximum)?;
        Ok(Self { meter, kind, maximum: Some(maximum) })
    }

    /// Records the terminal transfer and refunds its unused maximum.
    pub fn record(&mut self, actual: usize) -> io::Result<()> {
        let maximum = self.maximum.take().expect("a native byte reservation is consumed exactly once");
        self.meter.record_reserved_bytes(self.kind, maximum, actual as u64)
    }

    /// Records a completed prefix while retaining its unused credit.
    pub fn record_partial(&mut self, actual: usize) -> io::Result<()> {
        let maximum = self.maximum.as_mut().expect("native byte allowance is still owned");
        *maximum = maximum.checked_sub(actual as u64)
            .expect("physical prefix fits this request's owned byte allowance");
        self.meter.record_reserved_bytes(self.kind, actual as u64, actual as u64)
    }
}

impl<M: std::ops::Deref<Target: NativeReadMeter>> Drop for NativeByteReservation<M> {
    fn drop(&mut self) {
        if let Some(maximum) = self.maximum.take() {
            self.meter.cancel_reserved_bytes(self.kind, maximum);
        }
    }
}
