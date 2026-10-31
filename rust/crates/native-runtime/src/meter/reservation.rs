use crate::*;

/// One actual SDK/kernel attempt owns one operation credit. The result stays
/// separate from a late recording error so accepted work can retain custody.
pub(crate) fn native_operation_attempt<T>(
    meter: Option<&dyn NativeReadMeter>,
    kind: NativeOperationKind,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<(io::Result<T>, io::Result<()>)> {
    match meter {
        Some(meter) => {
            let mut token = NativeOperationReservation::new(meter, kind, 1)?;
            Ok(token.attempt(operation))
        }
        None => Ok((operation(), Ok(()))),
    }
}

/// Completes an operation whose successful value owns its resources. A late
/// recording failure drops that value; effects needing recovery must already
/// be retained by the caller before the operation returns.
pub(crate) fn native_control_attempt<T>(
    meter: Option<&dyn NativeReadMeter>,
    kind: NativeOperationKind,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let (result, recorded) = native_operation_attempt(meter, kind, operation)?;
    match result {
        Err(error) => Err(error),
        Ok(value) => recorded.map(|()| value),
    }
}

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
        Ok(Self {
            meter,
            kind,
            remaining: count,
        })
    }

    /// Runs an admitted operation even when recording reports a late error.
    pub fn attempt<T>(&mut self, operation: impl FnOnce() -> T) -> (T, io::Result<()>) {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .unwrap_or_else(|| std::process::abort());
        let recorded = self.meter.record_reserved_operation(self.kind);
        (operation(), recorded)
    }
}

impl<M: std::ops::Deref<Target: NativeReadMeter>> Drop for NativeOperationReservation<M> {
    fn drop(&mut self) {
        self.meter
            .cancel_reserved_operations(self.kind, self.remaining);
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
        Ok(Self {
            meter,
            kind,
            maximum: Some(maximum),
        })
    }

    /// Records the terminal transfer and refunds its unused maximum.
    pub fn record(&mut self, actual: usize) -> io::Result<()> {
        let maximum = self.maximum.take().ok_or(io::ErrorKind::InvalidInput)?;
        self.meter
            .record_reserved_bytes(self.kind, maximum, actual as u64)
    }
}

impl<M: std::ops::Deref<Target: NativeReadMeter>> Drop for NativeByteReservation<M> {
    fn drop(&mut self) {
        if let Some(maximum) = self.maximum.take() {
            self.meter.cancel_reserved_bytes(self.kind, maximum);
        }
    }
}
