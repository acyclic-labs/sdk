use crate::*;

/// Codec work withheld for a completion whose bytes are not available yet.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeCodecWork {
    /// Bytes encoded into an owned frame.
    pub encoded: u64,
    /// Bytes copied into an owned result or frame.
    pub copied: u64,
    /// Bytes supplied to a digest.
    pub hashed: u64,
}

impl NativeCodecWork {
    fn subtract(self, actual: Self) -> io::Result<Self> {
        Ok(Self {
            encoded: self
                .encoded
                .checked_sub(actual.encoded)
                .ok_or(io::ErrorKind::InvalidData)?,
            copied: self
                .copied
                .checked_sub(actual.copied)
                .ok_or(io::ErrorKind::InvalidData)?,
            hashed: self
                .hashed
                .checked_sub(actual.hashed)
                .ok_or(io::ErrorKind::InvalidData)?,
        })
    }
}

/// Original-ledger credit that competing work cannot spend after preparation.
pub struct NativeCodecReservation<M: std::ops::Deref<Target: NativeReadMeter>> {
    meter: M,
    remaining: NativeCodecWork,
}

impl<M: std::ops::Deref<Target: NativeReadMeter>> NativeCodecReservation<M> {
    /// Withholds all possible completion branches before issuing an effect.
    pub fn new(meter: M, maximum: NativeCodecWork) -> io::Result<Self> {
        meter.reserve_codec(maximum)?;
        Ok(Self {
            meter,
            remaining: maximum,
        })
    }

    /// Consumes a completed prefix without a new cancellation admission.
    pub fn record(&mut self, actual: NativeCodecWork) -> io::Result<()> {
        self.remaining = self.remaining.subtract(actual)?;
        self.meter.record_reserved_codec(actual)
    }
}

impl<M: std::ops::Deref<Target: NativeReadMeter>> Drop for NativeCodecReservation<M> {
    fn drop(&mut self) {
        self.meter.cancel_reserved_codec(self.remaining);
    }
}
