use super::*;

// Shared by prepared Unix arguments and process storage on every OS. Physical
// capacity is destroyed before the original invocation releases its credit.
pub(crate) struct ReadBuffer<T> {
    pub(crate) data: Vec<T>,
    allocation: ReadAllocation,
}

impl<T> ReadBuffer<T> {
    pub(crate) fn meter(&self) -> &Arc<dyn NativeReadMeter> {
        self.allocation.meter()
    }

    pub(crate) fn new(count: usize, meter: &Arc<dyn NativeReadMeter>) -> io::Result<Self> {
        let bytes = count
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(io::ErrorKind::InvalidInput)?;
        let allocation = if bytes == 0 {
            ReadAllocation::empty(meter)
        } else {
            ReadAllocation::new(meter, bytes)?
        };
        let mut buffer = Self {
            data: Vec::new(),
            allocation,
        };
        if bytes != 0 {
            buffer
                .data
                .try_reserve_exact(count)
                .map_err(|error| meter.allocation_failed(error))?;
            let actual = buffer
                .data
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(io::ErrorKind::InvalidInput)?;
            buffer.allocation.reconcile_capacity(actual)?;
        }
        Ok(buffer)
    }
}
