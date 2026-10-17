use super::*;

/// Paid storage for a closed Unix spawn on the existing process-tree engine.
#[cfg(unix)]
pub struct PreparedClosedProcess {
    pub(super) arguments: PreparedProcessBuffer<std::mem::MaybeUninit<*mut libc::c_char>>,
    pub(super) environment: PreparedProcessBuffer<PreparedProcessBuffer<u8>>,
    pub(super) environment_pointers: PreparedProcessBuffer<*mut libc::c_char>,
}

#[cfg(unix)]
pub(super) struct PreparedProcessBuffer<T> {
    pub(super) data: Vec<T>,
    _allocation: crate::ReadAllocation,
}

// SAFETY: argument slots are inert outside synchronous spawn and are
// overwritten before reuse. Environment pointers address this frame's stable owned bytes.
#[cfg(unix)]
unsafe impl Send for PreparedClosedProcess {}

#[cfg(unix)]
impl std::fmt::Debug for PreparedClosedProcess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedClosedProcess")
            .field(
                "argument_count",
                &self.arguments.data.len().saturating_sub(2),
            )
            .field("environment_entries", &self.environment.data.len())
            .finish_non_exhaustive()
    }
}

#[cfg(unix)]
pub(super) fn prepared_process_vec<T>(
    count: usize,
    meter: &std::sync::Arc<dyn crate::NativeReadMeter>,
) -> io::Result<PreparedProcessBuffer<T>> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(io::ErrorKind::InvalidInput)?;
    let allocation = if bytes == 0 {
        crate::ReadAllocation::empty(meter)
    } else {
        crate::ReadAllocation::new(meter, bytes)?
    };
    let mut buffer = PreparedProcessBuffer {
        data: Vec::new(),
        _allocation: allocation,
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
        buffer._allocation.reconcile_capacity(actual)?;
    }
    Ok(buffer)
}
