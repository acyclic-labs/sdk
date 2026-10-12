use super::*;

#[derive(Debug)]
pub(crate) enum MemoryFailure {
    Provider(StreamError),
    Observer(std::io::Error),
}

impl From<StreamError> for MemoryFailure {
    fn from(error: StreamError) -> Self {
        Self::Provider(error)
    }
}

impl From<std::io::Error> for MemoryFailure {
    fn from(error: std::io::Error) -> Self {
        Self::Observer(error)
    }
}

pub(crate) type MemoryResult<T> = Result<T, MemoryFailure>;


#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Allocation = Option<ReadAllocation>;
#[cfg(target_arch = "wasm32")]
pub(crate) type Allocation = ();

pub(crate) fn absorb(output: &mut Allocation, incoming: Allocation) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(mut incoming) = incoming {
        if let Some(output) = output.as_mut() {
            output.absorb(&mut incoming)?;
        } else {
            *output = Some(incoming);
        }
    }
    #[cfg(target_arch = "wasm32")]
    let _ = (output, incoming);
    Ok(())
}

pin_project_lite::pin_project! {
    /// A value retaining its actual allocation until physical storage retires.
    pub struct NativeStreamOutput<T> {
        #[pin]
        pub(crate) value: T,
        pub(crate) allocation: Allocation,
    }
}

impl<T> NativeStreamOutput<T> {
    #[cfg(not(target_arch = "wasm32"))]
    /// Transfers already admitted storage.
    pub fn new(value: T, allocation: ReadAllocation) -> Self {
        Self {
            value,
            allocation: Some(allocation),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Borrows the output with its allocation owner.
    pub fn value(&self) -> &T {
        &self.value
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Borrows the output while retaining its allocation owner.
    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Transfers the value and its original allocation owner.
    pub fn into_parts(self) -> (T, ReadAllocation) {
        (
            self.value,
            self.allocation
                .expect("public native outputs have their original owner"),
        )
    }

    pub(crate) fn map<U>(self, convert: impl FnOnce(T) -> U) -> NativeStreamOutput<U> {
        NativeStreamOutput {
            value: convert(self.value),
            allocation: self.allocation,
        }
    }
}

impl<T: Default> NativeStreamOutput<T> {
    pub(crate) fn default() -> Self {
        Self {
            value: T::default(),
            allocation: Allocation::default(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ByteReservation<'a> = Option<NativeByteReservation<&'a dyn NativeReadMeter>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type ByteReservation<'a> = Option<&'a ()>;

pub(crate) fn record_bytes(
    reservation: &mut ByteReservation<'_>,
    bytes: usize,
) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(reservation) = reservation {
        return reservation.record(bytes);
    }
    let _ = (reservation, bytes);
    Ok(())
}

pub(crate) fn check(_meter: WireMeter<'_>) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        meter.check()?;
    }
    Ok(())
}

pub(crate) fn allocate(_meter: WireMeter<'_>, bytes: usize) -> std::io::Result<Allocation> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        return Ok(Some(if bytes == 0 {
            ReadAllocation::empty(meter)
        } else {
            ReadAllocation::new(meter, bytes)?
        }));
    }
    let _ = bytes;
    Ok(Allocation::default())
}

pub(crate) fn vec<T>(
    _meter: WireMeter<'_>,
    capacity: usize,
) -> std::io::Result<NativeStreamOutput<Vec<T>>> {
    let mut funding = crate::memory::NativeStreamOutput::fund(_meter);
    let value = funding.vector(capacity)?;
    Ok(funding.map(|_| value))
}

pub(crate) fn admit_item(_meter: WireMeter<'_>) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        meter.admit_examination(1, 0)?;
    }
    check(_meter)
}

pub(crate) fn record_item(_meter: WireMeter<'_>) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        meter.record_examination(1, 0)?;
    }
    Ok(())
}

pub(crate) fn bytes<'a>(
    _meter: WireMeter<'a>,
    write: bool,
    bytes: usize,
) -> std::io::Result<ByteReservation<'a>> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        return NativeByteReservation::new(
            meter.as_ref(),
            if write {
                NativeOperationKind::Write
            } else {
                NativeOperationKind::Read
            },
            bytes,
        )
        .map(Some);
    }
    let _ = (write, bytes);
    Ok(None)
}

pub(crate) fn boundary(_meter: WireMeter<'_>, write: bool) -> std::io::Result<()> {
    check(_meter)?;
    let _ = write;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        // The boundary is this SDK call, not an opaque transport/syscall.
        let mut operation = NativeOperationReservation::new(
            meter.as_ref(),
            if write {
                NativeOperationKind::Write
            } else {
                NativeOperationKind::Read
            },
            1,
        )?;
        let ((), recorded) = operation.attempt(|| ());
        recorded?;
    }
    Ok(())
}

pub(crate) fn adopt(_meter: WireMeter<'_>, bytes: usize) -> std::io::Result<Allocation> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        return Ok(Some(ReadAllocation::adopt(meter, bytes)?));
    }
    let _ = bytes;
    Ok(Allocation::default())
}

pub(crate) fn retain_path(_meter: WireMeter<'_>, path: &StreamPath) -> std::io::Result<StreamPath> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        return path.retain_native(meter);
    }
    Ok(path.clone())
}

pub(crate) fn parent_path(
    _meter: WireMeter<'_>,
    path: &StreamPath,
) -> std::io::Result<Option<StreamPath>> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = _meter {
        return path.parent_native(meter);
    }
    Ok(path.parent())
}

pub(crate) fn provider_result<T>(result: MemoryResult<T>) -> Result<T, StreamError> {
    match result {
        Ok(value) => Ok(value),
        Err(MemoryFailure::Provider(error)) => Err(error),
        Err(MemoryFailure::Observer(_)) => {
            unreachable!("an ordinary memory operation has no fallible meter")
        }
    }
}

pub(crate) fn ordinary<T>(result: MemoryResult<NativeStreamOutput<T>>) -> Result<T, StreamError> {
    provider_result(result).map(|output| output.value)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_result<T>(result: MemoryResult<T>) -> std::io::Result<Result<T, StreamError>> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(MemoryFailure::Provider(error)) => Ok(Err(error)),
        Err(MemoryFailure::Observer(error)) => Err(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native<T>(
    result: MemoryResult<NativeStreamOutput<T>>,
    meter: &Arc<dyn NativeReadMeter>,
) -> NativeStreamResult<T> {
    native_result(result).map(|result| {
        result.map(|output| {
            NativeStreamOutput::new(
                output.value,
                output
                    .allocation
                    .unwrap_or_else(|| ReadAllocation::empty(meter)),
            )
        })
    })
}
