//! All physical storage and codec credit are issued before the external effect.
use super::*;
use crate::PreparedOwnedBytes;
use acyclic_native_runtime::{
    NativeCodecReservation, NativeCodecWork, NativeReadMeter, ReadAllocation,
};
use std::{io, ops::Range};

mod controls;
pub(crate) use controls::PreparedHeaderControls;

#[cfg(test)]
mod tests;

pub(crate) struct PreparedHeaders {
    controls: crate::record_headers::PreparedHeaderControls,
    bytes: PreparedOwnedBytes<ReadAllocation>,
    codec: NativeCodecReservation<Arc<dyn NativeReadMeter>>,
    maximum: usize,
}

impl PreparedHeaders {
    pub(crate) fn new(meter: &Arc<dyn NativeReadMeter>, maximum: usize) -> io::Result<Self> {
        Self::with_controls(meter, maximum, PreparedHeaderControls::new(meter, maximum)?)
    }

    pub(super) fn new_shape(
        meter: &Arc<dyn NativeReadMeter>,
        maximum: usize,
        keys: usize,
        attributes: usize,
    ) -> io::Result<Self> {
        let controls = PreparedHeaderControls::new_shape(meter, maximum, keys, attributes)?;
        Self::with_controls(meter, maximum, controls)
    }

    fn with_controls(
        meter: &Arc<dyn NativeReadMeter>,
        maximum: usize,
        controls: crate::record_headers::PreparedHeaderControls,
    ) -> io::Result<Self> {
        let mut allocation = ReadAllocation::empty(meter);
        let bytes = crate::native::vector(maximum, meter, &mut allocation)?;
        let bytes =
            PreparedOwnedBytes::new_buffer(bytes, allocation, meter).map_err(|failure| {
                match failure.into_error() {
                    crate::PreparedOwnedBytesError::Admission(error) => error,
                    crate::PreparedOwnedBytesError::NotFullyInitialized => {
                        io::ErrorKind::InvalidData.into()
                    }
                }
            })?;
        let codec = NativeCodecReservation::new(
            Arc::clone(meter),
            NativeCodecWork {
                copied: maximum as u64,
                ..NativeCodecWork::default()
            },
        )?;
        meter.check()?;
        Ok(Self {
            controls,
            bytes,
            codec,
            maximum,
        })
    }

    pub(crate) fn finish(self, headers: &RecordHeaders) -> io::Result<RecordHeaders> {
        if headers.keys().is_empty() && headers.attributes().is_empty() {
            return Ok(RecordHeaders::default());
        }
        let Self {
            controls,
            mut bytes,
            mut codec,
            maximum,
        } = self;
        if headers.payload_bytes() > maximum {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut keys: [Range<usize>; MAX_RECORD_KEYS] = std::array::from_fn(|_| 0..0);
        let mut attributes: [(Range<usize>, Range<usize>); MAX_RECORD_ATTRIBUTES] =
            std::array::from_fn(|_| (0..0, 0..0));
        for (key, range) in headers.keys().iter().zip(&mut keys) {
            *range = append(&mut bytes, key)?;
        }
        for (attribute, (name, value)) in headers.attributes().iter().zip(&mut attributes) {
            *name = append(&mut bytes, attribute.name.as_bytes())?;
            *value = append(&mut bytes, attribute.value)?;
        }
        let length = bytes.buffer().len();
        let frozen = bytes
            .freeze(length)
            .map_err(|_| io::ErrorKind::InvalidData)?;
        codec.record(NativeCodecWork {
            copied: length as u64,
            ..NativeCodecWork::default()
        })?;
        controls.finish(
            frozen,
            keys.get(..headers.keys().len())
                .ok_or(io::ErrorKind::InvalidData)?,
            attributes
                .get(..headers.attributes().len())
                .ok_or(io::ErrorKind::InvalidData)?,
        )
    }
}

fn append(
    bytes: &mut PreparedOwnedBytes<ReadAllocation>,
    value: &[u8],
) -> io::Result<Range<usize>> {
    let start = bytes.buffer().len();
    bytes
        .extend_within_capacity(value)
        .ok_or(io::ErrorKind::InvalidData)?;
    Ok(start..bytes.buffer().len())
}
