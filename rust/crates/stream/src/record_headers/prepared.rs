//! All physical storage and codec credit are issued before the external effect.
use super::*;
use crate::PreparedOwnedBytes;
use acyclic_native_runtime::{
    NativeCodecReservation, NativeCodecWork, NativeReadMeter, ReadAllocation,
};
use std::io;

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
    pub(crate) fn new_shape(
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
        for key in headers.keys() {
            append(&mut bytes, key)?;
        }
        let key_bytes = bytes.buffer().len();
        for attribute in headers.attributes() {
            append(&mut bytes, attribute.name.as_bytes())?;
            append(&mut bytes, attribute.value)?;
        }
        let length = bytes.buffer().len();
        let frozen = bytes
            .freeze(length)
            .map_err(|_| io::ErrorKind::InvalidData)?;
        codec.record(NativeCodecWork {
            copied: length as u64,
            ..NativeCodecWork::default()
        })?;
        // Reconstruct views from lengths rather than retaining another set of
        // ranges. Iterator clones validate the entire shape before filling the
        // already funded controls, with no allocation after issuance.
        let mut offset = 0;
        let keys = headers.keys().iter().map(move |key| {
            let start = offset;
            offset += key.len();
            start..offset
        });
        let mut offset = key_bytes;
        let attributes = headers.attributes().iter().map(move |attribute| {
            let start = offset;
            offset += attribute.name.len();
            let name = start..offset;
            let start = offset;
            offset += attribute.value.len();
            (name, start..offset)
        });
        controls.finish(frozen, keys, attributes)
    }
}

fn append(bytes: &mut PreparedOwnedBytes<ReadAllocation>, value: &[u8]) -> io::Result<()> {
    bytes
        .extend_within_capacity(value)
        .ok_or(io::ErrorKind::InvalidData)?;
    Ok(())
}
