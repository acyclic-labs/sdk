//! Header controls can retain an existing admitted page without copying names.
use super::super::attributes::PackedAttribute;
use super::*;
use std::ops::Range;

#[cfg(test)]
mod tests;

pub(crate) struct PreparedHeaderControls {
    data: Arc<HeaderData>,
    codec: NativeCodecReservation<Arc<dyn NativeReadMeter>>,
    maximum: usize,
}

impl PreparedHeaderControls {
    pub(crate) fn new_shape(
        meter: &Arc<dyn NativeReadMeter>,
        maximum: usize,
        keys: usize,
        attributes: usize,
    ) -> io::Result<Self> {
        if maximum > crate::MAX_RECORD_BYTES {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut allocation = ReadAllocation::new(meter, RecordHeaders::control_bytes())?;
        let keys = crate::native::vector(keys, meter, &mut allocation)?;
        let values = crate::native::vector(attributes, meter, &mut allocation)?;
        let copied = (keys.capacity() * std::mem::size_of::<OwnedBytes>()) as u64
            + (values.capacity() * std::mem::size_of::<PackedAttribute>()) as u64;
        let data = Arc::new(HeaderData {
            keys,
            attributes: AttributeStorage::Packed {
                bytes: OwnedBytes::new(),
                values,
            },
            _allocation: Some(allocation),
        });
        let codec = NativeCodecReservation::new(
            Arc::clone(meter),
            NativeCodecWork {
                copied,
                ..NativeCodecWork::default()
            },
        )?;
        meter.check()?;
        Ok(Self {
            data,
            codec,
            maximum,
        })
    }

    /// Ranges are checked before forming any immutable slices. Both ingress
    /// shapes use the same canonical key/name and shared-payload validation.
    pub(crate) fn finish(
        self,
        bytes: OwnedBytes,
        keys: impl ExactSizeIterator<Item = Range<usize>> + Clone,
        attributes: impl ExactSizeIterator<Item = (Range<usize>, Range<usize>)> + Clone,
    ) -> io::Result<RecordHeaders> {
        if keys.len() == 0 && attributes.len() == 0 {
            return Ok(RecordHeaders::default());
        }
        let Self {
            mut data,
            mut codec,
            maximum,
        } = self;
        let data_mut = Arc::get_mut(&mut data).ok_or(io::ErrorKind::InvalidData)?;
        let AttributeStorage::Packed {
            bytes: arena,
            values,
        } = &mut data_mut.attributes
        else {
            unreachable!("prepared headers use packed storage")
        };
        if keys.len() > data_mut.keys.capacity() || attributes.len() > values.capacity() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        validate(&bytes, keys.clone(), attributes.clone(), maximum)?;
        for range in keys {
            data_mut.keys.push(bytes.slice(range.clone()));
        }
        for (name, value) in attributes {
            values.push(PackedAttribute {
                name: name.clone(),
                value: bytes.slice(value.clone()),
            });
        }
        *arena = bytes;
        codec.record(NativeCodecWork {
            copied: (data_mut.keys.len() * std::mem::size_of::<OwnedBytes>()) as u64
                + (values.len() * std::mem::size_of::<PackedAttribute>()) as u64,
            ..NativeCodecWork::default()
        })?;
        Ok(RecordHeaders(Some(data)))
    }
}

fn validate(
    bytes: &[u8],
    keys: impl Iterator<Item = Range<usize>>,
    attributes: impl Iterator<Item = (Range<usize>, Range<usize>)>,
    maximum: usize,
) -> io::Result<()> {
    let mut validation = super::super::validation::Validation::default();
    for range in keys {
        let key = bytes.get(range.clone()).ok_or(io::ErrorKind::InvalidData)?;
        validation
            .key(key)
            .map_err(|_| io::ErrorKind::InvalidData)?;
    }
    for (name, value) in attributes {
        let name = std::str::from_utf8(bytes.get(name.clone()).ok_or(io::ErrorKind::InvalidData)?)
            .map_err(|_| io::ErrorKind::InvalidData)?;
        let value = bytes.get(value.clone()).ok_or(io::ErrorKind::InvalidData)?;
        validation
            .attribute(name, value)
            .map_err(|_| io::ErrorKind::InvalidData)?;
    }
    validation.finish(maximum).map_err(|error| match error {
        StreamError::LimitExceeded => io::ErrorKind::InvalidInput.into(),
        _ => io::ErrorKind::InvalidData.into(),
    })
}
