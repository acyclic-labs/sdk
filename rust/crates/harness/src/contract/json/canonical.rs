use super::*;

/// Allocates only after each logical payload/item has passed admission.
pub fn canonical_json_bytes_bounded<
    T: Serialize + ?Sized,
    F: FnMut(JsonCodecCharge) -> Result<()>,
>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Vec<u8>> {
    encode(
        value,
        JsonBounds {
            maximum_depth: bounds.maximum_depth.min(256),
            ..bounds
        },
        code,
        charge,
    )
}

/// Produces an admitted JSON value.
pub fn json_value_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T,
    bounds: JsonBounds,
    native_code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Value> {
    let bounds = JsonBounds {
        maximum_depth: bounds.maximum_depth.min(256),
        ..bounds
    };
    let mut meter = Meter {
        bounds,
        items: 0,
        bytes: 0,
        charge,
        failure: None,
        bound_failure: None,
        native_code,
        retained: 0,
        tree_only: true,
    };
    let value = encode_value(value, &mut meter)?;
    let owned = owned_logical_bytes(&value);
    let released = meter.retained.checked_sub(owned).ok_or_else(|| {
        CodecError::Bound("JSON retention accounting differs").into_root_with_code(native_code)
    })?;
    if released != 0 {
        if let Err(error) = meter.release(released) {
            return Err(meter.root_error(error));
        }
    }
    if let Err(error) = meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(meter.root_error(error));
    }
    meter.retained = 0;
    Ok(value)
}

pub(super) fn encode_value<T: Serialize + ?Sized>(
    value: &T,
    meter: &mut Meter<'_>,
) -> Result<Value> {
    let mut value = match value.serialize(Encoder {
        meter: &mut *meter,
        depth: 0,
    }) {
        Ok(value) => value,
        Err(error) => return Err(meter.root_error(error)),
    };
    if let Some(error) = meter.first_error() {
        return Err(error);
    }
    if let Err(error) = meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(meter.root_error(error));
    }
    value.sort_all_objects();
    Ok(value)
}

pub(super) struct Output<'a, 'b> {
    bytes: &'a mut dyn std::io::Write,
    maximum: usize,
    written: usize,
    meter: &'a mut Meter<'b>,
}

impl std::io::Write for Output<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let remaining = self
            .maximum
            .checked_sub(self.written)
            .ok_or_else(|| self.meter.bound("JSON exceeds admitted output").into_io())?;
        if bytes.len() > remaining {
            return Err(self.meter.bound("JSON exceeds admitted output").into_io());
        }
        self.meter
            .charge(JsonCodecCharge::Work {
                bytes: bytes.len() as u64,
                items: 0,
            })
            .map_err(CodecError::into_io)?;
        self.meter
            .charge(JsonCodecCharge::Copy {
                bytes: bytes.len() as u64,
            })
            .map_err(CodecError::into_io)?;
        self.bytes.write_all(bytes)?;
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn write_canonical_output(
    value: &Value,
    bytes: &mut dyn std::io::Write,
    maximum: usize,
    meter: &mut Meter<'_>,
) -> Result<usize> {
    let mut output = Output {
        bytes,
        maximum,
        written: 0,
        meter,
    };
    if let Err(error) = serde_json::to_writer(&mut output, value) {
        return Err(output.meter.root_error(error.into()));
    }
    Ok(output.written)
}

pub(super) fn encode<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T,
    bounds: JsonBounds,
    native_code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Vec<u8>> {
    let mut meter = Meter {
        bounds,
        items: 0,
        bytes: 0,
        charge,
        failure: None,
        bound_failure: None,
        native_code,
        retained: 0,
        tree_only: false,
    };
    let value = encode_value(value, &mut meter)?;
    let size = meter
        .size(&value)
        .map_err(|error| meter.root_error(error))?;
    if size > bounds.maximum_bytes {
        return Err(meter.root_error(CodecError::Bound("JSON exceeds declared byte bound")));
    }
    let size = usize::try_from(size)
        .map_err(|_| meter.root_error(CodecError::Bound("JSON output exceeds address space")))?;
    if let Err(error) = meter.retain(size) {
        return Err(meter.root_error(error));
    }
    (meter.charge)(JsonCodecCharge::Work {
        bytes: size as u64,
        items: 0,
    })?;
    if let Err(error) = meter
        .charge(JsonCodecCharge::CopyAdmission { bytes: size as u64 })
        .and_then(|()| meter.allocation(size))
    {
        return Err(meter.root_error(error));
    }
    let mut bytes = Vec::new();
    if size != 0 {
        bytes.try_reserve_exact(size).map_err(Error::Allocation)?;
    }
    write_canonical_output(&value, &mut bytes, size, &mut meter)?;
    let released = meter.retained - size as u64;
    drop(value);
    if let Err(error) = meter.release(released) {
        return Err(meter.root_error(error));
    }
    meter.retained = 0;
    Ok(bytes)
}
